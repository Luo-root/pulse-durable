package postgres

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Frozen verbatim SQL from 30f92f1, with check.sql assembled from that commit's
// check.go and catalogSQL. This is the real pre-active-suffix schema4 layout.
//
//go:embed testdata/schema4/*.sql
var schema4Fixture embed.FS

// Each legacy fixture is created in its own new database. Migrate must reject
// before grants/DDL/data changes; the production schema is never downgraded.
func TestMigrateRejectsOldSchemasWithoutChangingDDLACLOrData(t *testing.T) {
	dsn := os.Getenv("PULSE_DISTRIBUTED_TEST_DSN")
	if dsn == "" || os.Getenv("PULSE_DISTRIBUTED_TEST_ALLOW_DESTRUCTIVE") != "yes" {
		t.Skip("dedicated DSN and destructive opt-in required")
	}
	ctx := context.Background()
	admin, e := connect(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close()
	cfg, e := pgxpool.ParseConfig(dsn)
	if e != nil {
		t.Fatal("invalid test DSN")
	}
	for _, version := range []int{1, 2, 3, 4} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			name := "pulse_old_reject_" + strconv.FormatInt(time.Now().UnixNano(), 36)
			quoted := pgx.Identifier{name}.Sanitize()
			if _, e = admin.Exec(ctx, "CREATE DATABASE "+quoted); e != nil {
				t.Fatal(scrub(e, false))
			}
			defer func() {
				if _, e := admin.Exec(ctx, "DROP DATABASE "+quoted+" WITH (FORCE)"); e != nil {
					t.Error(scrub(e, false))
				}
			}()
			u := url.URL{Scheme: "postgres", Host: net.JoinHostPort(cfg.ConnConfig.Host, strconv.Itoa(int(cfg.ConnConfig.Port))), Path: "/" + name, User: url.UserPassword(cfg.ConnConfig.User, cfg.ConnConfig.Password)}
			q := u.Query()
			q.Set("sslmode", "disable")
			u.RawQuery = q.Encode()
			isolated, e := connect(ctx, u.String())
			if e != nil {
				t.Fatal(e)
			}
			defer isolated.Close()
			workerDSN := ""
			if version == 4 {
				for _, file := range []string{"schema.sql", "maintenance.sql", "check.sql"} {
					b, err := schema4Fixture.ReadFile("testdata/schema4/" + file)
					if err != nil {
						t.Fatal(err)
					}
					if _, err = isolated.Exec(ctx, string(b)); err != nil {
						t.Fatal(scrub(err, false))
					}
				}
				// A genuine old layout, not current DDL with a changed version.
				var suffixColumns int
				if err := isolated.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema='pulse_durable_distributed' AND table_name='runs' AND column_name IN ('active_log_bytes','active_log_count')`).Scan(&suffixColumns); err != nil || suffixColumns != 0 {
					t.Fatalf("schema4 fixture has active suffix columns: %d, %v", suffixColumns, err)
				}
				if _, err := isolated.Exec(ctx, `INSERT INTO pulse_durable_distributed.runs(namespace,run_id,task,digest,route) VALUES('legacy','retained',decode('0001ff00','hex'),'fixture','{}')`); err != nil {
					t.Fatal(scrub(err, false))
				}
				role := "pulse_old_worker_" + strconv.FormatInt(time.Now().UnixNano(), 36)
				quotedRole := pgx.Identifier{role}.Sanitize()
				if _, err := isolated.Exec(ctx, "CREATE ROLE "+quotedRole+" LOGIN PASSWORD 'distributed-test-only'; GRANT USAGE ON SCHEMA pulse_durable_distributed TO "+quotedRole+"; GRANT EXECUTE ON FUNCTION pulse_durable_distributed.check_schema() TO "+quotedRole); err != nil {
					t.Fatal(scrub(err, false))
				}
				defer func() {
					if _, err := isolated.Exec(ctx, "DROP OWNED BY "+quotedRole+"; DROP ROLE "+quotedRole); err != nil {
						t.Error(scrub(err, false))
					}
				}()
				sig, err := signature(ctx, isolated)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = isolated.Exec(ctx, "UPDATE pulse_durable_distributed.meta SET signature=$1", sig); err != nil {
					t.Fatal(scrub(err, false))
				}
				workerURL := u
				workerURL.User = url.UserPassword(role, "distributed-test-only")
				workerDSN = workerURL.String()
				worker, err := connect(ctx, workerDSN)
				if err != nil {
					t.Fatal(err)
				}
				var oldOK bool
				err = worker.QueryRow(ctx, "SELECT pulse_durable_distributed.check_schema()").Scan(&oldOK)
				worker.Close()
				if err != nil || !oldOK {
					t.Fatalf("old schema4 is not self-consistent for its worker: %v, %v", oldOK, err)
				}
			} else {
				_, e = isolated.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA pulse_durable_distributed;
 REVOKE ALL ON SCHEMA pulse_durable_distributed FROM PUBLIC;
 CREATE TABLE pulse_durable_distributed.meta(version integer PRIMARY KEY CHECK(version=%d),signature text NOT NULL);
 INSERT INTO pulse_durable_distributed.meta VALUES(%d,'legacy-signature');
 CREATE TABLE pulse_durable_distributed.runs(namespace text,run_id text,record bytea);
 INSERT INTO pulse_durable_distributed.runs VALUES('legacy','retained',decode('0001ff00','hex'));
 REVOKE ALL ON ALL TABLES IN SCHEMA pulse_durable_distributed FROM PUBLIC;`, version, version))
				if e != nil {
					t.Fatal(scrub(e, false))
				}
			}
			observe := func() (string, string, string) {
				t.Helper()
				var ddl, acl, data string
				if e := isolated.QueryRow(ctx, catalogSQL).Scan(&ddl); e != nil {
					t.Fatal(scrub(e, false))
				}
				if e := isolated.QueryRow(ctx, `SELECT string_agg(x,E'\n' ORDER BY x) FROM (
 SELECT 'schema:'||coalesce(nspacl::text,'') x FROM pg_namespace WHERE nspname='pulse_durable_distributed'
 UNION ALL SELECT 'relation:'||relname||':'||coalesce(relacl::text,'') FROM pg_class WHERE relnamespace='pulse_durable_distributed'::regnamespace
 UNION ALL SELECT 'function:'||oid::regprocedure::text||':'||coalesce(proacl::text,'') FROM pg_proc WHERE pronamespace='pulse_durable_distributed'::regnamespace) a`).Scan(&acl); e != nil {
					t.Fatal(scrub(e, false))
				}
				if e := isolated.QueryRow(ctx, `SELECT jsonb_build_object('meta',(SELECT jsonb_agg(to_jsonb(m)) FROM pulse_durable_distributed.meta m),'runs',(SELECT jsonb_agg(to_jsonb(r)) FROM pulse_durable_distributed.runs r))::text`).Scan(&data); e != nil {
					t.Fatal(scrub(e, false))
				}
				return ddl, acl, data
			}
			ddl, acl, data := observe()
			if e = Migrate(ctx, u.String(), Grant{Role: "must-not-be-created", Namespace: "denied", Permissions: []string{"worker"}}); e == nil || !strings.Contains(e.Error(), "unsupported physical schema") {
				t.Fatalf("old schema accepted: %v", e)
			}
			if c, err := New(ctx, u.String()); err == nil {
				c.Close()
				t.Fatal("New accepted old schema with owner credentials")
			}
			if workerDSN != "" {
				if c, err := New(ctx, workerDSN); err == nil {
					c.Close()
					t.Fatal("New accepted self-consistent old schema4 worker")
				}
				var accessRows int
				if err := isolated.QueryRow(ctx, "SELECT count(*) FROM pulse_durable_distributed.access").Scan(&accessRows); err != nil || accessRows != 0 {
					t.Fatalf("rejection changed access data: %d, %v", accessRows, err)
				}
			}
			afterDDL, afterACL, afterData := observe()
			if ddl != afterDDL || acl != afterACL || data != afterData {
				t.Fatal("rejected Migrate changed DDL, ACL, metadata or record bytes")
			}
		})
	}
}

func TestMigrationErrorRedaction(t *testing.T) {
	e := migrationError("upgrade v1 tables", &pgconn.PgError{Code: "23514", Message: "secret SQL and password", Detail: "private row"})
	if !errors.Is(e, d.ErrInvalid) || !strings.Contains(e.Error(), "23514") || !strings.Contains(e.Error(), "upgrade v1 tables") || strings.Contains(e.Error(), "secret") || strings.Contains(e.Error(), "private") {
		t.Fatalf("migration diagnostics lost category or exposed details: %v", e)
	}
}
func TestReplacePreservesFunction(t *testing.T) {
	s, e := replaceCreate(schemaSQL, "api")
	if e != nil || !strings.HasPrefix(s, "CREATE OR REPLACE FUNCTION") || strings.Contains(s, "DROP FUNCTION") || !strings.HasSuffix(s, "$$;") {
		t.Fatalf("unsafe function replacement: %v", e)
	}
	if _, e = replaceCreate("", "missing"); e == nil {
		t.Fatal("missing migration body accepted")
	}
}
