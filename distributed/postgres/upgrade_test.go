package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestUpgradeV1PreservesHistoryAndACL(t *testing.T) {
	_, admin, _ := integration(t)
	ctx := context.Background()
	tx, e := admin.Begin(ctx)
	if e != nil {
		t.Fatal(scrub(e, false))
	}
	defer tx.Rollback(ctx)
	// Reconstruct the v1 seven-column layout transactionally; all live data
	// remains untouched after rollback. Include both claimed and unused history.
	if _, e = tx.Exec(ctx, `ALTER TABLE pulse_durable_distributed.leases DROP CONSTRAINT IF EXISTS leases_check;
ALTER TABLE pulse_durable_distributed.leases DROP CONSTRAINT IF EXISTS leases_resolution_check;
ALTER TABLE pulse_durable_distributed.leases DROP COLUMN closed;
ALTER TABLE pulse_durable_distributed.meta DROP CONSTRAINT meta_version_check;
UPDATE pulse_durable_distributed.meta SET version=1;
ALTER TABLE pulse_durable_distributed.meta ADD CONSTRAINT meta_version_check CHECK(version=1)`); e != nil {
		t.Fatal(scrub(e, false))
	}
	var before, acl string
	if e = tx.QueryRow(ctx, `SELECT coalesce(jsonb_agg(to_jsonb(l) ORDER BY namespace,lease_id)::text,'[]') FROM pulse_durable_distributed.leases l`).Scan(&before); e != nil {
		t.Fatal(scrub(e, false))
	}
	if e = tx.QueryRow(ctx, `SELECT string_agg(oid::text||':'||proacl::text,',' ORDER BY oid) FROM pg_proc WHERE pronamespace='pulse_durable_distributed'::regnamespace`).Scan(&acl); e != nil {
		t.Fatal(scrub(e, false))
	}
	sig, e := signature(ctx, tx)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = tx.Exec(ctx, `UPDATE pulse_durable_distributed.meta SET signature=$1`, sig); e != nil {
		t.Fatal(scrub(e, false))
	}
	if e = upgradeV1(ctx, tx); e != nil {
		t.Fatal(e)
	}
	if e = validate(ctx, tx, false); e != nil {
		t.Fatal(e)
	}
	var after, gotACL string
	if e = tx.QueryRow(ctx, `SELECT coalesce(jsonb_agg(to_jsonb(l)-'closed' ORDER BY namespace,lease_id)::text,'[]') FROM pulse_durable_distributed.leases l`).Scan(&after); e != nil {
		t.Fatal(scrub(e, false))
	}
	if e = tx.QueryRow(ctx, `SELECT string_agg(oid::text||':'||proacl::text,',' ORDER BY oid) FROM pg_proc WHERE pronamespace='pulse_durable_distributed'::regnamespace`).Scan(&gotACL); e != nil {
		t.Fatal(scrub(e, false))
	}
	if before != after || acl != gotACL {
		t.Fatal("migration changed historical values or function identity/ACL")
	}
}

func TestUpgradeRejectsDriftBeforeDDL(t *testing.T) {
	_, admin, _ := integration(t)
	ctx := context.Background()
	tx, e := admin.Begin(ctx)
	if e != nil {
		t.Fatal(scrub(e, false))
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, `ALTER TABLE pulse_durable_distributed.meta DROP CONSTRAINT meta_version_check; UPDATE pulse_durable_distributed.meta SET version=1; ALTER TABLE pulse_durable_distributed.leases ALTER COLUMN claimed_at SET DEFAULT now()`); e != nil {
		t.Fatal(scrub(e, false))
	}
	if e = upgradeV1(ctx, tx); e == nil || !strings.Contains(e.Error(), "incompatible v1 schema DDL") {
		t.Fatalf("drift accepted: %v", e)
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
