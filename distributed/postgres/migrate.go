package postgres

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

//go:embed schema.sql
var schemaSQL string

// Grant authorizes an existing database role for exactly one namespace. Rights
// are query, submit, worker, cancel. Grant query as well for recovery queries.
// The caller must provision separate non-owner, non-superuser login roles.
type Grant struct {
	Role        string
	Namespace   string
	Permissions []string
}

// Migrate installs a new physical schema or verifies the exact installed one.
// Older schemas are rejected without changing their data or DDL. Logical records
// remain format 2; physical schema version 3 is not a logical record upgrade.
// This explicit administrator entry point may grant API execution and namespace
// permissions, but never grants table access. Run using dedicated owner credentials.
func Migrate(ctx context.Context, dsn string, grants ...Grant) error {
	if ctx == nil {
		return errors.New("distributed postgres: nil context")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	p, e := connect(ctx, dsn)
	if e != nil {
		return e
	}
	defer p.Close()
	tx, e := p.Begin(ctx)
	if e != nil {
		return scrub(e, false)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(5788618723189269577); SET LOCAL search_path=pg_catalog,pulse_durable_distributed,pg_temp"); e != nil {
		return scrub(e, false)
	}
	var version int
	if e = tx.QueryRow(ctx, "SELECT current_setting('server_version_num')::integer").Scan(&version); e != nil {
		return fmt.Errorf("distributed postgres: version query failed: %w", scrub(e, false))
	}
	if version < 160000 {
		return errors.New("distributed postgres: PostgreSQL 16+ required")
	}
	var exists bool
	if e = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='pulse_durable_distributed')").Scan(&exists); e != nil {
		return scrub(e, false)
	}
	if !exists {
		if _, e = tx.Exec(ctx, schemaSQL); e != nil {
			return scrub(e, false)
		}
		if _, e = tx.Exec(ctx, checkSQL); e != nil {
			return scrub(e, false)
		}
		sig, e := signature(ctx, tx)
		if e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, "UPDATE pulse_durable_distributed.meta SET signature=$1", sig); e != nil {
			return scrub(e, false)
		}
	} else {
		var installed int
		if e = tx.QueryRow(ctx, "SELECT version FROM pulse_durable_distributed.meta").Scan(&installed); e != nil {
			return scrub(e, false)
		}
		if installed != 3 {
			return errors.New("distributed postgres: unsupported physical schema; use a new database (old data unchanged)")
		}
		if e = validate(ctx, tx, false); e != nil {
			return e
		}
	}
	if _, e = tx.Exec(ctx, `REVOKE ALL ON FUNCTION pulse_durable_distributed.api(text,jsonb),pulse_durable_distributed.check_schema() FROM PUBLIC`); e != nil {
		return scrub(e, false)
	}
	for _, g := range grants {
		if !validText(g.Role) || !validText(g.Namespace) || len(g.Permissions) == 0 {
			return errors.New("distributed postgres: invalid grant")
		}
		var safe bool
		e = tx.QueryRow(ctx, `SELECT NOT rolsuper AND NOT rolcreaterole AND NOT rolcreatedb AND NOT rolbypassrls AND NOT pg_has_role($1,(SELECT nspowner FROM pg_namespace WHERE nspname='pulse_durable_distributed'),'MEMBER') FROM pg_roles WHERE rolname=$1`, g.Role).Scan(&safe)
		if e != nil || !safe {
			return errors.New("distributed postgres: grant requires isolated non-owner role")
		}
		role := pgx.Identifier{g.Role}.Sanitize()
		if _, e = tx.Exec(ctx, "GRANT USAGE ON SCHEMA pulse_durable_distributed TO "+role+"; GRANT EXECUTE ON FUNCTION pulse_durable_distributed.api(text,jsonb),pulse_durable_distributed.check_schema() TO "+role); e != nil {
			return scrub(e, false)
		}
		for _, permission := range g.Permissions {
			switch permission {
			case "query", "worker", "submit", "cancel":
			default:
				return errors.New("distributed postgres: invalid permission")
			}
			if _, e = tx.Exec(ctx, "INSERT INTO pulse_durable_distributed.access VALUES($1,$2,$3) ON CONFLICT DO NOTHING", g.Role, g.Namespace, permission); e != nil {
				return scrub(e, false)
			}
		}
	}
	// Grants change ACLs but not the schema signature, which intentionally excludes
	// role-specific ACL contents. PUBLIC and effective worker privileges are checked.
	if e = tx.Commit(ctx); e != nil {
		return scrub(e, true)
	}
	return nil
}

type querier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// The signature covers exact columns, defaults, constraints, indexes, functions
// (including SECURITY DEFINER and fixed search_path), triggers, policies, and
// object counts. ACL checks are separate so explicit role grants remain possible.
const catalogSQL = `SELECT coalesce(string_agg(x,E'\n' ORDER BY x),'') FROM (
 SELECT 'relation:'||c.relname||':'||c.relkind::text||':'||c.relrowsecurity::text||':'||c.relforcerowsecurity::text AS x FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='pulse_durable_distributed'
 UNION ALL SELECT 'column:'||c.relname||':'||a.attname||':'||a.attnum||':'||format_type(a.atttypid,a.atttypmod)||':'||a.attnotnull||':'||a.attidentity::text||':'||a.attgenerated::text||':'||coalesce(pg_get_expr(ad.adbin,ad.adrelid),'') FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef ad ON ad.adrelid=c.oid AND ad.adnum=a.attnum WHERE n.nspname='pulse_durable_distributed' AND a.attnum>0 AND NOT a.attisdropped
 UNION ALL SELECT 'constraint:'||c.relname||':'||conname||':'||pg_get_constraintdef(k.oid,true) FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid WHERE connamespace='pulse_durable_distributed'::regnamespace
 UNION ALL SELECT 'index:'||indexdef FROM pg_indexes WHERE schemaname='pulse_durable_distributed'
 UNION ALL SELECT 'function:'||pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='pulse_durable_distributed'
 UNION ALL SELECT 'trigger:'||pg_get_triggerdef(t.oid) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='pulse_durable_distributed' AND NOT t.tgisinternal
 UNION ALL SELECT 'policy:'||policyname FROM pg_policies WHERE schemaname='pulse_durable_distributed'
) q`

func signature(ctx context.Context, q querier) (string, error) {
	var s string
	if e := q.QueryRow(ctx, catalogSQL).Scan(&s); e != nil {
		return "", scrub(e, false)
	}
	return digest([]byte(s)), nil
}
func validate(ctx context.Context, q querier, worker bool) error {
	var version int
	if e := q.QueryRow(ctx, "SELECT current_setting('server_version_num')::integer").Scan(&version); e != nil {
		return fmt.Errorf("distributed postgres: version query failed: %w", scrub(e, false))
	}
	if version < 160000 {
		return errors.New("distributed postgres: PostgreSQL 16+ required")
	}
	var n, v int
	var sig string
	if e := q.QueryRow(ctx, "SELECT count(*),coalesce(min(version),0),coalesce(min(signature),'') FROM pulse_durable_distributed.meta").Scan(&n, &v, &sig); e != nil {
		// Worker roles deliberately cannot SELECT meta. Use a definer validation
		// function rather than granting even incidental table reads.
		if worker {
			return validateWorker(ctx, q)
		}
		return scrub(e, false)
	}
	if n != 1 || v != 3 {
		return errors.New("distributed postgres: incompatible schema version")
	}
	current, e := signature(ctx, q)
	if e != nil {
		return e
	}
	if sig != current {
		return errors.New("distributed postgres: incompatible schema DDL")
	}
	if worker {
		return validateWorker(ctx, q)
	}
	return nil
}
func validateWorker(ctx context.Context, q querier) error {
	var ok bool
	if e := q.QueryRow(ctx, "SELECT pulse_durable_distributed.check_schema()").Scan(&ok); e != nil {
		return scrub(e, false)
	}
	if !ok {
		return fmt.Errorf("distributed postgres: incompatible schema or unsafe worker privileges")
	}
	return nil
}
