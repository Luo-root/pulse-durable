package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

func upgradeV1(ctx context.Context, tx pgx.Tx) error {
	// Verify the complete installed catalog before changing anything. In
	// particular, a seven-column lookalike must not authorize a migration.
	var n, version int
	var stored string
	if e := tx.QueryRow(ctx, `SELECT count(*),coalesce(min(version),0),coalesce(min(signature),'') FROM pulse_durable_distributed.meta`).Scan(&n, &version, &stored); e != nil {
		return migrationError("read v1 metadata", e)
	}
	actual, e := signature(ctx, tx)
	if e != nil {
		return e
	}
	if n != 1 || version != 1 || stored != actual {
		return errors.New("distributed postgres: incompatible v1 schema DDL")
	}
	var shape bool
	if e = tx.QueryRow(ctx, `SELECT array_agg(attname::text||':'||format_type(atttypid,atttypmod) ORDER BY attnum)=ARRAY['namespace:text','lease_id:text','worker_id:text','boot_id:text','run_id:text','epoch:bigint','claimed_at:timestamp with time zone'] FROM pg_attribute WHERE attrelid='pulse_durable_distributed.leases'::regclass AND attnum>0 AND NOT attisdropped`).Scan(&shape); e != nil {
		return migrationError("check v1 leases", e)
	}
	if !shape {
		return errors.New("distributed postgres: unsupported v1 lease schema")
	}
	// Keep table OIDs, primary keys, historical rows and all ACLs. V1 lease
	// rows represent past claims; no historical run/epoch is discarded.
	if _, e = tx.Exec(ctx, `ALTER TABLE pulse_durable_distributed.leases ADD COLUMN closed boolean NOT NULL DEFAULT false;
ALTER TABLE pulse_durable_distributed.leases ALTER COLUMN run_id DROP NOT NULL, ALTER COLUMN epoch DROP NOT NULL;
UPDATE pulse_durable_distributed.leases SET closed=true WHERE run_id IS NULL OR epoch IS NULL OR epoch<=0;
ALTER TABLE pulse_durable_distributed.leases ALTER COLUMN closed SET DEFAULT true;
ALTER TABLE pulse_durable_distributed.leases ADD CONSTRAINT leases_resolution_check CHECK(closed OR (run_id IS NOT NULL AND epoch>0));
ALTER TABLE pulse_durable_distributed.meta DROP CONSTRAINT meta_version_check;
UPDATE pulse_durable_distributed.meta SET version=2;
ALTER TABLE pulse_durable_distributed.meta ADD CONSTRAINT meta_version_check CHECK(version=2)`); e != nil {
		return migrationError("upgrade v1 tables", e)
	}
	api, e := replaceCreate(schemaSQL, "api")
	if e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, api); e != nil {
		return migrationError("replace api", e)
	}
	if _, e = tx.Exec(ctx, strings.Replace(checkSQL, "CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1)); e != nil {
		return migrationError("replace schema check", e)
	}
	sig, e := signature(ctx, tx)
	if e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `UPDATE pulse_durable_distributed.meta SET signature=$1`, sig); e != nil {
		return migrationError("sign v2 schema", e)
	}
	return nil
}

// CREATE OR REPLACE preserves function identity, dependencies and role grants.
func replaceCreate(sql, name string) (string, error) {
	head := "CREATE FUNCTION pulse_durable_distributed." + name + "("
	i := strings.Index(sql, head)
	if i < 0 {
		return "", errors.New("distributed postgres: missing migration function")
	}
	end := strings.Index(sql[i:], "$$;")
	if end < 0 {
		return "", errors.New("distributed postgres: incomplete migration function")
	}
	return strings.Replace(sql[i:i+end+3], "CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1), nil
}

func migrationError(stage string, err error) error {
	return fmt.Errorf("distributed postgres: migration %s: %w", stage, scrub(err, false))
}
