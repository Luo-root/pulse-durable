package crossmachine

import (
	"context"
	"errors"
	"os"
	"strings"

	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/Luo-root/pulse-durable/distributed/postgres"
	"github.com/Luo-root/pulse-durable/distributed/systemtest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStore struct {
	backend            *postgres.Coordinator
	admin              *pgxpool.Pool
	namespace, version string
}

// OpenPostgres requires distinct explicit administrator and worker environment
// keys. Prepare creates a fresh login role using the worker DSN's credentials and
// grants only this namespace. No drop, cleanup DDL, or credential output occurs.
// An existing role is rejected in prepare mode; provision once, then disable it.
func OpenPostgres(ctx context.Context, c Config) (*PostgresStore, error) {
	bad := func() (*PostgresStore, error) {
		return nil, errors.New("database preparation/open failed (details omitted)")
	}
	if os.Getenv("PULSE_DISTRIBUTED_TEST_ALLOW") != "yes" || c.AdminDSNEnv == "" || c.WorkerDSNEnv == "" || strings.EqualFold(c.AdminDSNEnv, c.WorkerDSNEnv) {
		return bad()
	}
	adminDSN, workerDSN := os.Getenv(c.AdminDSNEnv), os.Getenv(c.WorkerDSNEnv)
	if adminDSN == "" || workerDSN == "" || adminDSN == workerDSN {
		return bad()
	}
	wc, err := pgx.ParseConfig(workerDSN)
	if err != nil || wc.User == "" {
		return bad()
	}
	admin, err := pgxpool.New(ctx, adminDSN)
	if err != nil {
		return bad()
	}
	ok := false
	defer func() {
		if !ok {
			admin.Close()
		}
	}()
	if c.Prepare {
		if wc.Password == "" {
			return bad()
		}
		var exists bool
		if admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)`, wc.User).Scan(&exists) != nil || exists {
			return bad()
		}
		var used bool
		// Schema may not exist yet; Migrate installs it before checking namespace use.
		if postgres.Migrate(ctx, adminDSN) != nil {
			return bad()
		}
		if admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pulse_durable_distributed.access WHERE namespace=$1) OR EXISTS(SELECT 1 FROM pulse_durable_distributed.runs WHERE namespace=$1)`, c.Namespace).Scan(&used) != nil || used {
			return bad()
		}
		// Utility statements cannot bind identifiers/passwords. Quote both explicitly;
		// standard_conforming_strings is set locally for predictable literal quoting.
		tx, e := admin.Begin(ctx)
		if e != nil {
			return bad()
		}
		defer tx.Rollback(context.Background())
		if _, e = tx.Exec(ctx, "SET LOCAL standard_conforming_strings=on"); e != nil {
			return bad()
		}
		sql := "CREATE ROLE " + pgx.Identifier{wc.User}.Sanitize() + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS PASSWORD '" + strings.ReplaceAll(wc.Password, "'", "''") + "'"
		if _, e = tx.Exec(ctx, sql); e != nil {
			return bad()
		}
		if tx.Commit(ctx) != nil {
			return bad()
		}
		if postgres.Migrate(ctx, adminDSN, postgres.Grant{Role: wc.User, Namespace: c.Namespace, Permissions: []string{"query", "submit", "worker"}}) != nil {
			return bad()
		}
	}
	// Refuse reuse of task-bearing namespaces even when preparation was external.
	var used bool
	if admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pulse_durable_distributed.runs WHERE namespace=$1)`, c.Namespace).Scan(&used) != nil || used {
		return bad()
	}
	backend, err := postgres.New(ctx, workerDSN)
	if err != nil {
		return bad()
	}
	ok = true
	return &PostgresStore{backend: backend, admin: admin, namespace: c.Namespace, version: c.Version}, nil
}
func (s *PostgresStore) Close() { s.backend.Close(); s.admin.Close() }
func (s *PostgresStore) Submit(ctx context.Context, id string) error {
	task, err := systemtest.Task(ctx, s.namespace, id, s.version)
	if err != nil {
		return errors.New("task construction failed")
	}
	result, err := s.backend.Submit(ctx, task)
	if err != nil || result.Deduplicated {
		return errors.New("task submission failed")
	}
	return nil
}
func (s *PostgresStore) Get(ctx context.Context, id string) (TaskEvidence, error) {
	v, err := s.backend.Get(ctx, d.Identity{Namespace: s.namespace, RunID: id})
	if err != nil {
		return TaskEvidence{}, errors.New("task observation failed")
	}
	return TaskEvidence{ID: id, Status: v.Status, Epoch: v.Epoch, Revision: v.Revision}, nil
}
func (s *PostgresStore) Capacity(ctx context.Context) (map[string]int, error) {
	rows, err := s.admin.Query(ctx, `SELECT worker_id,count(*) FROM pulse_durable_distributed.runs WHERE namespace=$1 AND status='running' GROUP BY worker_id`, s.namespace)
	if err != nil {
		return nil, errors.New("capacity query failed")
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var id string
		var n int
		if rows.Scan(&id, &n) != nil {
			return nil, errors.New("capacity scan failed")
		}
		counts[id] = n
	}
	if rows.Err() != nil {
		return nil, errors.New("capacity rows failed")
	}
	return counts, nil
}
