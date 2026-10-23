// Package postgresstore implements a PostgreSQL-backed durable Store.
package postgresstore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"sync"
	"unicode/utf8"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const schemaVersion = 1
const schemaName = "pulse_durable"
const tableName = "runs"

// Reserved only for serializing schema initialization across Store instances.
const initAdvisoryKey int64 = 0x50554c5345445552

var _ durable.Store = (*Store)(nil)

type Store struct {
	pool     *pgxpool.Pool
	mu       sync.Mutex
	sessions map[*session]struct{}
	closed   bool
	closeErr error
}

// New creates or validates the durable schema using dsn. Passwords are never
// included in returned errors. Pool sizing follows pgxpool defaults.
func New(ctx context.Context, dsn string) (*Store, error) {
	if ctx == nil {
		return nil, errors.New("postgresstore: nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if dsn == "" {
		return nil, errors.New("postgresstore: DSN required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("postgresstore: invalid DSN")
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.New("postgresstore: connect failed")
	}
	s := &Store{pool: pool, sessions: make(map[*session]struct{})}
	if err := s.initialize(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) initialize(ctx context.Context) (resultErr error) {
	c, err := s.pool.Acquire(ctx)
	if err != nil {
		return scrubPGError(err)
	}
	defer c.Release()
	if _, err = c.Exec(ctx, `SELECT pg_advisory_lock($1)`, initAdvisoryKey); err != nil {
		return scrubPGError(err)
	}
	defer func() {
		var unlocked bool
		err := c.QueryRow(context.Background(), `SELECT pg_advisory_unlock($1)`, initAdvisoryKey).Scan(&unlocked)
		if err != nil {
			resultErr = errors.Join(resultErr, scrubPGError(err))
		}
		if !unlocked {
			resultErr = errors.Join(resultErr, errors.New("postgresstore: initialization lock was not held"))
		}
	}()
	tx, err := c.Begin(ctx)
	if err != nil {
		return scrubPGError(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS pulse_durable`); err != nil {
		return scrubPGError(err)
	}
	if _, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS pulse_durable.schema_meta (version integer NOT NULL)`); err != nil {
		return scrubPGError(err)
	}
	if _, err = tx.Exec(ctx, `CREATE TABLE IF NOT EXISTS pulse_durable.runs (run_id text PRIMARY KEY NOT NULL, revision text NOT NULL, record bytea NOT NULL)`); err != nil {
		return scrubPGError(err)
	}
	if err = validateSchema(ctx, tx); err != nil {
		return err
	}
	var count int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM pulse_durable.schema_meta`).Scan(&count); err != nil {
		return scrubPGError(err)
	}
	if count == 0 {
		if _, err = tx.Exec(ctx, `INSERT INTO pulse_durable.schema_meta(version) VALUES (1)`); err != nil {
			return scrubPGError(err)
		}
	} else if count != 1 {
		return errors.New("postgresstore: unsupported schema version")
	}
	var version int
	if err = tx.QueryRow(ctx, `SELECT version FROM pulse_durable.schema_meta`).Scan(&version); err != nil {
		return scrubPGError(err)
	}
	if version != schemaVersion {
		return errors.New("postgresstore: unsupported schema version")
	}
	if err = tx.Commit(ctx); err != nil {
		return scrubPGError(err)
	}
	return nil
}

type txQuerier interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

func validateSchema(ctx context.Context, q txQuerier) error {
	var n int
	if err := q.QueryRow(ctx, `SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='pulse_durable' AND c.relkind IN ('r','p','v','m','f','S')`).Scan(&n); err != nil {
		return scrubPGError(err)
	}
	if n != 2 {
		return errors.New("postgresstore: incompatible schema objects")
	}
	if err := q.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema='pulse_durable' AND table_name='schema_meta'`).Scan(&n); err != nil {
		return scrubPGError(err)
	}
	if n != 1 {
		return errors.New("postgresstore: incompatible schema")
	}
	if err := q.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema='pulse_durable' AND table_name='schema_meta' AND column_name='version' AND data_type='integer' AND is_nullable='NO'`).Scan(&n); err != nil {
		return scrubPGError(err)
	}
	if n != 1 {
		return errors.New("postgresstore: incompatible schema")
	}
	if err := q.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema='pulse_durable' AND table_name='runs'`).Scan(&n); err != nil {
		return scrubPGError(err)
	}
	if n != 3 {
		return errors.New("postgresstore: incompatible schema")
	}
	if err := q.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema='pulse_durable' AND table_name='runs' AND ((column_name='run_id' AND data_type='text' AND is_nullable='NO') OR (column_name='revision' AND data_type='text' AND is_nullable='NO') OR (column_name='record' AND data_type='bytea' AND is_nullable='NO'))`).Scan(&n); err != nil {
		return scrubPGError(err)
	}
	if n != 3 {
		return errors.New("postgresstore: incompatible schema")
	}
	if err := q.QueryRow(ctx, `SELECT count(*) FROM pg_class c JOIN pg_namespace ns ON ns.oid=c.relnamespace WHERE ns.nspname='pulse_durable' AND c.relkind='i'`).Scan(&n); err != nil {
		return scrubPGError(err)
	}
	if n != 1 {
		return errors.New("postgresstore: incompatible schema indexes")
	}
	// The INSERT ... ON CONFLICT (run_id) contract requires an actual
	// single-column primary key on runs.run_id, not merely some constraint
	// and index with the expected counts.
	if err := q.QueryRow(ctx, `SELECT count(*) FROM pg_constraint con
		JOIN pg_class c ON c.oid=con.conrelid
		JOIN pg_namespace ns ON ns.oid=c.relnamespace
		JOIN pg_attribute a ON a.attrelid=c.oid AND a.attname='run_id'
		WHERE ns.nspname='pulse_durable' AND c.relname='runs'
		  AND con.contype='p' AND cardinality(con.conkey)=1
		  AND a.attnum = ANY(con.conkey)`).Scan(&n); err != nil {
		return scrubPGError(err)
	}
	if n != 1 {
		return errors.New("postgresstore: runs.run_id is not the required single-column primary key")
	}
	return nil
}

func scrubPGError(err error) error {
	if err == nil {
		return nil
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		return fmt.Errorf("postgresstore: PostgreSQL error %s", pe.Code)
	}
	return err
}

func (s *Store) Acquire(ctx context.Context, runID string) (durable.Session, error) {
	if ctx == nil {
		return nil, errors.New("postgresstore: nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if runID == "" || !utf8.ValidString(runID) {
		return nil, errors.New("postgresstore: run ID must be nonempty UTF-8")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, os.ErrClosed
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, scrubPGError(err)
	}
	key := advisoryKey(runID)
	var locked bool
	if err = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, key).Scan(&locked); err != nil {
		conn.Release()
		return nil, scrubPGError(err)
	}
	if !locked {
		conn.Release()
		return nil, durable.ErrBusy
	}
	x := &session{store: s, conn: conn, runID: runID, key: key}
	s.sessions[x] = struct{}{}
	return x, nil
}

func advisoryKey(runID string) int64 {
	h := sha256.Sum256([]byte(runID))
	return int64(binary.BigEndian.Uint64(h[:8]))
}

func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	for x := range s.sessions {
		x.mu.Lock()
		s.closeErr = errors.Join(s.closeErr, x.closeLocked())
		x.mu.Unlock()
		delete(s.sessions, x)
	}
	s.pool.Close()
	return s.closeErr
}

type session struct {
	mu       sync.Mutex
	store    *Store
	conn     *pgxpool.Conn
	runID    string
	key      int64
	closed   bool
	closeErr error
}

var _ durable.Session = (*session)(nil)

func (x *session) Load(ctx context.Context) (durable.RunRecord, error) {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.closed {
		return durable.RunRecord{}, os.ErrClosed
	}
	if ctx == nil {
		return durable.RunRecord{}, errors.New("postgresstore: nil context")
	}
	var revision string
	var data []byte
	err := x.conn.QueryRow(ctx, `SELECT revision, record FROM pulse_durable.runs WHERE run_id=$1`, x.runID).Scan(&revision, &data)
	if errors.Is(err, pgx.ErrNoRows) {
		return durable.RunRecord{}, durable.ErrNotFound
	}
	if err != nil {
		return durable.RunRecord{}, scrubPGError(err)
	}
	n, err := parseRevision(revision)
	if err != nil {
		return durable.RunRecord{}, err
	}
	r, err := recordio.Decode(data)
	if err != nil {
		return durable.RunRecord{}, err
	}
	if r.RunID != x.runID || r.Revision != n {
		return durable.RunRecord{}, errors.New("postgresstore: stored record identity or revision mismatch")
	}
	return r, nil
}

func parseRevision(v string) (uint64, error) {
	var n uint64
	if v == "" {
		return 0, errors.New("postgresstore: invalid stored revision")
	}
	for _, c := range v {
		if c < '0' || c > '9' {
			return 0, errors.New("postgresstore: invalid stored revision")
		}
		d := uint64(c - '0')
		if n > (math.MaxUint64-d)/10 {
			return 0, errors.New("postgresstore: invalid stored revision")
		}
		n = n*10 + d
	}
	if n == 0 || fmt.Sprint(n) != v {
		return 0, errors.New("postgresstore: invalid stored revision")
	}
	return n, nil
}

func (x *session) Save(ctx context.Context, record durable.RunRecord, expected uint64) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if x.closed {
		return os.ErrClosed
	}
	if ctx == nil {
		return errors.New("postgresstore: nil context")
	}
	if record.RunID != x.runID || expected == math.MaxUint64 || record.Revision != expected+1 {
		return durable.ErrConflict
	}
	data, err := recordio.Encode(record)
	if err != nil {
		return err
	}
	tx, err := x.conn.Begin(ctx)
	if err != nil {
		return scrubPGError(err)
	}
	defer tx.Rollback(ctx)
	var current string
	err = tx.QueryRow(ctx, `SELECT revision FROM pulse_durable.runs WHERE run_id=$1`, x.runID).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		if expected != 0 {
			return durable.ErrConflict
		}
	} else if err != nil {
		return scrubPGError(err)
	} else if n, e := parseRevision(current); e != nil || n != expected {
		return durable.ErrConflict
	}
	var tag pgconn.CommandTag
	if expected == 0 {
		tag, err = tx.Exec(ctx, `INSERT INTO pulse_durable.runs(run_id,revision,record) VALUES($1,$2,$3) ON CONFLICT (run_id) DO NOTHING`, x.runID, fmt.Sprint(record.Revision), data)
	} else {
		tag, err = tx.Exec(ctx, `UPDATE pulse_durable.runs SET revision=$1,record=$2 WHERE run_id=$3 AND revision=$4`, fmt.Sprint(record.Revision), data, x.runID, fmt.Sprint(expected))
	}
	if err != nil {
		return scrubPGError(err)
	}
	if tag.RowsAffected() != 1 {
		return durable.ErrConflict
	}
	if err = tx.Commit(ctx); err != nil {
		return scrubPGError(err)
	}
	return nil
}

func (x *session) closeLocked() error {
	if x.closed {
		return x.closeErr
	}
	x.closed = true
	_, x.closeErr = x.conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, x.key)
	x.conn.Release()
	return scrubPGError(x.closeErr)
}
func (x *session) Close() error {
	x.store.mu.Lock()
	defer x.store.mu.Unlock()
	x.mu.Lock()
	defer x.mu.Unlock()
	err := x.closeLocked()
	delete(x.store.sessions, x)
	return err
}
