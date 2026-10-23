// Package sqlitestore implements a local, single-host durable Store using SQLite.
// Sessions hold nonblocking OS run locks, never long-lived SQLite transactions.
package sqlitestore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/internal/runlock"
	"github.com/Luo-root/pulse-durable/recordio"
	_ "modernc.org/sqlite"
)

// Store owns a file database and its active sessions. Create it with New.
// Close releases all sessions and the database; it is safe to call repeatedly.
type Store struct {
	mu       sync.Mutex
	db       *sql.DB
	locks    string
	sessions map[*session]struct{}
	closed   bool
	closeErr error
}

var _ durable.Store = (*Store)(nil)

// New opens an independent file database, creating its parent directory if needed.
// path is a filename, not a SQLite URI; in-memory databases are not supported.
// Schema version 1 is initialized atomically; unknown versions are rejected.
func New(path string) (*Store, error) {
	return NewContext(context.Background(), path)
}

// NewContext is New with cancellable database initialization. Filesystem calls
// remain subject to the operating system's normal blocking behavior.
func NewContext(ctx context.Context, path string) (*Store, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if path == "" || path == ":memory:" {
		return nil, errors.New("sqlitestore: file path required")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(filepath.Dir(absolute), 0700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(absolute, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = file.Close(); err != nil {
		return nil, err
	}
	absolute, err = filepath.EvalSymlinks(absolute)
	if err != nil {
		return nil, err
	}
	uriPath := filepath.ToSlash(absolute)
	if filepath.VolumeName(absolute) != "" && uriPath[0] != '/' {
		uriPath = "/" + uriPath
	}
	u := url.URL{Scheme: "file", Path: uriPath}
	q := u.Query()
	q.Add("_pragma", "synchronous(FULL)")
	// Wait only for short SQLite operations on other runs, never run ownership.
	q.Add("_pragma", "busy_timeout(5000)")
	q.Set("_txlock", "immediate")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	// A single connection serializes this Store's short database operations,
	// not session ownership or business execution.
	db.SetMaxOpenConns(1)
	if err = initialize(ctx, db, absolute+".init.lock"); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	locks := absolute + ".runlocks"
	if err = os.MkdirAll(locks, 0700); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return &Store{db: db, locks: locks, sessions: make(map[*session]struct{})}, nil
}

const schemaSQL = `CREATE TABLE durable_runs (
            run_id TEXT PRIMARY KEY NOT NULL,
            revision TEXT NOT NULL CHECK(typeof(revision) = 'text'),
            record BLOB NOT NULL CHECK(typeof(record) = 'blob')
        ) WITHOUT ROWID`

// Serialize initialization (including journal-mode transitions) across processes.
// Unlike run ownership, constructors wait up to five seconds, or ctx cancellation.
func initialize(ctx context.Context, db *sql.DB, lockPath string) (resultErr error) {
	waitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var lock *runlock.Lock
	for {
		var err error
		lock, err = runlock.Acquire(waitCtx, lockPath)
		if err == nil {
			break
		}
		if !errors.Is(err, durable.ErrBusy) {
			return err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return waitCtx.Err()
		case <-timer.C:
		}
	}
	defer func() { resultErr = errors.Join(resultErr, lock.Close()) }()
	// Reject unrelated databases before any persistent PRAGMA or schema change.
	if err := validateSchema(ctx, db); err != nil {
		return err
	}
	var mode string
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode=WAL").Scan(&mode); err != nil {
		return err
	}
	if !strings.EqualFold(mode, "wal") {
		return fmt.Errorf("sqlitestore: WAL unavailable: %s", mode)
	}
	if err := db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
		return err
	}
	if !strings.EqualFold(mode, "wal") {
		return errors.New("sqlitestore: journal mode is not WAL")
	}
	var version, sourceID string
	if err := db.QueryRowContext(ctx, "SELECT sqlite_version(), sqlite_source_id()").Scan(&version, &sourceID); err != nil {
		return err
	}
	if version == "" || sourceID == "" {
		return errors.New("sqlitestore: missing SQLite build identity")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var schemaVersion int
	if err := tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&schemaVersion); err != nil {
		return err
	}
	if schemaVersion == 0 {
		if _, err := tx.ExecContext(ctx, schemaSQL); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "PRAGMA user_version=1"); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func validateSchema(ctx context.Context, db *sql.DB) error {
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version != 0 && version != 1 {
		return fmt.Errorf("sqlitestore: unsupported schema version %d", version)
	}
	rows, err := db.QueryContext(ctx, "SELECT type, name, sql FROM sqlite_schema WHERE substr(name, 1, 7) != 'sqlite_'")
	if err != nil {
		return err
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var kind, name string
		var definition sql.NullString
		if err := rows.Scan(&kind, &name, &definition); err != nil {
			return err
		}
		count++
		// Only this module's exact v1 DDL (ignoring whitespace) is accepted.
		// This checks PK, NOT NULL, storage types, CHECKs and WITHOUT ROWID;
		// superficially similar or externally modified schemas are not adopted.
		if version != 1 || kind != "table" || name != "durable_runs" || strings.Join(strings.Fields(definition.String), " ") != strings.Join(strings.Fields(schemaSQL), " ") {
			return errors.New("sqlitestore: incompatible or unrelated database schema")
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if version == 1 && count != 1 {
		return errors.New("sqlitestore: missing v1 schema")
	}
	return nil
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("sqlitestore: context is nil")
	}
	return ctx.Err()
}

// Acquire immediately returns durable.ErrBusy if this run is already owned,
// including by another process using this same database pathname.
// RunIDs are nonempty UTF-8 strings; their SHA-256 determines the lock filename.
func (s *Store) Acquire(ctx context.Context, runID string) (durable.Session, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, os.ErrClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.db == nil {
		return nil, os.ErrClosed
	}
	if runID == "" || !utf8.ValidString(runID) {
		return nil, errors.New("sqlitestore: run ID must be nonempty UTF-8")
	}
	sum := sha256.Sum256([]byte(runID))
	lock, err := runlock.Acquire(ctx, filepath.Join(s.locks, fmt.Sprintf("%x.lock", sum)))
	if err != nil {
		return nil, err
	}
	session := &session{store: s, runID: runID, lock: lock}
	s.sessions[session] = struct{}{}
	return session, nil
}

// Close waits for in-flight operations, releases run locks, and closes SQLite.
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
	for session := range s.sessions {
		session.mu.Lock()
		s.closeErr = errors.Join(s.closeErr, session.closeLocked())
		session.mu.Unlock()
		delete(s.sessions, session)
	}
	if s.db != nil {
		s.closeErr = errors.Join(s.closeErr, s.db.Close())
	}
	return s.closeErr
}

type session struct {
	mu       sync.Mutex
	store    *Store
	runID    string
	lock     *runlock.Lock
	closed   bool
	closeErr error
}

var _ durable.Session = (*session)(nil)

type querier interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (s *session) load(ctx context.Context, q querier) (durable.RunRecord, error) {
	var revision string
	var data []byte
	err := q.QueryRowContext(ctx, "SELECT revision, record FROM durable_runs WHERE run_id = ?", s.runID).Scan(&revision, &data)
	if errors.Is(err, sql.ErrNoRows) {
		return durable.RunRecord{}, durable.ErrNotFound
	}
	if err != nil {
		return durable.RunRecord{}, err
	}
	n, err := strconv.ParseUint(revision, 10, 64)
	if err != nil || n == 0 || strconv.FormatUint(n, 10) != revision {
		return durable.RunRecord{}, errors.New("sqlitestore: invalid stored revision")
	}
	r, err := recordio.Decode(data)
	if err != nil {
		return durable.RunRecord{}, err
	}
	if r.RunID != s.runID || r.Revision != n {
		return durable.RunRecord{}, errors.New("sqlitestore: stored record identity or revision mismatch")
	}
	return r, nil
}

func (s *session) Load(ctx context.Context) (durable.RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return durable.RunRecord{}, os.ErrClosed
	}
	if err := checkContext(ctx); err != nil {
		return durable.RunRecord{}, err
	}
	return s.load(ctx, s.store.db)
}

// Save atomically inserts revision 1 or updates exactly expectedRevision.
// Revision is decimal TEXT so all uint64 values remain exact. No retries occur.
// Corrupt existing records are rejected rather than silently overwritten.
func (s *session) Save(ctx context.Context, record durable.RunRecord, expectedRevision uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if record.RunID != s.runID || expectedRevision == ^uint64(0) || record.Revision != expectedRevision+1 {
		return durable.ErrConflict
	}
	data, err := recordio.Encode(record)
	if err != nil {
		return err
	}
	tx, err := s.store.db.BeginTx(ctx, nil)
	if err != nil {
		return saveContextError(ctx, err)
	}
	defer tx.Rollback()
	current, err := s.load(ctx, tx)
	if err != nil && !errors.Is(err, durable.ErrNotFound) {
		return saveContextError(ctx, err)
	}
	if current.Revision != expectedRevision {
		return durable.ErrConflict
	}
	var result sql.Result
	if expectedRevision == 0 {
		result, err = tx.ExecContext(ctx, "INSERT INTO durable_runs(run_id, revision, record) VALUES (?, ?, ?) ON CONFLICT(run_id) DO NOTHING", s.runID, strconv.FormatUint(record.Revision, 10), data)
	} else {
		result, err = tx.ExecContext(ctx, "UPDATE durable_runs SET revision = ?, record = ? WHERE run_id = ? AND revision = ?", strconv.FormatUint(record.Revision, 10), data, s.runID, strconv.FormatUint(expectedRevision, 10))
	}
	if err != nil {
		return saveContextError(ctx, err)
	}
	n, err := result.RowsAffected()
	if err != nil {
		return saveContextError(ctx, err)
	}
	if n != 1 {
		return durable.ErrConflict
	}
	return tx.Commit()
}

// saveContextError preserves the driver's error unless cancellation is known to
// have happened before the transaction could commit. Commit errors remain opaque:
// a successful commit followed by an error is an uncertain checkpoint outcome.
func saveContextError(ctx context.Context, err error) error {
	if err != nil && ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

func (s *session) closeLocked() error {
	if !s.closed {
		s.closed = true
		s.closeErr = s.lock.Close()
	}
	return s.closeErr
}

func (s *session) Close() error {
	// Always take Store before session, matching Store.Close and Acquire.
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.closeLocked()
	delete(s.store.sessions, s)
	return err
}
