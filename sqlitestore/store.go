// sqlitestore 包使用 SQLite 实现本地单主机持久化 Store。
// 会话持有非阻塞操作系统运行锁，绝不持有长期 SQLite 事务。
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
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/internal/runlock"
	_ "modernc.org/sqlite"
)

// Store 拥有文件数据库及其活跃会话。通过 New 创建。
// Close 释放所有会话及数据库；可安全重复调用。
type Store struct {
	mu       sync.Mutex
	db       *sql.DB
	locks    string
	sessions map[*session]struct{}
	closed   bool
	closeErr error
}

var _ durable.Store = (*Store)(nil)

// New 打开独立文件数据库，必要时创建其父目录。
// path 是文件名，不是 SQLite URI；不支持内存数据库。
// 原子初始化模式版本 3；拒绝旧版本及未知版本。
func New(path string) (*Store, error) {
	return NewContext(context.Background(), path)
}

// NewContext 是支持取消数据库初始化的 New。文件系统调用
// 仍遵循操作系统正常阻塞行为。
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
	// 仅等待其他运行的短 SQLite 操作，绝不等待运行所有权。
	q.Add("_pragma", "busy_timeout(5000)")
	q.Set("_txlock", "immediate")
	u.RawQuery = q.Encode()
	db, err := sql.Open("sqlite", u.String())
	if err != nil {
		return nil, err
	}
	// 单连接串行化此 Store 的短数据库操作，
	// 而非会话所有权或业务执行。
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

const snapshotSQL = `CREATE TABLE durable_snapshots (
 run_id TEXT NOT NULL,
 revision TEXT NOT NULL CHECK(typeof(revision) = 'text'),
 record BLOB NOT NULL CHECK(typeof(record) = 'blob'),
 digest BLOB NOT NULL CHECK(length(digest) = 32),
 chain BLOB NOT NULL CHECK(length(chain) = 32),
 PRIMARY KEY(run_id, revision)
) WITHOUT ROWID`
const headSQL = `CREATE TABLE durable_heads (
 run_id TEXT PRIMARY KEY NOT NULL,
 revision TEXT NOT NULL CHECK(typeof(revision) = 'text'),
 chain BLOB NOT NULL CHECK(length(chain) = 32)
) WITHOUT ROWID`
const batchSQL = `CREATE TABLE durable_batches (
 run_id TEXT NOT NULL,
 revision TEXT NOT NULL CHECK(typeof(revision) = 'text'),
 parent_revision TEXT NOT NULL CHECK(typeof(parent_revision) = 'text'),
 parent_chain BLOB NOT NULL CHECK(length(parent_chain) = 32),
 digest BLOB NOT NULL CHECK(length(digest) = 32),
 chain BLOB NOT NULL CHECK(length(chain) = 32),
 wire BLOB NOT NULL CHECK(typeof(wire) = 'blob'),
 PRIMARY KEY(run_id, revision)
) WITHOUT ROWID`
const manifestSQL = `CREATE TABLE durable_manifests (
 run_id TEXT PRIMARY KEY NOT NULL,
 generation TEXT NOT NULL CHECK(typeof(generation) = 'text'),
 base_revision TEXT NOT NULL CHECK(typeof(base_revision) = 'text'),
 base_chain BLOB NOT NULL CHECK(length(base_chain) = 32)
) WITHOUT ROWID`

const maxChainBatches = 100000
const maxChainBytes = 256 << 20

// 跨进程串行化初始化（包括日志模式转换）。
// 与运行所有权不同，构造函数最多等待五秒，或直到 ctx 取消。
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
	// 在任何持久化 PRAGMA 或模式修改前拒绝无关数据库。
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
		for _, ddl := range []string{snapshotSQL, headSQL, batchSQL, manifestSQL} {
			if _, err := tx.ExecContext(ctx, ddl); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, "PRAGMA user_version=3"); err != nil {
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
	if version != 0 && version != 3 {
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
		expected := map[string]string{"durable_snapshots": snapshotSQL, "durable_heads": headSQL, "durable_batches": batchSQL, "durable_manifests": manifestSQL}[name]
		if version != 3 || kind != "table" || expected == "" || strings.Join(strings.Fields(definition.String), " ") != strings.Join(strings.Fields(expected), " ") {
			return errors.New("sqlitestore: incompatible or unrelated database schema")
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if version == 3 && count != 4 {
		return errors.New("sqlitestore: missing v3 schema")
	}
	return nil
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("sqlitestore: context is nil")
	}
	return ctx.Err()
}

// 若此运行已被拥有，Acquire 立即返回 durable.ErrBusy，
// 包括另一进程使用相同数据库路径名持有的情况。
// RunID 是非空 UTF-8 字符串；其 SHA-256 决定锁文件名。
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

// Close 等待进行中的操作、释放运行锁并关闭 SQLite。
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
	mu         sync.Mutex
	store      *Store
	runID      string
	lock       *runlock.Lock
	closed     bool
	closeErr   error
	cache      *durable.RunRecord
	chain      [32]byte
	batchCount int
	batchBytes int
	poisoned   bool
}

var _ durable.Session = (*session)(nil)

// saveContextError 保留驱动错误，除非已知取消发生在
// 事务可能提交之前。提交错误仍不作解析：
// 成功提交后又返回错误意味着检查点结果不确定。
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
	// 始终先获取 Store 再获取 session，与 Store.Close 和 Acquire 一致。
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.closeLocked()
	delete(s.store.sessions, s)
	return err
}
