// Package filestore 提供本机单宿主持久文件存储。
//
// 每个 RunID 使用独立的操作系统文件锁和 JSON 记录；锁随会话关闭或进程退出释放。
// 记录中的所有元数据字符串及 map 键须为有效 UTF-8；Data 字节由业务 Codec 解释。
// 读取时拒绝非法原始 UTF-8 和未配对的 JSON Unicode 代理项，不静默替换字符。
// 文件系统必须支持独占文件锁和同目录原子替换。本包不提供分布式租约、fencing 或跨机器派发，
// 也不承诺断电时的持久性；业务编码及目录的访问权限由调用方管理。
package filestore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/internal/runlock"
	"github.com/Luo-root/pulse-durable/recordio"
)

// Store 将每个运行的完整记录保存在调用方指定的目录中。
// Store 可被并发使用；同一目录的不同 Store 实例共享操作系统文件锁。
// 必须通过 New 创建 Store。
type Store struct {
	dir string
}

var _ durable.Store = (*Store)(nil)

// New 创建本机文件存储，并在需要时创建 dir。
// dir 不能为空；相对路径在创建时转换为绝对路径，不受之后工作目录变化影响。
// 在支持 Unix 权限位的平台，新目录使用 0700 权限，新文件使用 0600 权限。
// Windows 的访问权限由目录 ACL 控制；已有目录的权限不修改。
func New(dir string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("filestore: directory is empty")
	}
	absolute, err := filepath.Abs(dir)
	if err != nil {
		return nil, fmt.Errorf("filestore: resolve directory: %w", err)
	}
	if err := os.MkdirAll(absolute, 0700); err != nil {
		return nil, fmt.Errorf("filestore: create directory: %w", err)
	}
	return &Store{dir: absolute}, nil
}

// Acquire 非阻塞地取得 runID 的独占会话。
// 同一运行已被本进程或另一进程持有时返回 durable.ErrBusy。
// runID 必须是非空 UTF-8 字符串，按原文的 SHA-256 定位文件，不将其作为路径使用。
// 锁保持到 Session.Close 或进程退出；锁文件保留，避免替换锁文件产生两个独占域。
// 取得锁后清理本运行遗留的暂存文件；JSON 记录始终是唯一权威，清理失败返回错误。
func (s *Store) Acquire(ctx context.Context, runID string) (durable.Session, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if s == nil || s.dir == "" {
		return nil, errors.New("filestore: store was not created with New")
	}
	if runID == "" {
		return nil, errors.New("filestore: run ID is empty")
	}
	if !utf8.ValidString(runID) {
		return nil, errors.New("filestore: run ID is not UTF-8")
	}
	base := fileBase(runID)
	lock, err := runlock.Acquire(ctx, filepath.Join(s.dir, base+".lock"))
	if err != nil {
		return nil, fmt.Errorf("filestore: acquire lock: %w", err)
	}
	if err := removeStaleTemporaries(s.dir, base); err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	return &session{
		dir:   s.dir,
		path:  filepath.Join(s.dir, base+".json"),
		runID: runID,
		lock:  lock,
	}, nil
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("filestore: context is nil")
	}
	return ctx.Err()
}

func fileBase(runID string) string {
	sum := sha256.Sum256([]byte(runID))
	return hex.EncodeToString(sum[:])
}

// removeStaleTemporaries 只在持有 base 对应的运行锁时调用，不认回未提交的记录。
func removeStaleTemporaries(dir, base string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("filestore: list temporary records: %w", err)
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		random, ok := strings.CutPrefix(entry.Name(), base+"-")
		if !ok {
			continue
		}
		random, ok = strings.CutSuffix(random, ".tmp")
		if !ok {
			continue
		}
		// Go 1.25 的 os.CreateTemp 使用无前导零的 uint32 十进制随机数。
		// 严格匹配实际生成格式，避免清理名字相近的调用方文件。
		number, err := strconv.ParseUint(random, 10, 32)
		if err != nil || strconv.FormatUint(number, 10) != random {
			continue
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil {
			return fmt.Errorf("filestore: remove temporary record: %w", err)
		}
	}
	return nil
}

// session 的全部操作串行执行；Close 与正在进行的读写互斥。
type session struct {
	mu       sync.Mutex
	dir      string
	path     string
	runID    string
	lock     *runlock.Lock
	closed   bool
	closeErr error
}

var _ durable.Session = (*session)(nil)

func (s *session) Load(ctx context.Context) (durable.RunRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return durable.RunRecord{}, os.ErrClosed
	}
	if err := checkContext(ctx); err != nil {
		return durable.RunRecord{}, err
	}
	record, err := s.load()
	if err != nil {
		return durable.RunRecord{}, err
	}
	if err := checkContext(ctx); err != nil {
		return durable.RunRecord{}, err
	}
	return record, nil
}

// load 只在会话锁内调用。不存在与损坏、权限及读取故障分别返回。
func (s *session) load() (durable.RunRecord, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return durable.RunRecord{}, durable.ErrNotFound
	}
	if err != nil {
		return durable.RunRecord{}, fmt.Errorf("filestore: read record: %w", err)
	}
	record, err := recordio.Decode(data)
	if err != nil {
		return durable.RunRecord{}, fmt.Errorf("filestore: decode record: %w", err)
	}
	if record.RunID != s.runID {
		return durable.RunRecord{}, errors.New("filestore: stored run ID does not match session")
	}
	if record.Revision == 0 {
		return durable.RunRecord{}, errors.New("filestore: stored revision is zero")
	}
	return record, nil
}

// Save 按磁盘上的 Revision 执行 CAS，缺少记录时将其版本视为 0。
// 编码前拒绝非法 UTF-8 元数据（包括 map 键），不检查业务 Data 字节。
// CAS 或元数据校验失败不会改变旧记录及 Revision。
// 同目录暂存、同步和替换完成前，目标文件不会被部分修改。
// 替换已成功而目录同步失败时返回错误，提交结果不确定；调用方须重新 Load 权威记录，
// 不能据此回滚旧记录或盲目重试。确认替换后不因随后发生的取消撤回提交。
func (s *session) Save(ctx context.Context, record durable.RunRecord, expected uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if record.RunID != s.runID {
		return fmt.Errorf("filestore: record run ID does not match session: %w", durable.ErrConflict)
	}
	if expected == ^uint64(0) || record.Revision != expected+1 {
		return fmt.Errorf("filestore: next revision does not follow expected revision: %w", durable.ErrConflict)
	}
	current, err := s.load()
	if err != nil && !errors.Is(err, durable.ErrNotFound) {
		return err
	}
	if current.Revision != expected {
		return fmt.Errorf("filestore: stored revision differs from expected revision: %w", durable.ErrConflict)
	}
	data, err := recordio.Encode(record)
	if err != nil {
		return fmt.Errorf("filestore: encode record: %w", err)
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(s.dir, fileBase(s.runID)+"-*.tmp")
	if err != nil {
		return fmt.Errorf("filestore: create temporary record: %w", err)
	}
	// 这里只清理本次调用自己创建的暂存文件，绝不删除目标或锁文件。
	defer os.Remove(temporary.Name())
	if _, err := temporary.Write(data); err != nil {
		return errors.Join(fmt.Errorf("filestore: write temporary record: %w", err), temporary.Close())
	}
	if err := temporary.Sync(); err != nil {
		return errors.Join(fmt.Errorf("filestore: sync temporary record: %w", err), temporary.Close())
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("filestore: close temporary record: %w", err)
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if err := replaceFile(temporary.Name(), s.path); err != nil {
		return fmt.Errorf("filestore: replace record: %w", err)
	}
	if err := syncDirectory(s.dir); err != nil {
		return fmt.Errorf("filestore: record replaced but directory sync failed: %w", err)
	}
	return nil
}

func (s *session) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.closeErr
	}
	s.closed = true
	s.closeErr = s.lock.Close()
	return s.closeErr
}
