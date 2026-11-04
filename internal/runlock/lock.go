// runlock 包提供非阻塞、进程范围的本地文件锁。
// 它不拥有记录，也不清理临时文件。调用方必须保留锁
// 文件：删除锁文件可能创建两个独立的独占锁域。
package runlock

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
)

// Lock 持有操作系统锁，直到 Close 或进程退出。不得复制。
type Lock struct {
	mu     sync.Mutex
	file   *os.File
	closed bool
	err    error
}

// Acquire 以 0600 模式打开 path，并非阻塞地锁定 Windows 上的首字节，
// 或 Unix 上的整个文件。竞争时返回 durable.ErrBusy。父
// 目录必须存在。这不是分布式租约或栅栏机制。
func Acquire(ctx context.Context, path string) (*Lock, error) {
	if ctx == nil {
		return nil, errors.New("runlock: context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("runlock: open lock: %w", err)
	}
	if err := lockFile(file); err != nil {
		return nil, errors.Join(err, file.Close())
	}
	lock := &Lock{file: file}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	return lock, nil
}

// Close 并发安全且幂等，包括其返回错误。
func (l *Lock) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.closed {
		l.closed = true
		l.err = errors.Join(unlockFile(l.file), l.file.Close())
	}
	return l.err
}
