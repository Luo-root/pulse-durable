// Package runlock provides nonblocking, process-scoped local file locks.
// It neither owns records nor cleans up temporary files. Callers must retain lock
// files: removing a lock file can create two independent exclusive lock domains.
package runlock

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
)

// Lock holds an OS lock until Close or process exit. It must not be copied.
type Lock struct {
	mu     sync.Mutex
	file   *os.File
	closed bool
	err    error
}

// Acquire opens path with mode 0600 and nonblockingly locks its first byte on
// Windows, or the file on Unix. Contention returns durable.ErrBusy. The parent
// directory must exist. This is not a distributed lease or a fencing mechanism.
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

// Close is concurrency-safe and idempotent, including its returned error.
func (l *Lock) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.closed {
		l.closed = true
		l.err = errors.Join(unlockFile(l.file), l.file.Close())
	}
	return l.err
}
