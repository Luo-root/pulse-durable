//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package filestore

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	durable "github.com/Luo-root/pulse-durable"
)

func lockFile(file *os.File) error {
	for {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return durable.ErrBusy
		}
		if err != nil {
			return fmt.Errorf("filestore: lock file: %w", err)
		}
		return nil
	}
}

func unlockFile(file *os.File) error {
	for {
		err := syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			return fmt.Errorf("filestore: unlock file: %w", err)
		}
		return nil
	}
}

func replaceFile(source, target string) error {
	return os.Rename(source, target)
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	err = directory.Sync()
	// 部分文件系统不支持目录同步。其它故障仍报告，保留已经替换的记录。
	if errors.Is(err, syscall.EINVAL) || errors.Is(err, syscall.ENOTSUP) {
		err = nil
	}
	return errors.Join(err, directory.Close())
}
