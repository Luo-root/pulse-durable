//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package filestore

import (
	"errors"
	"os"
	"syscall"
)

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
