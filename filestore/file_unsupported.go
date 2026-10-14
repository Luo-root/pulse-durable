//go:build !windows && !linux && !darwin && !freebsd && !openbsd && !netbsd && !dragonfly

package filestore

import (
	"errors"
	"os"
)

// 不支持的平台明确拒绝取得会话，不能退化为没有操作系统锁的存储。
func lockFile(*os.File) error {
	return errors.ErrUnsupported
}

func unlockFile(*os.File) error {
	return errors.ErrUnsupported
}

func replaceFile(string, string) error {
	return errors.ErrUnsupported
}

func syncDirectory(string) error {
	return errors.ErrUnsupported
}
