//go:build windows

package filestore

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"

	durable "github.com/Luo-root/pulse-durable"
)

var (
	kernel32     = syscall.NewLazyDLL("kernel32.dll")
	lockFileEx   = kernel32.NewProc("LockFileEx")
	unlockFileEx = kernel32.NewProc("UnlockFileEx")
	moveFileEx   = kernel32.NewProc("MoveFileExW")
)

func lockFile(file *os.File) error {
	var overlapped syscall.Overlapped
	// LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY，锁住固定的第一个字节。
	ok, _, err := lockFileEx.Call(file.Fd(), 0x2|0x1, 0, 1, 0, uintptr(unsafe.Pointer(&overlapped)))
	if ok != 0 {
		return nil
	}
	if err == syscall.Errno(33) { // ERROR_LOCK_VIOLATION
		return durable.ErrBusy
	}
	return fmt.Errorf("filestore: lock file: %w", err)
}

func unlockFile(file *os.File) error {
	var overlapped syscall.Overlapped
	ok, _, err := unlockFileEx.Call(file.Fd(), 0, 1, 0, uintptr(unsafe.Pointer(&overlapped)))
	if ok != 0 {
		return nil
	}
	return fmt.Errorf("filestore: unlock file: %w", err)
}

func replaceFile(source, target string) error {
	from, err := syscall.UTF16PtrFromString(source)
	if err != nil {
		return err
	}
	to, err := syscall.UTF16PtrFromString(target)
	if err != nil {
		return err
	}
	// 与目标同目录、同卷，禁止跨卷复制；替换已有文件，并尽力同步移动。
	// MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH。
	ok, _, err := moveFileEx.Call(uintptr(unsafe.Pointer(from)), uintptr(unsafe.Pointer(to)), 0x1|0x8)
	if ok != 0 {
		return nil
	}
	return err
}

func syncDirectory(string) error {
	// Windows 不支持通过 os.File.Sync 同步目录；文件已 Sync，移动使用 WRITE_THROUGH。
	return nil
}
