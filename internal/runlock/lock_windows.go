//go:build windows

package runlock

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
)

func lockFile(file *os.File) error {
	var overlapped syscall.Overlapped
	ok, _, err := lockFileEx.Call(file.Fd(), 0x2|0x1, 0, 1, 0, uintptr(unsafe.Pointer(&overlapped)))
	if ok != 0 {
		return nil
	}
	if err == syscall.Errno(33) {
		return durable.ErrBusy
	}
	return fmt.Errorf("runlock: lock file: %w", err)
}

func unlockFile(file *os.File) error {
	var overlapped syscall.Overlapped
	ok, _, err := unlockFileEx.Call(file.Fd(), 0, 1, 0, uintptr(unsafe.Pointer(&overlapped)))
	if ok != 0 {
		return nil
	}
	return fmt.Errorf("runlock: unlock file: %w", err)
}
