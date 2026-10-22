//go:build windows

package filestore

import (
	"syscall"
	"unsafe"
)

var (
	kernel32   = syscall.NewLazyDLL("kernel32.dll")
	moveFileEx = kernel32.NewProc("MoveFileExW")
)

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
