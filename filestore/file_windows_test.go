//go:build windows

package filestore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
)

func openWithoutDeleteSharing(t *testing.T, path string, share uint32) syscall.Handle {
	t.Helper()
	name, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	handle, err := syscall.CreateFile(name, syscall.GENERIC_READ, share, nil, syscall.OPEN_EXISTING,
		syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := syscall.CloseHandle(handle); err != nil {
			t.Error(err)
		}
	})
	return handle
}

func TestWindowsReplaceFailurePreservesRecordAndCleansTemporary(t *testing.T) {
	store := newTestStore(t)
	session := acquireTestSession(t, store, "run")
	record := testRecord("run")
	if err := session.Save(context.Background(), record, 0); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(store.dir, fileBase("run")+".json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// 允许读写但禁止删除共享：读取/CAS 可通过，Windows 必须拒绝随后原子替换。
	openWithoutDeleteSharing(t, path, syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE)
	record.Revision = 2
	if err := session.Save(context.Background(), record, 1); err == nil {
		t.Fatal("Save succeeded despite target handle forbidding replacement")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("failed replacement changed original record: %v", err)
	}
	entries, err := os.ReadDir(store.dir)
	if err != nil || len(entries) != 2 {
		t.Fatalf("failed replacement left temporary files: %v", err)
	}
}

func TestWindowsDeniedReadIsNotMissing(t *testing.T) {
	store := newTestStore(t)
	session := acquireTestSession(t, store, "run")
	if err := session.Save(context.Background(), testRecord("run"), 0); err != nil {
		t.Fatal(err)
	}
	// 不允许共享读取的已有句柄产生真实读取拒绝，不依赖 Windows 忽略的 Unix mode bits。
	openWithoutDeleteSharing(t, filepath.Join(store.dir, fileBase("run")+".json"), 0)
	if _, err := session.Load(context.Background()); err == nil || errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("denied Load error = %v; want access failure", err)
	}
}
