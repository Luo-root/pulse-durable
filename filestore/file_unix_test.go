//go:build linux || darwin || freebsd || openbsd || netbsd || dragonfly

package filestore

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
)

func TestUnixDeniedReadIsNotMissing(t *testing.T) {
	store := newTestStore(t)
	session := acquireTestSession(t, store, "run")
	if err := session.Save(context.Background(), testRecord("run"), 0); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(store.dir, fileBase("run")+".json")
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0600) })
	if _, err := os.ReadFile(path); err == nil {
		t.Skip("current user can bypass file permissions")
	}
	if _, err := session.Load(context.Background()); err == nil || errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("denied Load error = %v; want access failure", err)
	}
}

func TestUnixWriteFailurePreservesRecord(t *testing.T) {
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
	if err := os.Chmod(store.dir, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(store.dir, 0700) })
	// root 等具有绕过权限的用户无法用 mode bits 制造写失败。
	probe, err := os.CreateTemp(store.dir, "permission-probe-*")
	if err == nil {
		_ = probe.Close()
		_ = os.Remove(probe.Name())
		t.Skip("current user can bypass directory permissions")
	}
	record.Revision = 2
	if err := session.Save(context.Background(), record, 1); err == nil {
		t.Fatal("Save succeeded in non-writable directory")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("failed Save changed original record: %v", err)
	}
}
