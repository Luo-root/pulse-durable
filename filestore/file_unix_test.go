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
	if err := session.Initialize(context.Background(), incrementalSeed("run")); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(store.dir, fileBase("run")+".head")
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

func TestUnixTemporaryCleanupFailureReleasesLock(t *testing.T) {
	store := newTestStore(t)
	initial := acquireTestSession(t, store, "run")
	if err := initial.Close(); err != nil {
		t.Fatal(err)
	}
	temporary := createTestTemporary(t, store, "run")
	probe := createTestTemporary(t, store, "probe")
	if err := os.Chmod(store.dir, 0500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(store.dir, 0700) })
	if err := os.Remove(probe); err == nil {
		t.Skip("current user can bypass directory permissions")
	}
	for range 2 {
		session, err := store.Acquire(context.Background(), "run")
		if session != nil {
			_ = session.Close()
		}
		if session != nil || !errors.Is(err, os.ErrPermission) || errors.Is(err, durable.ErrBusy) {
			t.Fatalf("Acquire with undeletable temporary: session=%v err=%v", session, err)
		}
	}
	if _, err := os.Stat(temporary); err != nil {
		t.Fatalf("failed cleanup changed temporary: %v", err)
	}
	if err := os.Chmod(store.dir, 0700); err != nil {
		t.Fatal(err)
	}
	acquireTestSession(t, store, "run")
	if _, err := os.Stat(temporary); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary survived after directory permissions were restored: %v", err)
	}
}

func TestUnixWriteFailurePreservesRecord(t *testing.T) {
	store := newTestStore(t)
	session := acquireTestSession(t, store, "run")
	record := incrementalSeed("run")
	if err := session.Initialize(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(store.dir, fileBase("run")+".head")
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
	if _, err := session.Apply(context.Background(), revisionBatch(t, record)); err == nil {
		t.Fatal("Save succeeded in non-writable directory")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("failed Save changed original record: %v", err)
	}
}
