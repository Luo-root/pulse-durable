//go:build windows || linux || darwin || freebsd || openbsd || netbsd || dragonfly

package runlock

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
)

func TestLockLifetime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.lock")
	lock, err := Acquire(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lock.Close() })
	if other, err := Acquire(context.Background(), path); !errors.Is(err, durable.ErrBusy) {
		if other != nil {
			_ = other.Close()
		}
		t.Fatalf("contention = %v", err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := lock.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if _, err := os.Stat(path); err != nil {
		t.Fatal("lock file removed:", err)
	}
	next, err := Acquire(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := next.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.lock")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Acquire(ctx, path); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := Acquire(nil, path); err == nil {
		t.Fatal("nil context accepted")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("created lock: %v", err)
	}
}

func TestProcessLock(t *testing.T) {
	if mode := os.Getenv("DURABLE_RUNLOCK_HELPER"); mode != "" {
		lock, err := Acquire(context.Background(), os.Getenv("DURABLE_RUNLOCK_PATH"))
		if mode == "busy" {
			if !errors.Is(err, durable.ErrBusy) {
				t.Fatalf("child contention: %v", err)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		// Exit without Close: only the OS can release the held lock.
		_ = lock
		os.Exit(0)
	}
	path := filepath.Join(t.TempDir(), "run.lock")
	run := func(mode string) {
		t.Helper()
		cmd := exec.Command(os.Args[0], "-test.run=^TestProcessLock$")
		for _, entry := range os.Environ() {
			if !strings.HasPrefix(entry, "DURABLE_RUNLOCK_HELPER=") && !strings.HasPrefix(entry, "DURABLE_RUNLOCK_PATH=") {
				cmd.Env = append(cmd.Env, entry)
			}
		}
		cmd.Env = append(cmd.Env, "DURABLE_RUNLOCK_HELPER="+mode, "DURABLE_RUNLOCK_PATH="+path)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child %s: %v\n%s", mode, err, output)
		}
	}
	lock, err := Acquire(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lock.Close() })
	run("busy")
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	run("exit")
	next, err := Acquire(context.Background(), path)
	if err != nil {
		t.Fatal("process exit did not release lock:", err)
	}
	if err := next.Close(); err != nil {
		t.Fatal(err)
	}
}
