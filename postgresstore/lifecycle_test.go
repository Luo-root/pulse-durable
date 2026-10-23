package postgresstore

import (
	"context"
	"errors"
	"net/url"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/jackc/pgx/v5/pgxpool"
)

// These tests intentionally run serially with the schema-resetting PG tests.
func TestPGLifecycle(t *testing.T) {
	dsn := os.Getenv("PULSE_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("PULSE_POSTGRES_TEST_DSN is not set")
	}
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal(err)
		}
		q := u.Query()
		q.Set("pool_max_conns", "1")
		u.RawQuery = q.Encode()
		dsn = u.String()
	} else {
		dsn += " pool_max_conns=1"
	}
	if _, err := pgxpool.ParseConfig(dsn); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"release", "store-close", "same-run", "pending-same-run", "cancel", "concurrent-close"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			s, err := New(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			// Cancel acquisitions before closing, so the old implementation can also
			// clean up on failure instead of stranding a test goroutine holding mu.
			defer s.Close()
			defer cancel()
			a, err := s.Acquire(ctx, "lifecycle-a")
			if err != nil {
				t.Fatal(err)
			}
			type result struct {
				session durable.Session
				err     error
			}
			resultCh := make(chan result, 1)
			run := "lifecycle-b"
			if mode == "same-run" {
				run = "lifecycle-a"
			}
			// Background has no caller deadline: the Store owns cancellation on Close.
			// A cancellable wrapper is retained solely for failure cleanup on baseline.
			acquireCtx, stop := context.WithCancel(context.Background())
			defer stop()
			go func() { b, e := s.Acquire(acquireCtx, run); resultCh <- result{b, e} }()
			receive := func() result {
				t.Helper()
				select {
				case r := <-resultCh:
					return r
				case <-ctx.Done():
					stop()
					t.Fatal("Acquire did not complete")
					return result{}
				}
			}
			if mode == "same-run" {
				select {
				case r := <-resultCh:
					if r.session != nil {
						r.session.Close()
					}
					if !errors.Is(r.err, durable.ErrBusy) {
						t.Fatalf("same run: %v", r.err)
					}
				case <-time.After(time.Second):
					stop()
					r := receive()
					if r.session != nil {
						r.session.Close()
					}
					t.Fatal("same run waited for exhausted pool")
				}
				return
			}
			// EmptyAcquireWaitTime is recorded only after completion. CanceledAcquireCount
			// is also too late. AcquiredConns + the pending reservation give a precise
			// barrier without sleeping or depending on scheduler timing.
			for {
				s.mu.Lock()
				pending := len(s.pending)
				s.mu.Unlock()
				if pending == 1 && s.pool.Stat().AcquiredConns() == 1 {
					break
				}
				select {
				case <-ctx.Done():
					stop()
					t.Fatal("pending acquire barrier timed out")
				default:
					runtime.Gosched()
				}
			}
			switch mode {
			case "pending-same-run":
				b, e := s.Acquire(ctx, run)
				if b != nil {
					b.Close()
				}
				if !errors.Is(e, durable.ErrBusy) {
					t.Fatalf("pending duplicate: %v", e)
				}
				stop()
			case "cancel":
				stop()
			case "store-close", "concurrent-close":
				done := make(chan error, 1)
				go func() {
					var wg sync.WaitGroup
					n := 1
					if mode == "concurrent-close" {
						n = 8
					}
					errs := make(chan error, n+1)
					for i := 0; i < n; i++ {
						wg.Add(1)
						go func() { defer wg.Done(); errs <- s.Close() }()
					}
					wg.Add(1)
					go func() {
						defer wg.Done()
						for {
							s.mu.Lock()
							closed := s.closed
							s.mu.Unlock()
							if closed {
								break
							}
							runtime.Gosched()
						}
						errs <- a.Close()
					}()
					wg.Wait()
					close(errs)
					var combined error
					for e := range errs {
						combined = errors.Join(combined, e)
					}
					done <- combined
				}()
				select {
				case e := <-done:
					if e != nil {
						t.Fatal(e)
					}
				case <-ctx.Done():
					stop()
					<-done
					t.Fatal("Close blocked behind pending Acquire")
				}
			default:
				done := make(chan error, 1)
				go func() { done <- a.Close() }()
				select {
				case e := <-done:
					if e != nil {
						t.Fatal(e)
					}
				case <-ctx.Done():
					stop()
					<-done
					t.Fatal("Session.Close blocked behind pending Acquire")
				}
			}
			r := receive()
			if r.session != nil {
				defer r.session.Close()
			}
			if mode == "release" {
				if r.err != nil {
					t.Fatal(r.err)
				}
			} else if r.err == nil {
				t.Fatal("canceled/closed acquisition succeeded")
			}
			if mode == "cancel" || mode == "pending-same-run" {
				if err := a.Close(); err != nil {
					t.Fatal(err)
				}
				b, e := s.Acquire(ctx, run)
				if e != nil {
					t.Fatalf("reservation leaked: %v", e)
				}
				b.Close()
			}
		})
	}
}
