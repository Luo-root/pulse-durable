package postgresstore

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	durable "github.com/Luo-root/pulse-durable"
)

func TestPGUncertainUnlockDiscardsConnection(t *testing.T) {
	dsn := os.Getenv("PULSE_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("PULSE_POSTGRES_TEST_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	acquired, err := s.Acquire(ctx, "uncertain-unlock")
	if err != nil {
		t.Fatal(err)
	}
	x := acquired.(*session)
	// Invalidate the expected lock state. Close must not return this backend
	// to the idle pool even when PostgreSQL returns false without a SQL error.
	if _, err = x.conn.Exec(ctx, `SELECT pg_advisory_unlock_all()`); err != nil {
		t.Fatal(err)
	}
	pid := x.conn.Conn().PgConn().PID()
	if err = x.Close(); err == nil {
		t.Fatal("expected uncertain lock state error")
	}
	c, err := s.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Release()
	if c.Conn().PgConn().PID() == pid {
		t.Fatal("uncertain backend returned to pool")
	}
}

func TestPGCrossStoreSaturatedPool(t *testing.T) {
	dsn := os.Getenv("PULSE_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("PULSE_POSTGRES_TEST_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first, err := New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	a, err := first.Acquire(ctx, "remote-held")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	// Fill only the second Store's pool. Its local reservation table has no
	// knowledge of first's run; a caller deadline bounds this capacity wait.
	held := make([]interface{ Release() }, 0)
	defer func() {
		for _, c := range held {
			c.Release()
		}
	}()
	for i := int32(0); i < second.pool.Stat().MaxConns(); i++ {
		c, e := second.pool.Acquire(ctx)
		if e != nil {
			t.Fatal(e)
		}
		held = append(held, c)
	}
	wait, stop := context.WithTimeout(ctx, 50*time.Millisecond)
	_, err = second.Acquire(wait, "remote-held")
	stop()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("saturated cross-store acquire: %v", err)
	}
	for _, c := range held {
		c.Release()
	}
	held = nil
	if b, e := second.Acquire(ctx, "remote-held"); !errors.Is(e, durable.ErrBusy) {
		if b != nil {
			b.Close()
		}
		t.Fatalf("remote advisory lock: %v", e)
	}
}
