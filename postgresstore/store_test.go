package postgresstore

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestAdvisoryKeyStable(t *testing.T) {
	if advisoryKey("x") != advisoryKey("x") || advisoryKey("x") == advisoryKey("y") {
		t.Fatal("unexpected advisory key")
	}
}

func TestParseRevision(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want uint64
		ok   bool
	}{
		{"1", 1, true}, {"18446744073709551615", math.MaxUint64, true},
		{"0", 0, false}, {"01", 0, false}, {"18446744073709551616", 0, false}, {"", 0, false},
	} {
		got, err := parseRevision(tc.in)
		if (err == nil) != tc.ok || got != tc.want {
			t.Errorf("parseRevision(%q)=(%d,%v)", tc.in, got, err)
		}
	}
}

func TestNewRequiresDSN(t *testing.T) {
	if _, err := New(context.Background(), ""); err == nil {
		t.Fatal("expected error")
	}
}

func TestPGChildAcquire(t *testing.T) {
	if os.Getenv("PULSE_POSTGRES_CHILD") != "1" {
		t.Skip("set by parent contract test")
	}
	store, err := New(context.Background(), postgresDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	s, err := store.Acquire(context.Background(), os.Getenv("PULSE_POSTGRES_CHILD_RUN"))
	if !errors.Is(err, durable.ErrBusy) {
		t.Fatalf("Acquire error=%v, want ErrBusy", err)
	}
	if s != nil {
		_ = s.Close()
		t.Fatal("busy acquire returned session")
	}
}

func TestPGContract(t *testing.T) {
	dsn := os.Getenv("PULSE_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("PULSE_POSTGRES_TEST_DSN is not set")
	}
	resetPGSchema(t, dsn)
	defer resetPGSchema(t, dsn)

	first, err := New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	a, err := first.Acquire(context.Background(), "same-run")
	if err != nil {
		t.Fatal(err)
	}
	if b, err := second.Acquire(context.Background(), "same-run"); !errors.Is(err, durable.ErrBusy) || b != nil {
		t.Fatalf("same RunID: session=%v err=%v", b, err)
	}
	b, err := second.Acquire(context.Background(), "other-run")
	if err != nil {
		t.Fatal(err)
	}
	_ = b.Close()

	record := testRecord("same-run", 1, []byte{0, 1, 2, 0xff})
	if err := a.Save(context.Background(), record, 0); err != nil {
		t.Fatal(err)
	}
	loaded, err := a.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(loaded.Scopes["scope"].Nodes["node"].Outputs[0].Data) != string(record.Scopes["scope"].Nodes["node"].Outputs[0].Data) {
		t.Fatal("blob changed during save/load")
	}
	if _, err := second.Acquire(context.Background(), "same-run"); !errors.Is(err, durable.ErrBusy) {
		t.Fatalf("lock not held: %v", err)
	}
	if err := a.Save(context.Background(), testRecord("same-run", 2, nil), 0); !errors.Is(err, durable.ErrConflict) {
		t.Fatalf("stale CAS error=%v", err)
	}
	if err := a.Save(context.Background(), testRecord("wrong-run", 2, nil), 1); !errors.Is(err, durable.ErrConflict) {
		t.Fatalf("wrong RunID error=%v", err)
	}
	if err := a.Save(context.Background(), testRecord("same-run", 1, nil), math.MaxUint64); !errors.Is(err, durable.ErrConflict) {
		t.Fatalf("MaxUint64 error=%v", err)
	}
	if err := a.Save(context.Background(), testRecord("same-run", 2, nil), 1); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Load(context.Background()); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed Load=%v", err)
	}

	cross, err := first.Acquire(context.Background(), "cross-process")
	if err != nil {
		t.Fatal(err)
	}
	if err := runChildBusy(dsn, "cross-process"); err != nil {
		t.Fatal(err)
	}
	if err := cross.Close(); err != nil {
		t.Fatal(err)
	}

	pool := first.pool
	maxConns := int(pool.Stat().MaxConns())
	held := make([]*pgxpool.Conn, 0, maxConns)
	for i := 0; i < maxConns; i++ {
		c, err := pool.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, c)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	if _, err := pool.Acquire(ctx); err == nil {
		t.Fatal("pool exhaustion unexpectedly succeeded")
	}
	cancel()
	for _, c := range held {
		c.Release()
	}
	pool.Close()
	if _, err := pool.Acquire(context.Background()); err == nil {
		t.Fatal("closed pool acquire unexpectedly succeeded")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Acquire(context.Background(), "after-close"); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Acquire after close=%v", err)
	}
}

func postgresDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("PULSE_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Fatal("PULSE_POSTGRES_TEST_DSN is not set")
	}
	return dsn
}

func resetPGSchema(t *testing.T, dsn string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse test DSN: %v", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS pulse_durable CASCADE`); err != nil {
		t.Fatalf("reset test schema: %v", err)
	}
}

func runChildBusy(dsn, runID string) error {
	cmd := exec.Command(os.Args[0], "-test.run=^TestPGChildAcquire$", "-test.v")
	env := append(os.Environ(), "PULSE_POSTGRES_CHILD=1", "PULSE_POSTGRES_CHILD_RUN="+runID, "PULSE_POSTGRES_TEST_DSN="+dsn)
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("child contract failed: %w (%s)", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func testRecord(runID string, revision uint64, blob []byte) durable.RunRecord {
	return durable.RunRecord{Format: durable.RecordFormat, RunID: runID, Revision: revision, Version: "v1", Scopes: map[string]durable.ScopeRecord{"scope": {Graph: "g", Nodes: map[string]durable.NodeRecord{"node": {NodeID: "node", Attempt: "attempt", Reason: "completed", Outputs: []durable.ValueRecord{{Name: "blob", TypeID: "bytes", Version: "v1", State: "ready", Data: blob}}}}}}}
}
