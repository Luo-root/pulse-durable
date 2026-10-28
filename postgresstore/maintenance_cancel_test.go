package postgresstore

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	durable "github.com/Luo-root/pulse-durable"
)

func TestPGMaintenanceCancelPublication(t *testing.T) {
	dsn := os.Getenv("PULSE_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("opt-in isolated database required")
	}
	ctx := context.Background()
	resetPGSchema(t, dsn)
	defer resetPGSchema(t, dsn)
	s, err := New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a, err := s.Acquire(ctx, "cancel-publication")
	if err != nil {
		t.Fatal(err)
	}
	r := initialRecord("cancel-publication")
	if err = a.Initialize(ctx, r); err != nil {
		t.Fatal(err)
	}
	if _, err = a.Apply(ctx, attemptBatch(t, r)); err != nil {
		t.Fatal(err)
	}
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := s.InspectStorage(ctx, r.RunID)
	if err != nil {
		t.Fatal(err)
	}
	// Hold the publication row while the compactor reads/builds. The cancelled
	// UPDATE must roll back the newly inserted snapshot and release its run lock.
	blocker, err := s.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer rollback(blocker)
	if _, err = blocker.Exec(ctx, `SELECT run_id FROM pulse_durable.heads WHERE run_id=$1 FOR UPDATE`, r.RunID); err != nil {
		t.Fatal(err)
	}
	limited, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if _, err = s.CompactStorage(limited, durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: 2, ExpectedGeneration: 1}); err == nil {
		t.Fatal("blocked publication succeeded")
	}
	if err = blocker.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	after, err := s.InspectStorage(ctx, r.RunID)
	if err != nil || after != before {
		t.Fatalf("cancelled publication changed state: %+v %v", after, err)
	}
	var count int
	if err = s.pool.QueryRow(ctx, `SELECT count(*) FROM pulse_durable.snapshots WHERE run_id=$1`, r.RunID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("cancelled synthesis left a snapshot")
	}
	if _, err = s.CompactStorage(ctx, durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: 2, ExpectedGeneration: 1}); err != nil {
		t.Fatalf("lock not released: %v", err)
	}
}

func TestPGUnknownSchemaUntouched(t *testing.T) {
	dsn := os.Getenv("PULSE_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("opt-in isolated database required")
	}
	ctx := context.Background()
	resetPGSchema(t, dsn)
	defer resetPGSchema(t, dsn)
	s, err := New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.pool.Exec(ctx, `CREATE TABLE pulse_durable.unknown_layout(marker text NOT NULL); INSERT INTO pulse_durable.unknown_layout(marker) VALUES('retain')`); err != nil {
		t.Fatal(err)
	}
	if other, e := New(ctx, dsn); e == nil {
		other.Close()
		t.Fatal("unknown layout accepted")
	}
	var marker string
	if err = s.pool.QueryRow(ctx, `SELECT marker FROM pulse_durable.unknown_layout`).Scan(&marker); err != nil {
		t.Fatal(err)
	}
	if marker != "retain" {
		t.Fatal("unknown layout modified")
	}
	if !errors.Is(errCorrupt, durable.ErrInvalidRecord) {
		t.Fatal("corruption sentinel lost")
	}
}
