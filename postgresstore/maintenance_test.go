package postgresstore

import (
	"context"
	"errors"
	"math"
	"os"
	"reflect"
	"sync"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
)

func TestMaintenanceLocalBusyCancelClosed(t *testing.T) {
	s := &Store{sessions: map[*session]struct{}{}}
	x := &session{runID: "busy"}
	s.sessions[x] = struct{}{}
	ctx := context.Background()
	if _, err := s.InspectStorage(ctx, "busy"); !errors.Is(err, durable.ErrBusy) {
		t.Fatal(err)
	}
	if _, err := s.LoadRevision(ctx, "busy", 1); !errors.Is(err, durable.ErrBusy) {
		t.Fatal(err)
	}
	if _, err := s.CompactStorage(ctx, durable.CompactStorageRequest{RunID: "busy", ThroughRevision: 1, ExpectedGeneration: 1}); !errors.Is(err, durable.ErrBusy) {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.CompactStorage(canceled, durable.CompactStorageRequest{RunID: "busy"}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	s.closed = true
	if _, err := s.InspectStorage(ctx, "busy"); !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
}

func TestSnapshotArbitraryRevisionNestedEmpty(t *testing.T) {
	for _, empty := range []bool{false, true} {
		r := initialRecord("nested")
		r.Revision = 42
		n := durable.NodeRecord{NodeID: "n"}
		if empty {
			n.Outputs = []durable.ValueRecord{}
			r.Attempts = map[string]durable.AttemptRecord{}
			r.Streams = map[string]durable.StreamRecord{}
		}
		scope := r.Scopes["scope"]
		scope.Nodes["n"] = n
		r.Scopes["scope"] = scope
		data, err := encodeSnapshot(r)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeSnapshot(data)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(r, got) {
			t.Fatal("nested nil/empty or revision changed")
		}
	}
}

func TestPGMaintenanceHistorySynthesis(t *testing.T) {
	dsn := os.Getenv("PULSE_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("opt-in isolated database required; not database acceptance")
	}
	ctx := context.Background()
	resetPGSchema(t, dsn)
	defer resetPGSchema(t, dsn)
	s, err := New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a, err := s.Acquire(ctx, "history")
	if err != nil {
		t.Fatal(err)
	}
	r := initialRecord("history")
	if err = a.Initialize(ctx, r); err != nil {
		t.Fatal(err)
	}
	records := map[uint64]durable.RunRecord{1: durable.CloneRecord(r)}
	for r.Revision < 5 {
		b := nodeBatch(t, r, []byte{byte(r.Revision)})
		if _, err = a.Apply(ctx, b); err != nil {
			t.Fatal(err)
		}
		if _, err = b.ApplyTo(&r); err != nil {
			t.Fatal(err)
		}
		records[r.Revision] = durable.CloneRecord(r)
	}
	if _, err = s.CompactStorage(ctx, durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: 3, ExpectedGeneration: 1}); !errors.Is(err, durable.ErrBusy) {
		t.Fatalf("active session: %v", err)
	}
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := s.InspectStorage(ctx, r.RunID)
	if err != nil {
		t.Fatal(err)
	}
	images := map[uint64]durable.RevisionImage{}
	for revision, want := range records {
		got, e := s.LoadRevision(ctx, r.RunID, revision)
		if e != nil {
			t.Fatal(e)
		}
		if !reflect.DeepEqual(got.Record, want) {
			t.Fatalf("revision %d", revision)
		}
		images[revision] = got
	}
	for _, request := range []durable.CompactStorageRequest{{RunID: r.RunID, ThroughRevision: 0, ExpectedGeneration: 1}, {RunID: r.RunID, ThroughRevision: 6, ExpectedGeneration: 1}, {RunID: r.RunID, ThroughRevision: 3, ExpectedGeneration: 0}, {RunID: r.RunID, ThroughRevision: 3, ExpectedGeneration: 2}} {
		if _, e := s.CompactStorage(ctx, request); !errors.Is(e, durable.ErrConflict) {
			t.Fatalf("invalid request: %v", e)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = s.CompactStorage(canceled, durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: 3, ExpectedGeneration: 1}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	state, err := s.CompactStorage(ctx, durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: 3, ExpectedGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	if state.Generation != 2 || state.BaseRevision != 3 || state.BaseChain != images[3].Chain || state.HeadChain != before.HeadChain || state.HeadRevision != before.HeadRevision {
		t.Fatalf("synthesis changed business head: %+v", state)
	}
	var batches, snapshots int
	if err = s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM pulse_durable.batches WHERE run_id=$1),(SELECT count(*) FROM pulse_durable.snapshots WHERE run_id=$1)`, r.RunID).Scan(&batches, &snapshots); err != nil {
		t.Fatal(err)
	}
	if batches != 4 || snapshots != 2 {
		t.Fatal("history reclaimed or synthesized snapshot missing")
	}
	for revision, want := range images {
		got, e := s.LoadRevision(ctx, r.RunID, revision)
		if e != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("history changed at %d: %v", revision, e)
		}
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a, err = s.Acquire(ctx, r.RunID)
	if err != nil {
		t.Fatal(err)
	}
	got, err := a.Load(ctx)
	if err != nil || !reflect.DeepEqual(got, r) {
		t.Fatalf("reopen: %v", err)
	}
	if usage := a.(*session).Usage(); usage.Batches != 2 {
		t.Fatalf("suffix budget: %+v", usage)
	}
	b := attemptBatch(t, r)
	if _, err = a.Apply(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, err = b.ApplyTo(&r); err != nil {
		t.Fatal(err)
	}
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := s.InspectStorage(ctx, r.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if after.Generation != 2 || after.BaseRevision != 3 || after.HeadRevision != 6 {
		t.Fatalf("postappend: %+v", after)
	}
	image, err := s.LoadRevision(ctx, r.RunID, 6)
	if err != nil || !reflect.DeepEqual(image.Record, r) {
		t.Fatalf("postappend image: %v", err)
	}
	if _, err = s.CompactStorage(ctx, durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: 2, ExpectedGeneration: 2}); !errors.Is(err, durable.ErrConflict) {
		t.Fatal(err)
	}
	// 两个使用独立连接池的压缩器：锁竞争或过期代际
	// 应阻止同一提案的第二次发布。
	other, err := New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, store := range []*Store{s, other} {
		wg.Add(1)
		go func(store *Store) {
			defer wg.Done()
			<-start
			_, e := store.CompactStorage(ctx, durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: 5, ExpectedGeneration: 2})
			results <- e
		}(store)
	}
	close(start)
	wg.Wait()
	close(results)
	success := 0
	for e := range results {
		if e == nil {
			success++
		} else if !errors.Is(e, durable.ErrBusy) && !errors.Is(e, durable.ErrConflict) {
			t.Fatal(e)
		}
	}
	if success != 1 {
		t.Fatalf("publications=%d", success)
	}
	state, err = s.InspectStorage(ctx, r.RunID)
	if err != nil || state.Generation != 3 || state.HeadChain != after.HeadChain {
		t.Fatalf("two compactors: %+v %v", state, err)
	}
	// 代际溢出在任何合成或业务修改前被拒绝。
	if _, err = s.pool.Exec(ctx, `UPDATE pulse_durable.heads SET generation=$2 WHERE run_id=$1`, r.RunID, "18446744073709551615"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CompactStorage(ctx, durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: 6, ExpectedGeneration: math.MaxUint64}); !errors.Is(err, durable.ErrLimitExceeded) {
		t.Fatal(err)
	}
}

func TestPGMaintenanceCorruptExactTarget(t *testing.T) {
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
	a, err := s.Acquire(ctx, "corrupt-target")
	if err != nil {
		t.Fatal(err)
	}
	r := initialRecord("corrupt-target")
	if err = a.Initialize(ctx, r); err != nil {
		t.Fatal(err)
	}
	b := attemptBatch(t, r)
	if _, err = a.Apply(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CompactStorage(ctx, durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: 2, ExpectedGeneration: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.pool.Exec(ctx, `UPDATE pulse_durable.snapshots SET chain=decode(repeat('00',32),'hex') WHERE run_id=$1 AND revision='2'`, r.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.LoadRevision(ctx, r.RunID, 2); !errors.Is(err, durable.ErrInvalidRecord) || errors.Is(err, durable.ErrHistoryExpired) {
		t.Fatalf("exact snapshot anchor: %v", err)
	}
	a, err = s.Acquire(ctx, r.RunID)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, err = a.Load(ctx); !errors.Is(err, durable.ErrInvalidRecord) {
		t.Fatalf("current differs from history validation: %v", err)
	}
}

func TestPGV2Untouched(t *testing.T) {
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
	if _, err = s.pool.Exec(ctx, `DROP TABLE pulse_durable.snapshots,pulse_durable.heads,pulse_durable.batches;
 CREATE TABLE pulse_durable.snapshots(run_id text PRIMARY KEY NOT NULL,record bytea NOT NULL,digest bytea NOT NULL,chain bytea NOT NULL);
 CREATE TABLE pulse_durable.heads(run_id text PRIMARY KEY NOT NULL,revision text NOT NULL,chain bytea NOT NULL);
 CREATE TABLE pulse_durable.batches(run_id text NOT NULL,revision text NOT NULL,parent_revision text NOT NULL,parent_chain bytea NOT NULL,digest bytea NOT NULL,chain bytea NOT NULL,wire bytea NOT NULL,PRIMARY KEY(run_id,revision));
 UPDATE pulse_durable.schema_meta SET version=2;
 INSERT INTO pulse_durable.snapshots(run_id,record,digest,chain) VALUES('old',decode('00ff','hex'),decode('01','hex'),decode('02','hex'));`); err != nil {
		t.Fatal(err)
	}
	if other, e := New(ctx, dsn); e == nil {
		other.Close()
		t.Fatal("v2 accepted")
	}
	var version, columns int
	var data []byte
	if err = s.pool.QueryRow(ctx, `SELECT version FROM pulse_durable.schema_meta`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err = s.pool.QueryRow(ctx, `SELECT record FROM pulse_durable.snapshots WHERE run_id='old'`).Scan(&data); err != nil {
		t.Fatal(err)
	}
	if err = s.pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_schema='pulse_durable'`).Scan(&columns); err != nil {
		t.Fatal(err)
	}
	if version != 2 || columns != 15 || !reflect.DeepEqual(data, []byte{0, 255}) {
		t.Fatal("v2 changed on rejection")
	}
}
