package durable

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestMemoryStoreSessionCASAndIsolation(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	s, err := store.Acquire(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Acquire(ctx, "run"); !errors.Is(err, ErrBusy) {
		t.Fatalf("concurrent acquire: %v", err)
	}
	if _, err := s.Load(ctx); !errors.Is(err, ErrNotFound) {
		t.Fatalf("empty load: %v", err)
	}
	r := contractSeed("run")
	if err := s.Initialize(ctx, r); err != nil {
		t.Fatal(err)
	}
	tx := newRecordTransaction(1)
	_ = tx.putNode("root", "node", nil, NodeRecord{NodeID: "node", Outputs: []ValueRecord{{Data: []byte("original")}}})
	_ = tx.putAttempt("attempt", nil, AttemptRecord{Index: 1})
	if _, err := s.Apply(ctx, ChangeBatch{changeHeader(r), tx}); err != nil {
		t.Fatal(err)
	}
	r, _ = s.Load(ctx)
	r.Scopes["root"].Nodes["node"].Outputs[0].Data[0] = 'X'
	r.Attempts["attempt"] = AttemptRecord{Index: 99}
	loaded, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(loaded.Scopes["root"].Nodes["node"].Outputs[0].Data) != "original" || loaded.Attempts["attempt"].Index != 1 {
		t.Fatal("Save did not isolate the record")
	}
	loaded.Scopes["root"].Nodes["node"].Outputs[0].Data[0] = 'Y'
	again, _ := s.Load(ctx)
	if string(again.Scopes["root"].Nodes["node"].Outputs[0].Data) != "original" {
		t.Fatal("Load did not isolate the record")
	}
	for _, bad := range []RunRecord{{RunID: "other", Revision: 2}, {RunID: "run", Revision: 3}, {RunID: "run", Revision: 1}} {
		if err := s.Initialize(ctx, bad); !errors.Is(err, ErrConflict) {
			t.Fatalf("invalid CAS accepted: %v", err)
		}
	}
	base, _ := s.Load(ctx)
	next := newRecordTransaction(base.Revision)
	old := base.Attempts["attempt"]
	updated := old
	updated.Index = 2
	_ = next.putAttempt("attempt", &old, updated)
	batch := ChangeBatch{changeHeader(base), next}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Apply(ctx, batch)
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	var successes, conflicts int
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrConflict):
			conflicts++
		default:
			t.Fatal(err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("CAS successes=%d conflicts=%d", successes, conflicts)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(ctx); err == nil {
		t.Fatal("closed Load accepted")
	}
	if _, err := s.Apply(ctx, batch); err == nil {
		t.Fatal("closed Save accepted")
	}
	reopened, err := store.Acquire(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	final, err := reopened.Load(ctx)
	if err != nil || final.Revision != 3 {
		t.Fatalf("reopened record: %+v, %v", final, err)
	}
}

func TestMemoryStoreContext(t *testing.T) {
	store := NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Acquire(ctx, "run"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	s, _ := store.Acquire(context.Background(), "run")
	defer s.Close()
	if err := s.Initialize(ctx, contractSeed("run")); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.Load(context.Background()); !errors.Is(err, ErrNotFound) {
		t.Fatal("canceled Save published data")
	}
}
