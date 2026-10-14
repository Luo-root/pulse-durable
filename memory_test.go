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
	r := RunRecord{RunID: "run", Revision: 1, Scopes: map[string]ScopeRecord{"root": {Nodes: map[string]NodeRecord{"node": {
		NodeID: "node", Outputs: []ValueRecord{{Data: []byte("original")}},
	}}}}, Attempts: map[string]AttemptRecord{"attempt": {Index: 1}}}
	if err := s.Save(ctx, r, 0); err != nil {
		t.Fatal(err)
	}
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
		if err := s.Save(ctx, bad, 1); !errors.Is(err, ErrConflict) {
			t.Fatalf("invalid CAS accepted: %v", err)
		}
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- s.Save(ctx, RunRecord{RunID: "run", Revision: 2}, 1)
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
	if err := s.Save(ctx, RunRecord{RunID: "run", Revision: 3}, 2); err == nil {
		t.Fatal("closed Save accepted")
	}
	reopened, err := store.Acquire(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	final, err := reopened.Load(ctx)
	if err != nil || final.Revision != 2 {
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
	if err := s.Save(ctx, RunRecord{RunID: "run", Revision: 1}, 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.Load(context.Background()); !errors.Is(err, ErrNotFound) {
		t.Fatal("canceled Save published data")
	}
}
