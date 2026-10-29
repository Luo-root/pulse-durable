package durable

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// buildGateContext pauses the first replay cancellation check, after capture
// released the store lock. It is test-only and uses the normal context contract.
type buildGateContext struct {
	context.Context
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (c *buildGateContext) Err() error {
	if c.calls.Add(1) == 4 {
		close(c.entered)
		<-c.release
	}
	return c.Context.Err()
}

func TestMemorySynthesisUnlockedBuild(t *testing.T) {
	for _, cancelBuild := range []bool{false, true} {
		t.Run(map[bool]string{false: "suffix", true: "cancel"}[cancelBuild], func(t *testing.T) {
			store := NewMemoryStore()
			s, err := store.Acquire(context.Background(), "gate")
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			r := contractSeed("gate")
			if err := s.Initialize(context.Background(), r); err != nil {
				t.Fatal(err)
			}
			appendAttempt := func(index int) {
				t.Helper()
				tx := newRecordTransaction(r.Revision)
				var expected *AttemptRecord
				if old, ok := r.Attempts["a"]; ok {
					expected = &old
				}
				if err := tx.putAttempt("a", expected, AttemptRecord{Index: index}); err != nil {
					t.Fatal(err)
				}
				if _, err := s.Apply(context.Background(), ChangeBatch{changeHeader(r), tx}); err != nil {
					t.Fatal(err)
				}
				r, err = s.Load(context.Background())
				if err != nil {
					t.Fatal(err)
				}
			}
			appendAttempt(1)
			before, err := store.InspectStorage(context.Background(), "gate")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			gate := &buildGateContext{Context: ctx, entered: make(chan struct{}), release: make(chan struct{})}
			done := make(chan error, 1)
			go func() {
				_, err := store.CompactStorage(gate, CompactStorageRequest{"gate", 2, before.Generation})
				done <- err
			}()
			select {
			case <-gate.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("build did not reach replay")
			}
			// The build is paused: a writer must still finish before it is released.
			appendAttempt(2)
			if cancelBuild {
				cancel()
			}
			close(gate.release)
			if err := <-done; cancelBuild && !errors.Is(err, context.Canceled) || !cancelBuild && err != nil {
				t.Fatal(err)
			}
			after, err := store.InspectStorage(context.Background(), "gate")
			if err != nil {
				t.Fatal(err)
			}
			wantGeneration := before.Generation + 1
			if cancelBuild {
				wantGeneration = before.Generation
			}
			if after.Generation != wantGeneration || after.HeadRevision != 3 || cancelBuild && after.BaseRevision != before.BaseRevision {
				t.Fatalf("unexpected state: %+v", after)
			}
			for revision := uint64(1); revision <= 3; revision++ {
				image, err := store.LoadRevision(context.Background(), "gate", revision)
				if err != nil || image.Record.Revision != revision {
					t.Fatalf("revision %d: %v", revision, err)
				}
				if revision == 3 && !reflect.DeepEqual(image.Record, r) {
					t.Fatal("concurrent suffix lost")
				}
			}
		})
	}
}

func TestMemorySynthesisPreservesHistoryAndBusinessChain(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	session, err := store.Acquire(ctx, "history")
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	r := contractSeed("history")
	if err := session.Initialize(ctx, r); err != nil {
		t.Fatal(err)
	}
	images := []RunRecord{CloneRecord(r)}
	for index := 1; index <= 3; index++ {
		tx := newRecordTransaction(r.Revision)
		old, exists := r.Attempts["a"]
		var expected *AttemptRecord
		if exists {
			expected = &old
		}
		if err := tx.putAttempt("a", expected, AttemptRecord{Index: index}); err != nil {
			t.Fatal(err)
		}
		if _, err := session.Apply(ctx, ChangeBatch{changeHeader(r), tx}); err != nil {
			t.Fatal(err)
		}
		r, err = session.Load(ctx)
		if err != nil {
			t.Fatal(err)
		}
		images = append(images, r)
	}
	before, err := store.InspectStorage(ctx, "history")
	if err != nil {
		t.Fatal(err)
	}
	after, err := store.CompactStorage(ctx, CompactStorageRequest{RunID: "history", ThroughRevision: 2, ExpectedGeneration: before.Generation})
	if err != nil {
		t.Fatal(err)
	}
	if after.Generation != before.Generation+1 || after.BaseRevision != 2 || after.HeadRevision != before.HeadRevision || after.HeadChain != before.HeadChain {
		t.Fatalf("synthesis changed head: before=%+v after=%+v", before, after)
	}
	for i, want := range images {
		got, err := store.LoadRevision(ctx, "history", uint64(i+1))
		if err != nil || !reflect.DeepEqual(got.Record, want) {
			t.Fatalf("revision %d changed: %v", i+1, err)
		}
	}
	// A session anchored before synthesis can append without reseeding its chain.
	tx := newRecordTransaction(r.Revision)
	old := r.Attempts["a"]
	if err := tx.putAttempt("a", &old, AttemptRecord{Index: 4}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Apply(ctx, ChangeBatch{changeHeader(r), tx}); err != nil {
		t.Fatal(err)
	}
	final, err := store.LoadRevision(ctx, "history", 5)
	if err != nil || final.Record.Attempts["a"].Index != 4 {
		t.Fatalf("post-synthesis append: %v", err)
	}
	final.Record.Attempts["a"] = AttemptRecord{Index: 99}
	again, err := store.LoadRevision(ctx, "history", 5)
	if err != nil || again.Record.Attempts["a"].Index != 4 {
		t.Fatal("history result aliases store")
	}
}

func TestMemoryFixtureHistoryFloor(t *testing.T) {
	store := NewMemoryStore()
	r := contractSeed("fixture")
	r.Revision = 7
	installMemoryFixtureLocked(store, r)
	if _, err := store.LoadRevision(context.Background(), r.RunID, 6); !errors.Is(err, ErrHistoryExpired) {
		t.Fatal(err)
	}
	s, err := store.Acquire(context.Background(), r.RunID)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	tx := newRecordTransaction(r.Revision)
	if err := tx.putAttempt("a", nil, AttemptRecord{Index: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply(context.Background(), ChangeBatch{changeHeader(r), tx}); err != nil {
		t.Fatal(err)
	}
	image, err := store.LoadRevision(context.Background(), r.RunID, 8)
	if err != nil || image.Record.Attempts["a"].Index != 1 {
		t.Fatalf("fixture suffix: %v", err)
	}
}

func TestMemoryHistoryCorruptionFailsClosed(t *testing.T) {
	for _, damage := range []string{"missing-history", "head-anchor", "snapshot-key", "snapshot-run", "snapshot-revision", "non-head-anchor"} {
		t.Run(damage, func(t *testing.T) {
			store := NewMemoryStore()
			s, err := store.Acquire(context.Background(), "bad")
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			r := contractSeed("bad")
			if err := s.Initialize(context.Background(), r); err != nil {
				t.Fatal(err)
			}
			tx := newRecordTransaction(r.Revision)
			if err := tx.putAttempt("a", nil, AttemptRecord{Index: 1}); err != nil {
				t.Fatal(err)
			}
			if _, err := s.Apply(context.Background(), ChangeBatch{changeHeader(r), tx}); err != nil {
				t.Fatal(err)
			}
			r, err = s.Load(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			before := CloneRecord(r)
			h := store.history["bad"]
			switch damage {
			case "missing-history":
				delete(store.history, "bad")
			case "head-anchor":
				h.anchors[2] = [32]byte{}
			case "snapshot-key":
				h.snapshots[0] = h.snapshots[1]
			case "snapshot-run":
				image := h.snapshots[1]
				image.Record.RunID = "wrong"
				h.snapshots[1] = image
			case "snapshot-revision":
				image := h.snapshots[1]
				image.Record.Revision = 3
				h.snapshots[1] = image
			case "non-head-anchor":
				h.anchors[1] = [32]byte{}
			}
			if _, err := store.LoadRevision(context.Background(), "bad", 1); !errors.Is(err, ErrInvalidRecord) || errors.Is(err, ErrHistoryExpired) {
				t.Fatalf("corruption: %v", err)
			}
			// Head corruption must fail before any mutation of shared maps.
			if damage == "missing-history" || damage == "head-anchor" {
				tx := newRecordTransaction(r.Revision)
				old := r.Attempts["a"]
				if err := tx.putAttempt("a", &old, AttemptRecord{Index: 2}); err != nil {
					t.Fatal(err)
				}
				if _, err := s.Apply(context.Background(), ChangeBatch{changeHeader(r), tx}); !errors.Is(err, ErrInvalidRecord) {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(before, store.records["bad"]) {
					t.Fatal("invalid history partially mutated record")
				}
			}
		})
	}
}

func TestMemoryHistoryRejectsMissingBatch(t *testing.T) {
	store := NewMemoryStore()
	session, err := store.Acquire(context.Background(), "broken")
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	r := contractSeed("broken")
	if err := session.Initialize(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	tx := newRecordTransaction(r.Revision)
	if err := tx.putAttempt("a", nil, AttemptRecord{Index: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Apply(context.Background(), ChangeBatch{changeHeader(r), tx}); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.history["broken"].batches = nil
	store.mu.Unlock()
	if _, err := store.LoadRevision(context.Background(), "broken", 2); err == nil || errors.Is(err, ErrHistoryExpired) {
		t.Fatalf("missing retained batch classified as available or expired: %v", err)
	}
}

func TestMemorySynthesisCASAndCancellation(t *testing.T) {
	store := NewMemoryStore()
	s, err := store.Acquire(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Initialize(context.Background(), contractSeed("run")); err != nil {
		t.Fatal(err)
	}
	request := CompactStorageRequest{RunID: "run", ThroughRevision: 1, ExpectedGeneration: 1}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := store.CompactStorage(context.Background(), request); results <- err }()
	}
	wg.Wait()
	close(results)
	success, conflict := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if errors.Is(err, ErrConflict) {
			conflict++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("CAS results %d/%d", success, conflict)
	}
	request.ExpectedGeneration = 2
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.CompactStorage(ctx, request); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	state, err := store.InspectStorage(context.Background(), "run")
	if err != nil || state.Generation != 2 {
		t.Fatal("cancellation published", err)
	}
	for _, req := range []CompactStorageRequest{{RunID: "run", ThroughRevision: 0, ExpectedGeneration: 2}, {RunID: "run", ThroughRevision: 2, ExpectedGeneration: 2}, {RunID: "run", ThroughRevision: 1, ExpectedGeneration: 0}} {
		if _, err := store.CompactStorage(context.Background(), req); !errors.Is(err, ErrConflict) {
			t.Fatalf("invalid request accepted: %+v %v", req, err)
		}
	}
	if _, err := store.LoadRevision(context.Background(), "run", 0); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
}
