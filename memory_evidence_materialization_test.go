package durable

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryHistoryEvidenceMaterializationBuildsClosedComponent(t *testing.T) {
	store := NewMemoryStore()
	session, err := store.Acquire(context.Background(), "evidence")
	if err != nil {
		t.Fatal(err)
	}
	seed := contractSeed("evidence")
	if err := session.Initialize(context.Background(), seed); err != nil {
		t.Fatal(err)
	}
	old := seed
	tx := newRecordTransaction(old.Revision)
	if err := tx.putAttempt("attempt", nil, AttemptRecord{Parent: old.RootScope, NodeID: "node", Index: 1, Limit: 1, State: AttemptActive}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Apply(context.Background(), ChangeBatch{changeHeader(old), tx}); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	state, err := store.InspectStorage(context.Background(), "evidence")
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.MaterializeHistoryEvidence(context.Background(), HistoryEvidenceMaterializeRequest{RunID: "evidence", ExpectedGeneration: state.Generation, Bounds: HistoryEvidenceBounds{MaxBatches: 10, MaxBytes: 1 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation != state.Generation+1 || got.HeadRevision != 2 || got.BaseRevision != 2 {
		t.Fatalf("unexpected materialized state: %+v", got)
	}
	loadedSession, err := store.Acquire(context.Background(), "evidence")
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := loadedSession.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := loadedSession.Close(); err != nil {
		t.Fatal(err)
	}
	if !hasCompleteSemanticEvidence(loaded) || loaded.Revision != 2 {
		t.Fatalf("materialized evidence is incomplete: %+v", loaded)
	}
	if err := ValidateSemanticEvidence(loaded); err != nil {
		t.Fatal(err)
	}
	if _, err := PlanHistoryCompression(loaded, HistoryCompressionRequest{RunID: loaded.RunID, ThroughRevision: loaded.Revision, MaxObjects: 100, MaxBytes: 1 << 20}); !errors.Is(err, ErrUnsupportedCapability) {
		// history_evidence has no deletion semantics; planning must stop rather than
		// accidentally treating the materialization marker as a compression candidate.
		t.Fatalf("materialization marker unexpectedly planned: %v", err)
	}
}

func TestMemoryHistoryEvidenceMaterializationRejectsMaxGeneration(t *testing.T) {
	store := NewMemoryStore()
	session, err := store.Acquire(context.Background(), "max-generation")
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Initialize(context.Background(), contractSeed("max-generation")); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.history["max-generation"].generation = ^uint64(0)
	store.mu.Unlock()
	state, err := store.InspectStorage(context.Background(), "max-generation")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MaterializeHistoryEvidence(context.Background(), HistoryEvidenceMaterializeRequest{RunID: "max-generation", ExpectedGeneration: state.Generation, Bounds: HistoryEvidenceBounds{MaxBatches: 10, MaxBytes: 1 << 20}}); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("max generation was not rejected: %v", err)
	}
}
func TestMemoryHistoryEvidenceMaterializationRejectsActiveSessionAndStaleGeneration(t *testing.T) {
	store := NewMemoryStore()
	session, err := store.Acquire(context.Background(), "busy")
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Initialize(context.Background(), contractSeed("busy")); err != nil {
		t.Fatal(err)
	}
	state, err := store.InspectStorage(context.Background(), "busy")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MaterializeHistoryEvidence(context.Background(), HistoryEvidenceMaterializeRequest{RunID: "busy", ExpectedGeneration: state.Generation, Bounds: HistoryEvidenceBounds{MaxBatches: 10, MaxBytes: 1 << 20}}); !errors.Is(err, ErrBusy) {
		t.Fatalf("active session was not rejected: %v", err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MaterializeHistoryEvidence(context.Background(), HistoryEvidenceMaterializeRequest{RunID: "busy", ExpectedGeneration: state.Generation + 1, Bounds: HistoryEvidenceBounds{MaxBatches: 10, MaxBytes: 1 << 20}}); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale generation was not rejected: %v", err)
	}
}
