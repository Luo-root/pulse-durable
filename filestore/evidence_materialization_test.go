package filestore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
)

func TestHistoryEvidenceMaterializationPublishesAndReopens(t *testing.T) {
	store := newTestStore(t)
	id := "evidence-file"
	session := acquireTestSession(t, store, id)
	seed := durable.RunRecord{RunID: id, Revision: 1, Version: "v1", Definition: digestText("definition"), Inputs: digestText("inputs"), RootScope: "root", Scopes: map[string]durable.ScopeRecord{"root": {Graph: "graph", Inputs: digestText("inputs"), Nodes: map[string]durable.NodeRecord{}}}, Attempts: map[string]durable.AttemptRecord{}}
	if err := session.Initialize(context.Background(), seed); err != nil {
		t.Fatal(err)
	}
	old := seed
	batch := revisionBatch(t, old)
	if _, err := session.Apply(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	state, err := store.InspectStorage(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.MaterializeHistoryEvidence(context.Background(), durable.HistoryEvidenceMaterializeRequest{RunID: id, ExpectedGeneration: state.Generation, Bounds: durable.HistoryEvidenceBounds{MaxBatches: 10, MaxBytes: 1 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation != state.Generation+1 || got.HeadRevision != 2 || got.BaseRevision != 2 {
		t.Fatalf("unexpected materialized state: %+v", got)
	}
	check := acquireTestSession(t, store, id)
	loaded, err := check.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !hasCompleteEvidenceForTest(loaded) || loaded.Revision != 2 {
		t.Fatalf("reopened evidence incomplete: %+v", loaded)
	}
}

func digestText(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func hasCompleteEvidenceForTest(r durable.RunRecord) bool {
	return r.HistorySummaries != nil && r.CommitIndex != nil && r.Provenance != nil
}

func TestHistoryEvidenceMaterializationRejectsRevisionOne(t *testing.T) {
	store := newTestStore(t)
	id := "evidence-file-revision-one"
	session := acquireTestSession(t, store, id)
	seed := durable.RunRecord{RunID: id, Revision: 1, Version: "v1", Definition: digestText("definition"), Inputs: digestText("inputs"), RootScope: "root", Scopes: map[string]durable.ScopeRecord{"root": {Graph: "graph", Inputs: digestText("inputs"), Nodes: map[string]durable.NodeRecord{}}}, Attempts: map[string]durable.AttemptRecord{}}
	if err := session.Initialize(context.Background(), seed); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	state, err := store.InspectStorage(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MaterializeHistoryEvidence(context.Background(), durable.HistoryEvidenceMaterializeRequest{RunID: id, ExpectedGeneration: state.Generation, Bounds: durable.HistoryEvidenceBounds{MaxBatches: 10, MaxBytes: 1 << 20}}); !errors.Is(err, durable.ErrUnsupportedCapability) {
		t.Fatalf("revision-one materialization was not rejected: %v", err)
	}
	again, err := store.InspectStorage(context.Background(), id)
	if err != nil || again != state {
		t.Fatalf("rejected revision-one materialization changed state: before=%+v after=%+v err=%v", state, again, err)
	}
}
