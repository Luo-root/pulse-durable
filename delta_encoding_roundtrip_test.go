package durable

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/Luo-root/pulse"
)

// This small synthetic test remains useful for protocol boundary values; it is
// intentionally named as such and is not the Runtime acceptance test below.
func TestChangeBatchWireSyntheticFiveHundredRoundTrips(t *testing.T) {
	for i := 0; i < 500; i++ {
		tx := newRecordTransaction(uint64(i))
		value := AttemptRecord{Parent: "p", NodeID: "n", Index: 1, Limit: 1, State: AttemptActive}
		if err := tx.putAttempt("a", nil, value); err != nil {
			t.Fatal(err)
		}
		b := ChangeBatch{header: ChangeHeader{BatchFormat: 1, LogicalFormat: 2, RunID: "r", BusinessVersion: "v", DefinitionDigest: "d", InputsDigest: "i", RootScope: "r", ExpectedRevision: uint64(i), Revision: uint64(i + 1)}, tx: tx}
		wire, err := MarshalChangeBatch(b)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := UnmarshalChangeBatch(wire)
		if err != nil {
			t.Fatal(err)
		}
		again, err := MarshalChangeBatch(decoded)
		if err != nil {
			t.Fatal(err)
		}
		if string(wire) != string(again) || decoded.Header() != b.Header() || decoded.OperationCount() != b.OperationCount() {
			t.Fatalf("round %d changed", i)
		}
	}
}

type runtimeWireStore struct {
	records    map[string]RunRecord
	wireBytes  [][]byte
	batchCount int
	applyCount int
	lastError  error
}

type runtimeWireSession struct {
	owner *runtimeWireStore
	runID string
}

func (s *runtimeWireStore) Acquire(_ context.Context, runID string) (Session, error) {
	return &runtimeWireSession{owner: s, runID: runID}, nil
}
func (s *runtimeWireSession) Close() error { return nil }
func (s *runtimeWireSession) Load(_ context.Context) (RunRecord, error) {
	r, ok := s.owner.records[s.runID]
	if !ok {
		return RunRecord{}, ErrNotFound
	}
	return cloneRecord(r), nil
}
func (s *runtimeWireSession) Save(_ context.Context, record RunRecord, revision uint64) error {
	old, ok := s.owner.records[s.runID]
	if !ok {
		if revision != 0 || record.Revision != 1 {
			return ErrConflict
		}
	} else if revision != old.Revision || record.Revision != revision+1 {
		return ErrConflict
	}
	s.owner.records[s.runID] = cloneRecord(record)
	return nil
}
func (s *runtimeWireSession) Apply(ctx context.Context, batch ChangeBatch) (CommitAck, error) {
	owner := s.owner
	defer func() {
		if p := recover(); p != nil {
			owner.lastError = fmt.Errorf("wire test Apply panic: %v", p)
			panic(p)
		}
	}()
	wire, err := MarshalChangeBatch(batch)
	if err != nil {
		owner.lastError = fmt.Errorf("marshal runtime batch: %w", err)
		return CommitAck{}, owner.lastError
	}
	decoded, err := UnmarshalChangeBatch(wire)
	if err != nil {
		owner.lastError = fmt.Errorf("unmarshal runtime batch at revision %d: %w", batch.Header().ExpectedRevision, err)
		return CommitAck{}, owner.lastError
	}
	again, err := MarshalChangeBatch(decoded)
	if err != nil {
		return CommitAck{}, fmt.Errorf("remarshal runtime batch: %w", err)
	}
	if string(wire) != string(again) {
		owner.lastError = fmt.Errorf("runtime batch canonical bytes changed")
		return CommitAck{}, owner.lastError
	}
	before := cloneRecord(owner.records[s.runID])
	originalProjection := cloneRecord(before)
	if _, err := batch.ApplyTo(&originalProjection); err != nil {
		owner.lastError = fmt.Errorf("original ApplyTo: %w", err)
		return CommitAck{}, owner.lastError
	}
	decodedProjection := cloneRecord(before)
	if _, err := decoded.ApplyTo(&decodedProjection); err != nil {
		owner.lastError = fmt.Errorf("decoded ApplyTo at revision %d: %w", batch.Header().ExpectedRevision, err)
		return CommitAck{}, owner.lastError
	}
	if !reflect.DeepEqual(originalProjection, decodedProjection) {
		owner.lastError = fmt.Errorf("original/decoded ApplyTo projection mismatch at revision %d", before.Revision)
		return CommitAck{}, owner.lastError
	}
	committed := cloneRecord(owner.records[s.runID])
	ack, err := decoded.ApplyTo(&committed)
	if err != nil {
		return CommitAck{}, err
	}
	owner.records[s.runID] = committed
	owner.wireBytes = append(owner.wireBytes, append([]byte(nil), wire...))
	owner.batchCount++
	owner.applyCount++
	return ack, nil
}

func TestChangeBatchWireRealRuntimeManagedFiveHundredRoundsAndResume(t *testing.T) {
	store := &runtimeWireStore{records: make(map[string]RunRecord)}
	total := 0
	graph := deltaManagedGraph(t, 500, &total)
	runtime, err := New(store, NewCodecs())
	if err != nil {
		t.Fatal(err)
	}
	opts := testOptions("wire-runtime-500")
	opts.RequireDelta = true
	if _, err := runtime.Run(t.Context(), graph, pulse.Inputs{}, opts); err != nil {
		if store.lastError != nil {
			t.Fatal(store.lastError)
		}
		t.Fatal(err)
	}
	if total != 125250 {
		t.Fatalf("runtime sum=%d, want 125250", total)
	}
	if store.batchCount < 1500 || store.applyCount != store.batchCount {
		t.Fatalf("runtime batches=%d apply calls=%d, want >=1500 equal calls", store.batchCount, store.applyCount)
	}
	for i, wire := range store.wireBytes {
		decoded, err := UnmarshalChangeBatch(wire)
		if err != nil {
			t.Fatalf("batch %d decode: %v", i, err)
		}
		again, err := MarshalChangeBatch(decoded)
		if err != nil {
			t.Fatalf("batch %d remarshal: %v", i, err)
		}
		if string(wire) != string(again) {
			t.Fatalf("batch %d bytes not stable", i)
		}
	}
	// A sentinel detects even a re-execution that recomputes the same final sum.
	total = -1
	beforeResumeTotal := total
	if _, err := runtime.Resume(t.Context(), graph, pulse.Inputs{}, opts); err != nil {
		t.Fatal(err)
	}
	if total != beforeResumeTotal {
		t.Fatalf("Resume re-executed business: sum changed from %d to %d", beforeResumeTotal, total)
	}
	// Resume may checkpoint source-generation metadata; the sentinel above checks
	// business execution independently of those legitimate recovery batches.
	// Confirm the persisted result is an independently decodable format-2 record.
	record := store.records["wire-runtime-500"]
	if record.Format != RecordFormat || record.Revision == 0 {
		t.Fatalf("persisted record format/revision: %d/%d", record.Format, record.Revision)
	}
	if data, err := json.Marshal(record); err != nil || len(data) == 0 {
		t.Fatalf("persisted record encoding failed: %v", err)
	}
}
