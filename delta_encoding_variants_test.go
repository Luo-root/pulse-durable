package durable

import (
	"reflect"
	"testing"
)

func TestChangeBatchWireAllOperationVariants(t *testing.T) {
	tx := newRecordTransaction(1)
	attempt := AttemptRecord{Parent: "root", NodeID: "n", Index: 1, Limit: 1, State: AttemptActive}
	if err := tx.putAttempt("a", nil, attempt); err != nil {
		t.Fatal(err)
	}
	if err := tx.createScope("child", ScopeRecord{Graph: "g", Path: "p", Inputs: "i"}); err != nil {
		t.Fatal(err)
	}
	node := NodeRecord{NodeID: "n", Attempt: "a", Reason: "completed", Outputs: []ValueRecord{{Name: "v", State: "ready", Data: []byte{}}}}
	if err := tx.putNode("child", "n", nil, node); err != nil {
		t.Fatal(err)
	}
	h := streamHeader{Stream: StreamDescriptorRecord{ID: StreamIDRecord{Scope: "child", NodeID: "n", Key: "k", Group: "g", Generation: 3}, Graph: "g", Path: "p", Attempt: "a", TypeID: "t", Version: "1"}, LastSequence: 9, AckedThrough: 2, Cursor: &ValueRecord{Name: "c", State: "ready", Data: []byte{}}}
	if err := tx.putStream("s", nil, h); err != nil {
		t.Fatal(err)
	}
	if err := tx.putPending("s", 3, nil, &StreamMessageRecord{Sequence: 3, Value: ValueRecord{Name: "v", State: "ready", Data: []byte{}}}); err != nil {
		t.Fatal(err)
	}
	msg := MessageAttemptRecord{Scope: "child", Parent: "a", NodeID: "n", Input: h.Stream.ID, Sequence: 3, Index: 1, Limit: 1, State: AttemptActive}
	if err := tx.putMessage("m", nil, msg); err != nil {
		t.Fatal(err)
	}
	mh := managedHeader{Scope: "child", Parent: "a", NodeID: "n"}
	if err := tx.putManaged("mng", nil, mh); err != nil {
		t.Fatal(err)
	}
	if err := tx.putManagedEntry(managedEntryKey{"mng", "x", false}, nil, &ValueRecord{Name: "x", State: "ready", Data: []byte{}}); err != nil {
		t.Fatal(err)
	}
	b := ChangeBatch{header: ChangeHeader{BatchFormat: 1, LogicalFormat: 2, RunID: "r", BusinessVersion: "v", DefinitionDigest: "d", InputsDigest: "i", RootScope: "root", ExpectedRevision: 1, Revision: 2}, tx: tx}
	wire, err := MarshalChangeBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalChangeBatch(wire)
	if err != nil {
		t.Fatal(err)
	}
	wire2, err := MarshalChangeBatch(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if string(wire) != string(wire2) {
		t.Fatal("variant wire not canonical after roundtrip")
	}
	// Apply against an exact precondition-compatible record, then compare the logical result.
	record := RunRecord{Format: 2, RunID: "r", Revision: 1, Version: "v", Definition: "d", Inputs: "i", RootScope: "root", Scopes: map[string]ScopeRecord{"root": {Nodes: map[string]NodeRecord{}}}, Attempts: map[string]AttemptRecord{}}
	if _, err := decoded.ApplyTo(&record); err != nil {
		t.Fatalf("decoded variants did not apply: %v", err)
	}
	if record.Revision != 2 || len(record.Scopes) != 2 || len(record.Streams) != 1 || len(record.Managed) != 1 {
		t.Fatalf("unexpected applied record: %+v", record)
	}
	stale := cloneRecord(record)
	stale.Revision = 1
	before := cloneRecord(stale)
	if _, err := decoded.ApplyTo(&stale); err == nil {
		t.Fatal("stale ApplyTo accepted")
	}
	if !reflect.DeepEqual(stale, before) {
		t.Fatal("failed ApplyTo modified record")
	}
}
