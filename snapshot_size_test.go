package durable

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/Luo-root/pulse-durable/internal/snapshotfmt"
)

func checkSnapshotDelta(t *testing.T, before RunRecord, tx *recordTransaction) RunRecord {
	t.Helper()
	batch := ChangeBatch{header: changeHeader(before), tx: tx}
	old, err := snapshotfmt.Encode(before)
	if err != nil {
		t.Fatal(err)
	}
	untouched := cloneRecord(before)
	delta, err := batch.SnapshotSizeDelta(before)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, untouched) {
		t.Fatal("size query changed record")
	}
	after := cloneRecord(before)
	if _, err := batch.ApplyTo(&after); err != nil {
		t.Fatal(err)
	}
	next, err := snapshotfmt.Encode(after)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(len(next) - len(old)); delta != want {
		t.Fatalf("delta=%d want=%d\nbefore=%s\nafter=%s", delta, want, old, next)
	}
	full, err := snapshotfmt.RecordSize(after)
	if err != nil || full != int64(len(next)) {
		t.Fatalf("full count=%d bytes=%d error=%v", full, len(next), err)
	}
	return after
}

func TestSnapshotSizeDeltaAllOperationsAndShapes(t *testing.T) {
	for mask := 0; mask < 32; mask++ {
		for _, rev := range []uint64{1, 9, 99, 999, math.MaxUint64 - 1} {
			t.Run(fmt.Sprintf("maps-%d-revision-%d", mask, rev), func(t *testing.T) {
				before := RunRecord{RunID: "size", Revision: rev, RootScope: "root"}
				if mask&1 != 0 {
					before.Scopes = map[string]ScopeRecord{}
				}
				if mask&2 != 0 {
					before.Attempts = map[string]AttemptRecord{}
				}
				if mask&4 != 0 {
					before.Streams = map[string]StreamRecord{}
				}
				if mask&8 != 0 {
					before.Messages = map[string]MessageAttemptRecord{}
				}
				if mask&16 != 0 {
					before.Managed = map[string]ManagedRecord{}
				}
				tx := newRecordTransaction(rev)
				key := "\"\\\n\x00<>&\u2028\u2029中文😀"
				value := ValueRecord{Name: key, Data: []byte{0, 255, 1}, Stream: &StreamDescriptorRecord{ID: StreamIDRecord{Generation: math.MaxUint64}}}
				must := func(err error) {
					t.Helper()
					if err != nil {
						t.Fatal(err)
					}
				}
				must(tx.createScope("root", ScopeRecord{Graph: "graph"}))
				must(tx.putNode("root", key, nil, NodeRecord{NodeID: key, Outputs: []ValueRecord{value}}))
				must(tx.putNode("root", "second", nil, NodeRecord{Outputs: []ValueRecord{}}))
				must(tx.putAttempt(key, nil, AttemptRecord{Index: 1, Limit: 2, State: AttemptActive}))
				must(tx.putStream("s", nil, streamHeader{Stream: StreamDescriptorRecord{ID: StreamIDRecord{Generation: 1}}, Cursor: &value}))
				must(tx.putPending("s", 1, nil, &StreamMessageRecord{Sequence: 1, Value: value}))
				must(tx.putPending("s", 2, nil, &StreamMessageRecord{Sequence: 2, Value: ValueRecord{Data: []byte{}}}))
				must(tx.putMessage(key, nil, MessageAttemptRecord{Index: 1, Limit: 2, State: AttemptActive}))
				must(tx.putManaged("m", nil, managedHeader{Scope: "root", NodeID: key}))
				must(tx.putManagedEntry(managedEntryKey{"m", key, true}, nil, &value))
				plain := ValueRecord{Name: "other", Data: nil}
				must(tx.putManagedEntry(managedEntryKey{"m", "other", false}, nil, &plain))
				after := checkSnapshotDelta(t, before, tx)
				if rev == math.MaxUint64-1 {
					return
				}
				tx = newRecordTransaction(after.Revision)
				a := after.Attempts[key]
				av := a
				av.State = AttemptFailed
				must(tx.putAttempt(key, &a, av))
				n := after.Scopes["root"].Nodes[key]
				nv := n
				nv.Outputs = []ValueRecord{}
				must(tx.putNode("root", key, &n, nv))
				h := headerOf(after.Streams["s"])
				hv := h
				hv.Cursor = nil
				hv.AckedThrough = 2
				must(tx.putStream("s", &h, hv))
				for _, p := range after.Streams["s"].Pending {
					must(tx.putPending("s", p.Sequence, &p, nil))
				}
				m := after.Messages[key]
				mv := m
				mv.State = AttemptFailed
				must(tx.putMessage(key, &m, mv))
				mh := managedHeaderOf(after.Managed["m"])
				must(tx.putManaged("m", &mh, mh))
				must(tx.putManagedEntry(managedEntryKey{"m", key, true}, &value, nil))
				must(tx.putManagedEntry(managedEntryKey{"m", "other", false}, &plain, nil))
				final := checkSnapshotDelta(t, after, tx)
				if final.Streams["s"].Pending == nil || final.Managed["m"].State != nil || final.Managed["m"].Values != nil {
					t.Fatal("container shape changed")
				}
			})
		}
	}
}

func TestSnapshotSizeDeltaReplacesAndCoalesces(t *testing.T) {
	for _, n := range []int{0, 1, 2, 3, 4, 17, 1024} {
		old := ValueRecord{Name: "x", Data: bytes.Repeat([]byte{0xff}, n)}
		before := RunRecord{RunID: "replace", Revision: 9, RootScope: "root", Scopes: map[string]ScopeRecord{"root": {Nodes: nil}}, Streams: map[string]StreamRecord{"s": {Pending: []StreamMessageRecord{{Sequence: 1, Value: old}}}}, Managed: map[string]ManagedRecord{"m": {State: []ValueRecord{old}, Values: []ValueRecord{}}}}
		tx := newRecordTransaction(before.Revision)
		next := old
		next.Data = []byte{}
		if err := tx.putManagedEntry(managedEntryKey{"m", "x", true}, &old, &next); err != nil {
			t.Fatal(err)
		}
		next.Data = nil
		if err := tx.putManagedEntry(managedEntryKey{"m", "x", true}, &old, &next); err != nil {
			t.Fatal(err)
		}
		p := before.Streams["s"].Pending[0]
		pv := p
		pv.Value = next
		if err := tx.putPending("s", 1, &p, &pv); err != nil {
			t.Fatal(err)
		}
		if err := tx.putNode("root", "n", nil, NodeRecord{}); err != nil {
			t.Fatal(err)
		}
		checkSnapshotDelta(t, before, tx)
	}
}

func TestSnapshotSizeDeltaRejectsInvalidWithoutMutation(t *testing.T) {
	before := RunRecord{RunID: "reject", Revision: 1, RootScope: "root"}
	for _, mode := range []string{"zero", "header", "precondition", "overflow", "bad-key", "bad-value"} {
		t.Run(mode, func(t *testing.T) {
			r := cloneRecord(before)
			tx := newRecordTransaction(r.Revision)
			_ = tx.putAttempt("a", nil, AttemptRecord{State: AttemptActive})
			batch := ChangeBatch{header: changeHeader(r), tx: tx}
			switch mode {
			case "zero":
				batch = ChangeBatch{}
			case "header":
				batch.header.RunID = "other"
			case "precondition":
				r.Attempts = map[string]AttemptRecord{"a": {}}
			case "overflow":
				r.Revision = math.MaxUint64
			case "bad-key":
				delete(tx.attempts, "a")
				_ = tx.putAttempt(string([]byte{0xff}), nil, AttemptRecord{})
			case "bad-value":
				tx.attempts["a"] = putAttempt{absent: true, value: AttemptRecord{NodeID: string([]byte{0xff})}}
			}
			copy := cloneRecord(r)
			if _, err := batch.SnapshotSizeDelta(r); err == nil {
				t.Fatal("invalid batch accepted")
			} else if mode == "bad-key" || mode == "bad-value" {
				if !errors.Is(err, ErrInvalidRecord) {
					t.Fatal(err)
				}
			}
			if !reflect.DeepEqual(r, copy) {
				t.Fatal("rejected query changed record")
			}
		})
	}
}

func BenchmarkSnapshotSizeDeltaUnrelatedHistory(b *testing.B) {
	for _, n := range []int{0, 1024, 16384} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			r := RunRecord{RunID: "benchmark", Revision: 9, RootScope: "root", Messages: make(map[string]MessageAttemptRecord, n)}
			for i := 0; i < n; i++ {
				r.Messages[fmt.Sprint(i)] = MessageAttemptRecord{Index: 1, Limit: 2}
			}
			tx := newRecordTransaction(r.Revision)
			_ = tx.putAttempt("new", nil, AttemptRecord{Index: 1, Limit: 1, State: AttemptActive})
			batch := ChangeBatch{header: changeHeader(r), tx: tx}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := batch.SnapshotSizeDelta(r); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
