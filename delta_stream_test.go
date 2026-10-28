package durable

import (
	"github.com/Luo-root/pulse"
	"reflect"
	"testing"
)

func TestDeltaSameStreamMultipleOperationsAndIsolation(t *testing.T) {
	s, legacy := transactionState()
	s.record.Streams = map[string]StreamRecord{"stream": {Pending: []StreamMessageRecord{
		{Sequence: 1, Value: ValueRecord{Data: []byte("one")}},
		{Sequence: 2, Value: ValueRecord{Data: []byte("two")}},
		{Sequence: 3, Value: ValueRecord{Data: []byte("three")}},
	}, LastSequence: 3}}
	// Capture a real older Save parameter, then select the next execution path.
	legacy.saved = cloneRecord(s.record)
	saved := cloneRecord(legacy.saved)
	authority := &deltaTestStore{record: cloneRecord(s.record)}
	s.session = &deltaTestSession{authority}
	before := cloneRecord(s.record)
	tx := newRecordTransaction(s.record.Revision)
	h := headerOf(s.record.Streams["stream"])
	next := h
	next.LastSequence = 5
	next.AckedThrough = 1
	if err := tx.putStream("stream", &h, next); err != nil {
		t.Fatal(err)
	}
	for _, seq := range []uint64{5, 1, 2, 4} {
		old := streamView{tx: tx, base: s.record, key: "stream"}.pending(seq)
		var value *StreamMessageRecord
		if seq != 1 {
			value = &StreamMessageRecord{Sequence: seq, Value: ValueRecord{Data: []byte("new")}}
		}
		if err := tx.putPending("stream", seq, old, value); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.commitRecordTransaction(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	p := s.record.Streams["stream"].Pending
	if len(p) != 4 || p[0].Sequence != 2 || p[1].Sequence != 3 || p[2].Sequence != 4 || p[3].Sequence != 5 {
		t.Fatalf("wrong pending order: %+v", p)
	}
	if !reflect.DeepEqual(saved, legacy.saved) {
		t.Fatal("delta mutated old Save argument")
	}
	batch := authority.batches[0]
	// Applying a retained batch to another authority must never expose its tables.
	independent := cloneRecord(before)
	if _, err := batch.ApplyTo(&independent); err != nil {
		t.Fatal(err)
	}
	independent.Streams["stream"].Pending[0].Value.Data[0] = 'X'
	authority.record.Streams["stream"].Pending[0].Value.Data[0] = 'Y'
	tx.pending[pendingKey{"stream", 2}].value.Value.Data[0] = 'Z'
	if string(s.record.Streams["stream"].Pending[0].Value.Data) != "new" {
		t.Fatal("backend/tx mutation reached runtime")
	}
	again := cloneRecord(before)
	if _, err := batch.ApplyTo(&again); err != nil {
		t.Fatal(err)
	}
	if string(again.Streams["stream"].Pending[0].Value.Data) != "new" {
		t.Fatal("record mutation reached retained batch")
	}
	// Wrong identity and stale revision fail without changing any object.
	for _, mutate := range []func(*RunRecord){func(r *RunRecord) { r.RunID = "other" }, func(r *RunRecord) { r.Revision++ }, func(r *RunRecord) { r.Version = "other" }} {
		bad := cloneRecord(before)
		mutate(&bad)
		unchanged := cloneRecord(bad)
		if _, err := batch.ApplyTo(&bad); err == nil || !reflect.DeepEqual(bad, unchanged) {
			t.Fatal("invalid header applied")
		}
	}
}

func TestDeltaIsMandatoryWithoutOptions(t *testing.T) {
	// Every Session uses one seed followed by genuine typed Apply calls.
	o := &deltaTestStore{}
	total := 0
	g := deltaManagedGraph(t, 10, &total)
	r, err := New(o, NewCodecs())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.Run(t.Context(), g, pulse.Inputs{}, testOptions("legacy-explicit")); err != nil {
		t.Fatal(err)
	}
	if o.calls < 10 || len(o.seeds) != 1 || total != 55 {
		t.Fatal("runtime did not use mandatory delta")
	}
}
