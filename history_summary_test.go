package durable

import (
	"errors"
	"testing"
)

func testSummaryRecord() RunRecord {
	return RunRecord{Format: RecordFormat, RunID: "run", Revision: 7,
		Attempts: map[string]AttemptRecord{
			"a1": {Parent: "parent", NodeID: "node", Boundary: "b", Index: 1, Limit: 3, State: AttemptFailed},
			"a2": {Parent: "parent", NodeID: "node", Boundary: "b", Index: 2, Limit: 3, State: AttemptFailed},
		},
		Streams: map[string]StreamRecord{}, Messages: map[string]MessageAttemptRecord{},
	}
}

func TestRetryPrefixHighWaterMarkAndIsolation(t *testing.T) {
	r := testSummaryRecord()
	got, err := r.RetryPrefix("parent", "node", "b")
	if err != nil {
		t.Fatal(err)
	}
	if got.PrunedThroughIndex != 2 || got.FailedCount != 2 || got.Limit != 3 || got.NextIndex != 3 {
		t.Fatalf("summary=%+v", got)
	}
	if got.LastPrunedDigest == "" || !validSummaryDigest(got.LastPrunedDigest) || !validSummaryDigest(got.IdentityDigest) {
		t.Fatalf("bad digest=%+v", got)
	}
	if r.Attempts["a1"].State != AttemptFailed {
		t.Fatal("record was mutated")
	}
}

func TestRetryPrefixStopsBeforeLaterActiveAttempt(t *testing.T) {
	r := testSummaryRecord()
	r.Attempts["a3"] = AttemptRecord{Parent: "parent", NodeID: "node", Boundary: "b", Index: 3, Limit: 3, State: AttemptActive}
	got, err := r.RetryPrefix("parent", "node", "b")
	if err != nil || got.PrunedThroughIndex != 2 || got.NextIndex != 3 {
		t.Fatalf("summary=%+v err=%v", got, err)
	}
}

func TestRetryPrefixRejectsGapStateAndLimit(t *testing.T) {
	cases := []func(*RunRecord){
		func(r *RunRecord) {
			r.Attempts["a3"] = AttemptRecord{Parent: "parent", NodeID: "node", Boundary: "b", Index: 3, Limit: 3, State: AttemptFailed}
			delete(r.Attempts, "a2")
		},
		func(r *RunRecord) { a := r.Attempts["a2"]; a.State = AttemptState("unknown"); r.Attempts["a2"] = a },
		func(r *RunRecord) { a := r.Attempts["a2"]; a.Limit = 4; r.Attempts["a2"] = a },
	}
	for _, mutate := range cases {
		r := testSummaryRecord()
		mutate(&r)
		if _, err := r.RetryPrefix("parent", "node", "b"); !errors.Is(err, ErrInvalid) {
			t.Fatalf("err=%v", err)
		}
	}
}

func TestMessagePrefixBoundaries(t *testing.T) {
	r := testSummaryRecord()
	in := StreamIDRecord{Scope: "s", NodeID: "source", Key: "items", Group: "g", Generation: 1}
	r.Streams[streamKey(in)] = StreamRecord{Stream: StreamDescriptorRecord{ID: in}, AckedThrough: 0}
	r.Messages["m1"] = MessageAttemptRecord{Scope: "scope", Parent: "proc", NodeID: "consumer", Input: in, Sequence: 1, Index: 1, Limit: 2, State: AttemptConfirmed}
	got, err := r.MessagePrefix("scope", "proc", "consumer", in, 1)
	if err != nil {
		t.Fatal(err)
	}
	// 前缀之后的未来消息仍可 pending，不应阻断当前已结算前缀。
	r.Streams[streamKey(in)] = StreamRecord{Stream: StreamDescriptorRecord{ID: in}, Pending: []StreamMessageRecord{{Sequence: 2}}}
	if _, err := r.MessagePrefix("scope", "proc", "consumer", in, 1); err != nil {
		t.Fatal("future pending changed settled prefix", err)
	}
	if got.SettledThroughSequence != 1 || !validSummaryDigest(got.FinalizedBudgetDigest) {
		t.Fatalf("summary=%+v", got)
	}
	if _, err := r.MessagePrefix("scope", "proc", "consumer", in, 0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero sequence: %v", err)
	}
	r.Streams[streamKey(in)] = StreamRecord{Stream: StreamDescriptorRecord{ID: in}, AckedThrough: 1}
	if _, err := r.MessagePrefix("scope", "proc", "consumer", in, 1); !errors.Is(err, ErrConflict) {
		t.Fatalf("old message: %v", err)
	}
}

func TestMessagePrefixRejectsUnsettledAndUnknown(t *testing.T) {
	r := testSummaryRecord()
	in := StreamIDRecord{Scope: "s", NodeID: "source", Key: "items", Group: "g", Generation: 1}
	r.Streams[streamKey(in)] = StreamRecord{Stream: StreamDescriptorRecord{ID: in}, Pending: []StreamMessageRecord{{Sequence: 1}}}
	r.Messages["m1"] = MessageAttemptRecord{Scope: "scope", Parent: "proc", NodeID: "consumer", Input: in, Sequence: 1, Index: 1, Limit: 1, State: AttemptActive}
	if _, err := r.MessagePrefix("scope", "proc", "consumer", in, 1); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unsettled: %v", err)
	}
	delete(r.Messages, "m1")
	r.Streams[streamKey(in)] = StreamRecord{Stream: StreamDescriptorRecord{ID: in}}
	if _, err := r.MessagePrefix("scope", "proc", "consumer", in, 1); !errors.Is(err, ErrUnknown) {
		t.Fatalf("unknown: %v", err)
	}
}
