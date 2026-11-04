package durable

import (
	"errors"
	"fmt"
	"maps"
	"reflect"
	"testing"

	"github.com/Luo-root/pulse"
)

func messageIndexFixture(t *testing.T, count int) (*runState, *deltaTestStore) {
	t.Helper()
	s, _ := transactionState()
	s.record.Messages = make(map[string]MessageAttemptRecord, count)
	for i := range count {
		s.record.Messages[fmt.Sprintf("message-%d", i)] = MessageAttemptRecord{
			Scope: "root", Parent: "old", NodeID: "work",
			Input:    StreamIDRecord{Scope: "root", NodeID: "source", Key: "items", Group: "default", Generation: 1},
			Sequence: uint64(i + 1), Index: 1, Limit: 2, State: AttemptActive,
		}
	}
	o := &deltaTestStore{record: cloneRecord(s.record), benchmark: true}
	s.session = &deltaTestSession{o}
	return s, o
}

func TestMessageIndexRebuildAndGroupIsolation(t *testing.T) {
	first := MessageAttemptRecord{Scope: "scope", Parent: "parent", NodeID: "node", Input: StreamIDRecord{
		Scope: "source", NodeID: "producer", Key: "items", Group: "g", Generation: 1}, Sequence: 3, Index: 1, Limit: 2, State: AttemptFailed}
	second := first
	second.Index, second.State, second.Input.Generation = 2, AttemptActive, 2
	variants := map[string]func(*MessageAttemptRecord){
		"scope":        func(m *MessageAttemptRecord) { m.Scope += "-other" },
		"parent":       func(m *MessageAttemptRecord) { m.Parent += "-other" },
		"node":         func(m *MessageAttemptRecord) { m.NodeID += "-other" },
		"stream-scope": func(m *MessageAttemptRecord) { m.Input.Scope += "-other" },
		"stream-node":  func(m *MessageAttemptRecord) { m.Input.NodeID += "-other" },
		"stream-key":   func(m *MessageAttemptRecord) { m.Input.Key += "-other" },
		"stream-group": func(m *MessageAttemptRecord) { m.Input.Group += "-other" },
		"sequence":     func(m *MessageAttemptRecord) { m.Sequence++ },
	}
	s := &runState{record: RunRecord{Messages: map[string]MessageAttemptRecord{"first": first, "second": second}}}
	for id, change := range variants {
		message := first
		change(&message)
		s.record.Messages[id] = message
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.rebuildMessageIndex(); err != nil {
		t.Fatal(err)
	}
	if !s.messageIndexReady || len(s.messageIndex) != len(variants)+1 {
		t.Fatalf("ready=%v groups=%d", s.messageIndexReady, len(s.messageIndex))
	}
	id, got, err := s.indexedMessage(first)
	if err != nil || id != "second" || got != second {
		t.Fatalf("latest=%q %+v error=%v", id, got, err)
	}
	for id := range variants {
		message := s.record.Messages[id]
		gotID, got, err := s.indexedMessage(message)
		if err != nil || gotID != id || got != message {
			t.Fatalf("group %q selected %q %+v error=%v", id, gotID, got, err)
		}
	}
}

func TestMessageIndexLazyInitializationAndMissDoesNotRebuild(t *testing.T) {
	s, _ := messageIndexFixture(t, 512)
	s.mu.Lock()
	defer s.mu.Unlock()
	candidate := s.record.Messages["message-0"]
	id, _, err := s.indexedMessage(candidate)
	if err != nil || id != "message-0" || !s.messageIndexReady {
		t.Fatalf("lazy lookup=%q ready=%v error=%v", id, s.messageIndexReady, err)
	}
	indexID := fmt.Sprintf("%p", s.messageIndex)
	candidate.Sequence = 999
	allocs := testing.AllocsPerRun(5, func() {
		id, got, err := s.indexedMessage(candidate)
		if err != nil || id != "" || got != (MessageAttemptRecord{}) {
			t.Fatalf("miss=%q %+v error=%v", id, got, err)
		}
	})
	// 未命中是正常新消息路径，不能扫描并重新分配全部历史索引。
	if allocs != 0 || fmt.Sprintf("%p", s.messageIndex) != indexID || len(s.messageIndex) != 512 {
		t.Fatalf("miss rebuilt index: allocations=%g groups=%d", allocs, len(s.messageIndex))
	}
}

func TestMessageIndexNoMessageBatchDoesNotRebuild(t *testing.T) {
	for _, ready := range []bool{false, true} {
		t.Run(fmt.Sprintf("ready=%v", ready), func(t *testing.T) {
			s, o := messageIndexFixture(t, 512)
			s.mu.Lock()
			defer s.mu.Unlock()
			if ready {
				if err := s.rebuildMessageIndex(); err != nil {
					t.Fatal(err)
				}
			}
			indexID := fmt.Sprintf("%p", s.messageIndex)
			before := maps.Clone(s.messageIndex)
			if err := s.commitRecordTransaction(t.Context(), stageTransactionFailure(t, s)); err != nil {
				t.Fatal(err)
			}
			if o.calls != 1 || s.messageIndexReady != ready || fmt.Sprintf("%p", s.messageIndex) != indexID || !maps.Equal(before, s.messageIndex) {
				t.Fatalf("ordinary batch rebuilt index: calls=%d ready=%v", o.calls, s.messageIndexReady)
			}
		})
	}
}

func TestMessageIndexConfirmedBatchUpdatesMatchRebuild(t *testing.T) {
	s, o := messageIndexFixture(t, 8)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.rebuildMessageIndex(); err != nil {
		t.Fatal(err)
	}
	indexID := fmt.Sprintf("%p", s.messageIndex)
	unrelated := maps.Clone(s.messageIndex)
	first := s.record.Messages["message-0"]
	key := messageGroup(first)
	delete(unrelated, key)
	apply := func(tx *recordTransaction) {
		t.Helper()
		if err := s.commitRecordTransaction(t.Context(), tx); err != nil {
			t.Fatal(err)
		}
		if fmt.Sprintf("%p", s.messageIndex) != indexID {
			t.Fatal("message batch replaced the complete index")
		}
		for key, entry := range unrelated {
			if s.messageIndex[key] != entry {
				t.Fatal("message batch changed an unrelated group")
			}
		}
		rebuilt := &runState{record: s.record}
		rebuilt.mu.Lock()
		err := rebuilt.rebuildMessageIndex()
		rebuilt.mu.Unlock()
		if err != nil || !maps.Equal(rebuilt.messageIndex, s.messageIndex) {
			t.Fatalf("incremental/rebuild mismatch: %v", err)
		}
	}
	failed := first
	failed.State = AttemptFailed
	tx := newRecordTransaction(s.record.Revision)
	if err := tx.putMessage("message-0", &first, failed); err != nil {
		t.Fatal(err)
	}
	apply(tx)
	if _, got, err := s.indexedMessage(first); err != nil || got != failed {
		t.Fatalf("state was not read from Messages: %+v %v", got, err)
	}
	// 同一批可同时包含较旧和最新尝试，map 遍历顺序不得影响最终定位。
	latest := failed
	latest.Index, latest.State, latest.Input.Generation = 2, AttemptActive, 2
	tx = newRecordTransaction(s.record.Revision)
	if err := tx.putMessage("message-0", &failed, failed); err != nil {
		t.Fatal(err)
	}
	if err := tx.putMessage("retry", nil, latest); err != nil {
		t.Fatal(err)
	}
	fresh := first
	fresh.Sequence = 99
	if err := tx.putMessage("fresh", nil, fresh); err != nil {
		t.Fatal(err)
	}
	apply(tx)
	confirmed := latest
	confirmed.State = AttemptConfirmed
	tx = newRecordTransaction(s.record.Revision)
	if err := tx.putMessage("retry", &latest, confirmed); err != nil {
		t.Fatal(err)
	}
	apply(tx)
	id, got, err := s.indexedMessage(first)
	if err != nil || id != "retry" || got != confirmed || o.calls != 3 {
		t.Fatalf("latest=%q %+v calls=%d error=%v", id, got, o.calls, err)
	}
}

func TestMessageIndexUncertainApplyPublishesNothing(t *testing.T) {
	for _, ready := range []bool{false, true} {
		for _, mode := range []string{"error", "panic", "panic nil", "ack"} {
			t.Run(fmt.Sprintf("ready=%v/%s", ready, mode), func(t *testing.T) {
				s, o := messageIndexFixture(t, 4)
				s.mu.Lock()
				defer s.mu.Unlock()
				if ready {
					if err := s.rebuildMessageIndex(); err != nil {
						t.Fatal(err)
					}
				}
				before, index := cloneRecord(s.record), maps.Clone(s.messageIndex)
				indexID := fmt.Sprintf("%p", s.messageIndex)
				o.after = func(ChangeBatch) error {
					if !reflect.DeepEqual(before, s.record) || !maps.Equal(index, s.messageIndex) || s.messageIndexReady != ready {
						t.Fatal("runtime published before acknowledgement")
					}
					switch mode {
					case "panic":
						panic("apply")
					case "panic nil":
						panic(nil)
					case "ack":
						return nil
					default:
						return errors.New("unknown acknowledgement")
					}
				}
				if mode == "ack" {
					s.session = badAckSession{&deltaTestSession{o}}
				}
				old := s.record.Messages["message-0"]
				failed, retry := old, old
				failed.State = AttemptFailed
				retry.Index = 2
				tx := newRecordTransaction(s.record.Revision)
				if err := tx.putMessage("message-0", &old, failed); err != nil {
					t.Fatal(err)
				}
				if err := tx.putMessage("retry", nil, retry); err != nil {
					t.Fatal(err)
				}
				err := s.commitRecordTransaction(t.Context(), tx)
				if err == nil || s.stopped != err || !reflect.DeepEqual(before, s.record) || !maps.Equal(index, s.messageIndex) ||
					s.messageIndexReady != ready || fmt.Sprintf("%p", s.messageIndex) != indexID || reflect.DeepEqual(before, o.record) {
					t.Fatal("uncertain apply published runtime record/index or did not reach authority")
				}
				if again := s.commitRecordTransaction(t.Context(), tx); again != err || o.calls != 1 {
					t.Fatal("uncertain apply was retried")
				}
			})
		}
	}
}

func TestMessageIndexInconsistencyCannotReplenishBudget(t *testing.T) {
	for _, mode := range []string{"missing record", "wrong index", "wrong group", "nil index"} {
		t.Run(mode, func(t *testing.T) {
			p := newStreamProtocol(t)
			if err := p.append(1); err != nil {
				t.Fatal(err)
			}
			opened, err := p.s.openMessage(t.Context(), p.messageRequest(1, 2))
			if err != nil {
				t.Fatal(err)
			}
			message := p.s.record.Messages[opened.Attempt.Scope]
			key := messageGroup(message)
			entry := p.s.messageIndex[key]
			switch mode {
			case "missing record":
				entry.id = "missing"
			case "wrong index":
				entry.index++
			case "wrong group":
				entry.id = "unrelated"
				other := message
				other.Sequence++
				p.s.record.Messages[entry.id] = other
			case "nil index":
				p.s.messageIndex = nil
			}
			if p.s.messageIndex != nil {
				p.s.messageIndex[key] = entry
			}
			before := cloneRecord(p.s.record)
			o := &deltaTestStore{record: cloneRecord(before)}
			p.s.session = &deltaTestSession{o}
			if _, err := p.s.openMessage(t.Context(), p.messageRequest(1, 2)); !errors.Is(err, ErrConflict) || p.s.stopped != err || o.calls != 0 || !reflect.DeepEqual(before, p.s.record) {
				t.Fatalf("inconsistent index reopened a message: %v", err)
			}
		})
	}
}

func TestMessageIndexLimitAndExactPreconditions(t *testing.T) {
	s, o := messageIndexFixture(t, 2)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.rebuildMessageIndex(); err != nil {
		t.Fatal(err)
	}
	before := maps.Clone(s.messageIndex)
	old := s.record.Messages["message-0"]
	expected, next := old, old
	expected.Limit++
	next.State = AttemptFailed
	tx := newRecordTransaction(s.record.Revision)
	if err := tx.putMessage("message-0", &expected, next); err != nil {
		t.Fatal(err)
	}
	if err := s.commitRecordTransaction(t.Context(), tx); !errors.Is(err, ErrConflict) || o.calls != 0 || !maps.Equal(before, s.messageIndex) || s.record.Messages["message-0"] != old {
		t.Fatalf("message precondition weakened: %v", err)
	}
	// 首次重建也检查历史尝试的 Limit，不能仅采用最新尝试的预算。
	for _, firstLimit := range []int{1, 3} {
		first := old
		first.Limit, first.State = firstLimit, AttemptFailed
		latest := old
		latest.Index = 2
		manual := &runState{record: RunRecord{Messages: map[string]MessageAttemptRecord{"first": first, "latest": latest}}}
		manual.mu.Lock()
		_, _, err := manual.indexedMessage(latest)
		manual.mu.Unlock()
		if !errors.Is(err, ErrConflict) || manual.stopped != err || manual.messageIndexReady {
			t.Fatalf("inconsistent history limit accepted: %v", err)
		}
	}
}

func TestMessageIndexGenerationRebindRetainsActiveAttempt(t *testing.T) {
	p := newStreamProtocol(t)
	if err := p.append(1); err != nil {
		t.Fatal(err)
	}
	opened, err := p.s.openMessage(t.Context(), p.messageRequest(1, 2))
	if err != nil {
		t.Fatal(err)
	}
	resumed := &runState{session: p.s.session, def: p.s.def, record: cloneRecord(p.s.record)}
	if err := resumed.validateLoaded(); err != nil {
		t.Fatal(err)
	}
	resumed.mu.Lock()
	err = resumed.rebuildMessageIndex()
	resumed.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	indexID := fmt.Sprintf("%p", resumed.messageIndex)
	input, err := resumed.openStream(t.Context(), p.openRequest(false))
	if err != nil {
		t.Fatal(err)
	}
	p.s, p.input = resumed, input
	got, err := resumed.openMessage(t.Context(), p.messageRequest(1, 2))
	if err != nil || got.Attempt.Scope != opened.Attempt.Scope || got.Attempt.Index != opened.Attempt.Index || fmt.Sprintf("%p", resumed.messageIndex) != indexID || len(resumed.record.Messages) != 1 ||
		resumed.record.Messages[opened.Attempt.Scope].Input.Generation != input.Stream.ID.Generation {
		t.Fatalf("generation rebind changed active budget: %+v %v", got, err)
	}
	if err := resumed.failMessage(t.Context(), opened.Attempt.Scope); err != nil {
		t.Fatal(err)
	}
	if _, err := resumed.openMessage(t.Context(), p.messageRequest(1, 3)); !errors.Is(err, ErrConflict) {
		t.Fatalf("generation rebind accepted a different retry limit: %v", err)
	}
	if err := resumed.commitMessage(t.Context(), pulse.CheckpointMessageCommit{Site: p.site, Scope: resumed.record.RootScope,
		Attempt: opened.Attempt.Scope, Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: input.Stream.ID, Sequence: 1}}}); err == nil {
		t.Fatal("failed message was committed")
	}
}
