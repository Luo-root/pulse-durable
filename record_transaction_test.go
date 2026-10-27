package durable

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/Luo-root/pulse"
)

type transactionSession struct {
	Session
	calls    int
	expected uint64
	saved    RunRecord
	after    func(RunRecord) error
}

func (s *transactionSession) Save(_ context.Context, r RunRecord, expected uint64) error {
	s.calls++
	s.expected, s.saved = expected, r
	if s.after != nil {
		return s.after(r)
	}
	return nil
}

func transactionState() (*runState, *transactionSession) {
	session := &transactionSession{}
	return &runState{session: session, record: RunRecord{
		Format: RecordFormat, RunID: "transaction", Revision: 4, RootScope: "root",
		Attempts: map[string]AttemptRecord{"old": {Parent: "root", NodeID: "work", Index: 1, Limit: 1, State: AttemptActive}},
		Scopes: map[string]ScopeRecord{"root": {Graph: "graph", Nodes: map[string]NodeRecord{
			"result": {NodeID: "result", Outputs: []ValueRecord{{Data: []byte("original")}}},
		}}},
	}}, session
}

func stageTransactionFailure(t *testing.T, s *runState) *recordTransaction {
	t.Helper()
	tx := newRecordTransaction(s.record.Revision)
	old := s.record.Attempts["old"]
	failed := old
	failed.State = AttemptFailed
	if err := tx.putAttempt("old", &old, failed); err != nil {
		t.Fatal(err)
	}
	return tx
}

func TestRecordTransactionStagesValuesAndCoalesces(t *testing.T) {
	s, session := transactionState()
	before := cloneRecord(s.record)
	tx := newRecordTransaction(s.record.Revision)
	old := s.record.Attempts["old"]
	value := old
	value.State = AttemptFailed
	if err := tx.putAttempt("old", &old, value); err != nil {
		t.Fatal(err)
	}
	// Both caller-owned arguments may change after staging. Neither the base
	// nor the captured precondition/final value may change with them.
	old.NodeID, value.NodeID = "caller mutation", "caller mutation"
	if !reflect.DeepEqual(s.record, before) || session.calls != 0 {
		t.Fatal("staging changed the live record or invoked Save")
	}
	base := before.Attempts["old"]
	final := base
	final.State = AttemptConfirmed
	if err := tx.putAttempt("old", &base, final); err != nil {
		t.Fatal(err)
	}
	created := AttemptRecord{Parent: "root", NodeID: "created", Index: 1, Limit: 1, State: AttemptActive}
	if err := tx.putAttempt("new", nil, created); err != nil {
		t.Fatal(err)
	}
	created.State = AttemptFailed
	if err := tx.putAttempt("new", nil, created); err != nil {
		t.Fatal(err)
	}
	created.NodeID = "caller mutation"
	if len(tx.attempts) != 2 {
		t.Fatalf("duplicate keys not coalesced: %d", len(tx.attempts))
	}
	if err := s.commitRecordTransaction(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	if session.calls != 1 || session.expected != before.Revision || s.record.Revision != before.Revision+1 || s.record.Format != 2 {
		t.Fatalf("unexpected legacy Save: calls=%d expected=%d record=%+v", session.calls, session.expected, s.record)
	}
	if s.record.Attempts["old"] != final || s.record.Attempts["new"].NodeID != "created" || s.record.Attempts["new"].State != AttemptFailed {
		t.Fatal("final staged values not published")
	}
	if err := s.commitRecordTransaction(t.Context(), tx); !errors.Is(err, ErrConflict) || session.calls != 1 {
		t.Fatalf("committed transaction reused: err=%v calls=%d", err, session.calls)
	}
}

func TestRecordTransactionRejectsDuplicatePreconditionConflict(t *testing.T) {
	for _, mode := range []string{"changed old", "old to absent", "absent to old"} {
		t.Run(mode, func(t *testing.T) {
			s, session := transactionState()
			before := cloneRecord(s.record)
			tx := newRecordTransaction(s.record.Revision)
			old := s.record.Attempts["old"]
			first, second := &old, &old
			changed := old
			changed.State = AttemptFailed
			switch mode {
			case "changed old":
				second = &changed
			case "old to absent":
				second = nil
			case "absent to old":
				first = nil
			}
			if err := tx.putAttempt("old", first, changed); err != nil {
				t.Fatal(err)
			}
			err := tx.putAttempt("old", second, old)
			if !errors.Is(err, ErrConflict) {
				t.Fatalf("conflicting preconditions accepted: %v", err)
			}
			if again := tx.putAttempt("other", nil, old); again != err {
				t.Fatal("invalid transaction accepted another put")
			}
			if got := s.commitRecordTransaction(t.Context(), tx); got != err || session.calls != 0 || !reflect.DeepEqual(s.record, before) {
				t.Fatalf("invalid transaction reached Save/publication: %v calls=%d", got, session.calls)
			}
		})
	}
}

func TestRecordTransactionPreconditions(t *testing.T) {
	for _, mode := range []string{"stale revision", "changed old", "missing old", "expected absent", "zero old is not absent"} {
		t.Run(mode, func(t *testing.T) {
			s, session := transactionState()
			tx := stageTransactionFailure(t, s)
			switch mode {
			case "stale revision":
				s.record.Revision++
			case "changed old":
				changed := s.record.Attempts["old"]
				changed.Limit++
				s.record.Attempts["old"] = changed
			case "missing old":
				delete(s.record.Attempts, "old")
			case "expected absent":
				tx = newRecordTransaction(s.record.Revision)
				if err := tx.putAttempt("old", nil, AttemptRecord{}); err != nil {
					t.Fatal(err)
				}
			case "zero old is not absent":
				tx = newRecordTransaction(s.record.Revision)
				zero := AttemptRecord{}
				if err := tx.putAttempt("missing", &zero, zero); err != nil {
					t.Fatal(err)
				}
			}
			before := cloneRecord(s.record)
			if err := s.commitRecordTransaction(t.Context(), tx); !errors.Is(err, ErrConflict) {
				t.Fatalf("expected conflict: %v", err)
			}
			if session.calls != 0 || !reflect.DeepEqual(s.record, before) {
				t.Fatal("precondition failure saved or published staged changes")
			}
		})
	}
}

func TestRecordTransactionSaveFailureDoesNotPublish(t *testing.T) {
	failure := errors.New("uncertain Save")
	for _, mode := range []string{"error", "panic", "panic nil"} {
		t.Run(mode, func(t *testing.T) {
			s, session := transactionState()
			before := cloneRecord(s.record)
			tx := stageTransactionFailure(t, s)
			session.after = func(r RunRecord) error {
				// A hostile Store may retain/mutate its argument or commit before
				// returning failure. It must not publish into runtime memory.
				r.Scopes["root"].Nodes["result"].Outputs[0].Data[0] = 'X'
				delete(r.Attempts, "old")
				switch mode {
				case "panic":
					panic("Save")
				case "panic nil":
					panic(nil)
				}
				return failure
			}
			err := s.commitRecordTransaction(t.Context(), tx)
			if err == nil || s.stopped != err || !reflect.DeepEqual(s.record, before) {
				t.Fatalf("uncertain Save published or was not sealed: err=%v stopped=%v", err, s.stopped)
			}
			if mode == "error" && !errors.Is(err, failure) {
				t.Fatalf("Save error identity lost: %v", err)
			}
			if again := s.commitRecordTransaction(t.Context(), tx); again != err || session.calls != 1 {
				t.Fatalf("uncertain transaction retried: err=%v calls=%d", again, session.calls)
			}
		})
	}
}

func TestRecordTransactionConfirmedCopiesAreIsolated(t *testing.T) {
	s, session := transactionState()
	oldRecord := s.record
	before := cloneRecord(oldRecord)
	tx := stageTransactionFailure(t, s)
	session.after = func(r RunRecord) error {
		if !reflect.DeepEqual(s.record, before) {
			t.Fatal("live record published before Save confirmation")
		}
		r.Scopes["root"].Nodes["result"].Outputs[0].Data[0] = 'X'
		r.Attempts["store only"] = AttemptRecord{}
		return nil
	}
	if err := s.commitRecordTransaction(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	confirmed := cloneRecord(s.record)
	// The pre-transaction base, retained Save argument and transaction are all
	// independently mutable after acknowledgement.
	oldRecord.Scopes["root"].Nodes["result"].Outputs[0].Data[0] = 'B'
	session.saved.Scopes["root"].Nodes["result"].Outputs[0].Data[0] = 'S'
	delete(session.saved.Attempts, "old")
	if err := tx.putAttempt("new", nil, AttemptRecord{}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(s.record, confirmed) || string(s.record.Scopes["root"].Nodes["result"].Outputs[0].Data) != "original" {
		t.Fatal("confirmed runtime record aliases base, Save argument or overlay")
	}
	if _, ok := s.record.Attempts["store only"]; ok {
		t.Fatal("Store mutation leaked into confirmed record")
	}
	s.record.Scopes["root"].Nodes["result"].Outputs[0].Data[0] = 'R'
	if session.saved.Scopes["root"].Nodes["result"].Outputs[0].Data[0] != 'S' {
		t.Fatal("live mutation changed retained Save argument")
	}
}

func TestRecordTransactionGuardsAndNilAttemptMap(t *testing.T) {
	for _, mode := range []string{"canceled", "nil context", "stopped", "revision exhausted", "empty", "nil attempts"} {
		t.Run(mode, func(t *testing.T) {
			s, session := transactionState()
			ctx := context.Background()
			switch mode {
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "nil context":
				ctx = nil
			case "stopped":
				s.stopped = errors.New("already stopped")
			case "revision exhausted":
				s.record.Revision = math.MaxUint64
			case "nil attempts":
				s.record.Attempts = nil
			}
			tx := newRecordTransaction(s.record.Revision)
			if mode != "empty" {
				if err := tx.putAttempt("new", nil, AttemptRecord{State: AttemptActive}); err != nil {
					t.Fatal(err)
				}
			}
			before := cloneRecord(s.record)
			err := s.commitRecordTransaction(ctx, tx)
			if mode == "nil attempts" {
				if err != nil || session.calls != 1 || s.record.Attempts["new"].State != AttemptActive {
					t.Fatalf("nil attempt map insertion: %v calls=%d", err, session.calls)
				}
				return
			}
			if mode == "empty" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("guard accepted transaction")
			}
			if mode == "canceled" {
				var notStarted *pulse.CheckpointNotStarted
				if !errors.As(err, &notStarted) || s.stopped != nil {
					t.Fatalf("cancellation lost pre-operation semantics: %v", err)
				}
			}
			if session.calls != 0 || !reflect.DeepEqual(s.record, before) {
				t.Fatal("guard saved or published changes")
			}
		})
	}
}
