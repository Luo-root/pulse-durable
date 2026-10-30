package durable

import (
	"errors"
	"io"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/Luo-root/pulse"
)

func TestMessageTransactionKeyPreconditionsAndCopies(t *testing.T) {
	for _, kind := range []string{"message", "header", "state", "value"} {
		t.Run(kind, func(t *testing.T) {
			s, session := transactionState()
			s.record.Messages = map[string]MessageAttemptRecord{"message": {Parent: "old", State: AttemptActive}}
			s.record.Managed = map[string]ManagedRecord{"managed": {Parent: "old", State: []ValueRecord{{Name: "state", Data: []byte("old")}}, Values: []ValueRecord{{Name: "value", Data: []byte("old")}}}}
			tx := newRecordTransaction(s.record.Revision)
			var err error
			switch kind {
			case "message":
				old := s.record.Messages["message"]
				next := old
				next.State = AttemptConfirmed
				err = tx.putMessage("message", &old, next)
			case "header":
				old := managedHeaderOf(s.record.Managed["managed"])
				err = tx.putManaged("managed", &old, managedHeader{Parent: "new"})
			default:
				state := kind == "state"
				values := s.record.Managed["managed"].Values
				if state {
					values = s.record.Managed["managed"].State
				}
				old := values[0]
				next := copyTransactionValue(old)
				next.Data[0] = 'N'
				err = tx.putManagedEntry(managedEntryKey{"managed", kind, state}, &old, &next)
				next.Data[0] = 'X'
			}
			if err != nil {
				t.Fatal(err)
			}
			// 修订号未变而旧对象改变时，必须在 Save 前失败。
			switch kind {
			case "message":
				v := s.record.Messages["message"]
				v.Index++
				s.record.Messages["message"] = v
			case "header":
				v := s.record.Managed["managed"]
				v.Parent = "other"
				s.record.Managed["managed"] = v
			case "state":
				s.record.Managed["managed"].State[0].Data[0] = 'X'
			case "value":
				s.record.Managed["managed"].Values[0].Data[0] = 'X'
			}
			before := cloneRecord(s.record)
			if err := s.commitRecordTransaction(t.Context(), tx); !errors.Is(err, ErrConflict) || session.calls != 0 || !reflect.DeepEqual(before, s.record) {
				t.Fatalf("stale %s precondition published: %v", kind, err)
			}
		})
	}
}

func TestMessageTransactionDuplicateManagedEntryAndRevision(t *testing.T) {
	s, session := transactionState()
	tx := newRecordTransaction(s.record.Revision)
	value := ValueRecord{Name: "key", Data: []byte{}}
	if err := tx.putManaged("new", nil, managedHeader{Scope: "root"}); err != nil {
		t.Fatal(err)
	}
	if err := tx.putManagedEntry(managedEntryKey{"new", "key", true}, nil, &value); err != nil {
		t.Fatal(err)
	}
	value.Data = []byte("later")
	if err := tx.putManagedEntry(managedEntryKey{"new", "key", true}, nil, &value); err != nil {
		t.Fatal(err)
	}
	if len(tx.managedEntries) != 1 {
		t.Fatal("duplicate key not coalesced")
	}
	if err := s.commitRecordTransaction(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	if session.calls != 1 || string(s.record.Managed["new"].State[0].Data) != "later" {
		t.Fatal("managed result not committed")
	}
	conflict := newRecordTransaction(s.record.Revision)
	old := s.record.Managed["new"].State[0]
	if err := conflict.putManagedEntry(managedEntryKey{"new", "key", true}, &old, nil); err != nil {
		t.Fatal(err)
	}
	if err := conflict.putManagedEntry(managedEntryKey{"new", "key", true}, nil, &value); !errors.Is(err, ErrConflict) {
		t.Fatal("conflicting preconditions accepted")
	}
	if !errors.Is(s.commitRecordTransaction(t.Context(), conflict), ErrConflict) || session.calls != 1 {
		t.Fatal("sticky conflict saved")
	}
	stale := newRecordTransaction(s.record.Revision - 1)
	if err := stale.putMessage("new", nil, MessageAttemptRecord{}); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(s.commitRecordTransaction(t.Context(), stale), ErrConflict) || session.calls != 1 {
		t.Fatal("stale revision saved")
	}
}

func TestMessageTransactionSharedStreamViewAndStateDeletion(t *testing.T) {
	p := newStreamProtocol(t)
	ctx := t.Context()
	store := &evolutionStore{}
	p.s.session = &evolutionSession{Session: p.s.session, owner: store}
	for i := uint64(1); i <= 3; i++ {
		if err := p.append(i); err != nil {
			t.Fatal(err)
		}
	}
	first, err := p.s.openMessage(ctx, p.messageRequest(1, 2))
	if err != nil {
		t.Fatal(err)
	}
	base := pulse.CheckpointMessageCommit{Site: p.site, Scope: p.s.record.RootScope, Attempt: first.Attempt.Scope,
		Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: p.input.Stream.ID, Sequence: 1}},
		State:  []pulse.CheckpointState{{Name: "sum", Type: reflect.TypeFor[int](), Value: 7}, {Name: "remove", Type: reflect.TypeFor[int](), Value: 8}}}
	if err := p.s.commitMessage(ctx, base); err != nil {
		t.Fatal(err)
	}
	second, err := p.s.openMessage(ctx, p.messageRequest(2, 2))
	if err != nil {
		t.Fatal(err)
	}
	commit := pulse.CheckpointMessageCommit{Site: p.site, Scope: p.s.record.RootScope, Attempt: second.Attempt.Scope,
		Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: p.input.Stream.ID, Sequence: 2}},
		State:  []pulse.CheckpointState{{Name: "sum", Type: reflect.TypeFor[int](), Value: 9}},
		Outputs: []pulse.CheckpointStreamAppend{
			{Site: p.site, Attempt: p.processor.Scope, Stream: p.output.Stream.ID, Message: pulse.CheckpointStreamMessage{Sequence: 1, Value: 10}},
			{Site: p.site, Attempt: p.processor.Scope, Stream: p.output.Stream.ID, Message: pulse.CheckpointStreamMessage{Sequence: 2, Value: 20}},
		}}
	before := cloneRecord(p.s.record)
	saves := len(store.history)
	commit.Outputs[1].Message.Sequence = 3
	if err := p.s.commitMessage(ctx, commit); err == nil || len(store.history) != saves || !reflect.DeepEqual(before, p.s.record) {
		t.Fatal("gap published")
	}
	commit.Outputs[1].Message.Sequence = 2
	if err := p.s.commitMessage(ctx, commit); err != nil {
		t.Fatal(err)
	}
	managed := p.s.record.Managed[managedKey(commit.Scope, p.processor.Scope, "processor")]
	output := p.s.record.Streams[streamKey(streamID(p.output.Stream.ID))]
	if len(store.history) != saves+1 || len(managed.State) != 1 || managed.State[0].Name != "sum" || string(managed.State[0].Data) != "9" || len(output.Pending) != 2 || output.LastSequence != 2 {
		t.Fatal("batch result incorrect")
	}
	if err := p.s.commitMessage(ctx, pulse.CheckpointMessageCommit{Scope: commit.Scope,
		Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: p.input.Stream.ID, Sequence: 3}},
		Stops:  []pulse.CheckpointStreamID{p.input.Stream.ID, p.output.Stream.ID}}); err != nil {
		t.Fatal(err)
	}
	input := p.s.record.Streams[streamKey(streamID(p.input.Stream.ID))]
	output = p.s.record.Streams[streamKey(streamID(p.output.Stream.ID))]
	if !input.Stream.Stopped || input.AckedThrough != 3 || len(input.Pending) != 0 || !output.Stream.Stopped || output.AckedThrough != 0 || len(output.Pending) != 0 {
		t.Fatal("ACK then Stop order incorrect")
	}
	if err := p.s.validateLoaded(); err != nil {
		t.Fatal(err)
	}
	store.assertHistory(t)
}

func TestMessageTransactionRejectsDuplicateValueAndForeignOutput(t *testing.T) {
	p := newStreamProtocol(t)
	ctx := t.Context()
	for i := uint64(1); i <= 2; i++ {
		if err := p.append(i); err != nil {
			t.Fatal(err)
		}
	}
	first, err := p.s.openMessage(ctx, p.messageRequest(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	commit := pulse.CheckpointMessageCommit{Site: p.site, Scope: p.s.record.RootScope, Attempt: first.Attempt.Scope,
		Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: p.input.Stream.ID, Sequence: 1}},
		Values: []pulse.CheckpointOutput{{Key: p.value, State: pulse.SlotStateReady, Value: 42}}}
	if err := p.s.commitMessage(ctx, commit); err != nil {
		t.Fatal(err)
	}
	second, err := p.s.openMessage(ctx, p.messageRequest(2, 1))
	if err != nil {
		t.Fatal(err)
	}
	commit.Attempt = second.Attempt.Scope
	commit.Inputs[0].Sequence = 2
	session := &transactionSession{}
	p.s.session = session
	before := cloneRecord(p.s.record)
	if err := p.s.commitMessage(ctx, commit); err == nil || session.calls != 0 || !reflect.DeepEqual(before, p.s.record) {
		t.Fatal("duplicate Value published")
	}
	commit.Values = nil
	commit.Outputs = []pulse.CheckpointStreamAppend{
		{Site: p.site, Attempt: p.processor.Scope, Stream: p.output.Stream.ID, Message: pulse.CheckpointStreamMessage{Sequence: 1, Value: 10}},
		{Site: p.site, Attempt: p.source.Scope, Stream: p.input.Stream.ID, Message: pulse.CheckpointStreamMessage{Sequence: 3, Value: 30}},
	}
	if err := p.s.commitMessage(ctx, commit); err == nil || session.calls != 0 || !reflect.DeepEqual(before, p.s.record) {
		t.Fatal("foreign output published")
	}
}

func TestMessageTransactionExactNilEmpty(t *testing.T) {
	for _, empty := range []bool{false, true} {
		s, session := transactionState()
		s.record.Managed = map[string]ManagedRecord{"m": {State: []ValueRecord{{Name: "x"}}}}
		tx := newRecordTransaction(s.record.Revision)
		old := ValueRecord{Name: "x"}
		if empty {
			old.Data = []byte{}
		}
		value := ValueRecord{Name: "x", Data: []byte{}}
		if err := tx.putManagedEntry(managedEntryKey{"m", "x", true}, &old, &value); err != nil {
			t.Fatal(err)
		}
		if tx.managedEntries[managedEntryKey{"m", "x", true}].value.Data == nil {
			t.Fatal("staging normalized empty")
		}
		err := s.commitRecordTransaction(t.Context(), tx)
		if empty {
			if !errors.Is(err, ErrConflict) || session.calls != 0 {
				t.Fatal("nil equaled empty")
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
}

func TestMessageTransactionRuntimeRepeatedResume(t *testing.T) {
	store := &evolutionStore{Store: NewMemoryStore()}
	g := testGraph(t, "message-resume")
	input, result := pulse.NewKey[pulse.Stream[int]]("input"), pulse.NewKey[int]("result")
	var calls atomic.Int32
	testAdd(t, g, pulse.NewNode("source", "", nil, pulse.Provides(input), func(rc *pulse.RunCtx) error {
		w, err := pulse.OpenStream(rc, input, pulse.WithBuffer(4), pulse.WithStreamSource("numbers", true))
		if err != nil {
			return err
		}
		for i := int(w.Position().Sequence) + 1; i <= 3; i++ {
			if err := w.SendAt(i, i); err != nil {
				return err
			}
		}
		return nil
	}))
	testAdd(t, g, pulse.NewNode("processor", "", pulse.Requires(input), pulse.Provides(result), func(rc *pulse.RunCtx) error {
		stream, err := pulse.Get(rc, input)
		if err != nil {
			return err
		}
		for {
			delivery, err := stream.Receive(rc)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			if err := pulse.HandleMessage(rc, delivery, func(rc *pulse.RunCtx, value int) error {
				calls.Add(1)
				sum, _, err := pulse.GetMessageState[int](rc, "sum")
				if err != nil {
					return err
				}
				if err := pulse.SetMessageState(rc, "sum", sum+value); err != nil {
					return err
				}
				if value == 3 {
					return pulse.Set(rc, result, sum+value)
				}
				return nil
			}); err != nil {
				return err
			}
		}
	}))
	if err := g.DeclareOutput(pulse.NewPort(result)); err != nil {
		t.Fatal(err)
	}
	rt := testRuntime(t, store, NewCodecs())
	if _, err := rt.Run(t.Context(), g, pulse.Inputs{}, testOptions("message-resume")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		e, err := rt.Resume(t.Context(), g, pulse.Inputs{}, testOptions("message-resume"))
		if err != nil {
			t.Fatal(err)
		}
		value, state, err := pulse.ReadOutput(e, result)
		if err != nil || value != 6 || state != pulse.SlotStateReady || calls.Load() != 3 {
			t.Fatalf("resume %d: value=%d state=%v calls=%d err=%v", i, value, state, calls.Load(), err)
		}
		store.assertHistory(t)
	}
}

func TestMessageTransactionSaveErrorAndPanic(t *testing.T) {
	for _, panicSave := range []bool{false, true} {
		s, session := transactionState()
		tx := newRecordTransaction(s.record.Revision)
		if err := tx.putMessage("new", nil, MessageAttemptRecord{Parent: "old", State: AttemptActive}); err != nil {
			t.Fatal(err)
		}
		before := cloneRecord(s.record)
		session.after = func(RunRecord) error {
			if panicSave {
				panic("save panic")
			}
			return errors.New("save error")
		}
		if err := s.commitRecordTransaction(t.Context(), tx); err == nil || s.stopped == nil || !reflect.DeepEqual(before, s.record) || session.calls != 1 {
			t.Fatal("failed Save published or was not sealed")
		}
		if err := s.commitRecordTransaction(t.Context(), tx); err == nil || session.calls != 1 {
			t.Fatal("sealed Save retried")
		}
	}
}
