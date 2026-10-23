package durable

import (
	"context"
	"errors"
	"fmt"
	"github.com/Luo-root/pulse"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFailedStreamOwnerAtomicAndIdempotent(t *testing.T) {
	p := newStreamProtocol(t)
	ctx := context.Background()
	if err := p.append(1); err != nil {
		t.Fatal(err)
	}
	// A second reading group is another endpoint of the same writer root.
	request := p.openRequest(false)
	request.Group = "second"
	sibling, err := p.s.openStream(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	finish := pulse.CheckpointStreamFinish{Site: p.site, Attempt: p.source.Scope, Stream: p.input.Stream.ID,
		Terminal: pulse.StreamFailed, LastSequence: 1, Cursor: 1}
	before := p.s.record.Revision
	if err := p.s.finishStream(ctx, finish); err != nil {
		t.Fatal(err)
	}
	if p.s.record.Revision != before+1 || p.s.record.Attempts[p.source.Scope].State != AttemptFailed {
		t.Fatal("terminal and source root were not one Save")
	}
	for _, stream := range p.s.record.Streams {
		if stream.Stream.Attempt == p.source.Scope && stream.Stream.Terminal != "failed" {
			t.Fatal("sibling stayed active")
		}
	}
	if p.s.record.Attempts[p.processor.Scope].State != AttemptActive {
		t.Fatal("another owner failed")
	}
	saved := cloneRecord(p.s.record)
	if err := p.s.finishStream(ctx, finish); err != nil {
		t.Fatal(err)
	}
	finish.Stream, finish.LastSequence, finish.Cursor = sibling.Stream.ID, 0, nil
	if err := p.s.finishStream(ctx, finish); err != nil {
		t.Fatal(err)
	}
	if err := p.s.failAttempt(ctx, p.source.Scope); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(saved, p.s.record) {
		t.Fatal("idempotent cleanup wrote again")
	}
	for name, mutate := range map[string]func(*pulse.CheckpointStreamFinish){
		"generation": func(r *pulse.CheckpointStreamFinish) { r.Stream.Generation++ },
		"sequence":   func(r *pulse.CheckpointStreamFinish) { r.LastSequence++ },
		"cursor":     func(r *pulse.CheckpointStreamFinish) { r.Cursor = 2 },
		"owner":      func(r *pulse.CheckpointStreamFinish) { r.Attempt = p.processor.Scope },
		"site":       func(r *pulse.CheckpointStreamFinish) { r.Site.Graph = "wrong" },
		"cancel":     func(r *pulse.CheckpointStreamFinish) { r.Terminal = pulse.StreamCanceled },
	} {
		t.Run(name, func(t *testing.T) {
			bad := finish
			mutate(&bad)
			if err := p.s.finishStream(ctx, bad); err == nil {
				t.Fatal("invalid repeated finish accepted")
			}
		})
	}
	if _, err := p.s.openStream(ctx, p.openRequest(false)); err == nil {
		t.Fatal("failed source reopened")
	}
	if err := p.append(2); err == nil {
		t.Fatal("failed source appended")
	}
	if err := p.s.commitMessage(ctx, pulse.CheckpointMessageCommit{Scope: p.s.record.RootScope,
		Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: p.input.Stream.ID, Sequence: 1}}}); err == nil {
		t.Fatal("failed source acknowledged")
	}
	if !reflect.DeepEqual(saved, p.s.record) {
		t.Fatal("rejected operation changed history")
	}
}

func TestFailedStreamSaveFailureAndLegacyConvergence(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprintf("after-save=%t", after), func(t *testing.T) {
			p := newStreamProtocol(t)
			ctx := context.Background()
			saved := cloneRecord(p.s.record)
			fault := errors.New("save unavailable")
			p.s.session = &terminalFaultSession{Session: p.s.session, after: after, failure: fault}
			err := p.s.finishStream(ctx, pulse.CheckpointStreamFinish{Site: p.site, Attempt: p.source.Scope,
				Stream: p.input.Stream.ID, Terminal: pulse.StreamFailed})
			if !errors.Is(err, fault) || !reflect.DeepEqual(saved, p.s.record) {
				t.Fatal("uncertain Save advanced memory or lost error")
			}
			stored, err := p.s.session.Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			want := AttemptActive
			if after {
				want = AttemptFailed
			}
			if stored.Attempts[p.source.Scope].State != want {
				t.Fatal("wrong authoritative Save outcome")
			}
		})
	}
	for _, terminal := range []string{"failed", "canceled", ""} {
		t.Run("legacy-"+terminal, func(t *testing.T) {
			p := newStreamProtocol(t)
			key := streamKey(streamID(p.input.Stream.ID))
			stream := p.s.record.Streams[key]
			stream.Stream.Terminal = terminal
			p.s.record.Streams[key] = stream
			if err := p.s.save(context.Background(), cloneRecord(p.s.record)); err != nil {
				t.Fatal(err)
			}
			if err := p.s.validateLoaded(); err != nil {
				t.Fatal(err)
			}
			before := cloneRecord(p.s.record)
			if err := p.s.reconcileFailedStreams(context.Background()); err != nil {
				t.Fatal(err)
			}
			if terminal == "failed" {
				if p.s.record.Attempts[p.source.Scope].State != AttemptFailed || p.s.record.Revision != before.Revision+1 {
					t.Fatal("legacy failure not reconciled")
				}
			} else if !reflect.DeepEqual(before, p.s.record) || p.s.scopeFailure(before.RootScope) != nil {
				t.Fatal("cancel/active became business failure")
			}
			stable := cloneRecord(p.s.record)
			if err := p.s.reconcileFailedStreams(context.Background()); err != nil || !reflect.DeepEqual(stable, p.s.record) {
				t.Fatal("reconciliation not idempotent")
			}
		})
	}
}

func TestLegacyStreamRepairSaveOutcomeIsAuthoritative(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprintf("after-save=%t", after), func(t *testing.T) {
			p := newStreamProtocol(t)
			request := p.openRequest(false)
			request.Group = "sibling"
			sibling, err := p.s.openStream(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			key := streamKey(streamID(p.input.Stream.ID))
			stream := p.s.record.Streams[key]
			stream.Stream.Terminal = "failed"
			p.s.record.Streams[key] = stream
			if err := p.s.save(context.Background(), cloneRecord(p.s.record)); err != nil {
				t.Fatal(err)
			}
			before := cloneRecord(p.s.record)
			base := p.s.session
			p.s.session = &terminalFaultSession{Session: base, after: after, failure: pulse.ErrSkipped}
			if err := p.s.validateLoaded(); err != nil {
				t.Fatal(err)
			}
			if err := p.s.reconcileFailedStreams(context.Background()); !errors.Is(err, pulse.ErrSkipped) || p.s.stopped == nil || !reflect.DeepEqual(before, p.s.record) {
				t.Fatalf("uncertain repair did not stop without advancing memory: %v", err)
			}
			stored, err := base.Load(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			want := AttemptActive
			if after {
				want = AttemptFailed
			}
			if stored.Attempts[p.source.Scope].State != want {
				t.Fatal("repair claimed a Save that did not happen")
			}
			next := &runState{session: base, def: p.s.def, record: stored}
			if err := next.validateLoaded(); err != nil {
				t.Fatal(err)
			}
			if err := next.reconcileFailedStreams(context.Background()); err != nil {
				t.Fatal(err)
			}
			if next.record.Attempts[p.source.Scope].State != AttemptFailed || next.record.Streams[streamKey(streamID(sibling.Stream.ID))].Stream.Terminal != "failed" {
				t.Fatal("repair left an active sibling")
			}
			stable := cloneRecord(next.record)
			if err := next.reconcileFailedStreams(context.Background()); err != nil || !reflect.DeepEqual(stable, next.record) {
				t.Fatal("authoritative repair repeated Save")
			}
		})
	}
}

type terminalFaultSession struct {
	Session
	after   bool
	failure error
}

func (s *terminalFaultSession) Save(ctx context.Context, record RunRecord, expected uint64) error {
	if s.after {
		if err := s.Session.Save(ctx, record, expected); err != nil {
			return err
		}
	}
	return s.failure
}

func TestConsumerCanConfirmBeforeItsStreamProducerCompletes(t *testing.T) {
	confirmed := make(chan struct{})
	var once sync.Once
	var witnessed atomic.Bool
	store := &faultStore{Store: NewMemoryStore(), match: func(r RunRecord) bool {
		if recordHasNode(r, "early-consumer", "sink") && !recordHasNode(r, "early-consumer", "source") {
			witnessed.Store(true)
			once.Do(func() { close(confirmed) })
		}
		return false
	}}
	g := testGraph(t, "early-consumer")
	key := pulse.NewKey[pulse.Stream[int]]("stream")
	testAdd(t, g, pulse.NewNode("source", "", nil, pulse.Provides(key), func(rc *pulse.RunCtx) error {
		writer, err := pulse.OpenStream(rc, key, pulse.WithStreamSource("early-source", true))
		if err != nil {
			return err
		}
		if err := writer.SendAt(1, 1); err != nil {
			return err
		}
		select {
		case <-confirmed:
			return nil
		case <-rc.Context().Done():
			return rc.Context().Err()
		}
	}))
	testAdd(t, g, pulse.NewNode("sink", "", pulse.Requires(key), nil, func(rc *pulse.RunCtx) error {
		stream, err := pulse.Get(rc, key)
		if err != nil {
			return err
		}
		delivery, err := stream.Receive(rc)
		if err != nil {
			return err
		}
		if err := delivery.Ack(); err != nil {
			return err
		}
		return stream.Stop(rc)
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := testRuntime(t, store, NewCodecs()).Run(ctx, g, pulse.Inputs{}, testOptions("run")); err != nil {
		t.Fatal(err)
	}
	if !witnessed.Load() {
		t.Fatal("consumer had to wait for complete source NodeRecord")
	}
	record := readRecord(t, store, "run")
	for _, stream := range record.Streams {
		if stream.Stream.Terminal != "stopped" || !stream.Stream.Stopped {
			t.Fatal("normal reading stop was not preserved")
		}
	}
}

func TestStreamGenerationAndCompletedSourceTail(t *testing.T) {
	p := newStreamProtocol(t)
	ctx := context.Background()
	for i := uint64(1); i <= 3; i++ {
		if err := p.append(i); err != nil {
			t.Fatal(err)
		}
	}
	// 先确认 2，连续前沿仍为 0；确认 1 才推进至 2。
	if err := p.s.commitMessage(ctx, pulse.CheckpointMessageCommit{Scope: p.s.record.RootScope, Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: p.input.Stream.ID, Sequence: 2}}}); err != nil {
		t.Fatal(err)
	}
	record := p.s.record.Streams[streamKey(streamID(p.input.Stream.ID))]
	if record.AckedThrough != 0 || len(record.Pending) != 2 {
		t.Fatalf("invalid sparse acknowledgement: %+v", record)
	}
	descriptor := p.input.Stream
	descriptor.Terminal = pulse.StreamCompleted
	if err := p.s.commit(ctx, pulse.CheckpointCommit{Site: p.site, Scope: p.s.record.RootScope, Attempt: p.source.Scope,
		Node:    pulse.NodeCheckpoint{NodeID: "source", Reason: pulse.NodeCompleted, Outputs: []pulse.CheckpointOutput{{Key: p.inKey, State: pulse.SlotStateReady, Stream: &descriptor}}},
		Streams: []pulse.CheckpointStreamFinish{{Site: p.site, Attempt: p.source.Scope, Stream: p.input.Stream.ID, Terminal: pulse.StreamCompleted, LastSequence: 3, Cursor: 3}}}); err != nil {
		t.Fatal(err)
	}
	if err := p.s.validateLoaded(); err != nil {
		t.Fatal(err)
	}
	resumed := &runState{session: p.s.session, def: p.s.def, record: cloneRecord(p.s.record)}
	request := p.openRequest(false)
	request.Restore = &descriptor
	restored, err := resumed.openStream(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Stream.ID.Generation != 2 || len(restored.Pending) != 2 || restored.Cursor != 3 {
		t.Fatalf("bad recovered tail: %+v", restored)
	}
	revision := resumed.record.Revision
	again, err := resumed.openStream(ctx, request)
	if err != nil || again.Stream.ID != restored.Stream.ID || resumed.record.Revision != revision {
		t.Fatal("repeated Open advanced generation or revision")
	}
	if err := resumed.commitMessage(ctx, pulse.CheckpointMessageCommit{Scope: resumed.record.RootScope, Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: descriptor.ID, Sequence: 1}}}); err == nil {
		t.Fatal("old delivery advanced a new generation")
	}
	for _, sequence := range []uint64{1, 3} {
		if err := resumed.commitMessage(ctx, pulse.CheckpointMessageCommit{Scope: resumed.record.RootScope, Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: restored.Stream.ID, Sequence: sequence}}}); err != nil {
			t.Fatal(err)
		}
	}
	record = resumed.record.Streams[streamKey(streamID(descriptor.ID))]
	if record.AckedThrough != 3 || len(record.Pending) != 0 {
		t.Fatal("completed source tail was not acknowledged")
	}
	for _, mutate := range []func(*pulse.CheckpointStreamDescriptor){
		func(d *pulse.CheckpointStreamDescriptor) { d.Source.ID = "forged" },
		func(d *pulse.CheckpointStreamDescriptor) { d.ID.Generation = 99 },
		func(d *pulse.CheckpointStreamDescriptor) { d.Terminal = pulse.StreamStopped },
	} {
		bad := descriptor
		mutate(&bad)
		request.Restore = &bad
		if _, err := resumed.openStream(ctx, request); err == nil {
			t.Fatal("forged completed reference reopened")
		}
	}
}

func TestMessageCommitCoordinatesStateValueOutputAndAck(t *testing.T) {
	p := newStreamProtocol(t)
	ctx := context.Background()
	if err := p.append(1); err != nil {
		t.Fatal(err)
	}
	attempt, err := p.s.openMessage(ctx, p.messageRequest(1, 2))
	if err != nil {
		t.Fatal(err)
	}
	commit := pulse.CheckpointMessageCommit{Site: p.site, Scope: p.s.record.RootScope, Attempt: attempt.Attempt.Scope,
		Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: p.input.Stream.ID, Sequence: 1}},
		State:  []pulse.CheckpointState{{Name: "sum", Type: reflect.TypeFor[int](), Value: 1}},
		Values: []pulse.CheckpointOutput{{Key: p.value, State: pulse.SlotStateReady, Value: 99}},
		Outputs: []pulse.CheckpointStreamAppend{{Site: p.site, Attempt: p.processor.Scope, Stream: p.output.Stream.ID, Message: pulse.CheckpointStreamMessage{Sequence: 1, Value: 10}},
			{Site: p.site, Attempt: p.processor.Scope, Stream: p.output.Stream.ID, Message: pulse.CheckpointStreamMessage{Sequence: 2, Value: "wrong type"}}}}
	before := cloneRecord(p.s.record)
	if err := p.s.commitMessage(ctx, commit); err == nil {
		t.Fatal("invalid second output committed")
	}
	if !reflect.DeepEqual(before, p.s.record) {
		t.Fatal("failed transaction partly changed state, output or ack")
	}
	commit.Outputs = commit.Outputs[:1]
	if err := p.s.commitMessage(ctx, commit); err != nil {
		t.Fatal(err)
	}
	if p.s.record.Revision != before.Revision+1 || p.s.record.Messages[attempt.Attempt.Scope].State != AttemptConfirmed ||
		len(p.s.record.Streams[streamKey(streamID(p.input.Stream.ID))].Pending) != 0 || len(p.s.record.Streams[streamKey(streamID(p.output.Stream.ID))].Pending) != 1 {
		t.Fatal("message transaction did not commit atomically")
	}
	managed := p.s.record.Managed[managedKey(p.s.record.RootScope, p.processor.Scope, "processor")]
	if len(managed.State) != 1 || len(managed.Values) != 1 {
		t.Fatal("message state or ordinary value missing")
	}
	if err := p.s.validateLoaded(); err != nil {
		t.Fatal(err)
	}
	// 深复制所有新增容器、编码字节和 descriptor。
	copy := cloneRecord(p.s.record)
	copy.Managed[managedKey(p.s.record.RootScope, p.processor.Scope, "processor")].State[0].Data[0] = '9'
	stream := copy.Streams[streamKey(streamID(p.output.Stream.ID))]
	stream.Pending[0].Value.Data[0] = '9'
	if reflect.DeepEqual(copy, p.s.record) || string(managed.State[0].Data) != "1" {
		t.Fatal("cloned record aliases payload bytes")
	}
}

func TestMessageBudgetSurvivesGenerationAndFailedSourceRejectsAck(t *testing.T) {
	p := newStreamProtocol(t)
	ctx := context.Background()
	if err := p.append(1); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 2; i++ {
		a, err := p.s.openMessage(ctx, p.messageRequest(1, 2))
		if err != nil || a.Attempt.Index != i {
			t.Fatalf("attempt %d: %+v %v", i, a, err)
		}
		if err := p.s.failMessage(ctx, a.Attempt.Scope); err != nil {
			t.Fatal(err)
		}
	}
	resumed := &runState{session: p.s.session, def: p.s.def, record: cloneRecord(p.s.record)}
	input, err := resumed.openStream(ctx, p.openRequest(false))
	if err != nil {
		t.Fatal(err)
	}
	p.s, p.input = resumed, input
	a, err := resumed.openMessage(ctx, p.messageRequest(1, 2))
	if err != nil || !a.Attempt.Exhausted || a.Attempt.Index != 2 {
		t.Fatalf("resume replenished budget: %+v %v", a, err)
	}
	if _, err := resumed.openMessage(ctx, p.messageRequest(1, 3)); !errors.Is(err, ErrConflict) {
		t.Fatalf("limit drift: %v", err)
	}
	if err := resumed.failAttempt(ctx, p.source.Scope); err != nil {
		t.Fatal(err)
	}
	if err := resumed.commitMessage(ctx, pulse.CheckpointMessageCommit{Scope: resumed.record.RootScope, Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: input.Stream.ID, Sequence: 1}}}); err == nil {
		t.Fatal("failed source accepted Ack")
	}
}

func TestRuntimeMessageRetryAndExplicitState(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	g := testGraph(t, "message-pipeline")
	input, output, final := pulse.NewKey[pulse.Stream[int]]("input"), pulse.NewKey[pulse.Stream[int]]("output"), pulse.NewKey[int]("sum")
	testAdd(t, g, pulse.NewNode("source", "", nil, pulse.Provides(input), func(rc *pulse.RunCtx) error {
		w, err := pulse.OpenStream(rc, input, pulse.WithBuffer(4), pulse.WithStreamSource("integers", true))
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
	var trace, received []int
	var failed bool
	testAdd(t, g, pulse.NewNode("processor", "", pulse.Requires(input), pulse.Deps(pulse.Provides(output), pulse.Provides(final)), func(rc *pulse.RunCtx) error {
		w, err := pulse.OpenStream(rc, output, pulse.WithBuffer(4), pulse.WithStreamSource("processed", true))
		if err != nil {
			return err
		}
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
				trace = append(trace, value)
				sum, _, err := pulse.GetMessageState[int](rc, "sum")
				if err != nil {
					return err
				}
				if err := pulse.SetMessageState(rc, "sum", sum+value); err != nil {
					return err
				}
				if err := w.Send(value * 10); err != nil {
					return err
				}
				if value == 2 && !failed {
					failed = true
					return errors.New("retry this input")
				}
				if value == 3 {
					return pulse.Set(rc, final, sum+value)
				}
				return nil
			}, pulse.WithMessageRetry(2, 0)); err != nil {
				return err
			}
		}
	}))
	testAdd(t, g, pulse.NewNode("sink", "", pulse.Requires(output), nil, func(rc *pulse.RunCtx) error {
		stream, err := pulse.Get(rc, output)
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
			received = append(received, delivery.Value)
			if err := delivery.Ack(); err != nil {
				return err
			}
		}
	}))
	if err := g.DeclareOutput(pulse.NewPort(final)); err != nil {
		t.Fatal(err)
	}
	store := NewMemoryStore()
	e, err := testRuntime(t, store, NewCodecs()).Run(ctx, g, pulse.Inputs{}, testOptions("run"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(trace, []int{1, 2, 2, 3}) || !reflect.DeepEqual(received, []int{10, 20, 30}) {
		t.Fatalf("trace=%v outputs=%v", trace, received)
	}
	if value, _, err := pulse.ReadOutput(e, final); err != nil || value != 6 {
		t.Fatalf("sum=%d err=%v", value, err)
	}
}

func TestBeforeStartErrorsReleaseSession(t *testing.T) {
	for _, panicCallback := range []bool{false, true} {
		store := NewMemoryStore()
		rt := testRuntime(t, store, NewCodecs())
		g := testGraph(t, "before-start")
		opts := testOptions("run")
		opts.BeforeStart = func(*pulse.Execution) error {
			if panicCallback {
				panic("host failed")
			}
			return errors.New("host failed")
		}
		if _, err := rt.Run(context.Background(), g, pulse.Inputs{}, opts); err == nil {
			t.Fatal("callback failure ignored")
		}
		session, err := store.Acquire(context.Background(), "run")
		if err != nil {
			t.Fatalf("session leaked: %v", err)
		}
		_ = session.Close()
	}
}

func TestCompletedSourceHostTailResume(t *testing.T) {
	store := NewMemoryStore()
	confirmed := make(chan struct{})
	var once sync.Once
	backend := &streamObservationStore{Store: store, afterSave: func(record RunRecord) {
		if recordHasNode(record, "host-tail", "source") {
			once.Do(func() { close(confirmed) })
		}
	}}
	rt := testRuntime(t, backend, NewCodecs())
	var calls atomic.Int32
	key := pulse.NewKey[pulse.Stream[int]]("numbers")
	g := testGraph(t, "host-tail")
	if err := g.DeclareOutput(pulse.NewPort(key)); err != nil {
		t.Fatal(err)
	}
	testAdd(t, g, pulse.NewNode("source", "", nil, pulse.Provides(key), func(rc *pulse.RunCtx) error {
		calls.Add(1)
		w, err := pulse.OpenStream(rc, key, pulse.WithBuffer(2), pulse.WithStreamSource("host-numbers", false))
		if err != nil {
			return err
		}
		if err := w.Send(1); err != nil {
			return err
		}
		return w.Send(2)
	}))
	firstCtx, firstCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer firstCancel()
	firstRead := make(chan error, 1)
	opts := testOptions("run")
	opts.BeforeStart = func(e *pulse.Execution) error {
		sub, err := pulse.SubscribeStream(e, key)
		if err != nil {
			return err
		}
		go func() {
			_, err := receiveHost(firstCtx, sub)
			if err == nil {
				select {
				case <-confirmed:
				case <-firstCtx.Done():
					err = firstCtx.Err()
				}
			}
			firstCancel()
			firstRead <- err
		}()
		return nil
	}
	if _, err := rt.Run(firstCtx, g, pulse.Inputs{}, opts); err == nil {
		t.Fatal("unacknowledged host tail reported success")
	} else {
		t.Logf("first run: %v", err)
	}
	select {
	case err := <-firstRead:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("host reader never started or source never confirmed")
	}
	before := readRecord(t, store, "run")
	if !recordHasNode(before, "host-tail", "source") {
		t.Fatal("source did not complete before host interruption")
	}
	secondCtx, secondCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer secondCancel()
	read := make(chan error, 1)
	var got []int
	opts.BeforeStart = func(e *pulse.Execution) error {
		sub, err := pulse.SubscribeStream(e, key)
		if err != nil {
			return err
		}
		go func() {
			for {
				d, err := receiveHost(secondCtx, sub)
				if err == io.EOF {
					read <- nil
					return
				}
				if err != nil {
					read <- err
					return
				}
				got = append(got, d.Value)
				if err := d.Ack(); err != nil {
					read <- err
					return
				}
			}
		}()
		return nil
	}
	if _, err := rt.Resume(secondCtx, g, pulse.Inputs{}, opts); err != nil {
		t.Fatal(err)
	}
	if err := <-read; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatalf("source calls=%d tail=%v", calls.Load(), got)
	}
}

// 信号必须在真实 Save 成功之后发出，不能把 proposed record 当作来源已确认。
type streamObservationStore struct {
	Store
	afterSave func(RunRecord)
}

func (s *streamObservationStore) Acquire(ctx context.Context, id string) (Session, error) {
	session, err := s.Store.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	return &streamObservationSession{Session: session, afterSave: s.afterSave}, nil
}

type streamObservationSession struct {
	Session
	afterSave func(RunRecord)
}

func (s *streamObservationSession) Save(ctx context.Context, record RunRecord, expected uint64) error {
	if err := s.Session.Save(ctx, record, expected); err != nil {
		return err
	}
	s.afterSave(record)
	return nil
}

func receiveHost(ctx context.Context, sub *pulse.StreamSubscription[int]) (pulse.Delivery[int], error) {
	for {
		delivery, err := sub.Receive(ctx)
		if !errors.Is(err, pulse.ErrGraphNotStarted) {
			return delivery, err
		}
		select {
		case <-ctx.Done():
			return delivery, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

// 第二条输入的持久消息尝试已打开、两个分支的容量仍由第一条占据之后，
// 才让两个消费者 Ack 第一条并 Stop。Tee 的提交预检必须正常收尾，不能确认
// 已无分支接纳的第二条，也不能把正常 Stop 锁存为可靠失败。
func TestTeeAllStopMessageAttemptRunAndResume(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	sourceConfirmed, secondOpened := make(chan struct{}), make(chan struct{})
	var sourceOnce, secondOnce sync.Once
	var openedSecond atomic.Bool
	store := &streamObservationStore{Store: NewMemoryStore(), afterSave: func(record RunRecord) {
		if _, ok := record.Scopes[record.RootScope].Nodes["source"]; ok {
			sourceOnce.Do(func() { close(sourceConfirmed) })
		}
		for _, message := range record.Messages {
			if message.NodeID == "tee" && message.Sequence == 2 && message.State == AttemptActive {
				openedSecond.Store(true)
				secondOnce.Do(func() { close(secondOpened) })
			}
		}
	}}
	codecs := NewCodecs()
	runtime := testRuntime(t, store, codecs)
	g := testGraph(t, "tee-all-stop", pulse.WithMaxRunning(1))
	input := pulse.NewKey[pulse.Stream[int]]("input")
	branches := pulse.Keys(pulse.NewKey[pulse.Stream[int]]("left"), pulse.NewKey[pulse.Stream[int]]("right"))
	var sourceCalls, teeCalls atomic.Int32
	var sinkCalls [2]atomic.Int32
	var source pulse.Stream[int]
	var endpoints [2]pulse.Stream[int]
	wait := func(rc *pulse.RunCtx, signal <-chan struct{}) error {
		return rc.WaitStream(func(ctx context.Context) error {
			select {
			case <-signal:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}
	if err := pulse.ProduceStream(g, "source", "先完整接纳两条", input, func(rc *pulse.RunCtx, writer *pulse.StreamWriter[int]) error {
		sourceCalls.Add(1)
		var err error
		source, err = pulse.Get(rc, input)
		if err != nil {
			return err
		}
		for value := 1; value <= 2; value++ {
			if err := writer.SendAt(value, value); err != nil {
				return err
			}
		}
		return nil
	}, pulse.WithBuffer(2), pulse.WithStreamSource("tee-all-stop/integers", true)); err != nil {
		t.Fatal(err)
	}
	if err := pulse.TeeStream(g, "tee", "全支停止不确认未转发消息", input, branches, pulse.WithBuffer(1),
		pulse.WithStreamAspects(func(rc *pulse.RunCtx, next func(*pulse.RunCtx) error) error {
			teeCalls.Add(1)
			if err := wait(rc, sourceConfirmed); err != nil {
				return err
			}
			return next(rc)
		})); err != nil {
		t.Fatal(err)
	}
	for i, key := range branches {
		testAdd(t, g, pulse.NewNode(key.Name(), "只确认第一条后停止", pulse.Requires(key), nil, func(rc *pulse.RunCtx) error {
			sinkCalls[i].Add(1)
			var err error
			endpoints[i], err = pulse.Get(rc, key)
			if err != nil {
				return err
			}
			first, err := endpoints[i].Receive(rc)
			if err != nil {
				return err
			}
			if first.Sequence != 1 || first.Value != 1 {
				return fmt.Errorf("%s received %+v, want only the first input", key.Name(), first)
			}
			if err := wait(rc, secondOpened); err != nil {
				return err
			}
			if snapshot := source.Snapshot(); snapshot.LastSequence != 2 || snapshot.AckedThrough != 1 {
				return fmt.Errorf("second message was not pending before Stop: %+v", snapshot)
			}
			// 两支与 Tee 共享唯一业务额度。此处 Ack 唤醒 Tee 后，它必须等本
			// 消费者 Stop 并退出才能恢复；最后一支退出后预检必定看到全 Stop。
			if err := first.Ack(); err != nil {
				return err
			}
			return endpoints[i].Stop(rc)
		}))
	}
	options := testOptions("tee-all-stop-run")
	execution, err := runtime.Run(ctx, g, pulse.Inputs{}, options)
	if err != nil {
		t.Fatalf("normal all-branch Stop became a run failure: %v", err)
	}
	if !openedSecond.Load() {
		t.Fatal("did not exercise Stop after the second durable message attempt opened")
	}
	if snapshot := source.Snapshot(); snapshot.LastSequence != 2 || snapshot.AckedThrough != 1 || snapshot.InFlight != 0 || !snapshot.Stopped || snapshot.Terminal != pulse.StreamCompleted || snapshot.Err != nil {
		t.Fatalf("input Stop fabricated an acknowledgement or source terminal: %+v", snapshot)
	}
	for i, stream := range endpoints {
		if snapshot := stream.Snapshot(); snapshot.LastSequence != 1 || snapshot.AckedThrough != 1 || snapshot.InFlight != 0 || !snapshot.Stopped || snapshot.Terminal != pulse.StreamStopped || snapshot.Err != nil {
			t.Fatalf("branch %d received or confirmed extra work: %+v", i, snapshot)
		}
	}
	before := readRecord(t, store, options.RunID)
	assertTeeAllStopRecord(t, before)
	definition, err := inspectDefinition(g, codecs.snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if err := (&runState{def: definition, record: cloneRecord(before)}).validateLoaded(); err != nil {
		t.Fatalf("normal Stop left an invalid Messages/Attempts record: %v", err)
	}
	var restored atomic.Int32
	options.OnEvent = func(event Event) error {
		if event.Kind != EventRestored {
			return fmt.Errorf("unexpected resume event: %+v", event)
		}
		restored.Add(1)
		return nil
	}
	// 新 Runtime 从 MemoryStore 重新 Load，不能依靠前一次 runState 的内存账。
	resumed, err := testRuntime(t, store, codecs).Resume(ctx, g, pulse.Inputs{}, options)
	if err != nil {
		t.Fatalf("all-branch Stop could not Resume: %v", err)
	}
	if resumed.ID() == execution.ID() || restored.Load() != 4 || sourceCalls.Load() != 1 || teeCalls.Load() != 1 || sinkCalls[0].Load() != 1 || sinkCalls[1].Load() != 1 {
		t.Fatalf("Resume reentered completed business: restored=%d source=%d tee=%d sinks=[%d,%d]", restored.Load(), sourceCalls.Load(), teeCalls.Load(), sinkCalls[0].Load(), sinkCalls[1].Load())
	}
	after := readRecord(t, store, options.RunID)
	assertTeeAllStopRecord(t, after)
	if !reflect.DeepEqual(before.Messages, after.Messages) || !reflect.DeepEqual(before.Attempts, after.Attempts) || !reflect.DeepEqual(before.Managed, after.Managed) || !reflect.DeepEqual(before.Scopes, after.Scopes) {
		t.Fatal("Resume changed historical messages, attempts, state or completed nodes")
	}
	if err := (&runState{def: definition, record: cloneRecord(after)}).validateLoaded(); err != nil {
		t.Fatalf("Resume left invalid persisted relationships: %v", err)
	}
	t.Logf("Run/Resume preserved input last=2 acked=1 stopped=true; Tee message 1 confirmed, message 2 active on stopped input; source/tee/sinks called once; revisions=%d->%d", before.Revision, after.Revision)
}

func assertTeeAllStopRecord(t *testing.T, record RunRecord) {
	t.Helper()
	if len(record.Streams) != 3 || len(record.Messages) != 2 || len(record.Attempts) != 4 || len(record.Scopes[record.RootScope].Nodes) != 4 {
		t.Fatalf("unexpected stream/message/attempt/node counts: %d/%d/%d/%d", len(record.Streams), len(record.Messages), len(record.Attempts), len(record.Scopes[record.RootScope].Nodes))
	}
	for _, stream := range record.Streams {
		last, terminal := uint64(1), string(pulse.StreamStopped)
		if stream.Stream.ID.Key == "input" {
			last, terminal = 2, string(pulse.StreamCompleted)
		}
		if stream.LastSequence != last || stream.AckedThrough != 1 || len(stream.Pending) != 0 || !stream.Stream.Stopped || stream.Stream.Terminal != terminal {
			t.Fatalf("invalid all-stop stream record: %+v", stream)
		}
	}
	for id, attempt := range record.Attempts {
		node, exists := record.Scopes[record.RootScope].Nodes[attempt.NodeID]
		if !exists || node.Attempt != id || node.Reason != string(pulse.NodeCompleted) || attempt.State != AttemptConfirmed || attempt.Parent != record.RootScope || attempt.Boundary != "" || attempt.Index != 1 || attempt.Limit != 1 {
			t.Fatalf("normal Stop left an unfinished/failed node attempt: id=%s attempt=%+v node=%+v", id, attempt, node)
		}
	}
	seen := make(map[uint64]bool)
	for id, message := range record.Messages {
		parent, exists := record.Attempts[message.Parent]
		stream := record.Streams[streamKey(message.Input)]
		wantState := AttemptConfirmed
		if message.Sequence == 2 {
			wantState = AttemptActive // Stop 消除读取义务，不虚构确认或失败。
		}
		if !exists || parent.NodeID != "tee" || parent.State != AttemptConfirmed || message.NodeID != "tee" || message.Scope != record.RootScope || message.Input.Key != "input" || message.Sequence < 1 || message.Sequence > 2 || seen[message.Sequence] || message.State != wantState || message.Index != 1 || message.Limit != 1 || !stream.Stream.Stopped {
			t.Fatalf("invalid message/parent/stopped-input relation: id=%s message=%+v parent=%+v", id, message, parent)
		}
		seen[message.Sequence] = true
	}
}
