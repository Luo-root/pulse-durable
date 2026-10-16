package durable

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
)

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
