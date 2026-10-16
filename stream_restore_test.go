package durable

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
)

func streamAliasGraph(t *testing.T, id string, gate <-chan struct{}, consume func(*pulse.RunCtx, pulse.Stream[int]) error, calls *atomic.Int32) *pulse.Graph {
	t.Helper()
	g, child := testGraph(t, id), testGraph(t, "pass")
	original, alias := pulse.NewKey[pulse.Stream[int]]("original"), pulse.NewKey[pulse.Stream[int]]("alias")
	inlet := pulse.NewKey[pulse.Stream[int]]("inlet")
	trigger, childTrigger := pulse.NewKey[int]("trigger"), pulse.NewKey[int]("child-trigger")
	testAdd(t, g, pulse.NewNode("source", "", nil, pulse.Provides(original), func(rc *pulse.RunCtx) error {
		calls.Add(1)
		writer, err := pulse.OpenStream(rc, original, pulse.WithBuffer(2), pulse.WithStreamSource("alias-source", true))
		if err != nil {
			return err
		}
		for next := int(writer.Position().Sequence) + 1; next <= 2; next++ {
			if err := writer.SendAt(next, next); err != nil {
				return err
			}
		}
		return nil
	}))
	testAdd(t, g, pulse.NewNode("trigger", "", nil, pulse.Provides(trigger), func(rc *pulse.RunCtx) error {
		select {
		case <-gate:
			return pulse.Set(rc, trigger, 1)
		case <-rc.Context().Done():
			return rc.Context().Err()
		}
	}))
	if err := child.DeclareInput(pulse.NewPort(inlet), pulse.NewPort(childTrigger)); err != nil {
		t.Fatal(err)
	}
	if err := child.DeclareOutput(pulse.NewPort(inlet)); err != nil {
		t.Fatal(err)
	}
	testAdd(t, child, pulse.NewNode("wait", "", pulse.Requires(childTrigger), nil, nil))
	if err := pulse.Sub(g, "mount", "", []pulse.SubBind{pulse.In(original, inlet), pulse.In(trigger, childTrigger), pulse.Out(inlet, alias)}, child); err != nil {
		t.Fatal(err)
	}
	testAdd(t, g, pulse.NewNode("sink", "", pulse.Requires(alias), nil, func(rc *pulse.RunCtx) error {
		stream, err := pulse.Get(rc, alias)
		if err != nil {
			return err
		}
		return consume(rc, stream)
	}))
	return g
}

func TestStreamAliasGenerationAcrossTwoResumes(t *testing.T) {
	base := NewMemoryStore()
	var calls atomic.Int32
	blocked := func(rc *pulse.RunCtx, _ pulse.Stream[int]) error {
		<-rc.Context().Done()
		return rc.Context().Err()
	}
	run := func(store Store, gate <-chan struct{}, consume func(*pulse.RunCtx, pulse.Stream[int]) error, resume bool) error {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		rt := testRuntime(t, store, NewCodecs())
		g := streamAliasGraph(t, "alias-generation", gate, consume, &calls)
		if resume {
			_, err := rt.Resume(ctx, g, pulse.Inputs{}, testOptions("alias-generation"))
			return err
		}
		_, err := rt.Run(ctx, g, pulse.Inputs{}, testOptions("alias-generation"))
		return err
	}
	firstFault := &faultStore{Store: base, after: true, failure: errors.New("interrupt after source"), match: func(r RunRecord) bool {
		return recordHasNode(r, "alias-generation", "source")
	}}
	if err := run(firstFault, make(chan struct{}), blocked, false); err == nil || !firstFault.fired.Load() {
		t.Fatalf("source interruption did not fire: %v", err)
	}
	secondGate := make(chan struct{})
	var once sync.Once
	observed := &streamObservationStore{Store: base, afterSave: func(r RunRecord) {
		for _, stream := range r.Streams {
			if stream.Stream.ID.Generation == 2 {
				once.Do(func() { close(secondGate) })
			}
		}
	}}
	secondFault := &faultStore{Store: observed, after: true, failure: errors.New("interrupt after alias"), match: func(r RunRecord) bool {
		return recordHasNode(r, "alias-generation", "mount")
	}}
	if err := run(secondFault, secondGate, blocked, true); err == nil || !secondFault.fired.Load() {
		t.Fatalf("alias interruption did not fire: %v", err)
	}
	record := readRecord(t, base, "alias-generation")
	nodes := record.Scopes[record.RootScope].Nodes
	if nodes["source"].Outputs[0].Stream.ID.Generation != 1 || nodes["mount"].Outputs[0].Stream.ID.Generation != 2 {
		t.Fatal("test did not preserve the different historical generations")
	}
	finalGate := make(chan struct{})
	close(finalGate)
	total := 0
	if err := run(base, finalGate, func(rc *pulse.RunCtx, stream pulse.Stream[int]) error {
		for {
			message, err := stream.Receive(rc)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			total += message.Value
			if err := message.Ack(); err != nil {
				return err
			}
		}
	}, true); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || total != 3 {
		t.Fatalf("source calls=%d total=%d", calls.Load(), total)
	}
}

func TestStreamAliasStoppedAfterCompletionResumes(t *testing.T) {
	base := NewMemoryStore()
	sourceDone, stopped := make(chan struct{}), make(chan struct{})
	var sourceOnce, stopOnce sync.Once
	var calls atomic.Int32
	observed := &streamObservationStore{Store: base, afterSave: func(r RunRecord) {
		if recordHasNode(r, "alias-stop", "source") {
			sourceOnce.Do(func() { close(sourceDone) })
		}
	}}
	build := func() *pulse.Graph {
		return streamAliasGraph(t, "alias-stop", stopped, func(rc *pulse.RunCtx, stream pulse.Stream[int]) error {
			if _, err := stream.Receive(rc); err != nil {
				return err
			}
			select {
			case <-sourceDone:
			case <-rc.Context().Done():
				return rc.Context().Err()
			}
			if err := stream.Stop(rc); err != nil {
				return err
			}
			stopOnce.Do(func() { close(stopped) })
			return nil
		}, &calls)
	}
	rt := testRuntime(t, observed, NewCodecs())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, build(), pulse.Inputs{}, testOptions("alias-stop")); err != nil {
		t.Fatal(err)
	}
	record := readRecord(t, base, "alias-stop")
	nodes := record.Scopes[record.RootScope].Nodes
	if nodes["source"].Outputs[0].Stream.Stopped || !nodes["mount"].Outputs[0].Stream.Stopped {
		t.Fatal("test did not preserve Stop before/after historical references")
	}
	if _, err := rt.Resume(ctx, build(), pulse.Inputs{}, testOptions("alias-stop")); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("completed source reran %d times", calls.Load())
	}
	t.Run("late-child-plan-keeps-one-identity", func(t *testing.T) {
		definition, err := inspectDefinition(build(), NewCodecs().snapshot())
		if err != nil {
			t.Fatal(err)
		}
		state := &runState{record: readRecord(t, base, "alias-stop"), def: definition}
		key := definition.graphs[definition.root].nodes["source"].Provides[0]
		historical := *nodes["source"].Outputs[0].Stream
		first, err := state.restoreStreamReference(key, historical)
		if err != nil {
			t.Fatal(err)
		}
		// 模拟较早恢复的端点已经 Open 升代际，较晚子图才请求同一来源。
		logical := streamKey(historical.ID)
		latest := state.record.Streams[logical]
		latest.Stream.ID.Generation++
		state.record.Streams[logical] = latest
		second, err := state.restoreStreamReference(key, historical)
		if err != nil || second != first {
			t.Fatalf("late alias acquired a different local ledger: first=%+v second=%+v err=%v", first, second, err)
		}
		invalid := historical
		invalid.SourceID = "unrelated-source"
		if _, err := state.restoreStreamReference(key, invalid); err == nil {
			t.Fatal("cached alias bypassed strict historical source validation")
		}
		invalid = historical
		invalid.ID.Generation = latest.Stream.ID.Generation + 1
		if _, err := state.restoreStreamReference(key, invalid); err == nil {
			t.Fatal("cached alias accepted a future generation")
		}
	})
}

func TestStreamLateChildRestoresSameEndpoint(t *testing.T) {
	base := NewMemoryStore()
	sourceDone, advanced := make(chan struct{}), make(chan struct{})
	var sourceOnce, advanceOnce sync.Once
	var calls atomic.Int32
	endpoints := make(chan pulse.Stream[int], 1)
	observed := &streamObservationStore{Store: base, afterSave: func(record RunRecord) {
		if recordHasNode(record, "late-root", "source") {
			sourceOnce.Do(func() { close(sourceDone) })
		}
		for _, stream := range record.Streams {
			if stream.Stream.ID.Generation == 2 {
				advanceOnce.Do(func() { close(advanced) })
			}
		}
	}}
	build := func(resume bool) *pulse.Graph {
		root, child, pass := testGraph(t, "late-root"), testGraph(t, "late-child"), testGraph(t, "late-pass")
		original, inlet := pulse.NewKey[pulse.Stream[int]]("original"), pulse.NewKey[pulse.Stream[int]]("inlet")
		passIn, alias := pulse.NewKey[pulse.Stream[int]]("pass-in"), pulse.NewKey[pulse.Stream[int]]("alias")
		trigger, childTrigger, passTrigger := pulse.NewKey[int]("trigger"), pulse.NewKey[int]("child-trigger"), pulse.NewKey[int]("pass-trigger")
		sum, final := pulse.NewKey[int]("sum"), pulse.NewKey[int]("final")
		if err := root.DeclareOutput(pulse.NewPort(original), pulse.NewPort(final)); err != nil {
			t.Fatal(err)
		}
		testAdd(t, root, pulse.NewNode("source", "", nil, pulse.Provides(original), func(rc *pulse.RunCtx) error {
			calls.Add(1)
			writer, err := pulse.OpenStream(rc, original, pulse.WithBuffer(2), pulse.WithStreamSource("late-source", true))
			if err != nil {
				return err
			}
			for next := int(writer.Position().Sequence) + 1; next <= 2; next++ {
				if err := writer.SendAt(next, next); err != nil {
					return err
				}
			}
			return nil
		}))
		testAdd(t, root, pulse.NewNode("trigger", "", nil, pulse.Provides(trigger), func(rc *pulse.RunCtx) error {
			select {
			case <-sourceDone:
				return pulse.Set(rc, trigger, 1)
			case <-rc.Context().Done():
				return rc.Context().Err()
			}
		}))
		if err := child.DeclareInput(pulse.NewPort(inlet), pulse.NewPort(childTrigger)); err != nil {
			t.Fatal(err)
		}
		if err := child.DeclareOutput(pulse.NewPort(sum)); err != nil {
			t.Fatal(err)
		}
		if err := pass.DeclareInput(pulse.NewPort(passIn), pulse.NewPort(passTrigger)); err != nil {
			t.Fatal(err)
		}
		if err := pass.DeclareOutput(pulse.NewPort(passIn)); err != nil {
			t.Fatal(err)
		}
		testAdd(t, pass, pulse.NewNode("wait", "", pulse.Requires(passTrigger), nil, nil))
		if err := pulse.Sub(child, "pass", "", []pulse.SubBind{pulse.In(inlet, passIn), pulse.In(childTrigger, passTrigger), pulse.Out(passIn, alias)}, pass); err != nil {
			t.Fatal(err)
		}
		testAdd(t, child, pulse.NewNode("consumer", "", pulse.Requires(alias), pulse.Provides(sum), func(rc *pulse.RunCtx) error {
			if !resume {
				<-rc.Context().Done()
				return rc.Context().Err()
			}
			stream, err := pulse.Get(rc, alias)
			if err != nil {
				return err
			}
			endpoints <- stream
			total := 0
			for {
				message, err := stream.Receive(rc)
				if err == io.EOF {
					return pulse.Set(rc, sum, total)
				}
				if err != nil {
					return err
				}
				total += message.Value
				if err := message.Ack(); err != nil {
					return err
				}
			}
		}))
		delayMount := func(rc *pulse.RunCtx, next func(*pulse.RunCtx) error) error {
			if resume {
				select {
				case <-advanced:
				case <-rc.Context().Done():
					return rc.Context().Err()
				}
			}
			return next(rc)
		}
		if err := pulse.Sub(root, "mount", "", []pulse.SubBind{pulse.In(original, inlet), pulse.In(trigger, childTrigger), pulse.Out(sum, final)}, child, delayMount); err != nil {
			t.Fatal(err)
		}
		return root
	}
	fault := &faultStore{Store: observed, after: true, failure: errors.New("interrupt after inner pass"), match: func(record RunRecord) bool {
		return recordHasNode(record, "late-child", "pass")
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if _, err := testRuntime(t, fault, NewCodecs()).Run(ctx, build(false), pulse.Inputs{}, testOptions("late-child")); err == nil || !fault.fired.Load() {
		t.Fatalf("wrong interruption: %v", err)
	}
	before := readRecord(t, base, "late-child")
	for _, stream := range before.Streams {
		if stream.Stream.ID.Generation != 1 || len(stream.Pending) != 2 {
			t.Fatal("test did not preserve two pending messages at generation 1")
		}
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel2()
	execution, err := testRuntime(t, observed, NewCodecs()).Resume(ctx2, build(true), pulse.Inputs{}, testOptions("late-child"))
	if err != nil {
		t.Fatal(err)
	}
	rootEndpoint, state, err := pulse.ReadOutput(execution, pulse.NewKey[pulse.Stream[int]]("original"))
	if err != nil || state != pulse.SlotStateReady {
		t.Fatal(err)
	}
	if childEndpoint := <-endpoints; childEndpoint != rootEndpoint {
		t.Fatal("late child created a second local endpoint")
	}
	total, state, err := pulse.ReadOutput(execution, pulse.NewKey[int]("final"))
	if err != nil || state != pulse.SlotStateReady || total != 3 || calls.Load() != 1 {
		t.Fatalf("calls=%d total=%d state=%s err=%v", calls.Load(), total, state, err)
	}
	if snapshot := rootEndpoint.Snapshot(); snapshot.AckedThrough != 2 || snapshot.InFlight != 0 {
		t.Fatalf("source ledger not shared: %+v", snapshot)
	}
}

func TestStreamCanonicalCacheChecksEveryReference(t *testing.T) {
	protocol := newStreamProtocol(t)
	if err := protocol.append(1); err != nil {
		t.Fatal(err)
	}
	descriptor := protocol.input.Stream
	descriptor.Terminal = pulse.StreamCompleted
	if err := protocol.s.commit(context.Background(), pulse.CheckpointCommit{Site: protocol.site, Scope: protocol.s.record.RootScope, Attempt: protocol.source.Scope,
		Node:    pulse.NodeCheckpoint{NodeID: "source", Reason: pulse.NodeCompleted, Outputs: []pulse.CheckpointOutput{{Key: protocol.inKey, State: pulse.SlotStateReady, Stream: &descriptor}}},
		Streams: []pulse.CheckpointStreamFinish{{Site: protocol.site, Attempt: protocol.source.Scope, Stream: protocol.input.Stream.ID, Terminal: pulse.StreamCompleted, LastSequence: 1, Cursor: 1}}}); err != nil {
		t.Fatal(err)
	}
	historical := protocol.s.record.Streams[streamKey(streamID(protocol.input.Stream.ID))].Stream
	expected, err := protocol.s.restoreStreamReference(protocol.inKey, historical)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*StreamDescriptorRecord){
		"source":            func(d *StreamDescriptorRecord) { d.SourceID = "forged" },
		"owner":             func(d *StreamDescriptorRecord) { d.Attempt = protocol.processor.Scope },
		"graph":             func(d *StreamDescriptorRecord) { d.Graph = "forged" },
		"path":              func(d *StreamDescriptorRecord) { d.Path = "forged" },
		"codec":             func(d *StreamDescriptorRecord) { d.TypeID = "forged" },
		"version":           func(d *StreamDescriptorRecord) { d.Version = "forged" },
		"capacity":          func(d *StreamDescriptorRecord) { d.Capacity++ },
		"resumable":         func(d *StreamDescriptorRecord) { d.Resumable = !d.Resumable },
		"terminal":          func(d *StreamDescriptorRecord) { d.Terminal = "stopped" },
		"stopped":           func(d *StreamDescriptorRecord) { d.Stopped = true },
		"future-generation": func(d *StreamDescriptorRecord) { d.ID.Generation++ },
		"zero-generation":   func(d *StreamDescriptorRecord) { d.ID.Generation = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			bad := historical
			mutate(&bad)
			if _, err := protocol.s.restoreStreamReference(protocol.inKey, bad); err == nil {
				t.Fatal("cached canonical reference bypassed validation")
			}
			actual, err := protocol.s.restoreStreamReference(protocol.inKey, historical)
			if err != nil || actual != expected {
				t.Fatal("rejected reference poisoned cache")
			}
		})
	}
}

func TestCompletedWriterTerminalSurvivesLaterStopResume(t *testing.T) {
	base := NewMemoryStore()
	sourceDone, stopped := make(chan struct{}), make(chan struct{})
	var sourceOnce, stopOnce sync.Once
	var calls atomic.Int32
	observed := &streamObservationStore{Store: base, afterSave: func(record RunRecord) {
		if recordHasNode(record, "terminal-history", "source") {
			sourceOnce.Do(func() { close(sourceDone) })
		}
	}}
	build := func() *pulse.Graph {
		g := streamAliasGraph(t, "terminal-history", stopped, func(rc *pulse.RunCtx, stream pulse.Stream[int]) error {
			if _, err := stream.Receive(rc); err != nil {
				return err
			}
			select {
			case <-sourceDone:
			case <-rc.Context().Done():
				return rc.Context().Err()
			}
			if err := stream.Stop(rc); err != nil {
				return err
			}
			stopOnce.Do(func() { close(stopped) })
			return nil
		}, &calls)
		if err := g.DeclareOutput(pulse.NewPort(pulse.NewKey[pulse.Stream[int]]("original"))); err != nil {
			t.Fatal(err)
		}
		return g
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	runtime := testRuntime(t, observed, NewCodecs())
	opts := testOptions("terminal-history")
	for _, resume := range []bool{false, true} {
		var e *pulse.Execution
		var err error
		if resume {
			e, err = runtime.Resume(ctx, build(), pulse.Inputs{}, opts)
		} else {
			e, err = runtime.Run(ctx, build(), pulse.Inputs{}, opts)
		}
		if err != nil {
			t.Fatal(err)
		}
		stream, state, err := pulse.ReadOutput(e, pulse.NewKey[pulse.Stream[int]]("original"))
		if err != nil || state != pulse.SlotStateReady {
			t.Fatalf("stream output after resume=%v: state=%s err=%v", resume, state, err)
		}
		snapshot := stream.Snapshot()
		if snapshot.Terminal != pulse.StreamCompleted || !snapshot.Stopped {
			t.Fatalf("resume=%v rewrote completed writer after later reader Stop: %+v", resume, snapshot)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("completed source reran %d times", calls.Load())
	}
}
