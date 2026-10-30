package durable

import (
	"context"
	"errors"
	"github.com/Luo-root/pulse"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

func TestActiveStreamReferenceChecksSourceBeforeCanonicalCache(t *testing.T) {
	protocol := newStreamProtocol(t)
	if err := protocol.append(1); err != nil {
		t.Fatal(err)
	}
	historical := protocol.s.record.Streams[streamKey(streamID(protocol.input.Stream.ID))].Stream
	want, err := protocol.s.restoreStreamReference(protocol.inKey, historical)
	if err != nil {
		t.Fatalf("valid live source reference was rejected: %v", err)
	}
	for name, mutate := range map[string]func(*StreamDescriptorRecord){
		"source":     func(d *StreamDescriptorRecord) { d.SourceID = "different-source" },
		"attempt":    func(d *StreamDescriptorRecord) { d.Attempt = protocol.processor.Scope },
		"graph":      func(d *StreamDescriptorRecord) { d.Graph = "different-graph" },
		"path":       func(d *StreamDescriptorRecord) { d.Path = "different-path" },
		"capacity":   func(d *StreamDescriptorRecord) { d.Capacity++ },
		"generation": func(d *StreamDescriptorRecord) { d.ID.Generation++ },
		"terminal":   func(d *StreamDescriptorRecord) { d.Terminal = string(pulse.StreamCompleted) },
		"stop":       func(d *StreamDescriptorRecord) { d.Stopped = true },
	} {
		t.Run(name, func(t *testing.T) {
			bad := historical
			mutate(&bad)
			if _, err := protocol.s.restoreStreamReference(protocol.inKey, bad); err == nil {
				t.Fatal("cached active reference bypassed source validation")
			}
			if got, err := protocol.s.restoreStreamReference(protocol.inKey, historical); err != nil || got != want {
				t.Fatalf("bad reference poisoned canonical source: %+v %v", got, err)
			}
		})
	}
	logical := streamKey(historical.ID)
	latest := protocol.s.record.Streams[logical]
	latest.Stream.ID.Generation++
	protocol.s.record.Streams[logical] = latest
	if got, err := protocol.s.restoreStreamReference(protocol.inKey, historical); err != nil || got != want {
		t.Fatalf("generation change created a new canonical reference: %+v %v", got, err)
	}
}

func TestActiveStreamReferenceRejectsInvalidOwner(t *testing.T) {
	for _, state := range []AttemptState{AttemptFailed, AttemptConfirmed} {
		t.Run(string(state), func(t *testing.T) {
			protocol := newStreamProtocol(t)
			reference := protocol.s.record.Streams[streamKey(streamID(protocol.input.Stream.ID))].Stream
			owner := protocol.s.record.Attempts[reference.Attempt]
			owner.State = state
			protocol.s.record.Attempts[reference.Attempt] = owner
			if err := protocol.s.checkStreamReference(protocol.s.record, reference); err == nil {
				t.Fatal("active source without a live owner was accepted")
			}
		})
	}
}

func TestActiveStreamReferenceRequiresResumableSourceOnRestore(t *testing.T) {
	protocol := newStreamProtocol(t)
	logical := streamKey(streamID(protocol.input.Stream.ID))
	stream := protocol.s.record.Streams[logical]
	stream.Stream.Resumable = false
	protocol.s.record.Streams[logical] = stream
	if err := protocol.s.checkStreamReference(protocol.s.record, stream.Stream); err != nil {
		t.Fatalf("normal run must permit a non-resumable active alias: %v", err)
	}
	if _, err := protocol.s.restoreStreamReference(protocol.inKey, stream.Stream); err == nil {
		t.Fatal("active non-resumable source was accepted for recovery")
	}
}

func TestCompleteSourceNodeStillRequiresStreamTerminal(t *testing.T) {
	protocol := newStreamProtocol(t)
	outputs, err := protocol.s.def.encodeOutputs([]pulse.CheckpointOutput{{Key: protocol.inKey, State: pulse.SlotStateReady, Stream: &protocol.input.Stream}})
	if err != nil {
		t.Fatal(err)
	}
	node := NodeRecord{NodeID: "source", Attempt: protocol.source.Scope, Reason: string(pulse.NodeCompleted), Outputs: outputs}
	if _, err := protocol.s.def.decodeNode(protocol.s.def.root, node); err == nil {
		t.Fatal("ordinary completed source accepted an unfinished owned stream")
	}
	before := cloneRecord(protocol.s.record)
	if err := protocol.s.commit(context.Background(), pulse.CheckpointCommit{Site: protocol.site, Scope: protocol.s.record.RootScope, Attempt: protocol.source.Scope,
		Node: pulse.NodeCheckpoint{NodeID: "source", Reason: pulse.NodeCompleted, Outputs: []pulse.CheckpointOutput{{Key: protocol.inKey, State: pulse.SlotStateReady, Stream: &protocol.input.Stream}}}}); err == nil {
		t.Fatal("source confirmed without a terminal")
	}
	if !reflect.DeepEqual(before, protocol.s.record) {
		t.Fatal("rejected source commit modified its record")
	}
}

func TestHistoricalStreamReferencePreservesSourceConstraints(t *testing.T) {
	for _, terminal := range []pulse.StreamTerminal{"", pulse.StreamFailed, pulse.StreamCanceled} {
		t.Run(string(terminal), func(t *testing.T) {
			protocol := newStreamProtocol(t)
			logical := streamKey(streamID(protocol.input.Stream.ID))
			stream := protocol.s.record.Streams[logical]
			history := stream.Stream // Sub 在其实际源结束前已确认。
			stream.Stream.Terminal = string(terminal)
			protocol.s.record.Streams[logical] = stream
			owner := protocol.s.record.Attempts[history.Attempt]
			owner.State = AttemptFailed
			protocol.s.record.Attempts[history.Attempt] = owner
			if err := protocol.s.checkStreamReferenceRecord(protocol.s.record, history, true); err != nil {
				t.Fatalf("valid historical association rejected: %v", err)
			}
			if err := protocol.s.checkStreamReference(protocol.s.record, history); err == nil {
				t.Fatal("historical shape validation authorized an invalid live source")
			}
			for name, mutate := range map[string]func(*StreamDescriptorRecord){
				"source":     func(d *StreamDescriptorRecord) { d.SourceID = "forged" },
				"attempt":    func(d *StreamDescriptorRecord) { d.Attempt = protocol.processor.Scope },
				"graph":      func(d *StreamDescriptorRecord) { d.Graph = "forged" },
				"path":       func(d *StreamDescriptorRecord) { d.Path = "forged" },
				"capacity":   func(d *StreamDescriptorRecord) { d.Capacity++ },
				"generation": func(d *StreamDescriptorRecord) { d.ID.Generation++ },
				"terminal":   func(d *StreamDescriptorRecord) { d.Terminal = string(pulse.StreamCompleted) },
				"stop":       func(d *StreamDescriptorRecord) { d.Stopped = true },
			} {
				t.Run(name, func(t *testing.T) {
					bad := history
					mutate(&bad)
					if err := protocol.s.checkStreamReferenceRecord(protocol.s.record, bad, true); err == nil {
						t.Fatal("structural history check discarded a source constraint")
					}
				})
			}
		})
	}
}

func TestStreamDescriptorStoppedRequiresStoppedGroup(t *testing.T) {
	scope, err := randomID()
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := randomID()
	if err != nil {
		t.Fatal(err)
	}
	definition := &definition{codecs: NewCodecs().snapshot()}
	key := pulse.CheckpointKey{Name: "values", Type: reflect.TypeFor[pulse.Stream[int]](), Element: reflect.TypeFor[int]()}
	descriptor := pulse.CheckpointStreamDescriptor{
		ID:    pulse.CheckpointStreamID{Scope: scope, NodeID: "source", Key: key.Name, Group: "default", Generation: 1},
		Graph: "descriptor-state", Attempt: attempt, Element: key.Element,
		Source: pulse.CheckpointStreamSource{ID: "numbers", Resumable: true}, Capacity: 1,
		Terminal: pulse.StreamStopped,
	}
	if _, err := definition.encodeDescriptor(key, descriptor); err == nil {
		t.Error("encoding accepted writer stopped without its reading group being stopped")
	}
	descriptor.Stopped = true
	record, err := definition.encodeDescriptor(key, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	record.Stopped = false
	if _, err := definition.decodeDescriptor(key, record); err == nil {
		t.Error("decoding accepted an impossible stopped/false historical reference")
	}
	// 读取组 Stop 可以早于或晚于写端完成；只有 stopped/false 是不可能的组合。
	for _, terminal := range []pulse.StreamTerminal{"", pulse.StreamCompleted, pulse.StreamStopped, pulse.StreamFailed, pulse.StreamCanceled} {
		for _, stopped := range []bool{false, true} {
			if terminal == pulse.StreamStopped && !stopped {
				continue
			}
			descriptor.Terminal, descriptor.Stopped = terminal, stopped
			record, err := definition.encodeDescriptor(key, descriptor)
			if err != nil {
				t.Fatalf("valid terminal=%q stopped=%v rejected: %v", terminal, stopped, err)
			}
			decoded, err := definition.decodeDescriptor(key, record)
			if err != nil || decoded != descriptor {
				t.Fatalf("valid terminal=%q stopped=%v changed on decode: %+v, %v", terminal, stopped, decoded, err)
			}
		}
	}
	t.Run("cached-history-is-checked-before-canonicalization", func(t *testing.T) {
		descriptor.Terminal, descriptor.Stopped = pulse.StreamStopped, true
		valid, err := definition.encodeDescriptor(key, descriptor)
		if err != nil {
			t.Fatal(err)
		}
		state := &runState{def: definition, record: RunRecord{
			Streams:  map[string]StreamRecord{streamKey(valid.ID): {Stream: valid}},
			Attempts: map[string]AttemptRecord{attempt: {Parent: scope, NodeID: "source", Index: 1, Limit: 1, State: AttemptConfirmed}},
			Scopes: map[string]ScopeRecord{scope: {Graph: descriptor.Graph, Nodes: map[string]NodeRecord{
				"source": {NodeID: "source", Attempt: attempt, Reason: string(pulse.NodeCompleted), Outputs: []ValueRecord{{Name: key.Name, Stream: &valid}}},
			}}},
		}}
		if _, err := state.restoreStreamReference(key, valid); err != nil {
			t.Fatal("valid reference did not warm the restore cache: ", err)
		}
		invalid := valid
		invalid.Stopped = false
		if _, err := state.restoreStreamReference(key, invalid); err == nil {
			t.Fatal("canonical cache concealed a stopped/false historical reference")
		}
	})
}

func TestChildStreamBindingRestoresWhileAnotherInputWasPending(t *testing.T) {
	var calls atomic.Int32
	build := func() *pulse.Graph {
		g := testGraph(t, "stream-bindings-parent")
		input, later := pulse.NewKey[pulse.Stream[int]]("input"), pulse.NewKey[int]("later")
		childInput, childLater := pulse.NewKey[pulse.Stream[int]]("child-input"), pulse.NewKey[int]("child-later")
		gate := make(chan struct{})
		var once sync.Once
		testAdd(t, g, pulse.NewNode("source", "", nil, pulse.Provides(input), func(rc *pulse.RunCtx) error {
			writer, err := pulse.OpenStream(rc, input, pulse.WithBuffer(4), pulse.WithStreamSource("binding-source", true))
			if err != nil {
				return err
			}
			for i := int(writer.Position().Sequence) + 1; i <= 3; i++ {
				if err := writer.SendAt(i, i); err != nil {
					return err
				}
			}
			return nil
		}))
		testAdd(t, g, pulse.NewNode("later", "", nil, pulse.Provides(later), func(rc *pulse.RunCtx) error {
			select {
			case <-gate:
			case <-rc.Context().Done():
				return rc.Context().Err()
			}
			return pulse.Set(rc, later, 7)
		}))
		child := testGraph(t, "stream-bindings-child")
		if err := child.DeclareInput(pulse.NewPort(childInput)); err != nil {
			t.Fatal(err)
		}
		if err := child.DeclareInput(pulse.NewPort(childLater)); err != nil {
			t.Fatal(err)
		}
		testAdd(t, child, pulse.NewNode("consume", "", pulse.Requires(childInput), nil, func(rc *pulse.RunCtx) error {
			stream, err := pulse.Get(rc, childInput)
			if err != nil {
				return err
			}
			// 第一轮在 late 仍 pending 时必须能进入这里，否则 gate 永远打不开。
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
					once.Do(func() { close(gate) })
					sum, _, err := pulse.GetMessageState[int](rc, "sum")
					if err != nil {
						return err
					}
					return pulse.SetMessageState(rc, "sum", sum+value)
				}); err != nil {
					return err
				}
			}
		}))
		testAdd(t, child, pulse.NewNode("after", "", pulse.Requires(childLater), nil, func(*pulse.RunCtx) error { return nil }))
		if err := pulse.Sub(g, "mount", "", []pulse.SubBind{pulse.In(input, childInput), pulse.In(later, childLater)}, child); err != nil {
			t.Fatal(err)
		}
		return g
	}
	store := &faultStore{Store: NewMemoryStore(), after: true, failure: errors.New("uncertain child message"), match: func(record RunRecord) bool {
		for _, message := range record.Messages {
			if message.NodeID == "consume" && message.State == AttemptConfirmed {
				return true
			}
		}
		return false
	}}
	rt := testRuntime(t, store, NewCodecs())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, build(), pulse.Inputs{}, testOptions("run")); err == nil {
		t.Fatal("fault ignored")
	} else {
		t.Logf("first interruption: %v", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if _, err := rt.Resume(ctx2, build(), pulse.Inputs{}, testOptions("run")); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatalf("child message calls=%d, want 3", calls.Load())
	}
}

func liveFeedbackGraph(t *testing.T, calls *atomic.Int32) *pulse.Graph {
	t.Helper()
	g, child := testGraph(t, "live-feedback"), testGraph(t, "independent-child")
	feedback, result := pulse.NewKey[int]("feedback"), pulse.NewKey[int]("result")
	inlet, answer := pulse.NewKey[int]("unused-inlet"), pulse.NewKey[int]("answer")
	if err := child.DeclareInput(pulse.NewPort(inlet)); err != nil {
		t.Fatal(err)
	}
	if err := child.DeclareOutput(pulse.NewPort(answer)); err != nil {
		t.Fatal(err)
	}
	testAdd(t, child, pulse.NewNode("independent", "不依赖入口的结果", nil, pulse.Provides(answer), func(rc *pulse.RunCtx) error {
		calls.Add(1)
		return pulse.Set(rc, answer, 42)
	}))
	if err := pulse.Sub(g, "mount", "入口可以仍在等待", []pulse.SubBind{pulse.In(feedback, inlet), pulse.Out(answer, result)}, child); err != nil {
		t.Fatal(err)
	}
	testAdd(t, g, pulse.NewNode("feedback", "子图完成后才产生入口值", pulse.Requires(result), pulse.Provides(feedback), func(rc *pulse.RunCtx) error {
		value, err := pulse.Get(rc, result)
		if err != nil {
			return err
		}
		return pulse.Set(rc, feedback, value+1)
	}))
	if err := g.DeclareOutput(pulse.NewPort(result), pulse.NewPort(feedback)); err != nil {
		t.Fatal(err)
	}
	return g
}

func TestLiveSubCommitDoesNotRequireUnusedInProducer(t *testing.T) {
	base := NewMemoryStore()
	fault := &faultStore{Store: base, after: true, failure: errors.New("interrupt after mount commit"), match: func(record RunRecord) bool {
		return recordHasNode(record, "live-feedback", "mount")
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var calls atomic.Int32
	opts := testOptions("live-feedback")
	if _, err := testRuntime(t, fault, NewCodecs()).Run(ctx, liveFeedbackGraph(t, &calls), pulse.Inputs{}, opts); err == nil || !fault.fired.Load() {
		t.Fatalf("mount did not commit before the unused inlet producer: %v", err)
	}
	before := readRecord(t, base, opts.RunID)
	if _, ok := before.Scopes[before.RootScope].Nodes["feedback"]; ok {
		t.Fatal("interruption occurred after feedback was already confirmed")
	}
	e, err := testRuntime(t, base, NewCodecs()).Resume(ctx, liveFeedbackGraph(t, &calls), pulse.Inputs{}, opts)
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]int{"result": 42, "feedback": 43} {
		got, state, err := pulse.ReadOutput(e, pulse.NewKey[int](name))
		if err != nil || state != pulse.SlotStateReady || got != want {
			t.Fatalf("restored %s=%d state=%s err=%v, want %d", name, got, state, err, want)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("confirmed child reran %d times", calls.Load())
	}
}

func TestOrdinaryCheckpointStillRequiresProducer(t *testing.T) {
	g := testGraph(t, "ordinary-dependency")
	key := pulse.NewKey[int]("input")
	testAdd(t, g, pulse.NewNode("source", "", nil, pulse.Provides(key), func(rc *pulse.RunCtx) error { return pulse.Set(rc, key, 1) }))
	testAdd(t, g, pulse.NewNode("sink", "", pulse.Requires(key), nil, nil))
	definition, err := inspectDefinition(g, NewCodecs().snapshot())
	if err != nil {
		t.Fatal(err)
	}
	state := &runState{def: definition, record: RunRecord{Scopes: map[string]ScopeRecord{
		"root": {Graph: definition.root.graph, Path: definition.root.path, Nodes: map[string]NodeRecord{}},
	}}}
	for _, historical := range []bool{false, true} {
		if err := state.checkDependencies("root", map[string]NodeRecord{}, "sink", historical); err == nil {
			t.Fatalf("ordinary dependency without a source record accepted, historical=%v", historical)
		}
	}
}
