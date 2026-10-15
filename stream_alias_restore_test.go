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
