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
