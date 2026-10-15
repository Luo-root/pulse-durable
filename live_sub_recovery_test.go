package durable

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
)

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
