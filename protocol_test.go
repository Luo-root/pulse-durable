package durable

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/Luo-root/pulse"
)

func TestPersistentRetryLimitMismatchStopsBeforeBusiness(t *testing.T) {
	store := &faultStore{Store: NewMemoryStore(), after: true, failure: errors.New("acknowledgment lost"), match: func(r RunRecord) bool {
		for _, a := range r.Attempts {
			if a.Boundary != "" && a.State == AttemptFailed {
				return true
			}
		}
		return false
	}}
	var calls atomic.Int32
	makeGraph := func(limit int) *pulse.Graph {
		g := testGraph(t, "graph")
		testAdd(t, g, pulse.NewNode("work", "", nil, nil, func(*pulse.RunCtx) error { calls.Add(1); return errors.New("failed") }, pulse.Retry(limit, 0)))
		return g
	}
	rt := testRuntime(t, store, NewCodecs())
	if _, err := rt.Run(context.Background(), makeGraph(2), pulse.Inputs{}, testOptions("run")); err == nil {
		t.Fatal("fault ignored")
	}
	if _, err := rt.Resume(context.Background(), makeGraph(3), pulse.Inputs{}, testOptions("run")); !errors.Is(err, ErrConflict) {
		t.Fatalf("Retry limit drift: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatal("limit mismatch entered business")
	}
}

func TestChildActualInputFingerprintRejectsChangedFixedValue(t *testing.T) {
	var calls atomic.Int32
	makeGraph := func(fixed int) *pulse.Graph {
		child := testGraph(t, "child")
		input, output := pulse.NewKey[int]("input"), pulse.NewKey[int]("output")
		if err := child.DeclareInput(pulse.NewPort(input)); err != nil {
			t.Fatal(err)
		}
		if err := child.DeclareOutput(pulse.NewPort(output)); err != nil {
			t.Fatal(err)
		}
		testAdd(t, child, pulse.NewNode("work", "", pulse.Requires(input), pulse.Provides(output), func(rc *pulse.RunCtx) error {
			calls.Add(1)
			v, err := pulse.Get(rc, input)
			if err != nil {
				return err
			}
			return pulse.Set(rc, output, v)
		}))
		g := testGraph(t, "parent")
		parentOut := pulse.NewKey[int]("parent-output")
		if err := pulse.Sub(g, "mount", "", []pulse.SubBind{pulse.Fix(input, fixed), pulse.Out(output, parentOut)}, child); err != nil {
			t.Fatal(err)
		}
		return g
	}
	store := &faultStore{Store: NewMemoryStore(), after: true, failure: errors.New("uncertain child commit"), match: func(r RunRecord) bool { return recordHasNode(r, "child", "work") }}
	rt := testRuntime(t, store, NewCodecs())
	if _, err := rt.Run(context.Background(), makeGraph(1), pulse.Inputs{}, testOptions("run")); err == nil {
		t.Fatal("fault ignored")
	}
	if _, err := rt.Resume(context.Background(), makeGraph(2), pulse.Inputs{}, testOptions("run")); !errors.Is(err, ErrConflict) {
		t.Fatalf("fixed input changed: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatal("changed child input entered business")
	}
	if _, err := rt.Resume(context.Background(), makeGraph(1), pulse.Inputs{}, testOptions("run")); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatal("same child input reran confirmed business")
	}
}

func TestReliableChildErrorCannotBeSwallowedByParent(t *testing.T) {
	var calls [3]atomic.Int32
	child, out := childChain(t, &calls)
	g := testGraph(t, "parent")
	bridged := pulse.NewKey[int]("bridged")
	if err := pulse.Sub(g, "mount", "", []pulse.SubBind{pulse.Out(out, bridged)}, child,
		func(rc *pulse.RunCtx, next func(*pulse.RunCtx) error) error { _ = next(rc); return nil }); err != nil {
		t.Fatal(err)
	}
	store := &faultStore{Store: NewMemoryStore(), after: true, failure: pulse.ErrSkipped, match: func(r RunRecord) bool { return recordHasNode(r, "child", "C2") }}
	rt := testRuntime(t, store, NewCodecs())
	if _, err := rt.Run(context.Background(), g, pulse.Inputs{}, testOptions("run")); err == nil {
		t.Fatal("parent swallowed reliability failure")
	}
	if recordHasNode(readRecord(t, store, "run"), "parent", "mount") {
		t.Fatal("parent committed after swallowed child storage error")
	}
	if _, err := rt.Resume(context.Background(), g, pulse.Inputs{}, testOptions("run")); err != nil {
		t.Fatal(err)
	}
	for i := range calls {
		if calls[i].Load() != 1 {
			t.Fatal("recovery ignored authoritative child records")
		}
	}
}

func TestProtocolRejectsFailedAncestorsAndCommitsNodeRootAtomically(t *testing.T) {
	ctx := context.Background()
	var calls [3]atomic.Int32
	child, output := childChain(t, &calls)
	g := testGraph(t, "parent")
	parentOut := pulse.NewKey[int]("bridged")
	if err := pulse.Sub(g, "mount", "", []pulse.SubBind{pulse.Out(output, parentOut)}, child); err != nil {
		t.Fatal(err)
	}
	def, err := inspectDefinition(g, NewCodecs().snapshot())
	if err != nil {
		t.Fatal(err)
	}
	store := NewMemoryStore()
	session, err := store.Acquire(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	rootScope, err := randomID()
	if err != nil {
		t.Fatal(err)
	}
	hash, err := def.inputsFingerprint(def.root, nil)
	if err != nil {
		t.Fatal(err)
	}
	state := &runState{session: session, def: def, record: RunRecord{Format: RecordFormat, RunID: "run", Version: "1", Definition: def.fingerprint,
		RootScope: rootScope, Scopes: make(map[string]ScopeRecord), Attempts: make(map[string]AttemptRecord)}}
	proposed := cloneRecord(state.record)
	proposed.Inputs = hash
	proposed.Scopes[rootScope] = ScopeRecord{Graph: "parent", Inputs: hash, Nodes: make(map[string]NodeRecord)}
	if err := state.save(ctx, proposed); err != nil {
		t.Fatal(err)
	}
	root, err := state.openAttempt(ctx, pulse.CheckpointAttemptRequest{Site: pulse.Site{Graph: "parent"}, Parent: rootScope, NodeID: "mount", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	retryReq := pulse.CheckpointAttemptRequest{Site: pulse.Site{Graph: "parent"}, Parent: root.Scope, NodeID: "mount", Boundary: "aspect/0", Limit: 2}
	first, err := state.openAttempt(ctx, retryReq)
	if err != nil {
		t.Fatal(err)
	}
	version := state.record.Revision
	again, err := state.openAttempt(ctx, retryReq)
	if err != nil || again.Scope != first.Scope || state.record.Revision != version {
		t.Fatal("active retry was not acknowledged without a new Save")
	}
	childSite := pulse.Site{Graph: "child", Path: "mount"}
	if _, err := state.openChild(ctx, pulse.CheckpointChild{Site: childSite, Attempt: first.Scope, NodeID: "mount", Graph: "child"}); err != nil {
		t.Fatal(err)
	}
	c1, err := state.openAttempt(ctx, pulse.CheckpointAttemptRequest{Site: childSite, Parent: first.Scope, NodeID: "C1", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	c2, err := state.openAttempt(ctx, pulse.CheckpointAttemptRequest{Site: childSite, Parent: first.Scope, NodeID: "C2", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	commitC1 := pulse.CheckpointCommit{Site: childSite, Scope: first.Scope, Attempt: c1.Scope, Node: pulse.NodeCheckpoint{NodeID: "C1", Reason: pulse.NodeCompleted,
		Outputs: []pulse.CheckpointOutput{{Key: pulse.CheckpointKey{Name: "one", Type: reflect.TypeFor[int]()}, State: pulse.SlotStateReady, Value: 1}}}}
	if err := state.commit(ctx, commitC1); err != nil {
		t.Fatal(err)
	}
	loaded, err := session.Load(ctx)
	if err != nil || loaded.Attempts[c1.Scope].State != AttemptConfirmed || loaded.Scopes[first.Scope].Nodes["C1"].Attempt != c1.Scope {
		t.Fatal("node record and root confirmation did not land together")
	}
	if err := state.failAttempt(ctx, first.Scope); err != nil {
		t.Fatal(err)
	}
	second, err := state.openAttempt(ctx, retryReq)
	if err != nil || second.Index != 2 || second.Scope == first.Scope {
		t.Fatalf("next retry: %+v %v", second, err)
	}
	version = state.record.Revision
	commitC2 := pulse.CheckpointCommit{Site: childSite, Scope: first.Scope, Attempt: c2.Scope, Node: pulse.NodeCheckpoint{NodeID: "C2", Reason: pulse.NodeCompleted,
		Outputs: []pulse.CheckpointOutput{{Key: pulse.CheckpointKey{Name: "two", Type: reflect.TypeFor[int]()}, State: pulse.SlotStateReady, Value: 2}}}}
	if err := state.commit(ctx, commitC2); err == nil {
		t.Fatal("failed ancestor allowed a late descendant commit")
	}
	if _, err := state.openChild(ctx, pulse.CheckpointChild{Site: childSite, Attempt: first.Scope, NodeID: "mount", Graph: "child"}); err == nil {
		t.Fatal("failed child scope reopened")
	}
	if state.record.Revision != version {
		t.Fatal("invalid old-scope mutation saved a record")
	}
	if err := state.failAttempt(ctx, second.Scope); err != nil {
		t.Fatal(err)
	}
	exhausted, err := state.openAttempt(ctx, retryReq)
	if err != nil || !exhausted.Exhausted || exhausted.Index != 2 {
		t.Fatalf("exhausted cursor: %+v %v", exhausted, err)
	}
}

func TestSaveFailureLeavesRuntimeRecordUnchanged(t *testing.T) {
	ctx := context.Background()
	base := NewMemoryStore()
	initial, err := base.Acquire(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	record := RunRecord{RunID: "run", Revision: 1, Scopes: map[string]ScopeRecord{"root": {Graph: "graph", Nodes: map[string]NodeRecord{}}}}
	if err := initial.Save(ctx, record, 0); err != nil {
		t.Fatal(err)
	}
	if err := initial.Close(); err != nil {
		t.Fatal(err)
	}
	store := &faultStore{Store: base, failure: errors.New("save failed"), match: func(RunRecord) bool { return true }}
	session, err := store.Acquire(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	state := &runState{session: session, record: cloneRecord(record)}
	proposed := cloneRecord(record)
	proposed.Scopes["root"].Nodes["new"] = NodeRecord{NodeID: "new"}
	if err := state.save(ctx, proposed); err == nil {
		t.Fatal("save failure ignored")
	}
	if state.record.Revision != 1 || len(state.record.Scopes["root"].Nodes) != 0 {
		t.Fatal("failed proposed record leaked into runtime memory")
	}
	loaded, err := session.Load(ctx)
	if err != nil || loaded.Revision != 1 || len(loaded.Scopes["root"].Nodes) != 0 {
		t.Fatal("failed CAS modified stored record")
	}
	if err := state.save(ctx, proposed); err == nil || store.saves.Load() != 1 {
		t.Fatal("uncertain Save was retried")
	}
}
