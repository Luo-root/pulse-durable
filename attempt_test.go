package durable

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Luo-root/pulse"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
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
	if err := state.initialize(ctx, proposed); err != nil {
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
	record := contractSeed("run")
	if err := initial.Initialize(ctx, record); err != nil {
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
	if _, err := session.Load(ctx); err != nil {
		t.Fatal(err)
	}
	tx := newRecordTransaction(record.Revision)
	_ = tx.putNode("root", "new", nil, NodeRecord{NodeID: "new"})
	if err := state.commitRecordTransaction(ctx, tx); err == nil {
		t.Fatal("save failure ignored")
	}
	if state.record.Revision != 1 || len(state.record.Scopes["root"].Nodes) != 0 {
		t.Fatal("failed proposed record leaked into runtime memory")
	}
	loaded, err := session.Load(ctx)
	if err != nil || loaded.Revision != 1 || len(loaded.Scopes["root"].Nodes) != 0 {
		t.Fatal("failed CAS modified stored record")
	}
	if err := state.commitRecordTransaction(ctx, tx); err == nil || store.saves.Load() != 1 {
		t.Fatal("uncertain Save was retried")
	}
}

func TestAttemptObservationReportsAllocationAndActiveReclamation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	g := testGraph(t, "observed")
	testAdd(t, g, pulse.NewNode("work", "真实桥分支", nil, nil, nil))
	definition, err := inspectDefinition(g, NewCodecs().snapshot())
	if err != nil {
		t.Fatal(err)
	}
	session, err := NewMemoryStore().Acquire(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	state := &runState{session: session, def: definition, record: RunRecord{
		RunID: "run", Version: "1", Definition: definition.fingerprint, RootScope: "root",
		Scopes: map[string]ScopeRecord{"root": {Graph: "observed", Nodes: map[string]NodeRecord{}}}, Attempts: map[string]AttemptRecord{},
	}}
	state.record.Inputs = digest(nil)
	rootSeed := state.record.Scopes["root"]
	rootSeed.Inputs = state.record.Inputs
	state.record.Scopes["root"] = rootSeed
	if err := state.initialize(ctx, cloneRecord(state.record)); err != nil {
		t.Fatal(err)
	}
	open := func(req pulse.CheckpointAttemptRequest, resumed bool) pulse.CheckpointAttempt {
		t.Helper()
		a, err := state.openAttempt(ctx, req)
		if err != nil || a.Resumed == nil || *a.Resumed != resumed {
			t.Fatalf("resumed=%v cursor=%+v err=%v", resumed, a, err)
		}
		return a
	}
	rootReq := pulse.CheckpointAttemptRequest{Site: pulse.Site{Graph: "observed", Execution: "first"}, Parent: "root", NodeID: "work", Limit: 1}
	root := open(rootReq, false)
	revision := state.record.Revision
	rootReq.Site.Execution = "fresh-execution"
	if active := open(rootReq, true); active.Scope != root.Scope || active.Index != root.Index || state.record.Revision != revision {
		t.Fatal("active root reallocation changed identity or saved a new revision")
	}
	retryReq := pulse.CheckpointAttemptRequest{Site: rootReq.Site, Parent: root.Scope, NodeID: "work", Boundary: "aspect/0", Limit: 2}
	first := open(retryReq, false)
	revision = state.record.Revision
	if active := open(retryReq, true); active.Scope != first.Scope || active.Index != 1 || state.record.Revision != revision {
		t.Fatal("active Retry reallocation changed identity, budget or revision")
	}
	if err := state.failAttempt(ctx, first.Scope); err != nil {
		t.Fatal(err)
	}
	second := open(retryReq, false)
	if second.Scope == first.Scope || second.Index != 2 {
		t.Fatalf("second allocation=%+v first=%+v", second, first)
	}
	if active := open(retryReq, true); active.Scope != second.Scope || active.Index != 2 {
		t.Fatal("second active attempt was not reclaimed")
	}
	if err := state.failAttempt(ctx, second.Scope); err != nil {
		t.Fatal(err)
	}
	revision = state.record.Revision
	exhausted, err := state.openAttempt(ctx, retryReq)
	if err != nil || !exhausted.Exhausted || exhausted.Index != 2 || exhausted.Resumed != nil || state.record.Revision != revision {
		t.Fatalf("exhaustion changed budget or claimed active execution: %+v err=%v", exhausted, err)
	}
	data, err := json.Marshal(state.record)
	if err != nil || strings.Contains(strings.ToLower(string(data)), "resumed") {
		t.Fatalf("observation leaked into persistent format: %s err=%v", data, err)
	}
}

func committedAttemptValues(t *testing.T, skipped bool) streamProtocol {
	t.Helper()
	p := newStreamProtocol(t)
	ctx := context.Background()
	if err := p.append(1); err != nil {
		t.Fatal(err)
	}
	message, err := p.s.openMessage(ctx, p.messageRequest(1, 2))
	if err != nil {
		t.Fatal(err)
	}
	output := pulse.CheckpointOutput{Key: p.value, State: pulse.SlotStateReady, Value: 42}
	if skipped {
		output.State, output.Value = pulse.SlotStateSkipped, nil
	}
	if err := p.s.commitMessage(ctx, pulse.CheckpointMessageCommit{Site: p.site, Scope: p.s.record.RootScope, Attempt: message.Attempt.Scope,
		Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: p.input.Stream.ID, Sequence: 1}},
		Values: []pulse.CheckpointOutput{output}}); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestActiveRootRestoresCommittedMessageValuesOnly(t *testing.T) {
	p := committedAttemptValues(t, false)
	ctx := context.Background()
	request := pulse.CheckpointAttemptRequest{Site: p.site, Parent: p.s.record.RootScope, NodeID: "processor", Limit: 1}
	before := cloneRecord(p.s.record)
	attempt, err := p.s.openAttempt(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Scope != p.processor.Scope || attempt.Index != 1 || attempt.Exhausted || len(attempt.Values) != 1 {
		t.Fatalf("active root did not reclaim its values: %+v", attempt)
	}
	value := attempt.Values[0]
	if value.Key != p.value || value.State != pulse.SlotStateReady || value.Value != 42 || value.Stream != nil {
		t.Fatalf("incorrect restored value: %+v", value)
	}
	if !reflect.DeepEqual(before, p.s.record) {
		t.Fatal("restoring ordinary values changed the durable record")
	}
	// Retry 和消息预算只返回自己的游标，不得夹带根节点输出。
	retry := pulse.CheckpointAttemptRequest{Site: p.site, Parent: p.processor.Scope, NodeID: "processor", Boundary: "aspect/0", Limit: 2}
	for range 2 {
		attempt, err := p.s.openAttempt(ctx, retry)
		if err != nil || len(attempt.Values) != 0 {
			t.Fatalf("Retry carried ordinary values: %+v %v", attempt, err)
		}
	}
	if err := p.append(2); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		message, err := p.s.openMessage(ctx, p.messageRequest(2, 2))
		if err != nil || len(message.Attempt.Values) != 0 {
			t.Fatalf("message attempt carried ordinary values: %+v %v", message, err)
		}
	}
	if err := p.s.failAttempt(ctx, p.processor.Scope); err != nil {
		t.Fatal(err)
	}
	exhausted, err := p.s.openAttempt(ctx, request)
	if err != nil || !exhausted.Exhausted || len(exhausted.Values) != 0 {
		t.Fatalf("failed root restored values: %+v %v", exhausted, err)
	}
}

func TestActiveRootRestoresSkippedMessageValue(t *testing.T) {
	p := committedAttemptValues(t, true)
	attempt, err := p.s.openAttempt(context.Background(), pulse.CheckpointAttemptRequest{Site: p.site, Parent: p.s.record.RootScope, NodeID: "processor", Limit: 1})
	if err != nil || len(attempt.Values) != 1 {
		t.Fatalf("skipped value restore: %+v %v", attempt, err)
	}
	value := attempt.Values[0]
	if value.Key != p.value || value.State != pulse.SlotStateSkipped || value.Value != nil || value.Stream != nil {
		t.Fatalf("skip was changed during recovery: %+v", value)
	}
}

func TestRestoredAttemptValuesRejectInvalidPayloads(t *testing.T) {
	p := committedAttemptValues(t, false)
	key := managedKey(p.s.record.RootScope, p.processor.Scope, "processor")
	cases := map[string]func(*ManagedRecord){
		"scope":              func(m *ManagedRecord) { m.Scope = p.source.Scope },
		"parent":             func(m *ManagedRecord) { m.Parent = p.source.Scope },
		"node":               func(m *ManagedRecord) { m.NodeID = "source" },
		"undeclared":         func(m *ManagedRecord) { m.Values[0].Name = "undeclared" },
		"duplicate":          func(m *ManagedRecord) { m.Values = append(m.Values, m.Values[0]) },
		"stream":             func(m *ManagedRecord) { m.Values[0].Stream = &StreamDescriptorRecord{} },
		"stream-declaration": func(m *ManagedRecord) { m.Values[0].Name = p.outKey.Name },
		"pending":            func(m *ManagedRecord) { m.Values[0].State = "pending" },
		"codec-type":         func(m *ManagedRecord) { m.Values[0].TypeID = "go/string" },
		"codec-version":      func(m *ManagedRecord) { m.Values[0].Version = "2" },
		"checksum":           func(m *ManagedRecord) { m.Values[0].Data = []byte("41") },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			record := cloneRecord(p.s.record)
			managed := record.Managed[key]
			mutate(&managed)
			record.Managed[key] = managed
			state := &runState{def: p.s.def, session: p.s.session, record: record}
			before := cloneRecord(record)
			_, err := state.openAttempt(context.Background(), pulse.CheckpointAttemptRequest{Site: p.site, Parent: record.RootScope, NodeID: "processor", Limit: 1})
			if err == nil {
				t.Fatal("invalid restored output accepted")
			}
			if !reflect.DeepEqual(before, state.record) {
				t.Fatal("invalid recovery changed the record")
			}
		})
	}
}
