package durable

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
)

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
	if err := state.save(ctx, cloneRecord(state.record)); err != nil {
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
