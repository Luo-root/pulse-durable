package durable

import (
	"errors"
	"reflect"
	"testing"

	"github.com/Luo-root/pulse"
)

func TestRecordTransactionScopeAndNodeBatch(t *testing.T) {
	for _, nodeFirst := range []bool{false, true} {
		s, session := transactionState()
		tx := newRecordTransaction(s.record.Revision)
		header := ScopeRecord{Graph: "child", Path: "mount", Inputs: "hash", Bindings: true, Nodes: map[string]NodeRecord{}}
		node := NodeRecord{NodeID: "result", Outputs: []ValueRecord{{Data: []byte("child")}}}
		stageNode := func() {
			t.Helper()
			if err := tx.putNode("child", "result", nil, node); err != nil {
				t.Fatal(err)
			}
		}
		if nodeFirst {
			stageNode()
		}
		if err := tx.createScope("child", header); err != nil {
			t.Fatal(err)
		}
		if !nodeFirst {
			stageNode()
		}
		// A header cannot retain the caller's map, even when it was empty.
		header.Nodes["injected"] = node
		if err := tx.createScope("child", ScopeRecord{Graph: "child", Path: "mount", Inputs: "hash", Bindings: true}); err != nil {
			t.Fatal(err)
		}
		old := s.record.Scopes["root"].Nodes["result"]
		if err := tx.putNode("root", "result", &old, node); err != nil {
			t.Fatal(err)
		}
		if err := s.commitRecordTransaction(t.Context(), tx); err != nil {
			t.Fatal(err)
		}
		child := s.record.Scopes["child"]
		if session.calls != 1 || child.Graph != "child" || !child.Bindings || len(child.Nodes) != 1 || string(child.Nodes["result"].Outputs[0].Data) != "child" {
			t.Fatalf("scope header and node did not merge: %+v calls=%d", child, session.calls)
		}
	}
}

func TestRecordTransactionNodeCopiesAndCoalesces(t *testing.T) {
	s, session := transactionState()
	old := s.record.Scopes["root"].Nodes["result"]
	old.Outputs[0].Stream = &StreamDescriptorRecord{}
	s.record.Scopes["root"].Nodes["result"] = old
	before := cloneRecord(s.record)
	tx := newRecordTransaction(s.record.Revision)
	expected := copyTransactionNode(old)
	value := copyTransactionNode(old)
	value.Outputs[0].Data = []byte("final")
	if err := tx.putNode("root", "result", &expected, value); err != nil {
		t.Fatal(err)
	}
	expected.Outputs[0].Data[0] = 'X'
	expected.Outputs[0].Stream.ID.Scope = "mutated"
	value.Outputs[0].Data[0] = 'X'
	value.Outputs[0].Stream.ID.Scope = "mutated"
	base := before.Scopes["root"].Nodes["result"]
	final := copyTransactionNode(base)
	final.Outputs[0].Data = []byte("coalesced")
	if err := tx.putNode("root", "result", &base, final); err != nil {
		t.Fatal(err)
	}
	final.Outputs[0].Data[0] = 'X'
	if len(tx.nodes) != 1 || !reflect.DeepEqual(s.record, before) {
		t.Fatal("staging mutated base or failed to coalesce")
	}
	if err := s.commitRecordTransaction(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	confirmed := cloneRecord(s.record)
	op := tx.nodes[nodeRecordKey{"root", "result"}]
	op.value.Outputs[0].Data[0] = 'T'
	op.value.Outputs[0].Stream.ID.Scope = "transaction"
	session.saved.Scopes["root"].Nodes["result"].Outputs[0].Data[0] = 'S'
	if !reflect.DeepEqual(s.record, confirmed) || string(s.record.Scopes["root"].Nodes["result"].Outputs[0].Data) != "coalesced" {
		t.Fatal("published node aliases staging or Store")
	}
}

func TestRecordTransactionNodePreconditionsAreExact(t *testing.T) {
	for _, mode := range []string{"missing scope", "scope exists", "new scope old node", "missing old", "absent exists", "changed old", "nil outputs", "nil data", "duplicate absent", "duplicate old", "scope conflict", "scope nonempty", "cross scope conflict"} {
		t.Run(mode, func(t *testing.T) {
			s, session := transactionState()
			tx := newRecordTransaction(s.record.Revision)
			old := copyTransactionNode(s.record.Scopes["root"].Nodes["result"])
			var err error
			switch mode {
			case "missing scope":
				err = tx.putNode("missing", "result", nil, old)
			case "scope exists":
				err = tx.createScope("root", ScopeRecord{})
			case "new scope old node":
				if err = tx.createScope("new", ScopeRecord{}); err == nil {
					err = tx.putNode("new", "result", &old, old)
				}
			case "missing old":
				err = tx.putNode("root", "missing", &old, old)
			case "absent exists":
				err = tx.putNode("root", "result", nil, old)
			case "changed old":
				err = tx.putNode("root", "result", &old, old)
				s.record.Scopes["root"].Nodes["result"].Outputs[0].Data[0] = 'X'
			case "nil outputs", "nil data":
				current := NodeRecord{NodeID: "result"}
				expected := current
				if mode == "nil outputs" {
					expected.Outputs = []ValueRecord{}
				} else {
					current.Outputs = []ValueRecord{{Data: nil}}
					expected.Outputs = []ValueRecord{{Data: []byte{}}}
				}
				s.record.Scopes["root"].Nodes["result"] = current
				err = tx.putNode("root", "result", &expected, old)
			case "duplicate absent":
				if err = tx.putNode("root", "result", nil, old); err == nil {
					err = tx.putNode("root", "result", &old, old)
				}
			case "duplicate old":
				if err = tx.putNode("root", "result", &old, old); err == nil {
					old.Outputs[0].Data = []byte("different")
					err = tx.putNode("root", "result", &old, old)
				}
			case "scope conflict":
				if err = tx.createScope("new", ScopeRecord{Graph: "first"}); err == nil {
					err = tx.createScope("new", ScopeRecord{Graph: "second"})
				}
			case "scope nonempty":
				err = tx.createScope("new", ScopeRecord{Nodes: map[string]NodeRecord{"result": old}})
			case "cross scope conflict":
				if err = tx.putNode("root", "new", nil, old); err == nil {
					err = tx.putNode("missing", "new", nil, old)
				}
			}
			if err != nil && !errors.Is(err, ErrConflict) {
				t.Fatal(err)
			}
			// An unrelated valid op must never land when any precondition fails.
			_ = tx.putAttempt("new", nil, AttemptRecord{})
			before := cloneRecord(s.record)
			if err := s.commitRecordTransaction(t.Context(), tx); !errors.Is(err, ErrConflict) {
				t.Fatalf("expected conflict: %v", err)
			}
			if session.calls != 0 || !reflect.DeepEqual(s.record, before) {
				t.Fatal("conflict saved or published part of transaction")
			}
		})
	}
}

func TestRecordTransactionNodeNilEmptyAndStaleBatch(t *testing.T) {
	for _, mode := range []string{"nil outputs", "empty outputs", "nil data", "empty data", "stale"} {
		t.Run(mode, func(t *testing.T) {
			s, session := transactionState()
			s.mu.Lock()
			defer s.mu.Unlock()
			old := NodeRecord{NodeID: "result"}
			switch mode {
			case "empty outputs":
				old.Outputs = []ValueRecord{}
			case "nil data":
				old.Outputs = []ValueRecord{{Data: nil}}
			case "empty data":
				old.Outputs = []ValueRecord{{Data: []byte{}}}
			}
			s.record.Scopes["root"].Nodes["result"] = old
			tx := newRecordTransaction(s.record.Revision)
			if err := tx.putNode("root", "result", &old, old); err != nil {
				t.Fatal(err)
			}
			if err := tx.putNode("root", "result", &old, old); err != nil {
				t.Fatal(err)
			}
			if err := tx.createScope("child", ScopeRecord{}); err != nil {
				t.Fatal(err)
			}
			if mode == "stale" {
				s.record.Revision++
			}
			prepared := false
			err := s.commitRecordTransactionPrepared(t.Context(), tx, func(*RunRecord) error { prepared = true; return nil })
			if mode == "stale" {
				if !errors.Is(err, ErrConflict) || prepared || session.calls != 0 {
					t.Fatalf("stale batch reached preparation/Save: %v", err)
				}
			} else if err != nil || !prepared || session.calls != 1 {
				t.Fatalf("exact nil/empty precondition rejected: %v", err)
			}
		})
	}
}

func TestRecordTransactionScopeNodeSaveFailure(t *testing.T) {
	for _, mode := range []string{"error", "panic", "panic nil"} {
		t.Run(mode, func(t *testing.T) {
			s, session := transactionState()
			before := cloneRecord(s.record)
			tx := stageTransactionFailure(t, s)
			if err := tx.createScope("child", ScopeRecord{Graph: "child"}); err != nil {
				t.Fatal(err)
			}
			if err := tx.putNode("child", "new", nil, NodeRecord{Outputs: []ValueRecord{{Data: []byte("new"), Stream: &StreamDescriptorRecord{}}}}); err != nil {
				t.Fatal(err)
			}
			session.after = func(r RunRecord) error {
				if !reflect.DeepEqual(s.record, before) {
					t.Fatal("published before confirmation")
				}
				r.Scopes["child"].Nodes["new"].Outputs[0].Data[0] = 'X'
				if mode == "panic" {
					panic("Save")
				}
				if mode == "panic nil" {
					panic(nil)
				}
				return errors.New("Save failed")
			}
			if err := s.commitRecordTransaction(t.Context(), tx); err == nil || s.stopped != err {
				t.Fatalf("failure not sealed: %v", err)
			}
			if session.calls != 1 || !reflect.DeepEqual(s.record, before) {
				t.Fatal("uncertain Save published scope/node")
			}
		})
	}
}

// Use real Runtime execution and retained Save arguments to check every
// checkpoint, not manually fabricated graph semantics.
func TestRecordTransactionRuntimeNodeConfirmationAtomic(t *testing.T) {
	for _, mode := range []string{"success", "error", "panic"} {
		t.Run(mode, func(t *testing.T) {
			store := &evolutionStore{Store: NewMemoryStore()}
			g := testGraph(t, "atomic")
			key := pulse.NewKey[int]("out")
			testAdd(t, g, pulse.NewNode("work", "", nil, pulse.Provides(key), func(rc *pulse.RunCtx) error { return pulse.Set(rc, key, 7) }))
			if err := g.DeclareOutput(pulse.NewPort(key)); err != nil {
				t.Fatal(err)
			}
			observed := false
			store.after = func(r RunRecord) error {
				for _, scope := range r.Scopes {
					for _, node := range scope.Nodes {
						if r.Attempts[node.Attempt].State != AttemptConfirmed {
							t.Fatal("node without confirmed root in Save")
						}
						observed = true
						if mode == "panic" {
							panic("uncertain node Save")
						}
						if mode == "error" {
							return errors.New("uncertain node Save")
						}
					}
				}
				for id, a := range r.Attempts {
					if a.State == AttemptConfirmed && r.Scopes[a.Parent].Nodes[a.NodeID].Attempt != id {
						t.Fatal("confirmed root without node in Save")
					}
				}
				return nil
			}
			_, err := testRuntime(t, store, NewCodecs()).Run(t.Context(), g, pulse.Inputs{}, testOptions("atomic"))
			if !observed || (err != nil) != (mode != "success") {
				t.Fatalf("unexpected Runtime result: observed=%v err=%v", observed, err)
			}
			store.assertHistory(t)
			store.after = nil
			if _, err := testRuntime(t, store, NewCodecs()).Resume(t.Context(), g, pulse.Inputs{}, testOptions("atomic")); err != nil {
				t.Fatal(err)
			}
			store.assertHistory(t)
		})
	}
}
