// 本文件直接检验子图作用域的恢复边界，相当于 Go 中针对领域协议的单元回归。
// 它核对稳定 In 绑定、旧值指纹拒绝及失败来源；快照字节由 recordio 的边界测试负责。
package durable

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Luo-root/pulse"
)

// scopeBindingState 构造已有确认成果但挂载尝试仍活跃的子图，避免通过业务重跑
// 掩盖恢复错误。输入同时包含 In 和固定值，使两种指纹确实可被区分。
func scopeBindingState(t *testing.T) (*runState, *transactionSession, pulse.CheckpointChild) {
	t.Helper()
	parent, child := testGraph(t, "binding-parent"), testGraph(t, "binding-child")
	parentInput, childInput := pulse.NewKey[int]("parent-input"), pulse.NewKey[int]("child-input")
	fixed, output := pulse.NewKey[int]("fixed"), pulse.NewKey[int]("output")
	if err := parent.DeclareInput(pulse.NewPort(parentInput)); err != nil {
		t.Fatal(err)
	}
	if err := child.DeclareInput(pulse.NewPort(childInput), pulse.NewPort(fixed)); err != nil {
		t.Fatal(err)
	}
	testAdd(t, child, pulse.NewNode("work", "", pulse.Requires(childInput, fixed), pulse.Provides(output), nil))
	stream := pulse.NewKey[pulse.Stream[int]]("stream")
	testAdd(t, child, pulse.NewNode("source", "", nil, pulse.Provides(stream), nil))
	testAdd(t, child, pulse.NewNode("consume", "", pulse.Requires(stream), nil, nil))
	if err := pulse.Sub(parent, "mount", "", []pulse.SubBind{pulse.In(parentInput, childInput), pulse.Fix(fixed, 11)}, child); err != nil {
		t.Fatal(err)
	}
	def, err := inspectDefinition(parent, NewCodecs().snapshot())
	if err != nil {
		t.Fatal(err)
	}
	root, mount, work := strings.Repeat("1", 32), strings.Repeat("2", 32), strings.Repeat("3", 32)
	parentKey := pulse.CheckpointKey{Name: parentInput.Name(), Type: reflect.TypeFor[int]()}
	childKey := pulse.CheckpointKey{Name: childInput.Name(), Type: reflect.TypeFor[int]()}
	fixedKey := pulse.CheckpointKey{Name: fixed.Name(), Type: reflect.TypeFor[int]()}
	req := pulse.CheckpointChild{
		Site: pulse.Site{Graph: "binding-child", Path: "mount"}, Attempt: mount, NodeID: "mount", Graph: "binding-child",
		Inputs:   []pulse.CheckpointOutput{{Key: childKey, State: pulse.SlotStateReady, Value: 7}, {Key: fixedKey, State: pulse.SlotStateReady, Value: 11}},
		Bindings: []pulse.CheckpointBinding{{Source: pulse.Site{Graph: "binding-parent"}, Scope: root, Attempt: mount, Parent: parentKey, Child: childKey}},
	}
	rootHash, err := def.inputsFingerprint(def.root, []pulse.CheckpointOutput{{Key: parentKey, State: pulse.SlotStateReady, Value: 7}})
	if err != nil {
		t.Fatal(err)
	}
	session := &transactionSession{}
	state := &runState{session: session, def: def, record: RunRecord{
		RunID: "scope-binding", Revision: 4, Version: "1", Definition: def.fingerprint, Inputs: rootHash, RootScope: root,
		Scopes: map[string]ScopeRecord{root: {Graph: "binding-parent", Inputs: rootHash, Nodes: map[string]NodeRecord{}}},
		Attempts: map[string]AttemptRecord{
			mount: {Parent: root, NodeID: "mount", Index: 1, Limit: 1, State: AttemptActive},
			work:  {Parent: mount, NodeID: "work", Index: 1, Limit: 1, State: AttemptConfirmed},
		},
	}}
	inputHash, err := state.childFingerprint(location{req.Graph, req.Site.Path}, req, root)
	if err != nil {
		t.Fatal(err)
	}
	values, err := def.encodeOutputs([]pulse.CheckpointOutput{{Key: pulse.CheckpointKey{Name: output.Name(), Type: reflect.TypeFor[int]()}, State: pulse.SlotStateReady, Value: 18}})
	if err != nil {
		t.Fatal(err)
	}
	state.record.Scopes[mount] = ScopeRecord{Graph: req.Graph, Path: req.Site.Path, Inputs: inputHash, Nodes: map[string]NodeRecord{
		"work": {NodeID: "work", Attempt: work, Reason: string(pulse.NodeCompleted), Outputs: values},
	}}
	session.base = &state.record
	return state, session, req
}

func TestScopeBindingCreatesCurrentFingerprint(t *testing.T) {
	state, session, req := scopeBindingState(t)
	want := state.record.Scopes[req.Attempt].Inputs
	delete(state.record.Scopes, req.Attempt)
	delete(state.record.Attempts, strings.Repeat("3", 32))
	if records, err := state.openChild(t.Context(), req); err != nil || len(records) != 0 {
		t.Fatalf("new child scope: records=%v err=%v", records, err)
	}
	if scope := state.record.Scopes[req.Attempt]; scope.Inputs != want || scope.Nodes == nil || session.calls != 1 {
		t.Fatalf("new scope did not persist the stable fingerprint: scope=%+v calls=%d", scope, session.calls)
	}
}

func TestScopeBindingRestoresOnlyCurrentFingerprint(t *testing.T) {
	for _, mode := range []string{"ready", "pending", "different observed value", "old value fingerprint"} {
		t.Run(mode, func(t *testing.T) {
			state, session, req := scopeBindingState(t)
			switch mode {
			case "pending":
				req.Inputs = req.Inputs[1:]
			case "different observed value":
				req.Inputs[0].Value = 99
			case "old value fingerprint":
				legacy, err := state.def.inputsFingerprint(location{req.Graph, req.Site.Path}, req.Inputs)
				if err != nil {
					t.Fatal(err)
				}
				scope := state.record.Scopes[req.Attempt]
				if scope.Inputs == legacy {
					t.Fatal("fixture does not distinguish binding and value fingerprints")
				}
				scope.Inputs = legacy
				state.record.Scopes[req.Attempt] = scope
			}
			before := cloneRecord(state.record)
			records, err := state.openChild(t.Context(), req)
			if mode == "old value fingerprint" {
				if !errors.Is(err, ErrConflict) || records != nil {
					t.Fatalf("old input-value fingerprint accepted: records=%v err=%v", records, err)
				}
			} else if err != nil || len(records) != 1 || records[0].NodeID != "work" || len(records[0].Outputs) != 1 || records[0].Outputs[0].Value != 18 {
				t.Fatalf("stable binding did not restore the confirmed result: records=%v err=%v", records, err)
			}
			// 恢复匹配与拒绝都只读已有事实，不能迁移旧指纹或消耗新的 revision。
			if session.calls != 0 || state.stopped != nil || !reflect.DeepEqual(state.record, before) {
				t.Fatalf("opening an existing scope changed its record: calls=%d stopped=%v", session.calls, state.stopped)
			}
		})
	}
}

func TestScopeBindingPreservesFailedSource(t *testing.T) {
	state, session, req := scopeBindingState(t)
	failed := strings.Repeat("4", 32)
	state.record.Attempts[failed] = AttemptRecord{Parent: req.Attempt, NodeID: "source", Index: 1, Limit: 1, State: AttemptFailed}
	before := cloneRecord(state.record)
	records, err := state.openChild(t.Context(), req)
	var failure *pulse.CheckpointScopeFailure
	if !errors.As(err, &failure) || failure.Scope != req.Attempt || errors.Is(err, ErrConflict) || records != nil {
		t.Fatalf("failed source lost its scope failure classification: records=%v err=%v", records, err)
	}
	if session.calls != 0 || state.stopped != nil || !reflect.DeepEqual(state.record, before) {
		t.Fatal("failed source was changed or confirmed results were republished")
	}
}

func TestScopeBindingWireRejectsLegacyShapeWithoutRewrite(t *testing.T) {
	state, _, req := scopeBindingState(t)
	scope := state.record.Scopes[req.Attempt]
	scope.Nodes = nil
	tx := newRecordTransaction(state.record.Revision)
	if err := tx.createScope(req.Attempt, scope); err != nil {
		t.Fatal(err)
	}
	wire, err := MarshalChangeBatch(ChangeBatch{header: changeHeaderFor(state.record, tx), tx: tx})
	if err != nil {
		t.Fatal(err)
	}
	var envelope struct {
		Version int `json:"version"`
		Ops     []struct {
			Value map[string]json.RawMessage `json:"value"`
		} `json:"ops"`
	}
	if err := json.Unmarshal(wire, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Version != 2 || len(envelope.Ops) != 1 || len(envelope.Ops[0].Value) != 4 || bytes.Contains(wire, []byte(`"bindings":`)) {
		t.Fatalf("wire scope does not have the current four-field shape: %s", wire)
	}
	if _, err := UnmarshalChangeBatch(wire); err != nil {
		t.Fatalf("current wire rejected: %v", err)
	}
	for _, flag := range []string{"false", "true"} {
		legacy := bytes.Replace(wire, []byte(`"nodes":null`), []byte(`"nodes":null,"bindings":`+flag), 1)
		if bytes.Equal(legacy, wire) {
			t.Fatal("legacy fixture did not add the removed field")
		}
		before := bytes.Clone(legacy)
		if _, err := UnmarshalChangeBatch(legacy); err == nil {
			t.Fatalf("legacy bindings=%s wire accepted", flag)
		}
		if !bytes.Equal(legacy, before) {
			t.Fatal("wire rejection rewrote the original bytes")
		}
	}
}
