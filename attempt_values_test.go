package durable

import (
	"context"
	"reflect"
	"testing"

	"github.com/Luo-root/pulse"
)

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
