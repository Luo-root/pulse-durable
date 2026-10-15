package durable

import (
	"context"
	"errors"
	"github.com/Luo-root/pulse"
	"reflect"
	"testing"
)

func TestStreamGenerationAndCompletedSourceTail(t *testing.T) {
	p := newStreamProtocol(t)
	ctx := context.Background()
	for i := uint64(1); i <= 3; i++ {
		if err := p.append(i); err != nil {
			t.Fatal(err)
		}
	}
	// 先确认 2，连续前沿仍为 0；确认 1 才推进至 2。
	if err := p.s.commitMessage(ctx, pulse.CheckpointMessageCommit{Scope: p.s.record.RootScope, Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: p.input.Stream.ID, Sequence: 2}}}); err != nil {
		t.Fatal(err)
	}
	record := p.s.record.Streams[streamKey(streamID(p.input.Stream.ID))]
	if record.AckedThrough != 0 || len(record.Pending) != 2 {
		t.Fatalf("invalid sparse acknowledgement: %+v", record)
	}
	descriptor := p.input.Stream
	descriptor.Terminal = pulse.StreamCompleted
	if err := p.s.commit(ctx, pulse.CheckpointCommit{Site: p.site, Scope: p.s.record.RootScope, Attempt: p.source.Scope,
		Node:    pulse.NodeCheckpoint{NodeID: "source", Reason: pulse.NodeCompleted, Outputs: []pulse.CheckpointOutput{{Key: p.inKey, State: pulse.SlotStateReady, Stream: &descriptor}}},
		Streams: []pulse.CheckpointStreamFinish{{Site: p.site, Attempt: p.source.Scope, Stream: p.input.Stream.ID, Terminal: pulse.StreamCompleted, LastSequence: 3, Cursor: 3}}}); err != nil {
		t.Fatal(err)
	}
	if err := p.s.validateLoaded(); err != nil {
		t.Fatal(err)
	}
	resumed := &runState{session: p.s.session, def: p.s.def, record: cloneRecord(p.s.record)}
	request := p.openRequest(false)
	request.Restore = &descriptor
	restored, err := resumed.openStream(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if restored.Stream.ID.Generation != 2 || len(restored.Pending) != 2 || restored.Cursor != 3 {
		t.Fatalf("bad recovered tail: %+v", restored)
	}
	revision := resumed.record.Revision
	again, err := resumed.openStream(ctx, request)
	if err != nil || again.Stream.ID != restored.Stream.ID || resumed.record.Revision != revision {
		t.Fatal("repeated Open advanced generation or revision")
	}
	if err := resumed.commitMessage(ctx, pulse.CheckpointMessageCommit{Scope: resumed.record.RootScope, Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: descriptor.ID, Sequence: 1}}}); err == nil {
		t.Fatal("old delivery advanced a new generation")
	}
	for _, sequence := range []uint64{1, 3} {
		if err := resumed.commitMessage(ctx, pulse.CheckpointMessageCommit{Scope: resumed.record.RootScope, Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: restored.Stream.ID, Sequence: sequence}}}); err != nil {
			t.Fatal(err)
		}
	}
	record = resumed.record.Streams[streamKey(streamID(descriptor.ID))]
	if record.AckedThrough != 3 || len(record.Pending) != 0 {
		t.Fatal("completed source tail was not acknowledged")
	}
	for _, mutate := range []func(*pulse.CheckpointStreamDescriptor){
		func(d *pulse.CheckpointStreamDescriptor) { d.Source.ID = "forged" },
		func(d *pulse.CheckpointStreamDescriptor) { d.ID.Generation = 99 },
		func(d *pulse.CheckpointStreamDescriptor) { d.Terminal = pulse.StreamStopped },
	} {
		bad := descriptor
		mutate(&bad)
		request.Restore = &bad
		if _, err := resumed.openStream(ctx, request); err == nil {
			t.Fatal("forged completed reference reopened")
		}
	}
}

func TestMessageCommitCoordinatesStateValueOutputAndAck(t *testing.T) {
	p := newStreamProtocol(t)
	ctx := context.Background()
	if err := p.append(1); err != nil {
		t.Fatal(err)
	}
	attempt, err := p.s.openMessage(ctx, p.messageRequest(1, 2))
	if err != nil {
		t.Fatal(err)
	}
	commit := pulse.CheckpointMessageCommit{Site: p.site, Scope: p.s.record.RootScope, Attempt: attempt.Attempt.Scope,
		Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: p.input.Stream.ID, Sequence: 1}},
		State:  []pulse.CheckpointState{{Name: "sum", Type: reflect.TypeFor[int](), Value: 1}},
		Values: []pulse.CheckpointOutput{{Key: p.value, State: pulse.SlotStateReady, Value: 99}},
		Outputs: []pulse.CheckpointStreamAppend{{Site: p.site, Attempt: p.processor.Scope, Stream: p.output.Stream.ID, Message: pulse.CheckpointStreamMessage{Sequence: 1, Value: 10}},
			{Site: p.site, Attempt: p.processor.Scope, Stream: p.output.Stream.ID, Message: pulse.CheckpointStreamMessage{Sequence: 2, Value: "wrong type"}}}}
	before := cloneRecord(p.s.record)
	if err := p.s.commitMessage(ctx, commit); err == nil {
		t.Fatal("invalid second output committed")
	}
	if !reflect.DeepEqual(before, p.s.record) {
		t.Fatal("failed transaction partly changed state, output or ack")
	}
	commit.Outputs = commit.Outputs[:1]
	if err := p.s.commitMessage(ctx, commit); err != nil {
		t.Fatal(err)
	}
	if p.s.record.Revision != before.Revision+1 || p.s.record.Messages[attempt.Attempt.Scope].State != AttemptConfirmed ||
		len(p.s.record.Streams[streamKey(streamID(p.input.Stream.ID))].Pending) != 0 || len(p.s.record.Streams[streamKey(streamID(p.output.Stream.ID))].Pending) != 1 {
		t.Fatal("message transaction did not commit atomically")
	}
	managed := p.s.record.Managed[managedKey(p.s.record.RootScope, p.processor.Scope, "processor")]
	if len(managed.State) != 1 || len(managed.Values) != 1 {
		t.Fatal("message state or ordinary value missing")
	}
	if err := p.s.validateLoaded(); err != nil {
		t.Fatal(err)
	}
	// 深复制所有新增容器、编码字节和 descriptor。
	copy := cloneRecord(p.s.record)
	copy.Managed[managedKey(p.s.record.RootScope, p.processor.Scope, "processor")].State[0].Data[0] = '9'
	stream := copy.Streams[streamKey(streamID(p.output.Stream.ID))]
	stream.Pending[0].Value.Data[0] = '9'
	if reflect.DeepEqual(copy, p.s.record) || string(managed.State[0].Data) != "1" {
		t.Fatal("cloned record aliases payload bytes")
	}
}

func TestMessageBudgetSurvivesGenerationAndFailedSourceRejectsAck(t *testing.T) {
	p := newStreamProtocol(t)
	ctx := context.Background()
	if err := p.append(1); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 2; i++ {
		a, err := p.s.openMessage(ctx, p.messageRequest(1, 2))
		if err != nil || a.Attempt.Index != i {
			t.Fatalf("attempt %d: %+v %v", i, a, err)
		}
		if err := p.s.failMessage(ctx, a.Attempt.Scope); err != nil {
			t.Fatal(err)
		}
	}
	resumed := &runState{session: p.s.session, def: p.s.def, record: cloneRecord(p.s.record)}
	input, err := resumed.openStream(ctx, p.openRequest(false))
	if err != nil {
		t.Fatal(err)
	}
	p.s, p.input = resumed, input
	a, err := resumed.openMessage(ctx, p.messageRequest(1, 2))
	if err != nil || !a.Attempt.Exhausted || a.Attempt.Index != 2 {
		t.Fatalf("resume replenished budget: %+v %v", a, err)
	}
	if _, err := resumed.openMessage(ctx, p.messageRequest(1, 3)); !errors.Is(err, ErrConflict) {
		t.Fatalf("limit drift: %v", err)
	}
	if err := resumed.failAttempt(ctx, p.source.Scope); err != nil {
		t.Fatal(err)
	}
	if err := resumed.commitMessage(ctx, pulse.CheckpointMessageCommit{Scope: resumed.record.RootScope, Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: input.Stream.ID, Sequence: 1}}}); err == nil {
		t.Fatal("failed source accepted Ack")
	}
}
