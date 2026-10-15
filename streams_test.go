package durable

import (
	"context"
	"github.com/Luo-root/pulse"
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
