package durable

import (
	"context"
	"github.com/Luo-root/pulse"
	"reflect"
	"testing"
)

func TestActiveNonResumableSourceRefusesRestoreWithoutWriting(t *testing.T) {
	p := newStreamProtocol(t)
	ctx := context.Background()
	request := p.openRequest(false)
	request.Source.Resumable = false
	key := streamKey(streamID(p.input.Stream.ID))
	record := p.s.record.Streams[key]
	record.Stream.Resumable = false
	p.s.record.Streams[key] = record
	// 同一轮认回仍是新来源；只有跨执行的 active 恢复才需要显式续读能力。
	opened, err := p.s.openStream(ctx, request)
	if err != nil || opened.Resumed {
		t.Fatalf("same-execution source reopened as resume: %+v %v", opened, err)
	}
	resumed := &runState{def: p.s.def, session: p.s.session, record: cloneRecord(p.s.record)}
	before := cloneRecord(resumed.record)
	if _, err := resumed.openStream(ctx, request); err == nil {
		t.Fatal("non-resumable active source restored")
	}
	if !reflect.DeepEqual(before, resumed.record) {
		t.Fatal("rejected recovery advanced generation")
	}
}

func TestLaterSourceFailureDoesNotCorruptEarlierConsumerRecord(t *testing.T) {
	p := newStreamProtocol(t)
	ctx := context.Background()
	sink, err := p.s.openAttempt(ctx, pulse.CheckpointAttemptRequest{Site: p.site, Parent: p.s.record.RootScope, NodeID: "sink", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := p.s.commit(ctx, pulse.CheckpointCommit{Site: p.site, Scope: p.s.record.RootScope, Attempt: sink.Scope,
		Node: pulse.NodeCheckpoint{NodeID: "sink", Reason: pulse.NodeCompleted}}); err != nil {
		t.Fatal(err)
	}
	if err := p.s.finishStream(ctx, pulse.CheckpointStreamFinish{Site: p.site, Attempt: p.processor.Scope, Stream: p.output.Stream.ID, Terminal: pulse.StreamFailed}); err != nil {
		t.Fatal(err)
	}
	if err := p.s.failAttempt(ctx, p.processor.Scope); err != nil {
		t.Fatal(err)
	}
	if err := p.s.validateLoaded(); err != nil {
		t.Fatalf("logical source failure reported as record corruption: %v", err)
	}
	if _, err := p.s.openStream(ctx, p.openRequest(true)); err == nil {
		t.Fatal("failed source reopened")
	}
}
