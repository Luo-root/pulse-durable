package durable

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/Luo-root/pulse"
)

func TestIncrementalRecordCorruptionIsRejected(t *testing.T) {
	p := newStreamProtocol(t)
	ctx := context.Background()
	if err := p.append(1); err != nil {
		t.Fatal(err)
	}
	a, err := p.s.openMessage(ctx, p.messageRequest(1, 2))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.s.commitMessage(ctx, pulse.CheckpointMessageCommit{Site: p.site, Scope: p.s.record.RootScope, Attempt: a.Attempt.Scope,
		Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: p.input.Stream.ID, Sequence: 1}}, State: []pulse.CheckpointState{{Name: "sum", Type: reflect.TypeFor[int](), Value: 1}}}); err != nil {
		t.Fatal(err)
	}
	if err := p.append(2); err != nil {
		t.Fatal(err)
	}
	streamKey := streamKey(streamID(p.input.Stream.ID))
	managedKey := managedKey(p.s.record.RootScope, p.processor.Scope, "processor")
	mutateStream := func(change func(*StreamRecord)) func(*RunRecord) {
		return func(r *RunRecord) { stream := r.Streams[streamKey]; change(&stream); r.Streams[streamKey] = stream }
	}
	cases := map[string]func(*RunRecord){
		"map-identity":  func(r *RunRecord) { r.Streams["forged"] = r.Streams[streamKey]; delete(r.Streams, streamKey) },
		"pending-order": mutateStream(func(r *StreamRecord) { r.Pending = append(r.Pending, r.Pending[0]) }),
		"frontier":      mutateStream(func(r *StreamRecord) { r.AckedThrough = 2 }),
		"message-codec": mutateStream(func(r *StreamRecord) { r.Pending[0].Value.TypeID = "unregistered" }),
		"message-state": mutateStream(func(r *StreamRecord) {
			r.Pending[0].Value.State = "skipped"
			r.Pending[0].Value.Data = nil
			r.Pending[0].Value.Digest = ""
		}),
		"cursor-codec": mutateStream(func(r *StreamRecord) { r.Cursor.Version = "unknown" }),
		"cursor-name":  mutateStream(func(r *StreamRecord) { r.Cursor.Name = "wrong" }),
		"terminal":     mutateStream(func(r *StreamRecord) { r.Stream.Terminal = "completed" }),
		"owner":        mutateStream(func(r *StreamRecord) { r.Stream.Attempt = p.processor.Scope }),
		"state-digest": func(r *RunRecord) {
			state := r.Managed[managedKey]
			state.State[0].Digest = "bad"
			r.Managed[managedKey] = state
		},
		"state-without-message":      func(r *RunRecord) { r.Messages = nil },
		"confirmed-without-managed":  func(r *RunRecord) { delete(r.Managed, managedKey) },
		"confirmed-with-nil-managed": func(r *RunRecord) { r.Managed = nil },
		"managed-storage-identity": func(r *RunRecord) {
			r.Managed["wrong-key"] = r.Managed[managedKey]
			delete(r.Managed, managedKey)
		},
		"managed-scope": func(r *RunRecord) {
			managed := r.Managed[managedKey]
			managed.Scope = p.processor.Scope
			r.Managed[managedKey] = managed
		},
		"managed-parent": func(r *RunRecord) {
			managed := r.Managed[managedKey]
			managed.Parent = p.source.Scope
			r.Managed[managedKey] = managed
		},
		"managed-node": func(r *RunRecord) {
			managed := r.Managed[managedKey]
			managed.NodeID = "source"
			r.Managed[managedKey] = managed
		},
		"budget": func(r *RunRecord) {
			message := r.Messages[a.Attempt.Scope]
			message.Limit = 0
			r.Messages[a.Attempt.Scope] = message
		},
		"message-future-generation": func(r *RunRecord) {
			message := r.Messages[a.Attempt.Scope]
			message.Input.Generation = 99
			r.Messages[a.Attempt.Scope] = message
		},
		"message-unrelated-input": func(r *RunRecord) {
			message := r.Messages[a.Attempt.Scope]
			message.Input = streamID(p.output.Stream.ID)
			r.Messages[a.Attempt.Scope] = message
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			copy := cloneRecord(p.s.record)
			mutate(&copy)
			s := &runState{def: p.s.def, record: copy}
			if err := s.validateLoaded(); err == nil {
				t.Fatal("corrupted record accepted")
			}
		})
	}
}

func TestEachConfirmedMessageRequiresItsOwnManagedHistory(t *testing.T) {
	p := newStreamProtocol(t)
	ctx := context.Background()
	owners := []string{p.processor.Scope}
	retry, err := p.s.openAttempt(ctx, pulse.CheckpointAttemptRequest{Site: p.site, Parent: p.processor.Scope, NodeID: "processor", Boundary: "retry", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	owners = append(owners, retry.Scope)
	for i, owner := range owners {
		p.processor.Scope = owner
		sequence := uint64(i + 1)
		if err := p.append(sequence); err != nil {
			t.Fatal(err)
		}
		message, err := p.s.openMessage(ctx, p.messageRequest(sequence, 1))
		if err != nil {
			t.Fatal(err)
		}
		if err := p.s.commitMessage(ctx, pulse.CheckpointMessageCommit{Site: p.site, Scope: p.s.record.RootScope, Attempt: message.Attempt.Scope,
			Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: p.input.Stream.ID, Sequence: sequence}}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.s.validateLoaded(); err != nil {
		t.Fatal(err)
	}
	for _, owner := range owners {
		t.Run(owner, func(t *testing.T) {
			record := cloneRecord(p.s.record)
			delete(record.Managed, managedKey(record.RootScope, owner, "processor"))
			if len(record.Managed) != 1 {
				t.Fatal("test did not retain another valid Managed container")
			}
			state := &runState{def: p.s.def, record: record}
			if err := state.validateLoaded(); err == nil {
				t.Fatal("another processing owner's container masked a missing confirmed history")
			}
		})
	}
}

func TestManagedHistoryValidationAcceptsLegitimateRecords(t *testing.T) {
	for _, scenario := range []string{"empty-managed", "manual-ack", "active-message", "failed-message", "older-generation", "confirmed-parent", "failed-parent", "failed-retry-ancestor"} {
		t.Run(scenario, func(t *testing.T) {
			p := newStreamProtocol(t)
			ctx := context.Background()
			if err := p.append(1); err != nil {
				t.Fatal(err)
			}
			if scenario == "failed-retry-ancestor" {
				attempt, err := p.s.openAttempt(ctx, pulse.CheckpointAttemptRequest{Site: p.site, Parent: p.processor.Scope, NodeID: "processor", Boundary: "retry", Limit: 2})
				if err != nil {
					t.Fatal(err)
				}
				p.processor = attempt
			}
			commit := pulse.CheckpointMessageCommit{Site: p.site, Scope: p.s.record.RootScope,
				Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: p.input.Stream.ID, Sequence: 1}}}
			if scenario == "manual-ack" {
				if err := p.s.commitMessage(ctx, commit); err != nil {
					t.Fatal(err)
				}
				if len(p.s.record.Messages)+len(p.s.record.Managed) != 0 {
					t.Fatal("manual Ack created processing history")
				}
			} else {
				message, err := p.s.openMessage(ctx, p.messageRequest(1, 2))
				if err != nil {
					t.Fatal(err)
				}
				switch scenario {
				case "active-message":
				case "failed-message":
					if err := p.s.failMessage(ctx, message.Attempt.Scope); err != nil {
						t.Fatal(err)
					}
				default:
					commit.Attempt = message.Attempt.Scope
					if err := p.s.commitMessage(ctx, commit); err != nil {
						t.Fatal(err)
					}
					managed, exists := p.s.record.Managed[managedKey(commit.Scope, p.processor.Scope, "processor")]
					if !exists || managed.Scope != commit.Scope || managed.Parent != p.processor.Scope || managed.NodeID != "processor" || len(managed.State)+len(managed.Values) != 0 {
						t.Fatal("empty message commit lost its identity-bearing Managed container")
					}
				}
				if (scenario == "active-message" || scenario == "failed-message") && len(p.s.record.Managed) != 0 {
					t.Fatal("unconfirmed message created a Managed container")
				}
			}
			switch scenario {
			case "older-generation":
				resumed := &runState{session: p.s.session, def: p.s.def, record: cloneRecord(p.s.record)}
				input, err := resumed.openStream(ctx, p.openRequest(false))
				if err != nil || input.Stream.ID.Generation != p.input.Stream.ID.Generation+1 {
					t.Fatalf("advance input generation: %+v %v", input, err)
				}
				p.s = resumed
			case "confirmed-parent":
				descriptor := p.output.Stream
				descriptor.Terminal = pulse.StreamCompleted
				if err := p.s.commit(ctx, pulse.CheckpointCommit{Site: p.site, Scope: p.s.record.RootScope, Attempt: p.processor.Scope,
					Node: pulse.NodeCheckpoint{NodeID: "processor", Reason: pulse.NodeCompleted, Outputs: []pulse.CheckpointOutput{
						{Key: p.outKey, State: pulse.SlotStateReady, Stream: &descriptor}, {Key: p.value, State: pulse.SlotStateSkipped}}},
					Streams: []pulse.CheckpointStreamFinish{{Site: p.site, Attempt: p.processor.Scope, Stream: descriptor.ID, Terminal: pulse.StreamCompleted}}}); err != nil {
					t.Fatal(err)
				}
			case "failed-parent", "failed-retry-ancestor":
				_, ancestor, err := p.s.rootAttempt(p.processor.Scope)
				if err != nil {
					t.Fatal(err)
				}
				if err := p.s.failAttempt(ctx, ancestor); err != nil {
					t.Fatal(err)
				}
			}
			before := cloneRecord(p.s.record)
			if err := p.s.validateLoaded(); err != nil {
				t.Fatalf("legitimate message history rejected: %v", err)
			}
			if !reflect.DeepEqual(before, p.s.record) {
				t.Fatal("history validation changed the record")
			}
		})
	}
}

func TestMessageSaveUncertaintyRetainsAuthoritativeTransaction(t *testing.T) {
	p := newStreamProtocol(t)
	ctx := context.Background()
	if err := p.append(1); err != nil {
		t.Fatal(err)
	}
	a, err := p.s.openMessage(ctx, p.messageRequest(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	fault := &faultStore{after: true, failure: fmt.Errorf("uncertain: %w", pulse.ErrSkipped), match: func(r RunRecord) bool { return r.Messages[a.Attempt.Scope].State == AttemptConfirmed }}
	p.s.session = &faultSession{Session: p.s.session, owner: fault}
	before := cloneRecord(p.s.record)
	commit := pulse.CheckpointMessageCommit{Site: p.site, Scope: before.RootScope, Attempt: a.Attempt.Scope, Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: p.input.Stream.ID, Sequence: 1}},
		State: []pulse.CheckpointState{{Name: "sum", Type: reflect.TypeFor[int](), Value: 1}}, Values: []pulse.CheckpointOutput{{Key: p.value, State: pulse.SlotStateReady, Value: 42}},
		Outputs: []pulse.CheckpointStreamAppend{{Site: p.site, Attempt: p.processor.Scope, Stream: p.output.Stream.ID, Message: pulse.CheckpointStreamMessage{Sequence: 1, Value: 10}}}}
	if err := p.s.commitMessage(ctx, commit); err == nil {
		t.Fatal("lost acknowledgement ignored")
	}
	if !reflect.DeepEqual(before, p.s.record) {
		t.Fatal("uncertain transaction advanced runtime memory")
	}
	if err := p.s.commitMessage(ctx, commit); err == nil || fault.saves.Load() != 1 {
		t.Fatal("uncertain transaction retried")
	}
	authoritative, err := p.s.session.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if authoritative.Revision != before.Revision+1 || authoritative.Messages[a.Attempt.Scope].State != AttemptConfirmed {
		t.Fatal("authoritative transaction was lost")
	}
	managed := authoritative.Managed[managedKey(before.RootScope, p.processor.Scope, "processor")]
	if len(managed.State) != 1 || len(managed.Values) != 1 || len(authoritative.Streams[streamKey(streamID(p.input.Stream.ID))].Pending) != 0 || authoritative.Streams[streamKey(streamID(p.output.Stream.ID))].LastSequence != 1 {
		t.Fatal("uncertain transaction saved only part of its results")
	}
}

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
