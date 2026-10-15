package durable

import (
	"context"
	"reflect"
	"testing"

	"github.com/Luo-root/pulse"
)

func TestActiveStreamReferenceChecksSourceBeforeCanonicalCache(t *testing.T) {
	protocol := newStreamProtocol(t)
	if err := protocol.append(1); err != nil {
		t.Fatal(err)
	}
	historical := protocol.s.record.Streams[streamKey(streamID(protocol.input.Stream.ID))].Stream
	want, err := protocol.s.restoreStreamReference(protocol.inKey, historical)
	if err != nil {
		t.Fatalf("valid live source reference was rejected: %v", err)
	}
	for name, mutate := range map[string]func(*StreamDescriptorRecord){
		"source":     func(d *StreamDescriptorRecord) { d.SourceID = "different-source" },
		"attempt":    func(d *StreamDescriptorRecord) { d.Attempt = protocol.processor.Scope },
		"graph":      func(d *StreamDescriptorRecord) { d.Graph = "different-graph" },
		"path":       func(d *StreamDescriptorRecord) { d.Path = "different-path" },
		"capacity":   func(d *StreamDescriptorRecord) { d.Capacity++ },
		"generation": func(d *StreamDescriptorRecord) { d.ID.Generation++ },
		"terminal":   func(d *StreamDescriptorRecord) { d.Terminal = string(pulse.StreamCompleted) },
		"stop":       func(d *StreamDescriptorRecord) { d.Stopped = true },
	} {
		t.Run(name, func(t *testing.T) {
			bad := historical
			mutate(&bad)
			if _, err := protocol.s.restoreStreamReference(protocol.inKey, bad); err == nil {
				t.Fatal("cached active reference bypassed source validation")
			}
			if got, err := protocol.s.restoreStreamReference(protocol.inKey, historical); err != nil || got != want {
				t.Fatalf("bad reference poisoned canonical source: %+v %v", got, err)
			}
		})
	}
	logical := streamKey(historical.ID)
	latest := protocol.s.record.Streams[logical]
	latest.Stream.ID.Generation++
	protocol.s.record.Streams[logical] = latest
	if got, err := protocol.s.restoreStreamReference(protocol.inKey, historical); err != nil || got != want {
		t.Fatalf("generation change created a new canonical reference: %+v %v", got, err)
	}
}

func TestActiveStreamReferenceRejectsInvalidOwner(t *testing.T) {
	for _, state := range []AttemptState{AttemptFailed, AttemptConfirmed} {
		t.Run(string(state), func(t *testing.T) {
			protocol := newStreamProtocol(t)
			reference := protocol.s.record.Streams[streamKey(streamID(protocol.input.Stream.ID))].Stream
			owner := protocol.s.record.Attempts[reference.Attempt]
			owner.State = state
			protocol.s.record.Attempts[reference.Attempt] = owner
			if err := protocol.s.checkStreamReference(protocol.s.record, reference); err == nil {
				t.Fatal("active source without a live owner was accepted")
			}
		})
	}
}

func TestActiveStreamReferenceRequiresResumableSourceOnRestore(t *testing.T) {
	protocol := newStreamProtocol(t)
	logical := streamKey(streamID(protocol.input.Stream.ID))
	stream := protocol.s.record.Streams[logical]
	stream.Stream.Resumable = false
	protocol.s.record.Streams[logical] = stream
	if err := protocol.s.checkStreamReference(protocol.s.record, stream.Stream); err != nil {
		t.Fatalf("normal run must permit a non-resumable active alias: %v", err)
	}
	if _, err := protocol.s.restoreStreamReference(protocol.inKey, stream.Stream); err == nil {
		t.Fatal("active non-resumable source was accepted for recovery")
	}
}

func TestCompleteSourceNodeStillRequiresStreamTerminal(t *testing.T) {
	protocol := newStreamProtocol(t)
	outputs, err := protocol.s.def.encodeOutputs([]pulse.CheckpointOutput{{Key: protocol.inKey, State: pulse.SlotStateReady, Stream: &protocol.input.Stream}})
	if err != nil {
		t.Fatal(err)
	}
	node := NodeRecord{NodeID: "source", Attempt: protocol.source.Scope, Reason: string(pulse.NodeCompleted), Outputs: outputs}
	if _, err := protocol.s.def.decodeNode(protocol.s.def.root, node); err == nil {
		t.Fatal("ordinary completed source accepted an unfinished owned stream")
	}
	before := cloneRecord(protocol.s.record)
	if err := protocol.s.commit(context.Background(), pulse.CheckpointCommit{Site: protocol.site, Scope: protocol.s.record.RootScope, Attempt: protocol.source.Scope,
		Node: pulse.NodeCheckpoint{NodeID: "source", Reason: pulse.NodeCompleted, Outputs: []pulse.CheckpointOutput{{Key: protocol.inKey, State: pulse.SlotStateReady, Stream: &protocol.input.Stream}}}}); err == nil {
		t.Fatal("source confirmed without a terminal")
	}
	if !reflect.DeepEqual(before, protocol.s.record) {
		t.Fatal("rejected source commit modified its record")
	}
}
