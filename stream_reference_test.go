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

func TestHistoricalStreamReferencePreservesSourceConstraints(t *testing.T) {
	for _, terminal := range []pulse.StreamTerminal{"", pulse.StreamFailed, pulse.StreamCanceled} {
		t.Run(string(terminal), func(t *testing.T) {
			protocol := newStreamProtocol(t)
			logical := streamKey(streamID(protocol.input.Stream.ID))
			stream := protocol.s.record.Streams[logical]
			history := stream.Stream // Sub was confirmed before its actual source ended.
			stream.Stream.Terminal = string(terminal)
			protocol.s.record.Streams[logical] = stream
			owner := protocol.s.record.Attempts[history.Attempt]
			owner.State = AttemptFailed
			protocol.s.record.Attempts[history.Attempt] = owner
			if err := protocol.s.checkStreamReferenceRecord(protocol.s.record, history, true); err != nil {
				t.Fatalf("valid historical association rejected: %v", err)
			}
			if err := protocol.s.checkStreamReference(protocol.s.record, history); err == nil {
				t.Fatal("historical shape validation authorized an invalid live source")
			}
			for name, mutate := range map[string]func(*StreamDescriptorRecord){
				"source":     func(d *StreamDescriptorRecord) { d.SourceID = "forged" },
				"attempt":    func(d *StreamDescriptorRecord) { d.Attempt = protocol.processor.Scope },
				"graph":      func(d *StreamDescriptorRecord) { d.Graph = "forged" },
				"path":       func(d *StreamDescriptorRecord) { d.Path = "forged" },
				"capacity":   func(d *StreamDescriptorRecord) { d.Capacity++ },
				"generation": func(d *StreamDescriptorRecord) { d.ID.Generation++ },
				"terminal":   func(d *StreamDescriptorRecord) { d.Terminal = string(pulse.StreamCompleted) },
				"stop":       func(d *StreamDescriptorRecord) { d.Stopped = true },
			} {
				t.Run(name, func(t *testing.T) {
					bad := history
					mutate(&bad)
					if err := protocol.s.checkStreamReferenceRecord(protocol.s.record, bad, true); err == nil {
						t.Fatal("structural history check discarded a source constraint")
					}
				})
			}
		})
	}
}

func TestStreamDescriptorStoppedRequiresStoppedGroup(t *testing.T) {
	scope, err := randomID()
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := randomID()
	if err != nil {
		t.Fatal(err)
	}
	definition := &definition{codecs: NewCodecs().snapshot()}
	key := pulse.CheckpointKey{Name: "values", Type: reflect.TypeFor[pulse.Stream[int]](), Element: reflect.TypeFor[int]()}
	descriptor := pulse.CheckpointStreamDescriptor{
		ID:    pulse.CheckpointStreamID{Scope: scope, NodeID: "source", Key: key.Name, Group: "default", Generation: 1},
		Graph: "descriptor-state", Attempt: attempt, Element: key.Element,
		Source: pulse.CheckpointStreamSource{ID: "numbers", Resumable: true}, Capacity: 1,
		Terminal: pulse.StreamStopped,
	}
	if _, err := definition.encodeDescriptor(key, descriptor); err == nil {
		t.Error("encoding accepted writer stopped without its reading group being stopped")
	}
	descriptor.Stopped = true
	record, err := definition.encodeDescriptor(key, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	record.Stopped = false
	if _, err := definition.decodeDescriptor(key, record); err == nil {
		t.Error("decoding accepted an impossible stopped/false historical reference")
	}
	// 读取组 Stop 可以早于或晚于写端完成；只有 stopped/false 是不可能的组合。
	for _, terminal := range []pulse.StreamTerminal{"", pulse.StreamCompleted, pulse.StreamStopped, pulse.StreamFailed, pulse.StreamCanceled} {
		for _, stopped := range []bool{false, true} {
			if terminal == pulse.StreamStopped && !stopped {
				continue
			}
			descriptor.Terminal, descriptor.Stopped = terminal, stopped
			record, err := definition.encodeDescriptor(key, descriptor)
			if err != nil {
				t.Fatalf("valid terminal=%q stopped=%v rejected: %v", terminal, stopped, err)
			}
			decoded, err := definition.decodeDescriptor(key, record)
			if err != nil || decoded != descriptor {
				t.Fatalf("valid terminal=%q stopped=%v changed on decode: %+v, %v", terminal, stopped, decoded, err)
			}
		}
	}
	t.Run("cached-history-is-checked-before-canonicalization", func(t *testing.T) {
		descriptor.Terminal, descriptor.Stopped = pulse.StreamStopped, true
		valid, err := definition.encodeDescriptor(key, descriptor)
		if err != nil {
			t.Fatal(err)
		}
		state := &runState{def: definition, record: RunRecord{
			Streams:  map[string]StreamRecord{streamKey(valid.ID): {Stream: valid}},
			Attempts: map[string]AttemptRecord{attempt: {Parent: scope, NodeID: "source", Index: 1, Limit: 1, State: AttemptConfirmed}},
			Scopes: map[string]ScopeRecord{scope: {Graph: descriptor.Graph, Nodes: map[string]NodeRecord{
				"source": {NodeID: "source", Attempt: attempt, Reason: string(pulse.NodeCompleted), Outputs: []ValueRecord{{Name: key.Name, Stream: &valid}}},
			}}},
		}}
		if _, err := state.restoreStreamReference(key, valid); err != nil {
			t.Fatal("valid reference did not warm the restore cache: ", err)
		}
		invalid := valid
		invalid.Stopped = false
		if _, err := state.restoreStreamReference(key, invalid); err == nil {
			t.Fatal("canonical cache concealed a stopped/false historical reference")
		}
	})
}
