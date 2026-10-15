package durable

import (
	"reflect"
	"testing"

	"github.com/Luo-root/pulse"
)

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
