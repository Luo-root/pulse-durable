package durable

import (
	"context"
	"reflect"
	"testing"

	"github.com/Luo-root/pulse"
)

type streamProtocol struct {
	s                 *runState
	source, processor pulse.CheckpointAttempt
	input, output     pulse.CheckpointStreamRecord
	inKey, outKey     pulse.CheckpointKey
	value             pulse.CheckpointKey
	site              pulse.Site
}

func newStreamProtocol(t *testing.T) streamProtocol {
	t.Helper()
	g := testGraph(t, "streams")
	input, output, value := pulse.NewKey[pulse.Stream[int]]("input"), pulse.NewKey[pulse.Stream[int]]("output"), pulse.NewKey[int]("value")
	testAdd(t, g, pulse.NewNode("source", "", nil, pulse.Provides(input), func(*pulse.RunCtx) error { return nil }))
	testAdd(t, g, pulse.NewNode("processor", "", pulse.Requires(input), pulse.Deps(pulse.Provides(output), pulse.Provides(value)), func(*pulse.RunCtx) error { return nil }))
	testAdd(t, g, pulse.NewNode("sink", "", pulse.Requires(output), nil, func(*pulse.RunCtx) error { return nil }))
	def, err := inspectDefinition(g, NewCodecs().snapshot())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	store := NewMemoryStore()
	session, err := store.Acquire(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	root, _ := randomID()
	hash, _ := def.inputsFingerprint(def.root, nil)
	s := &runState{session: session, def: def, record: RunRecord{Format: RecordFormat, RunID: "run", Version: "1", Definition: def.fingerprint,
		Inputs: hash, RootScope: root, Scopes: map[string]ScopeRecord{root: {Graph: "streams", Inputs: hash, Nodes: map[string]NodeRecord{}}}, Attempts: map[string]AttemptRecord{}}}
	if err := s.save(ctx, cloneRecord(s.record)); err != nil {
		t.Fatal(err)
	}
	p := streamProtocol{s: s, site: pulse.Site{Graph: "streams"}, inKey: pulse.CheckpointKey{Name: "input", Type: reflect.TypeFor[pulse.Stream[int]](), Element: reflect.TypeFor[int]()},
		outKey: pulse.CheckpointKey{Name: "output", Type: reflect.TypeFor[pulse.Stream[int]](), Element: reflect.TypeFor[int]()}, value: pulse.CheckpointKey{Name: "value", Type: reflect.TypeFor[int]()}}
	p.source, err = s.openAttempt(ctx, pulse.CheckpointAttemptRequest{Site: p.site, Parent: root, NodeID: "source", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	p.processor, err = s.openAttempt(ctx, pulse.CheckpointAttemptRequest{Site: p.site, Parent: root, NodeID: "processor", Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	p.input, err = s.openStream(ctx, p.openRequest(false))
	if err != nil {
		t.Fatal(err)
	}
	p.output, err = s.openStream(ctx, p.openRequest(true))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func (p streamProtocol) openRequest(output bool) pulse.CheckpointStreamOpen {
	key, attempt, node := p.inKey, p.source.Scope, "source"
	if output {
		key, attempt, node = p.outKey, p.processor.Scope, "processor"
	}
	return pulse.CheckpointStreamOpen{Site: p.site, Scope: p.s.record.RootScope, Attempt: attempt, NodeID: node, Key: key,
		Group: "default", Source: pulse.CheckpointStreamSource{ID: node, Resumable: true}, Capacity: 8}
}

func (p streamProtocol) append(sequence uint64) error {
	return p.s.appendStream(context.Background(), pulse.CheckpointStreamAppend{Site: p.site, Attempt: p.source.Scope, Stream: p.input.Stream.ID,
		Message: pulse.CheckpointStreamMessage{Sequence: sequence, Value: int(sequence)}, Cursor: int(sequence)})
}

func (p streamProtocol) messageRequest(sequence uint64, limit int) pulse.CheckpointMessageAttemptRequest {
	return pulse.CheckpointMessageAttemptRequest{Site: p.site, Scope: p.s.record.RootScope, Attempt: p.processor.Scope, NodeID: "processor",
		Input: pulse.CheckpointStreamAcknowledgement{Stream: p.input.Stream.ID, Sequence: sequence}, Limit: limit}
}
