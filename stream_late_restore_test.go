package durable

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
)

func TestStreamLateChildRestoresSameEndpoint(t *testing.T) {
	base := NewMemoryStore()
	sourceDone, advanced := make(chan struct{}), make(chan struct{})
	var sourceOnce, advanceOnce sync.Once
	var calls atomic.Int32
	endpoints := make(chan pulse.Stream[int], 1)
	observed := &streamObservationStore{Store: base, afterSave: func(record RunRecord) {
		if recordHasNode(record, "late-root", "source") {
			sourceOnce.Do(func() { close(sourceDone) })
		}
		for _, stream := range record.Streams {
			if stream.Stream.ID.Generation == 2 {
				advanceOnce.Do(func() { close(advanced) })
			}
		}
	}}
	build := func(resume bool) *pulse.Graph {
		root, child, pass := testGraph(t, "late-root"), testGraph(t, "late-child"), testGraph(t, "late-pass")
		original, inlet := pulse.NewKey[pulse.Stream[int]]("original"), pulse.NewKey[pulse.Stream[int]]("inlet")
		passIn, alias := pulse.NewKey[pulse.Stream[int]]("pass-in"), pulse.NewKey[pulse.Stream[int]]("alias")
		trigger, childTrigger, passTrigger := pulse.NewKey[int]("trigger"), pulse.NewKey[int]("child-trigger"), pulse.NewKey[int]("pass-trigger")
		sum, final := pulse.NewKey[int]("sum"), pulse.NewKey[int]("final")
		if err := root.DeclareOutput(pulse.NewPort(original), pulse.NewPort(final)); err != nil {
			t.Fatal(err)
		}
		testAdd(t, root, pulse.NewNode("source", "", nil, pulse.Provides(original), func(rc *pulse.RunCtx) error {
			calls.Add(1)
			writer, err := pulse.OpenStream(rc, original, pulse.WithBuffer(2), pulse.WithStreamSource("late-source", true))
			if err != nil {
				return err
			}
			for next := int(writer.Position().Sequence) + 1; next <= 2; next++ {
				if err := writer.SendAt(next, next); err != nil {
					return err
				}
			}
			return nil
		}))
		testAdd(t, root, pulse.NewNode("trigger", "", nil, pulse.Provides(trigger), func(rc *pulse.RunCtx) error {
			select {
			case <-sourceDone:
				return pulse.Set(rc, trigger, 1)
			case <-rc.Context().Done():
				return rc.Context().Err()
			}
		}))
		if err := child.DeclareInput(pulse.NewPort(inlet), pulse.NewPort(childTrigger)); err != nil {
			t.Fatal(err)
		}
		if err := child.DeclareOutput(pulse.NewPort(sum)); err != nil {
			t.Fatal(err)
		}
		if err := pass.DeclareInput(pulse.NewPort(passIn), pulse.NewPort(passTrigger)); err != nil {
			t.Fatal(err)
		}
		if err := pass.DeclareOutput(pulse.NewPort(passIn)); err != nil {
			t.Fatal(err)
		}
		testAdd(t, pass, pulse.NewNode("wait", "", pulse.Requires(passTrigger), nil, nil))
		if err := pulse.Sub(child, "pass", "", []pulse.SubBind{pulse.In(inlet, passIn), pulse.In(childTrigger, passTrigger), pulse.Out(passIn, alias)}, pass); err != nil {
			t.Fatal(err)
		}
		testAdd(t, child, pulse.NewNode("consumer", "", pulse.Requires(alias), pulse.Provides(sum), func(rc *pulse.RunCtx) error {
			if !resume {
				<-rc.Context().Done()
				return rc.Context().Err()
			}
			stream, err := pulse.Get(rc, alias)
			if err != nil {
				return err
			}
			endpoints <- stream
			total := 0
			for {
				message, err := stream.Receive(rc)
				if err == io.EOF {
					return pulse.Set(rc, sum, total)
				}
				if err != nil {
					return err
				}
				total += message.Value
				if err := message.Ack(); err != nil {
					return err
				}
			}
		}))
		delayMount := func(rc *pulse.RunCtx, next func(*pulse.RunCtx) error) error {
			if resume {
				select {
				case <-advanced:
				case <-rc.Context().Done():
					return rc.Context().Err()
				}
			}
			return next(rc)
		}
		if err := pulse.Sub(root, "mount", "", []pulse.SubBind{pulse.In(original, inlet), pulse.In(trigger, childTrigger), pulse.Out(sum, final)}, child, delayMount); err != nil {
			t.Fatal(err)
		}
		return root
	}
	fault := &faultStore{Store: observed, after: true, failure: errors.New("interrupt after inner pass"), match: func(record RunRecord) bool {
		return recordHasNode(record, "late-child", "pass")
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if _, err := testRuntime(t, fault, NewCodecs()).Run(ctx, build(false), pulse.Inputs{}, testOptions("late-child")); err == nil || !fault.fired.Load() {
		t.Fatalf("wrong interruption: %v", err)
	}
	before := readRecord(t, base, "late-child")
	for _, stream := range before.Streams {
		if stream.Stream.ID.Generation != 1 || len(stream.Pending) != 2 {
			t.Fatal("test did not preserve two pending messages at generation 1")
		}
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel2()
	execution, err := testRuntime(t, observed, NewCodecs()).Resume(ctx2, build(true), pulse.Inputs{}, testOptions("late-child"))
	if err != nil {
		t.Fatal(err)
	}
	rootEndpoint, state, err := pulse.ReadOutput(execution, pulse.NewKey[pulse.Stream[int]]("original"))
	if err != nil || state != pulse.SlotStateReady {
		t.Fatal(err)
	}
	if childEndpoint := <-endpoints; childEndpoint != rootEndpoint {
		t.Fatal("late child created a second local endpoint")
	}
	total, state, err := pulse.ReadOutput(execution, pulse.NewKey[int]("final"))
	if err != nil || state != pulse.SlotStateReady || total != 3 || calls.Load() != 1 {
		t.Fatalf("calls=%d total=%d state=%s err=%v", calls.Load(), total, state, err)
	}
	if snapshot := rootEndpoint.Snapshot(); snapshot.AckedThrough != 2 || snapshot.InFlight != 0 {
		t.Fatalf("source ledger not shared: %+v", snapshot)
	}
}

func TestStreamCanonicalCacheChecksEveryReference(t *testing.T) {
	protocol := newStreamProtocol(t)
	if err := protocol.append(1); err != nil {
		t.Fatal(err)
	}
	descriptor := protocol.input.Stream
	descriptor.Terminal = pulse.StreamCompleted
	if err := protocol.s.commit(context.Background(), pulse.CheckpointCommit{Site: protocol.site, Scope: protocol.s.record.RootScope, Attempt: protocol.source.Scope,
		Node:    pulse.NodeCheckpoint{NodeID: "source", Reason: pulse.NodeCompleted, Outputs: []pulse.CheckpointOutput{{Key: protocol.inKey, State: pulse.SlotStateReady, Stream: &descriptor}}},
		Streams: []pulse.CheckpointStreamFinish{{Site: protocol.site, Attempt: protocol.source.Scope, Stream: protocol.input.Stream.ID, Terminal: pulse.StreamCompleted, LastSequence: 1, Cursor: 1}}}); err != nil {
		t.Fatal(err)
	}
	historical := protocol.s.record.Streams[streamKey(streamID(protocol.input.Stream.ID))].Stream
	expected, err := protocol.s.restoreStreamReference(protocol.inKey, historical)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*StreamDescriptorRecord){
		"source":            func(d *StreamDescriptorRecord) { d.SourceID = "forged" },
		"owner":             func(d *StreamDescriptorRecord) { d.Attempt = protocol.processor.Scope },
		"graph":             func(d *StreamDescriptorRecord) { d.Graph = "forged" },
		"path":              func(d *StreamDescriptorRecord) { d.Path = "forged" },
		"codec":             func(d *StreamDescriptorRecord) { d.TypeID = "forged" },
		"version":           func(d *StreamDescriptorRecord) { d.Version = "forged" },
		"capacity":          func(d *StreamDescriptorRecord) { d.Capacity++ },
		"resumable":         func(d *StreamDescriptorRecord) { d.Resumable = !d.Resumable },
		"terminal":          func(d *StreamDescriptorRecord) { d.Terminal = "stopped" },
		"stopped":           func(d *StreamDescriptorRecord) { d.Stopped = true },
		"future-generation": func(d *StreamDescriptorRecord) { d.ID.Generation++ },
		"zero-generation":   func(d *StreamDescriptorRecord) { d.ID.Generation = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			bad := historical
			mutate(&bad)
			if _, err := protocol.s.restoreStreamReference(protocol.inKey, bad); err == nil {
				t.Fatal("cached canonical reference bypassed validation")
			}
			actual, err := protocol.s.restoreStreamReference(protocol.inKey, historical)
			if err != nil || actual != expected {
				t.Fatal("rejected reference poisoned cache")
			}
		})
	}
}
