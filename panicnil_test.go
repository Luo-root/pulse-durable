package durable

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
)

const panicNilModeEnv = "PULSE_DURABLE_PANICNIL_MODE"

// Each helper inherits the race-instrumented test binary, with its own timeout
// and GODEBUG. Never mutate process-wide GODEBUG while other tests may run.
func TestPanicNilModes(t *testing.T) {
	if os.Getenv(panicNilModeEnv) != "" {
		return
	}
	for _, mode := range []string{"0", "1"} {
		t.Run("panicnil="+mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0],
				"-test.run=^(TestPanicNil|TestRuntimeStoreOperationsAreCheckpointFailures$|TestRecoveryEventsClassifyRootAndChildFailures$)",
				"-test.count=1", "-test.timeout=15s", "-test.v")
			for _, entry := range os.Environ() {
				key, _, _ := strings.Cut(entry, "=")
				if !strings.EqualFold(key, "GODEBUG") && !strings.EqualFold(key, panicNilModeEnv) {
					cmd.Env = append(cmd.Env, entry)
				}
			}
			cmd.Env = append(cmd.Env, "GODEBUG=panicnil="+mode, panicNilModeEnv+"="+mode)
			output, err := cmd.CombinedOutput()
			if err != nil || ctx.Err() != nil {
				t.Fatalf("panicnil=%s helper: %v (deadline: %v)\n%s", mode, err, ctx.Err(), output)
			}
			t.Logf("panicnil=%s helper:\n%s", mode, output)
		})
	}
}

func TestPanicNilModeState(t *testing.T) {
	requirePanicNilChild(t)
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		panic(nil)
	}()
	if (recovered == nil) != (os.Getenv(panicNilModeEnv) == "1") {
		t.Fatalf("helper did not use its requested panicnil mode: recovered=%T", recovered)
	}
}

func requirePanicNilChild(t *testing.T) {
	t.Helper()
	if os.Getenv(panicNilModeEnv) == "" {
		t.Skip("covered in isolated default and legacy mode subprocesses")
	}
}

type panicNilSaveSession struct {
	Session
	calls int
}

func (s *panicNilSaveSession) Apply(context.Context, ChangeBatch) (CommitAck, error) {
	s.calls++
	panic(nil)
}

func TestPanicNilSaveStops(t *testing.T) {
	requirePanicNilChild(t)
	session := &panicNilSaveSession{}
	state := &runState{session: session, record: RunRecord{Revision: 2}}
	first := state.applyDelta(context.Background(), newRecordTransaction(2))
	if first == nil || state.stopped != first || state.record.Revision != 2 {
		t.Fatalf("Save panic was accepted or not latched: err=%v stopped=%v revision=%d", first, state.stopped, state.record.Revision)
	}
	if again := state.applyDelta(context.Background(), newRecordTransaction(2)); again != first || session.calls != 1 {
		t.Fatalf("Save continued after uncertainty: err=%v calls=%d", again, session.calls)
	}
}

type panicNilMetadataCodec struct {
	jsonCodec[int]
	method string
}

func (c panicNilMetadataCodec) Type() reflect.Type {
	if c.method == "Type" {
		panic(nil)
	}
	return c.jsonCodec.Type()
}

func (c panicNilMetadataCodec) ID() string {
	if c.method == "ID" {
		panic(nil)
	}
	return c.jsonCodec.ID()
}

func (c panicNilMetadataCodec) Version() string {
	if c.method == "Version" {
		panic(nil)
	}
	return c.jsonCodec.Version()
}

func TestPanicNilRegistration(t *testing.T) {
	requirePanicNilChild(t)
	for _, method := range []string{"Type", "ID", "Version"} {
		t.Run(method, func(t *testing.T) {
			registry := &Codecs{}
			codec := panicNilMetadataCodec{jsonCodec: jsonCodec[int]{id: "panicnil/int", version: "1"}, method: method}
			if err := registry.Register(codec); err == nil || err.Error() != "durable: codec registration panicked" {
				t.Fatalf("metadata panic was accepted: %v", err)
			}
			if err := RegisterJSON[int](registry, "panicnil/int", "1"); err != nil {
				t.Fatalf("failed metadata call partially registered the codec: %v", err)
			}
		})
	}
}

func TestPanicNilCodecCalls(t *testing.T) {
	requirePanicNilChild(t)
	for _, typ := range []reflect.Type{reflect.TypeFor[int](), reflect.TypeFor[any]()} {
		t.Run(typ.String(), func(t *testing.T) {
			codec := codecFunc{typeOf: typ, id: "panicnil/value", ver: "1",
				encodeFn: func(any) ([]byte, error) { panic(nil) },
				decodeFn: func([]byte) (any, error) { panic(nil) }}
			entry := codecEntry{typ: typ, id: codec.id, version: codec.ver, codec: codec}
			if data, err := entry.encode(7); err == nil || data != nil || !strings.HasSuffix(err.Error(), "encode panicked") {
				t.Fatalf("Encode panic was accepted: data=%q err=%v", data, err)
			}
			if value, err := entry.decode([]byte("7")); err == nil || value != nil || !strings.HasSuffix(err.Error(), "decode panicked") {
				t.Fatalf("Decode panic was accepted: value=%v err=%v", value, err)
			}
		})
	}
}

func TestPanicNilBeforeStart(t *testing.T) {
	requirePanicNilChild(t)
	for _, failure := range []string{"nil", "skip"} {
		t.Run(failure, func(t *testing.T) {
			store := NewMemoryStore()
			g := testGraph(t, "before-start-panicnil")
			var body atomic.Int32
			testAdd(t, g, pulse.NewNode("work", "", nil, nil, func(*pulse.RunCtx) error { body.Add(1); return nil }))
			opts := testOptions("run")
			opts.BeforeStart = func(*pulse.Execution) error {
				if failure == "skip" {
					panic(pulse.ErrSkipped)
				}
				panic(nil)
			}
			execution, err := testRuntime(t, store, NewCodecs()).Run(context.Background(), g, pulse.Inputs{}, opts)
			if err == nil || execution == nil || execution.Snapshot().Graph.Status != pulse.GraphStatusPending || body.Load() != 0 {
				t.Fatalf("BeforeStart panic entered business: execution=%v body=%d err=%v", execution != nil, body.Load(), err)
			}
			if failure == "skip" && !errors.Is(err, pulse.ErrSkipped) {
				t.Fatal("callback panic lost its error cause")
			}
			// A fresh runtime must be able to acquire and continue the initialized run.
			if _, err := testRuntime(t, store, NewCodecs()).Resume(context.Background(), g, pulse.Inputs{}, testOptions("run")); err != nil || body.Load() != 1 {
				t.Fatalf("BeforeStart panic leaked ownership or changed the record: body=%d err=%v", body.Load(), err)
			}
		})
	}
}

func TestPanicNilLaterRecoveryEvent(t *testing.T) {
	requirePanicNilChild(t)
	calls := 0
	state := &runState{event: func(Event) error {
		calls++
		if calls == 2 {
			panic(nil)
		}
		return nil
	}}
	err := state.restored("scope", []pulse.NodeCheckpoint{{NodeID: "first"}, {NodeID: "second"}, {NodeID: "third"}})
	if err == nil || calls != 2 {
		t.Fatalf("later callback panic was accepted: calls=%d err=%v", calls, err)
	}
}
