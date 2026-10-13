package durable

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/Luo-root/pulse"
)

func TestDynamicChannelValuesRejectedBeforeInputSave(t *testing.T) {
	channel := make(chan int)
	shared := []any{1, channel}
	for _, value := range []any{
		channel, (chan int)(nil), []any{channel}, map[string]any{"payload": channel},
		struct{ Payload any }{channel}, struct{ payload any }{channel},
		[]any{map[string]any{"payload": channel}}, []any{shared[:1], shared},
	} {
		t.Run(reflect.TypeOf(value).String(), func(t *testing.T) {
			codecs := &Codecs{}
			var encoded atomic.Int32
			if err := codecs.Register(codecFunc{typeOf: reflect.TypeFor[any](), id: "interface", ver: "1",
				encodeFn: func(any) ([]byte, error) { encoded.Add(1); return []byte("opaque"), nil },
				decodeFn: func([]byte) (any, error) { return nil, nil }}); err != nil {
				t.Fatal(err)
			}
			g := testGraph(t, "graph")
			input := pulse.NewKey[any]("input")
			if err := g.DeclareInput(pulse.NewPort(input)); err != nil {
				t.Fatal(err)
			}
			var calls atomic.Int32
			testAdd(t, g, pulse.NewNode("work", "", pulse.Requires(input), nil, func(*pulse.RunCtx) error { calls.Add(1); return nil }))
			var in pulse.Inputs
			if err := pulse.Seed(&in, input, value); err != nil {
				t.Fatal(err)
			}
			store := NewMemoryStore()
			if _, err := testRuntime(t, store, codecs).Run(context.Background(), g, in, testOptions("run")); err == nil {
				t.Fatal("dynamic channel input accepted")
			}
			if encoded.Load() != 0 || calls.Load() != 0 {
				t.Fatal("invalid input entered custom codec or business")
			}
			session, err := store.Acquire(context.Background(), "run")
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close()
			if _, err := session.Load(context.Background()); !errors.Is(err, ErrNotFound) {
				t.Fatal("dynamic channel input was persisted")
			}
			entry := codecs.snapshot()[reflect.TypeFor[any]()]
			badCodec := entry.codec.(codecFunc)
			badCodec.decodeFn = func([]byte) (any, error) { return value, nil }
			entry.codec = badCodec
			if _, err := entry.decode([]byte("opaque")); err == nil {
				t.Fatal("dynamic channel from Decode accepted")
			}
		})
	}
}

func TestDynamicValueCyclesAndNilRemainValid(t *testing.T) {
	type object struct{ Payload any }
	pointer := &object{}
	pointer.Payload = pointer
	mapped := map[string]any{}
	mapped["self"] = mapped
	sliced := make([]any, 1)
	sliced[0] = sliced
	for _, value := range []any{nil, (*object)(nil), []any(nil), map[string]any(nil), pointer, mapped, sliced} {
		if !validBusinessValue(reflect.TypeFor[any](), value) {
			t.Fatalf("ordinary nil/cycle rejected: %T", value)
		}
	}
	pointer.Payload = make(chan int)
	if validBusinessValue(reflect.TypeFor[any](), pointer) {
		t.Fatal("channel reachable through pointer accepted")
	}
}

func TestCustomCodecCycleRunResume(t *testing.T) {
	type object struct{ Payload any }
	codecs := &Codecs{}
	if err := codecs.Register(codecFunc{typeOf: reflect.TypeFor[*object](), id: "cyclic-object", ver: "1",
		encodeFn: func(any) ([]byte, error) { return []byte("self"), nil },
		decodeFn: func([]byte) (any, error) { o := &object{}; o.Payload = o; return o, nil }}); err != nil {
		t.Fatal(err)
	}
	g := testGraph(t, "graph")
	key := pulse.NewKey[*object]("object")
	var calls atomic.Int32
	testAdd(t, g, pulse.NewNode("work", "", nil, pulse.Provides(key), func(rc *pulse.RunCtx) error {
		calls.Add(1)
		o := &object{}
		o.Payload = o
		return pulse.Set(rc, key, o)
	}))
	if err := g.DeclareOutput(pulse.NewPort(key)); err != nil {
		t.Fatal(err)
	}
	rt := testRuntime(t, NewMemoryStore(), codecs)
	if _, err := rt.Run(context.Background(), g, pulse.Inputs{}, testOptions("run")); err != nil {
		t.Fatal(err)
	}
	e, err := rt.Resume(context.Background(), g, pulse.Inputs{}, testOptions("run"))
	if err != nil {
		t.Fatal(err)
	}
	value, state, err := pulse.ReadOutput(e, key)
	if err != nil || state != pulse.SlotStateReady || value == nil || value.Payload != value || calls.Load() != 1 {
		t.Fatal("custom cyclic value did not restore")
	}
}

func TestBusinessContextErrorsExhaustRootWithoutHostCancellation(t *testing.T) {
	for _, businessErr := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(businessErr.Error(), func(t *testing.T) {
			g := testGraph(t, "graph")
			var calls atomic.Int32
			testAdd(t, g, pulse.NewNode("work", "", nil, nil, func(*pulse.RunCtx) error { calls.Add(1); return businessErr }))
			store := NewMemoryStore()
			rt := testRuntime(t, store, NewCodecs())
			if _, err := rt.Run(context.Background(), g, pulse.Inputs{}, testOptions("run")); !errors.Is(err, businessErr) {
				t.Fatalf("business context error: %v", err)
			}
			if _, err := rt.Resume(context.Background(), g, pulse.Inputs{}, testOptions("run")); err == nil {
				t.Fatal("business context error reopened root")
			}
			if calls.Load() != 1 {
				t.Fatal("business context error was treated as host cancellation")
			}
		})
	}
}
