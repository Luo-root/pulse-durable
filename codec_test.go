package durable

import (
	"context"
	"errors"
	"fmt"
	"github.com/Luo-root/pulse"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

type codecFunc struct {
	typeOf   reflect.Type
	id, ver  string
	encodeFn func(any) ([]byte, error)
	decodeFn func([]byte) (any, error)
}

func (c codecFunc) Type() reflect.Type           { return c.typeOf }
func (c codecFunc) ID() string                   { return c.id }
func (c codecFunc) Version() string              { return c.ver }
func (c codecFunc) Encode(v any) ([]byte, error) { return c.encodeFn(v) }
func (c codecFunc) Decode(b []byte) (any, error) { return c.decodeFn(b) }

func TestCodecRegistryUsesRealTypes(t *testing.T) {
	type left int
	type right int
	c := NewCodecs()
	if err := RegisterJSON[left](c, "business/integer", "1"); err != nil {
		t.Fatal(err)
	}
	if err := RegisterJSON[right](c, "business/integer", "1"); !errors.Is(err, ErrConflict) {
		t.Fatalf("same ID with a different type: %v", err)
	}
	if err := RegisterJSON[left](c, "business/left", "2"); !errors.Is(err, ErrConflict) {
		t.Fatalf("same type with a different ID: %v", err)
	}
	if _, ok := c.snapshot()[reflect.TypeFor[right]()]; ok {
		t.Fatal("failed registration changed registry")
	}
	if err := RegisterJSON[chan int](c, "channel", "1"); err == nil {
		t.Fatal("channel registered")
	}
	if err := RegisterJSON[struct{ Stream chan int }](c, "nested-channel", "1"); err == nil {
		t.Fatal("nested channel registered")
	}
	if err := RegisterJSON[any](c, "interface", "1"); err == nil {
		t.Fatal("JSON interface registered")
	}
}

func TestCodecFailClosedAndRedactsErrors(t *testing.T) {
	const secret = "secret-business-value"
	base := codecFunc{typeOf: reflect.TypeFor[int](), id: "test/int", ver: "1",
		encodeFn: func(any) ([]byte, error) { return nil, fmt.Errorf("%s", secret) },
		decodeFn: func([]byte) (any, error) { return nil, fmt.Errorf("%s", secret) }}
	entry := codecEntry{typ: base.typeOf, id: base.id, version: base.ver, codec: base}
	_, err := entry.encode(1)
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("encode failure: %v", err)
	}
	_, err = entry.decode([]byte(secret))
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("decode failure: %v", err)
	}
	base.encodeFn = func(any) ([]byte, error) { panic(secret) }
	base.decodeFn = func([]byte) (any, error) { panic(secret) }
	entry.codec = base
	_, err = entry.encode(1)
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("encode panic: %v", err)
	}
	_, err = entry.decode([]byte("1"))
	if err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("decode panic: %v", err)
	}
	base.decodeFn = func([]byte) (any, error) { return "wrong", nil }
	entry.codec = base
	if _, err := entry.decode([]byte("1")); err == nil {
		t.Fatal("wrong decoded type accepted")
	}
	for _, value := range []any{nil, (*int)(nil), []byte(nil), map[string]int(nil)} {
		if _, err := entry.encode(value); err == nil {
			t.Fatalf("nil or wrong type accepted: %T", value)
		}
	}
}

func TestJSONCodecRejectsMissingCorruptNullAndTrailingData(t *testing.T) {
	c := NewCodecs().snapshot()[reflect.TypeFor[int]()]
	for _, data := range [][]byte{nil, []byte("{"), []byte("null"), []byte("1 2"), []byte(`"secret"`)} {
		if _, err := c.decode(data); err == nil {
			t.Fatalf("invalid encoded input accepted")
		}
	}
	value, err := c.decode([]byte("42"))
	if err != nil || value != 42 {
		t.Fatalf("JSON roundtrip: %v %v", value, err)
	}
	type message struct{ Name string }
	registry := &Codecs{}
	if err := RegisterJSON[message](registry, "message", "1"); err != nil {
		t.Fatal(err)
	}
	m := registry.snapshot()[reflect.TypeFor[message]()]
	if _, err := m.decode([]byte(`{"Unknown":1}`)); err == nil {
		t.Fatal("unknown fields accepted")
	}
}

// 空编码是 Codec 决定的合法表示；ready + checksum 已能区别未保存/损坏的数据。
func TestEmptyCodecEncodingRoundTripsConfirmedValue(t *testing.T) {
	type empty struct{}
	codecs := NewCodecs()
	if err := codecs.Register(codecFunc{typeOf: reflect.TypeFor[empty](), id: "test/empty", ver: "1",
		encodeFn: func(any) ([]byte, error) { return nil, nil },
		decodeFn: func(data []byte) (any, error) {
			if len(data) != 0 {
				t.Fatal("unexpected bytes")
			}
			return empty{}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	g := testGraph(t, "empty-encoding")
	key := pulse.NewKey[empty]("result")
	var calls int
	testAdd(t, g, pulse.NewNode("n", "合法空编码", nil, pulse.Provides(key), func(rc *pulse.RunCtx) error { calls++; return pulse.Set(rc, key, empty{}) }))
	if err := g.DeclareOutput(pulse.NewPort(key)); err != nil {
		t.Fatal(err)
	}
	store := NewMemoryStore()
	runtime := testRuntime(t, store, codecs)
	if _, err := runtime.Run(context.Background(), g, pulse.Inputs{}, testOptions("empty")); err != nil {
		t.Fatal(err)
	}
	e, err := runtime.Resume(context.Background(), g, pulse.Inputs{}, testOptions("empty"))
	if err != nil {
		t.Fatal(err)
	}
	_, state, err := pulse.ReadOutput(e, key)
	if state != pulse.SlotStateReady || err != nil || calls != 1 {
		t.Fatalf("state=%s err=%v calls=%d", state, err, calls)
	}
	record := readRecord(t, store, "empty")
	value := record.Scopes[record.RootScope].Nodes["n"].Outputs[0]
	if len(value.Data) != 0 || !validDigest(value.Digest) {
		t.Fatal("empty encoded value was not confirmed with checksum")
	}
	rewriteRecord(t, store, "empty", func(r *RunRecord) {
		scope := r.Scopes[r.RootScope]
		n := scope.Nodes["n"]
		n.Outputs[0].Digest = ""
		scope.Nodes["n"] = n
		r.Scopes[r.RootScope] = scope
	})
	if _, err := runtime.Resume(context.Background(), g, pulse.Inputs{}, testOptions("empty")); err == nil {
		t.Fatal("missing checksum was accepted as an empty encoded value")
	}
}

type codecCauseError struct{ cause error }

func (*codecCauseError) Error() string   { return "private codec value and bytes" }
func (e *codecCauseError) Unwrap() error { return e.cause }

func checkCodecCause(t *testing.T, err error, cause *codecCauseError) {
	t.Helper()
	var typed *codecCauseError
	if err == nil || !errors.Is(err, cause.cause) || !errors.As(err, &typed) || typed != cause {
		t.Fatalf("codec cause was lost: %v", err)
	}
	if strings.Contains(err.Error(), "private") {
		t.Fatalf("codec error leaked business text: %v", err)
	}
}

func TestCodecErrorsRetainCauseWithoutExposingText(t *testing.T) {
	cause := &codecCauseError{cause: errors.New("codec sentinel")}
	codec := codecFunc{typeOf: reflect.TypeFor[int](), id: "cause/int", ver: "1",
		encodeFn: func(any) ([]byte, error) { return []byte("private"), cause },
		decodeFn: func([]byte) (any, error) { return 7, cause }}
	entry := codecEntry{typ: codec.typeOf, id: codec.id, version: codec.ver, codec: codec}
	data, err := entry.encode(7)
	checkCodecCause(t, err, cause)
	if data != nil || err.Error() != `durable: codec "cause/int" encode failed` {
		t.Fatalf("encode returned partial data or changed safe context: %q %v", data, err)
	}
	value, err := entry.decode([]byte("private"))
	checkCodecCause(t, err, cause)
	if value != nil || err.Error() != `durable: codec "cause/int" decode failed` {
		t.Fatalf("decode returned partial value or changed safe context: %v %v", value, err)
	}
}

func TestCodecInvalidDecodedValuesAreNotErrorCauses(t *testing.T) {
	wrongErrorValue := &codecCauseError{cause: errors.New("not a returned error")}
	for _, value := range []any{nil, "private wrong type", wrongErrorValue} {
		codec := codecFunc{typeOf: reflect.TypeFor[int](), id: "cause/int", ver: "1",
			decodeFn: func([]byte) (any, error) { return value, nil }}
		entry := codecEntry{typ: codec.typeOf, id: codec.id, version: codec.ver, codec: codec}
		got, err := entry.decode([]byte("7"))
		var typed *codecCauseError
		if got != nil || err == nil || errors.As(err, &typed) || strings.Contains(err.Error(), "private") {
			t.Fatalf("invalid decoded value became an error cause or escaped validation: %T %v", value, err)
		}
	}
}

func TestCodecRuntimeErrorsRetainCauseAndStop(t *testing.T) {
	for _, mode := range []string{"encode-input", "encode-commit", "decode-commit", "decode-resume"} {
		t.Run(mode, func(t *testing.T) {
			cause := &codecCauseError{cause: errors.Join(errors.New("codec sentinel"), pulse.ErrSkipped)}
			var encodeFail, decodeFail atomic.Bool
			codec := codecFunc{typeOf: reflect.TypeFor[int](), id: "cause/int", ver: "1",
				encodeFn: func(any) ([]byte, error) {
					if encodeFail.Load() {
						return nil, cause
					}
					return []byte("7"), nil
				},
				decodeFn: func([]byte) (any, error) {
					if decodeFail.Load() {
						return nil, cause
					}
					return 7, nil
				}}
			codecs := &Codecs{}
			if err := codecs.Register(codec); err != nil {
				t.Fatal(err)
			}
			store := NewMemoryStore()
			rt := testRuntime(t, store, codecs)
			g := testGraph(t, "codec-cause")
			input, output := pulse.NewKey[int]("input"), pulse.NewKey[int]("output")
			var in pulse.Inputs
			requires := pulse.Requires[int]()
			if mode == "encode-input" {
				if err := g.DeclareInput(pulse.NewPort(input)); err != nil {
					t.Fatal(err)
				}
				if err := pulse.Seed(&in, input, 7); err != nil {
					t.Fatal(err)
				}
				requires = pulse.Requires(input)
			}
			var body, downstream atomic.Int32
			testAdd(t, g, pulse.NewNode("source", "", requires, pulse.Provides(output), func(rc *pulse.RunCtx) error {
				body.Add(1)
				return pulse.Set(rc, output, 7)
			}))
			testAdd(t, g, pulse.NewNode("downstream", "", pulse.Requires(output), nil, func(*pulse.RunCtx) error {
				downstream.Add(1)
				return nil
			}))
			if mode == "decode-resume" {
				if _, err := rt.Run(context.Background(), g, in, testOptions("run")); err != nil {
					t.Fatal(err)
				}
			}
			encodeFail.Store(strings.HasPrefix(mode, "encode-"))
			decodeFail.Store(strings.HasPrefix(mode, "decode-"))
			var err error
			if mode == "decode-resume" {
				_, err = rt.Resume(context.Background(), g, in, testOptions("run"))
			} else {
				_, err = rt.Run(context.Background(), g, in, testOptions("run"))
			}
			checkCodecCause(t, err, cause)
			var checkpoint *pulse.CheckpointError
			if !errors.As(err, &checkpoint) {
				t.Fatalf("codec operation was not a checkpoint failure: %v", err)
			}
			wantBody, wantDownstream := int32(1), int32(0)
			if mode == "encode-input" {
				wantBody = 0
			}
			if mode == "decode-resume" {
				wantDownstream = 1
			}
			if body.Load() != wantBody || downstream.Load() != wantDownstream {
				t.Fatalf("codec failure advanced business: body=%d downstream=%d", body.Load(), downstream.Load())
			}
			if mode != "encode-input" {
				record := readRecord(t, store, "run")
				if mode != "decode-resume" && recordHasNode(record, "codec-cause", "source") {
					t.Fatal("failed codec committed an output")
				}
				encodeFail.Store(false)
				decodeFail.Store(false)
				if _, err := rt.Resume(context.Background(), g, in, testOptions("run")); err != nil {
					t.Fatal(err)
				}
				if mode == "decode-resume" && (body.Load() != 1 || downstream.Load() != 1) {
					t.Fatal("decode failure discarded confirmed business")
				}
			}
		})
	}
}

func TestJSONRejectsNestedInterfacesBeforeRegistration(t *testing.T) {
	cases := []func(*Codecs) error{
		func(c *Codecs) error { return RegisterJSON[struct{ Payload any }](c, "object", "1") },
		func(c *Codecs) error { return RegisterJSON[[]any](c, "slice", "1") },
		func(c *Codecs) error { return RegisterJSON[map[string]any](c, "map", "1") },
		func(c *Codecs) error { return RegisterJSON[*struct{ Payload any }](c, "pointer", "1") },
	}
	for i, register := range cases {
		c := &Codecs{}
		if err := register(c); err == nil {
			t.Fatalf("case %d accepted a type whose dynamic identity JSON loses", i)
		}
		if len(c.snapshot()) != 0 {
			t.Fatal("failed JSON registration changed the registry")
		}
	}
	// 自指具体类型可以注册，类型遍历不能挂死或按“有指针”过宽拒绝。
	type concrete struct {
		Text string
		Next *concrete
	}
	if err := RegisterJSON[concrete](&Codecs{}, "concrete", "1"); err != nil {
		t.Fatal(err)
	}
}

func TestJSONRejectsInvalidUTF8IncludingFieldsAndMapKeys(t *testing.T) {
	bad := string([]byte{0xff})
	cases := []struct {
		codec Codec
		value any
	}{
		{jsonCodec[string]{"string", "1"}, bad},
		{jsonCodec[[]string]{"slice", "1"}, []string{bad}},
		{jsonCodec[map[string]string]{"map-key", "1"}, map[string]string{bad: "valid"}},
		{jsonCodec[map[string]string]{"map-value", "1"}, map[string]string{"valid": bad}},
		{jsonCodec[struct{ Text string }]{"object", "1"}, struct{ Text string }{bad}},
	}
	for _, tc := range cases {
		entry := codecEntry{typ: tc.codec.Type(), id: tc.codec.ID(), version: tc.codec.Version(), codec: tc.codec}
		if _, err := entry.encode(tc.value); err == nil {
			t.Fatalf("%s silently replaced invalid UTF-8", entry.id)
		}
	}
	if _, err := (jsonCodec[string]{}).Decode([]byte{'"', 0xff, '"'}); err == nil {
		t.Fatal("invalid UTF-8 JSON decoded silently")
	}
	value, err := NewCodecs().snapshot()[reflect.TypeFor[string]()].decode([]byte(`"中文🙂�"`))
	if err != nil || value != "中文🙂�" {
		t.Fatalf("valid UTF-8 was over-rejected: %v", err)
	}
}

func TestInvalidStringInputsDoNotAcquireMatchingFingerprint(t *testing.T) {
	g := testGraph(t, "string-input")
	input := pulse.NewKey[string]("input")
	if err := g.DeclareInput(pulse.NewPort(input)); err != nil {
		t.Fatal(err)
	}
	testAdd(t, g, pulse.NewNode("n", "输入指纹不能改写字节", pulse.Requires(input), nil, nil))
	store := NewMemoryStore()
	runtime := testRuntime(t, store, NewCodecs())
	for _, b := range []byte{0xff, 0xfe} {
		var in pulse.Inputs
		if err := pulse.Seed(&in, input, string([]byte{b})); err != nil {
			t.Fatal(err)
		}
		if _, err := runtime.Run(context.Background(), g, in, testOptions("run")); err == nil {
			t.Fatal("invalid input was persisted")
		}
	}
	s, err := store.Acquire(context.Background(), "run")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Load(context.Background()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("invalid input left a run record: %v", err)
	}
}

func TestCustomCodecPreservesArbitraryStringBytes(t *testing.T) {
	codecs := &Codecs{}
	if err := codecs.Register(codecFunc{typeOf: reflect.TypeFor[string](), id: "raw/string", ver: "1",
		encodeFn: func(v any) ([]byte, error) { return []byte(v.(string)), nil },
		decodeFn: func(b []byte) (any, error) { return string(b), nil },
	}); err != nil {
		t.Fatal(err)
	}
	g := testGraph(t, "raw-string")
	input, output := pulse.NewKey[string]("input"), pulse.NewKey[string]("output")
	if err := g.DeclareInput(pulse.NewPort(input)); err != nil {
		t.Fatal(err)
	}
	if err := g.DeclareOutput(pulse.NewPort(output)); err != nil {
		t.Fatal(err)
	}
	var calls int
	testAdd(t, g, pulse.NewNode("n", "自定义编码保留任意字节", pulse.Requires(input), pulse.Provides(output), func(rc *pulse.RunCtx) error {
		calls++
		v, err := pulse.Get(rc, input)
		if err != nil {
			return err
		}
		return pulse.Set(rc, output, v)
	}))
	store := NewMemoryStore()
	runtime := testRuntime(t, store, codecs)
	var in pulse.Inputs
	original := string([]byte{0xff})
	if err := pulse.Seed(&in, input, original); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Run(context.Background(), g, in, testOptions("raw")); err != nil {
		t.Fatal(err)
	}
	e, err := runtime.Resume(context.Background(), g, in, testOptions("raw"))
	if err != nil {
		t.Fatal(err)
	}
	v, state, err := pulse.ReadOutput(e, output)
	if err != nil || state != pulse.SlotStateReady || v != original || calls != 1 {
		t.Fatalf("bytes changed: state=%s error=%v calls=%d", state, err, calls)
	}
	var changed pulse.Inputs
	if err := pulse.Seed(&changed, input, string([]byte{0xfe})); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Resume(context.Background(), g, changed, testOptions("raw")); !errors.Is(err, ErrConflict) {
		t.Fatalf("different bytes obtained same fingerprint: %v", err)
	}
}

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
