package durable

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/Luo-root/pulse"
)

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
