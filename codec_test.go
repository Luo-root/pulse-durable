package durable

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
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
