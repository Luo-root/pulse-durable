package durable

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Luo-root/pulse"
)

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
