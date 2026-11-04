package durable_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)

func preparationFixture(t *testing.T) (*pulse.Graph, pulse.Key[[]byte], *pulse.Registry) {
	t.Helper()
	g, err := pulse.New("preparation", "safe preparation")
	if err != nil {
		t.Fatal(err)
	}
	k := pulse.NewKey[[]byte]("input")
	if err = g.DeclareInput(pulse.NewPort(k)); err != nil {
		t.Fatal(err)
	}
	if err = g.Add(pulse.NewNode("sink", "must never start", pulse.Requires(k), nil, func(*pulse.RunCtx) error { t.Error("business executed during preparation"); return nil })); err != nil {
		t.Fatal(err)
	}
	keys := pulse.NewRegistry()
	if err = pulse.RegisterKey(keys, k); err != nil {
		t.Fatal(err)
	}
	return g, k, keys
}
func TestPreparationSkipNilAndRuntimeFingerprints(t *testing.T) {
	for _, skip := range []bool{false, true} {
		g, k, keys := preparationFixture(t)
		codecs := durable.NewCodecs()
		var in pulse.Inputs
		var err error
		if skip {
			err = pulse.SkipSeed(&in, k)
		} else {
			err = pulse.Seed(&in, k, []byte(nil))
		}
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := durable.Prepare(context.Background(), g, in, codecs)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := durable.DecodeInputs(context.Background(), g, codecs, keys, prepared.Payload)
		if err != nil {
			t.Fatal(err)
		}
		second, err := durable.Prepare(context.Background(), g, decoded, codecs)
		if err != nil || prepared.Inputs != second.Inputs || !bytes.Equal(prepared.Payload, second.Payload) {
			t.Fatal("round trip", err)
		}
		store := durable.NewMemoryStore()
		runtime, err := durable.New(store, codecs)
		if err != nil {
			t.Fatal(err)
		}
		stopped := errors.New("stop before business")
		_, err = runtime.Run(context.Background(), g, decoded, durable.RunOptions{RunID: "r", Version: "v", BeforeStart: func(*pulse.Execution) error { return stopped }})
		if !errors.Is(err, stopped) {
			t.Fatal(err)
		}
		session, err := store.Acquire(context.Background(), "r")
		if err != nil {
			t.Fatal(err)
		}
		record, err := session.Load(context.Background())
		_ = session.Close()
		if err != nil {
			t.Fatal(err)
		}
		if record.Definition != prepared.Definition || record.Inputs != prepared.Inputs {
			t.Fatal("Runtime fingerprint differs")
		}
	}
}
func TestDecodeRejectsCorruptionAndMissing(t *testing.T) {
	g, k, keys := preparationFixture(t)
	c := durable.NewCodecs()
	var in pulse.Inputs
	_ = pulse.Seed(&in, k, []byte("x"))
	p, err := durable.Prepare(context.Background(), g, in, c)
	if err != nil {
		t.Fatal(err)
	}
	for _, payload := range [][]byte{[]byte("[]"), []byte("null"), append(p.Payload, ' '), append(p.Payload, p.Payload...), bytes.Replace(p.Payload, []byte(`"version":"1"`), []byte(`"version":"2"`), 1), bytes.Replace(p.Payload, []byte(`"state":"ready"`), []byte(`"state":"unknown"`), 1), bytes.Replace(p.Payload, []byte(`"data":"`), []byte(`"data":"Y`), 1)} {
		if _, err := durable.DecodeInputs(context.Background(), g, c, keys, payload); err == nil {
			t.Fatalf("accepted bad payload %s", payload)
		}
	}
	if _, err := durable.DecodeInputs(context.Background(), g, c, pulse.NewRegistry(), p.Payload); err == nil {
		t.Fatal("accepted missing keys")
	}
}
func TestPreparationManagedStreamAndDirectStreamRejection(t *testing.T) {
	g, err := pulse.New("managed-prep", "managed sources remain local graph code")
	if err != nil {
		t.Fatal(err)
	}
	k := pulse.NewKey[pulse.Stream[int]]("numbers")
	if err = pulse.ProduceStream(g, "source", "source", k, func(*pulse.RunCtx, *pulse.StreamWriter[int]) error { t.Error("source ran"); return nil }, pulse.WithBuffer(2), pulse.WithStreamSource("numbers/v1", true)); err != nil {
		t.Fatal(err)
	}
	if err = pulse.ConsumeStream(g, "consumer", "consumer", k, func(*pulse.RunCtx, int) error { t.Error("consumer ran"); return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err = durable.Prepare(context.Background(), g, pulse.Inputs{}, durable.NewCodecs()); err != nil {
		t.Fatal(err)
	}
	external, err := pulse.New("external-prep", "no external endpoint")
	if err != nil {
		t.Fatal(err)
	}
	if err = external.DeclareInput(pulse.NewPort(k)); err != nil {
		t.Fatal(err)
	}
	var in pulse.Inputs
	// 即使流声明被跳过，也不是可移植的有限值。
	if err = pulse.SkipSeed(&in, k); err != nil {
		t.Fatal(err)
	}
	if _, err = durable.Prepare(context.Background(), external, in, durable.NewCodecs()); err == nil {
		t.Fatal("external stream accepted")
	}
}

type panicPreparationCodec struct{ mode string }

func (p panicPreparationCodec) Type() reflect.Type { return reflect.TypeFor[[]byte]() }
func (p panicPreparationCodec) ID() string         { return "test/bytes" }
func (p panicPreparationCodec) Version() string    { return "1" }
func (p panicPreparationCodec) Encode(v any) ([]byte, error) {
	if p.mode == "encode" {
		panic("private-value")
	}
	return json.Marshal(v)
}
func (p panicPreparationCodec) Decode(b []byte) (any, error) {
	if p.mode == "decode" {
		panic("private-value")
	}
	var v []byte
	err := json.Unmarshal(b, &v)
	return v, err
}
func TestPreparationCodecPanicSanitized(t *testing.T) {
	g, k, keys := preparationFixture(t)
	var in pulse.Inputs
	_ = pulse.Seed(&in, k, []byte("private-value"))
	for _, mode := range []string{"encode", "decode"} {
		c := &durable.Codecs{}
		if err := c.Register(panicPreparationCodec{mode}); err != nil {
			t.Fatal(err)
		}
		p, err := durable.Prepare(context.Background(), g, in, c)
		if mode == "decode" {
			if err != nil {
				t.Fatal(err)
			}
			_, err = durable.DecodeInputs(context.Background(), g, c, keys, p.Payload)
		}
		if err == nil || strings.Contains(err.Error(), "private-value") {
			t.Fatal("panic leaked or not caught", err)
		}
	}
}
