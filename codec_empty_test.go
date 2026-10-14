package durable

import (
	"context"
	"reflect"
	"testing"

	"github.com/Luo-root/pulse"
)

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
