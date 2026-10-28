package filestore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	pulse "github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

// The full arbitrary-record metadata matrix is a codec boundary test: importing
// a completed record through Initialize is deliberately no longer permitted.
func TestRecordMetadataEveryStringAndMapKey(t *testing.T) {
	for _, mutation := range metadataMutations(reflect.ValueOf(incrementalMetadataRecord()), "") {
		t.Run(mutation.name, func(t *testing.T) {
			bad := "PRIVATE_METADATA_" + string([]byte{255})
			r := mutation.change(reflect.ValueOf(incrementalMetadataRecord()), bad).Interface().(durable.RunRecord)
			if _, err := recordio.Encode(r); err == nil || strings.Contains(err.Error(), "PRIVATE_METADATA") {
				t.Fatal("invalid metadata accepted or disclosed", err)
			}
			valid := mutation.change(reflect.ValueOf(incrementalMetadataRecord()), "中文/α/🚀/\ufffd/\x00/\"/\\uD800").Interface().(durable.RunRecord)
			data, err := recordio.Encode(valid)
			if err != nil {
				t.Fatal(err)
			}
			got, err := recordio.Decode(data)
			if err != nil || !reflect.DeepEqual(got, valid) {
				t.Fatal("Unicode metadata changed", err)
			}
		})
	}
}
func TestRecordCollidingMetadataKeys(t *testing.T) {
	for _, field := range []string{"Scopes", "Attempts", "Streams", "Messages", "Managed"} {
		t.Run(field, func(t *testing.T) {
			r := incrementalMetadataRecord()
			v := reflect.ValueOf(&r).Elem().FieldByName(field)
			keys := v.MapKeys()
			value := v.MapIndex(keys[0])
			m := reflect.MakeMap(v.Type())
			m.SetMapIndex(reflect.ValueOf(string([]byte{255})), value)
			m.SetMapIndex(reflect.ValueOf(string([]byte{254})), value)
			v.Set(m)
			if _, err := recordio.Encode(r); err == nil {
				t.Fatal("colliding keys accepted")
			}
		})
	}
}
func TestLoadRejectsInvalidUnicode(t *testing.T) {
	for _, text := range [][]byte{{'"', 255, '"'}, []byte(`"\ud800"`), []byte(`"\udfff"`), []byte(`"\udc00\ud800"`), []byte(`"\ud800\u0041"`), []byte(`"\ud800\ud800"`), []byte(`"\ud800\\udc00"`)} {
		r := testRecord("run")
		r.Version = "UNICODE_MARKER"
		data, err := recordio.Encode(r)
		if err != nil {
			t.Fatal(err)
		}
		data = bytes.ReplaceAll(data, []byte(`"UNICODE_MARKER"`), text)
		if _, err = recordio.Decode(data); err == nil {
			t.Fatal("invalid Unicode repaired")
		}
	}
}
func TestLoadAllowsUnicodeEscapes(t *testing.T) {
	r := testRecord("run")
	r.Version = "中🚀\ufffd\\uD800\""
	data, err := recordio.Encode(r)
	if err != nil {
		t.Fatal(err)
	}
	quoted, _ := json.Marshal(r.Version)
	data = bytes.ReplaceAll(data, quoted, []byte(`"\u4e2d\uD83D\ude80\ufffd\\uD800\""`))
	got, err := recordio.Decode(data)
	if err != nil || !reflect.DeepEqual(got, r) {
		t.Fatal("legal escapes changed", err)
	}
}
func TestInitializeRejectsInvalidMetadataWithoutPublication(t *testing.T) {
	for _, field := range []string{"version", "root", "graph", "path"} {
		t.Run(field, func(t *testing.T) {
			s := newTestStore(t)
			ss := acquireTestSession(t, s, "run")
			r := incrementalSeed("run")
			bad := "PRIVATE_" + string([]byte{255})
			switch field {
			case "version":
				r.Version = bad
			case "root":
				r.RootScope = bad
			case "graph", "path":
				v := r.Scopes["root"]
				if field == "graph" {
					v.Graph = bad
				} else {
					v.Path = bad
				}
				r.Scopes["root"] = v
			}
			if err := ss.Initialize(t.Context(), r); err == nil || strings.Contains(err.Error(), "PRIVATE_") {
				t.Fatal("bad metadata accepted/disclosed", err)
			}
			if _, err := ss.Load(t.Context()); !errors.Is(err, durable.ErrNotFound) {
				t.Fatal("invalid seed published", err)
			}
		})
	}
}
func TestRuntimeRejectsInvalidFileMetadata(t *testing.T) {
	bad := "invalid_" + string([]byte{255})
	for _, field := range []string{"version", "node-id", "graph-id", "codec-id", "codec-version", "output-name"} {
		t.Run(field, func(t *testing.T) {
			version, nodeID, graphID, codecID, codecVersion, name := "v1", "node", "graph", "payload", "1", "out"
			switch field {
			case "version":
				version = bad
			case "node-id":
				nodeID = bad
			case "graph-id":
				graphID = bad
			case "codec-id":
				codecID = bad
			case "codec-version":
				codecVersion = bad
			case "output-name":
				name = bad
			}
			codecs := &durable.Codecs{}
			if err := durable.RegisterJSON[int](codecs, codecID, codecVersion); err != nil {
				t.Fatal(err)
			}
			store := newTestStore(t)
			rt, err := durable.New(store, codecs)
			if err != nil {
				t.Fatal(err)
			}
			g, err := pulse.New(graphID, "")
			if err != nil {
				t.Fatal(err)
			}
			k := pulse.NewKey[int](name)
			if err = g.Add(pulse.NewNode(nodeID, "", nil, pulse.Provides(k), func(rc *pulse.RunCtx) error { return pulse.Set(rc, k, 7) })); err != nil {
				t.Fatal(err)
			}
			opts := durable.RunOptions{RunID: "run", Version: version}
			if _, err = rt.Run(t.Context(), g, pulse.Inputs{}, opts); err == nil {
				t.Fatal("bad metadata run succeeded")
			}
			ss := acquireTestSession(t, store, "run")
			r, err := ss.Load(t.Context())
			if err != nil && !errors.Is(err, durable.ErrNotFound) {
				t.Fatal(err)
			}
			for _, scope := range r.Scopes {
				if len(scope.Nodes) != 0 {
					t.Fatal("invalid metadata node published")
				}
			}
			ss.Close()
			if _, err = rt.Resume(t.Context(), g, pulse.Inputs{}, opts); err == nil {
				t.Fatal("bad metadata resume succeeded")
			}
		})
	}
}

type fileEmptyValue struct{}
type fileEmptyCodec struct{ nilEncoding bool }

func (fileEmptyCodec) Type() reflect.Type { return reflect.TypeFor[fileEmptyValue]() }
func (fileEmptyCodec) ID() string         { return "empty/空" }
func (fileEmptyCodec) Version() string    { return "版本/1" }
func (c fileEmptyCodec) Encode(any) ([]byte, error) {
	if c.nilEncoding {
		return nil, nil
	}
	return []byte{}, nil
}
func (fileEmptyCodec) Decode(data []byte) (any, error) {
	if len(data) != 0 {
		return nil, errors.New("unexpected encoding")
	}
	return fileEmptyValue{}, nil
}
func TestRuntimeFileUnicodeBinaryNilAndEmptyEncoding(t *testing.T) {
	codecs := durable.NewCodecs()
	for _, nilEncoding := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "nil"}[nilEncoding], func(t *testing.T) {
			registry := &durable.Codecs{}
			if err := registry.Register(fileEmptyCodec{nilEncoding}); err != nil {
				t.Fatal(err)
			}
			testRuntimeFileValue(t, registry, fileEmptyValue{})
		})
	}
	t.Run("binary", func(t *testing.T) { testRuntimeFileValue(t, codecs, []byte{255, 254, 0, 128}) })
	t.Run("typed-nil", func(t *testing.T) { testRuntimeFileValue[[]byte](t, codecs, nil) })
	t.Run("unicode", func(t *testing.T) { testRuntimeFileValue(t, codecs, "中文/🚀/\ufffd") })
}
func testRuntimeFileValue[T any](t *testing.T, codecs *durable.Codecs, value T) {
	t.Helper()
	rt, err := durable.New(newTestStore(t), codecs)
	if err != nil {
		t.Fatal(err)
	}
	g, err := pulse.New("图/🚀", "")
	if err != nil {
		t.Fatal(err)
	}
	k := pulse.NewKey[T]("结果/α")
	var calls atomic.Int32
	if err = g.Add(pulse.NewNode("节点/中", "", nil, pulse.Provides(k), func(rc *pulse.RunCtx) error { calls.Add(1); return pulse.Set(rc, k, value) })); err != nil {
		t.Fatal(err)
	}
	if err = g.DeclareOutput(pulse.NewPort(k)); err != nil {
		t.Fatal(err)
	}
	opts := durable.RunOptions{RunID: "运行/中", Version: "版本/α"}
	if _, err = rt.Run(context.Background(), g, pulse.Inputs{}, opts); err != nil {
		t.Fatal(err)
	}
	e, err := rt.Resume(context.Background(), g, pulse.Inputs{}, opts)
	if err != nil {
		t.Fatal(err)
	}
	got, state, err := pulse.ReadOutput(e, k)
	if err != nil || state != pulse.SlotStateReady || !reflect.DeepEqual(got, value) || calls.Load() != 1 {
		t.Fatalf("value changed: %v %s %d", err, state, calls.Load())
	}
}
