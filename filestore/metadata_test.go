package filestore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)

type metadataField struct {
	name string
	set  func(*durable.RunRecord, string)
}

func metadataFields() []metadataField {
	scope := func(set func(*durable.ScopeRecord, string)) func(*durable.RunRecord, string) {
		return func(r *durable.RunRecord, v string) {
			s := r.Scopes["scope"]
			set(&s, v)
			r.Scopes["scope"] = s
		}
	}
	node := func(set func(*durable.NodeRecord, string)) func(*durable.RunRecord, string) {
		return scope(func(s *durable.ScopeRecord, v string) {
			n := s.Nodes["node"]
			set(&n, v)
			s.Nodes["node"] = n
		})
	}
	output := func(set func(*durable.ValueRecord, string)) func(*durable.RunRecord, string) {
		return node(func(n *durable.NodeRecord, v string) { set(&n.Outputs[0], v) })
	}
	attempt := func(set func(*durable.AttemptRecord, string)) func(*durable.RunRecord, string) {
		return func(r *durable.RunRecord, v string) {
			a := r.Attempts["attempt"]
			set(&a, v)
			r.Attempts["attempt"] = a
		}
	}
	return []metadataField{
		{"run-id", func(r *durable.RunRecord, v string) { r.RunID = v }},
		{"version", func(r *durable.RunRecord, v string) { r.Version = v }},
		{"definition", func(r *durable.RunRecord, v string) { r.Definition = v }},
		{"inputs", func(r *durable.RunRecord, v string) { r.Inputs = v }},
		{"root-scope", func(r *durable.RunRecord, v string) { r.RootScope = v }},
		{"scope-key", func(r *durable.RunRecord, v string) { r.Scopes[v] = r.Scopes["scope"]; delete(r.Scopes, "scope") }},
		{"scope-graph", scope(func(s *durable.ScopeRecord, v string) { s.Graph = v })},
		{"scope-path", scope(func(s *durable.ScopeRecord, v string) { s.Path = v })},
		{"scope-inputs", scope(func(s *durable.ScopeRecord, v string) { s.Inputs = v })},
		{"node-key", scope(func(s *durable.ScopeRecord, v string) { s.Nodes[v] = s.Nodes["node"]; delete(s.Nodes, "node") })},
		{"node-id", node(func(n *durable.NodeRecord, v string) { n.NodeID = v })},
		{"node-attempt", node(func(n *durable.NodeRecord, v string) { n.Attempt = v })},
		{"node-reason", node(func(n *durable.NodeRecord, v string) { n.Reason = v })},
		{"output-name", output(func(vr *durable.ValueRecord, v string) { vr.Name = v })},
		{"output-type-id", output(func(vr *durable.ValueRecord, v string) { vr.TypeID = v })},
		{"output-version", output(func(vr *durable.ValueRecord, v string) { vr.Version = v })},
		{"output-state", output(func(vr *durable.ValueRecord, v string) { vr.State = v })},
		{"output-digest", output(func(vr *durable.ValueRecord, v string) { vr.Digest = v })},
		{"attempt-key", func(r *durable.RunRecord, v string) {
			r.Attempts[v] = r.Attempts["attempt"]
			delete(r.Attempts, "attempt")
		}},
		{"attempt-parent", attempt(func(a *durable.AttemptRecord, v string) { a.Parent = v })},
		{"attempt-node-id", attempt(func(a *durable.AttemptRecord, v string) { a.NodeID = v })},
		{"attempt-boundary", attempt(func(a *durable.AttemptRecord, v string) { a.Boundary = v })},
		{"attempt-state", attempt(func(a *durable.AttemptRecord, v string) { a.State = durable.AttemptState(v) })},
	}
}

func readMetadataFile(t *testing.T, store *Store, runID string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(store.dir, fileBase(runID)+".json"))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestSaveRejectsInvalidMetadata(t *testing.T) {
	bad := "PRIVATE_METADATA_" + string([]byte{0xff})
	for _, field := range metadataFields() {
		for _, update := range []bool{false, true} {
			mode := "first"
			if update {
				mode = "update"
			}
			t.Run(field.name+"/"+mode, func(t *testing.T) {
				store := newTestStore(t)
				session := acquireTestSession(t, store, "run")
				ctx := context.Background()
				initial := testRecord("run")
				var expected uint64
				var before []byte
				if update {
					if err := session.Save(ctx, initial, 0); err != nil {
						t.Fatal(err)
					}
					expected = 1
					before = readMetadataFile(t, store, "run")
				}
				proposed := testRecord("run")
				proposed.Revision = expected + 1
				field.set(&proposed, bad)
				err := session.Save(ctx, proposed, expected)
				if err == nil {
					t.Fatal("Save accepted invalid UTF-8 metadata")
				}
				if strings.Contains(err.Error(), "PRIVATE_METADATA") {
					t.Fatal("Save error exposed metadata value")
				}
				if field.name != "run-id" && !strings.Contains(err.Error(), "UTF-8") {
					t.Fatalf("Save error = %v; want UTF-8 rejection", err)
				}
				loaded, err := session.Load(ctx)
				wantFiles := 1
				if update {
					wantFiles = 2
					if err != nil || !reflect.DeepEqual(loaded, initial) || !bytes.Equal(before, readMetadataFile(t, store, "run")) {
						t.Fatalf("rejected Save changed old record or revision: %v", err)
					}
				} else if !errors.Is(err, durable.ErrNotFound) {
					t.Fatalf("rejected first Save created a record: %v", err)
				}
				entries, err := os.ReadDir(store.dir)
				if err != nil || len(entries) != wantFiles {
					t.Fatalf("rejected Save left temporary files: %v", err)
				}
			})
		}
	}
}

func TestSaveRejectsCollidingMetadataKeys(t *testing.T) {
	badA, badB := string([]byte{0xff}), string([]byte{0xfe})
	for _, name := range []string{"scope-key", "node-key", "attempt-key"} {
		t.Run(name, func(t *testing.T) {
			store := newTestStore(t)
			session := acquireTestSession(t, store, "run")
			initial := testRecord("run")
			if err := session.Save(context.Background(), initial, 0); err != nil {
				t.Fatal(err)
			}
			before := readMetadataFile(t, store, "run")
			proposed := testRecord("run")
			proposed.Revision = 2
			switch name {
			case "scope-key":
				proposed.Scopes = map[string]durable.ScopeRecord{badA: {Graph: "first"}, badB: {Graph: "second"}}
			case "node-key":
				scope := proposed.Scopes["scope"]
				scope.Nodes = map[string]durable.NodeRecord{badA: {NodeID: "first"}, badB: {NodeID: "second"}}
				proposed.Scopes["scope"] = scope
			case "attempt-key":
				proposed.Attempts = map[string]durable.AttemptRecord{badA: {NodeID: "first"}, badB: {NodeID: "second"}}
			}
			if err := session.Save(context.Background(), proposed, 1); err == nil {
				t.Fatal("Save accepted keys that JSON merges into one replacement key")
			}
			loaded, err := session.Load(context.Background())
			if err != nil || !reflect.DeepEqual(loaded, initial) || !bytes.Equal(before, readMetadataFile(t, store, "run")) {
				t.Fatalf("colliding keys changed old record or revision: %v", err)
			}
		})
	}
}

func TestSaveMetadataUnicodeRoundTrip(t *testing.T) {
	unicode := "中文/α/🚀/\ufffd/\x00/\"/\\uD800"
	for _, field := range metadataFields() {
		t.Run(field.name, func(t *testing.T) {
			r := testRecord("run")
			field.set(&r, unicode)
			store := newTestStore(t)
			session := acquireTestSession(t, store, r.RunID)
			if err := session.Save(context.Background(), r, 0); err != nil {
				t.Fatal(err)
			}
			loaded, err := session.Load(context.Background())
			if err != nil || !reflect.DeepEqual(loaded, r) {
				t.Fatalf("valid Unicode or opaque binary Data did not round-trip: %v", err)
			}
		})
	}
}

func TestSaveAllowsEmptyMetadataAndData(t *testing.T) {
	for _, mode := range []string{"nil-maps", "empty-maps", "nil-data", "empty-data", "binary-data"} {
		t.Run(mode, func(t *testing.T) {
			r := durable.RunRecord{RunID: "run", Revision: 1}
			if mode != "nil-maps" {
				r.Scopes = make(map[string]durable.ScopeRecord)
				r.Attempts = make(map[string]durable.AttemptRecord)
			}
			if strings.HasSuffix(mode, "-data") {
				var data []byte
				if mode == "empty-data" {
					data = []byte{}
				} else if mode == "binary-data" {
					data = []byte{0xff, 0xfe, 0, 0x80}
				}
				r.Scopes[""] = durable.ScopeRecord{Nodes: map[string]durable.NodeRecord{
					"": {Outputs: []durable.ValueRecord{{Data: data}}},
				}}
				r.Attempts[""] = durable.AttemptRecord{}
			}
			session := acquireTestSession(t, newTestStore(t), "run")
			if err := session.Save(context.Background(), r, 0); err != nil {
				t.Fatal(err)
			}
			// 既有 JSON omitempty 格式将零长度 Data 读回 nil。
			if mode == "empty-data" {
				r.Scopes[""].Nodes[""].Outputs[0].Data = nil
			}
			loaded, err := session.Load(context.Background())
			if err != nil || !reflect.DeepEqual(loaded, r) {
				t.Fatalf("empty metadata, nil or opaque Data was changed: %v", err)
			}
		})
	}
}

func TestLoadRejectsInvalidUnicode(t *testing.T) {
	cases := []struct {
		name string
		text []byte
	}{
		{"raw-utf8", []byte{'"', 0xff, '"'}},
		{"high-surrogate", []byte(`"\ud800"`)},
		{"low-surrogate", []byte(`"\udfff"`)},
		{"reversed-pair", []byte(`"\udc00\ud800"`)},
		{"high-non-low", []byte(`"\ud800\u0041"`)},
		{"high-high", []byte(`"\ud800\ud800"`)},
		{"escaped-low", []byte(`"\ud800\\udc00"`)},
	}
	for _, field := range metadataFields() {
		for _, tc := range cases {
			t.Run(field.name+"/"+tc.name, func(t *testing.T) {
				const marker = "UNICODE_MARKER"
				r := testRecord("run")
				field.set(&r, marker)
				store := newTestStore(t)
				session := acquireTestSession(t, store, r.RunID)
				if err := session.Save(context.Background(), r, 0); err != nil {
					t.Fatal(err)
				}
				data := bytes.ReplaceAll(readMetadataFile(t, store, r.RunID), []byte(`"`+marker+`"`), tc.text)
				path := filepath.Join(store.dir, fileBase(r.RunID)+".json")
				if err := os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
				loaded, err := session.Load(context.Background())
				if err == nil || errors.Is(err, durable.ErrNotFound) || !strings.Contains(err.Error(), "invalid Unicode") || !reflect.DeepEqual(loaded, durable.RunRecord{}) {
					t.Fatalf("Load silently repaired invalid JSON text: %v", err)
				}
				proposed := r
				proposed.Revision = 2
				if err := session.Save(context.Background(), proposed, 1); err == nil {
					t.Fatal("Save overwrote invalid Unicode record")
				}
				if !bytes.Equal(data, readMetadataFile(t, store, r.RunID)) {
					t.Fatal("rejected Load/Save changed corrupt record")
				}
			})
		}
	}
}

func TestLoadAllowsUnicodeEscapes(t *testing.T) {
	for _, field := range metadataFields() {
		t.Run(field.name, func(t *testing.T) {
			// 包含代理项对、合法替换字符和字面的反斜杠 u，不把它们误判为损坏。
			r := testRecord("run")
			const value = "中🚀\ufffd\\uD800\""
			field.set(&r, value)
			store := newTestStore(t)
			session := acquireTestSession(t, store, r.RunID)
			if err := session.Save(context.Background(), r, 0); err != nil {
				t.Fatal(err)
			}
			original := readMetadataFile(t, store, r.RunID)
			quoted, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			data := bytes.ReplaceAll(original, quoted, []byte(`"\u4e2d\uD83D\ude80\ufffd\\uD800\""`))
			if bytes.Equal(data, original) {
				t.Fatal("test did not change JSON spelling")
			}
			if err := os.WriteFile(filepath.Join(store.dir, fileBase(r.RunID)+".json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			loaded, err := session.Load(context.Background())
			if err != nil || !reflect.DeepEqual(loaded, r) {
				t.Fatalf("legal JSON Unicode escapes were rejected or changed: %v", err)
			}
		})
	}
}

func TestRuntimeRejectsInvalidFileMetadata(t *testing.T) {
	bad := "invalid_" + string([]byte{0xff})
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
			runtime, err := durable.New(store, codecs)
			if err != nil {
				t.Fatal(err)
			}
			g, err := pulse.New(graphID, "invalid metadata regression")
			if err != nil {
				t.Fatal(err)
			}
			k := pulse.NewKey[int](name)
			if err := g.Add(pulse.NewNode(nodeID, "", nil, pulse.Provides(k), func(rc *pulse.RunCtx) error { return pulse.Set(rc, k, 7) })); err != nil {
				t.Fatal(err)
			}
			opts := durable.RunOptions{RunID: "run", Version: version}
			if _, err := runtime.Run(context.Background(), g, pulse.Inputs{}, opts); err == nil {
				t.Fatal("Runtime.Run succeeded with metadata FileStore cannot preserve")
			}
			session := acquireTestSession(t, store, "run")
			r, err := session.Load(context.Background())
			if err != nil && !errors.Is(err, durable.ErrNotFound) {
				t.Fatal(err)
			}
			for _, scope := range r.Scopes {
				if len(scope.Nodes) != 0 {
					t.Fatal("failed metadata save published a completed node")
				}
			}
			if err := session.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.Resume(context.Background(), g, pulse.Inputs{}, opts); err == nil {
				t.Fatal("Resume succeeded with the same invalid metadata")
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
			if err := registry.Register(fileEmptyCodec{nilEncoding: nilEncoding}); err != nil {
				t.Fatal(err)
			}
			testRuntimeFileValue(t, registry, fileEmptyValue{})
		})
	}
	t.Run("binary", func(t *testing.T) { testRuntimeFileValue(t, codecs, []byte{0xff, 0xfe, 0, 0x80}) })
	t.Run("typed-nil", func(t *testing.T) { testRuntimeFileValue[[]byte](t, codecs, nil) })
	t.Run("unicode", func(t *testing.T) { testRuntimeFileValue(t, codecs, "中文/🚀/\ufffd") })
}

func testRuntimeFileValue[T any](t *testing.T, codecs *durable.Codecs, value T) {
	t.Helper()
	runtime, err := durable.New(newTestStore(t), codecs)
	if err != nil {
		t.Fatal(err)
	}
	g, err := pulse.New("图/🚀", "合法持久编码")
	if err != nil {
		t.Fatal(err)
	}
	k := pulse.NewKey[T]("结果/α")
	var calls atomic.Int32
	if err := g.Add(pulse.NewNode("节点/中", "", nil, pulse.Provides(k), func(rc *pulse.RunCtx) error {
		calls.Add(1)
		return pulse.Set(rc, k, value)
	})); err != nil {
		t.Fatal(err)
	}
	if err := g.DeclareOutput(pulse.NewPort(k)); err != nil {
		t.Fatal(err)
	}
	opts := durable.RunOptions{RunID: "运行/中", Version: "版本/α"}
	if _, err := runtime.Run(context.Background(), g, pulse.Inputs{}, opts); err != nil {
		t.Fatal(err)
	}
	e, err := runtime.Resume(context.Background(), g, pulse.Inputs{}, opts)
	if err != nil {
		t.Fatal(err)
	}
	got, state, err := pulse.ReadOutput(e, k)
	if err != nil || state != pulse.SlotStateReady || !reflect.DeepEqual(got, value) || calls.Load() != 1 {
		t.Fatalf("valid file run/resume changed value: state=%s error=%v calls=%d", state, err, calls.Load())
	}
}
