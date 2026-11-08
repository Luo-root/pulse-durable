package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/filestore"
	"github.com/Luo-root/pulse-durable/recordio"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func fixtureWireValue(v reflect.Value) any {
	if !v.IsValid() {
		return nil
	}
	switch v.Kind() {
	case reflect.Interface, reflect.Pointer:
		if v.IsNil() {
			return nil
		}
		return fixtureWireValue(v.Elem())
	case reflect.Uint64:
		return strconv.FormatUint(v.Uint(), 10)
	case reflect.Struct:
		m := map[string]any{}
		for i := 0; i < v.NumField(); i++ {
			f := v.Type().Field(i)
			if f.PkgPath != "" {
				continue
			}
			name := strings.Split(f.Tag.Get("json"), ",")[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = f.Name
			}
			m[name] = fixtureWireValue(v.Field(i))
		}
		return m
	case reflect.Map:
		if v.IsNil() {
			return nil
		}
		m := map[string]any{}
		for _, k := range v.MapKeys() {
			m[k.String()] = fixtureWireValue(v.MapIndex(k))
		}
		return m
	case reflect.Slice:
		if v.IsNil() {
			return nil
		}
		a := make([]any, v.Len())
		for i := range a {
			a[i] = fixtureWireValue(v.Index(i))
		}
		return a
	default:
		return v.Interface()
	}
}

// installHistoryFixture 从原始 Initialize 快照构造类型化历史，
// 通过生产 Initialize/Apply 生成完整布局，不复制物理格式编码规则。
// 调用方须持有目标运行锁；只替换 t.TempDir 内的故障测试夹具。
func installHistoryFixture(t *testing.T, dir string, r durable.RunRecord) {
	t.Helper()
	sum := sha256.Sum256([]byte(r.RunID))
	base := hex.EncodeToString(sum[:])
	headPath := filepath.Join(dir, base+".head")
	data, err := os.ReadFile(headPath)
	if err != nil {
		t.Fatal(err)
	}
	var h struct {
		OriginID     string   `json:"origin_id"`
		OriginDigest [32]byte `json:"origin_digest"`
	}
	if err = json.Unmarshal(data, &h); err != nil {
		t.Fatal(err)
	}
	// 从 Initialize 的原始字节开始，即使当前头已压缩。
	snapshot, err := os.ReadFile(filepath.Join(dir, base+".snapshot-"+h.OriginID))
	if err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(snapshot) != h.OriginDigest {
		t.Fatal("fixture origin snapshot digest mismatch")
	}
	seed, err := recordio.DecodeSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err = durable.ValidateInitialRecord(seed, r.RunID); err != nil {
		t.Fatal(err)
	}
	ops := []map[string]any{}
	add := func(kind, key, scope, name, seq string, state bool, value any) {
		op := map[string]any{"kind": kind, "key": key, "expected": nil, "value": fixtureWireValue(reflect.ValueOf(value))}
		if scope != "" {
			op["scope"] = scope
		}
		if name != "" {
			op["name"] = name
		}
		if seq != "" {
			op["sequence"] = seq
		}
		if state {
			op["state"] = true
		}
		ops = append(ops, op)
	}
	for k, v := range r.Scopes {
		if k != r.RootScope {
			s := v
			s.Nodes = map[string]durable.NodeRecord{}
			add("scope", k, "", "", "", false, s)
		}
	}
	for k, v := range r.Attempts {
		add("attempt", k, "", "", "", false, v)
	}
	for scope, s := range r.Scopes {
		for k, v := range s.Nodes {
			add("node", "", scope, k, "", false, v)
		}
	}
	for k, v := range r.Streams {
		add("stream", k, "", "", "", false, map[string]any{"Stream": v.Stream, "LastSequence": v.LastSequence, "AckedThrough": v.AckedThrough, "Cursor": v.Cursor})
		for _, p := range v.Pending {
			add("pending", "", k, "", strconv.FormatUint(p.Sequence, 10), false, p)
		}
	}
	for k, v := range r.Messages {
		add("message", k, "", "", "", false, v)
	}
	for k, v := range r.Managed {
		add("managed", k, "", "", "", false, map[string]any{"Scope": v.Scope, "Parent": v.Parent, "NodeID": v.NodeID})
		for _, v := range v.State {
			add("managed_entry", "", k, v.Name, "", true, v)
		}
		for _, v := range v.Values {
			add("managed_entry", "", k, v.Name, "", false, v)
		}
	}
	sort.Slice(ops, func(i, j int) bool {
		a, b := ops[i], ops[j]
		for _, k := range []string{"kind", "key", "scope", "name", "sequence"} {
			av, _ := a[k].(string)
			bv, _ := b[k].(string)
			if av != bv {
				return av < bv
			}
		}
		av, _ := a["state"].(bool)
		bv, _ := b["state"].(bool)
		return !av && bv
	})
	generic := ops
	wire, err := json.Marshal(map[string]any{"version": durable.ChangeBatchWireVersion, "header": map[string]string{"run_id": r.RunID, "business_version": r.Version, "definition_digest": r.Definition, "inputs_digest": r.Inputs, "root_scope": r.RootScope, "expected_revision": "1", "revision": "2"}, "ops": generic})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := durable.UnmarshalChangeBatch(wire)
	if err != nil {
		t.Fatalf("fixture typed decode: %v wire=%s", err, wire)
	}
	fixtureDir := t.TempDir()
	fixture, err := filestore.New(fixtureDir)
	if err != nil {
		t.Fatal(err)
	}
	ss, err := fixture.Acquire(t.Context(), seed.RunID)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ss.Close() })
	if err = ss.Initialize(t.Context(), seed); err != nil {
		t.Fatal(err)
	}
	if _, err = ss.Apply(t.Context(), batch); err != nil {
		t.Fatal(err)
	}
	if err = ss.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = fixture.AuditHistory(t.Context(), durable.AuditHistoryRequest{RunID: seed.RunID}); err != nil {
		t.Fatal(err)
	}
	// 先复制不可变对象，最后替换测试 head；不动调用方持有的锁文件。
	entries, err := os.ReadDir(fixtureDir)
	if err != nil {
		t.Fatal(err)
	}
	var head []byte
	for _, entry := range entries {
		if entry.Name() == base+".lock" {
			continue
		}
		data, err := os.ReadFile(filepath.Join(fixtureDir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if entry.Name() == base+".head" {
			head = data
			continue
		}
		f, err := os.OpenFile(filepath.Join(dir, entry.Name()), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if err != nil {
			t.Fatal(err)
		}
		_, writeErr := f.Write(data)
		closeErr := f.Close()
		if writeErr != nil || closeErr != nil {
			t.Fatalf("fixture copy: write=%v close=%v", writeErr, closeErr)
		}
	}
	if len(head) == 0 {
		t.Fatal("fixture head missing")
	}
	if err = os.WriteFile(headPath, head, 0600); err != nil {
		t.Fatal(err)
	}
}
