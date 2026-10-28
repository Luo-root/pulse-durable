package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	durable "github.com/Luo-root/pulse-durable"
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

// installHistoryFixture constructs an explicitly synthetic typed history, not a
// storage adapter. Initialize's actual empty snapshot is retained; every object
// is introduced by a public decoded typed batch. Used only with t.TempDir data.
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
		Format         int      `json:"format"`
		BaseRevision   uint64   `json:"base_revision"`
		SnapshotDigest [32]byte `json:"snapshot_digest"`
		ChainAnchor    [32]byte `json:"chain_anchor"`
		HeadRevision   uint64   `json:"head_revision"`
		HeadChain      [32]byte `json:"head_chain"`
		BatchBytes     uint64   `json:"batch_bytes"`
		SnapshotID     string   `json:"snapshot_id"`
		HeadID         string   `json:"head_id"`
	}
	if err = json.Unmarshal(data, &h); err != nil {
		t.Fatal(err)
	}
	snapshot, err := os.ReadFile(filepath.Join(dir, base+".snapshot-"+h.SnapshotID))
	if err != nil {
		t.Fatal(err)
	}
	seed, err := recordio.Decode(snapshot)
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
	wire, err := json.Marshal(map[string]any{"version": 1, "header": map[string]string{"batch_format": "1", "logical_format": "2", "run_id": r.RunID, "business_version": r.Version, "definition_digest": r.Definition, "inputs_digest": r.Inputs, "root_scope": r.RootScope, "expected_revision": "1", "revision": "2"}, "ops": generic})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := durable.UnmarshalChangeBatch(wire)
	if err != nil {
		t.Fatalf("fixture typed decode: %v wire=%s", err, wire)
	}
	wire, err = durable.MarshalChangeBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = batch.ApplyTo(&seed); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(wire)
	chain, err := durable.NextChain(h.ChainAnchor, digest, 2)
	if err != nil {
		t.Fatal(err)
	}
	idbytes := sha256.Sum256(append([]byte("fixture"), wire...))
	id := hex.EncodeToString(idbytes[:16])
	env, err := json.Marshal(map[string]any{"parent_revision": 1, "parent_chain": h.ChainAnchor, "parent_id": "", "digest": digest, "chain": chain, "wire": wire})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, base+".batch-"+id), env, 0600); err != nil {
		t.Fatal(err)
	}
	h.HeadRevision = 2
	h.HeadChain = chain
	h.HeadID = id
	h.BatchBytes = uint64(len(env))
	data, err = json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(headPath, data, 0600); err != nil {
		t.Fatal(err)
	}
}
