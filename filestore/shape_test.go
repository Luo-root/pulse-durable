package filestore

import (
	"encoding/json"
	durable "github.com/Luo-root/pulse-durable"
	"strconv"
	"testing"
)

func shapeBatch(t *testing.T, r durable.RunRecord, expected, value any) durable.ChangeBatch {
	t.Helper()
	h := map[string]string{"batch_format": "1", "logical_format": "2", "run_id": r.RunID, "business_version": r.Version, "definition_digest": r.Definition, "inputs_digest": r.Inputs, "root_scope": r.RootScope, "expected_revision": strconv.FormatUint(r.Revision, 10), "revision": strconv.FormatUint(r.Revision+1, 10)}
	wire, err := json.Marshal(map[string]any{"version": 1, "header": h, "ops": []any{map[string]any{"kind": "node", "key": "", "scope": r.RootScope, "name": "n", "expected": expected, "value": value}}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := durable.UnmarshalChangeBatch(wire)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestLoadAndReplayPreserveEmptyDataPreconditions(t *testing.T) {
	st := newTestStore(t)
	ss := acquireTestSession(t, st, "shape")
	seed := incrementalSeed("shape")
	if err := ss.Initialize(t.Context(), seed); err != nil {
		t.Fatal(err)
	}
	emptyValue := map[string]any{"name": "out", "type_id": "bytes", "version": "1", "state": "ready", "digest": "", "data": []any{}, "stream": nil}
	node := map[string]any{"node_id": "n", "attempt": "a", "reason": "completed", "outputs": []any{emptyValue}}
	if _, err := ss.Apply(t.Context(), shapeBatch(t, seed, nil, node)); err != nil {
		t.Fatal(err)
	}
	r, err := ss.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if r.Scopes[r.RootScope].Nodes["n"].Outputs[0].Data == nil {
		t.Fatal("Load normalized empty Data")
	}
	ss.Close()
	if _, err = st.CompactStorage(t.Context(), durable.CompactStorageRequest{RunID: "shape", ThroughRevision: 2, ExpectedGeneration: 1}); err != nil {
		t.Fatal(err)
	}
	ss = acquireTestSession(t, st, "shape")
	r, err = ss.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if r.Scopes[r.RootScope].Nodes["n"].Outputs[0].Data == nil {
		t.Fatal("replay normalized empty Data")
	}
	if _, err = ss.Apply(t.Context(), shapeBatch(t, r, node, node)); err != nil {
		t.Fatalf("exact empty Data precondition rejected: %v", err)
	}
}
