package filestore

import (
	"encoding/json"
	durable "github.com/Luo-root/pulse-durable"
	"strconv"
	"testing"
)

// revisionBatch is a decoded, real typed attempt operation with an exact absent
// precondition. It never writes a snapshot or bypasses Session.Apply.
func revisionBatch(t testing.TB, r durable.RunRecord) durable.ChangeBatch {
	t.Helper()
	wire, err := json.Marshal(map[string]any{"version": 1, "header": map[string]string{"batch_format": "1", "logical_format": "2", "run_id": r.RunID, "business_version": r.Version, "definition_digest": r.Definition, "inputs_digest": r.Inputs, "root_scope": r.RootScope, "expected_revision": strconv.FormatUint(r.Revision, 10), "revision": strconv.FormatUint(r.Revision+1, 10)}, "ops": []any{map[string]any{"kind": "attempt", "key": "attempt-" + strconv.FormatUint(r.Revision, 10), "expected": nil, "value": durable.AttemptRecord{Parent: r.RootScope, NodeID: "node", Index: 1, Limit: 1, State: durable.AttemptActive}}}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := durable.UnmarshalChangeBatch(wire)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
