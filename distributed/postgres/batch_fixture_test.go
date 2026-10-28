package postgres

import (
	"encoding/json"
	durable "github.com/Luo-root/pulse-durable"
	"strconv"
	"testing"
)

// A no-op typed transaction still advances the revision. Construct through the
// public wire decoder rather than duplicating the private transaction algorithm.
func nextBatch(t *testing.T, r durable.RunRecord) durable.ChangeBatch {
	t.Helper()
	wire, e := json.Marshal(map[string]any{"version": 1, "header": map[string]string{
		"batch_format": "1", "logical_format": "2", "run_id": r.RunID, "business_version": r.Version, "definition_digest": r.Definition, "inputs_digest": r.Inputs, "root_scope": r.RootScope, "expected_revision": strconv.FormatUint(r.Revision, 10), "revision": strconv.FormatUint(r.Revision+1, 10)}, "ops": []any{}})
	if e != nil {
		t.Fatal(e)
	}
	b, e := durable.UnmarshalChangeBatch(wire)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
