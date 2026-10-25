package soaktest

import (
	"encoding/json"
	"testing"
	"time"

	durable "github.com/Luo-root/pulse-durable"
)

func TestChecksumRecord(t *testing.T) {
	id := "task-000000000001"
	v, _ := json.Marshal(checksum(id, 42))
	r := durable.RunRecord{RunID: id, RootScope: "root", Scopes: map[string]durable.ScopeRecord{"root": {Nodes: map[string]durable.NodeRecord{"work": {Reason: "completed", Outputs: []durable.ValueRecord{{Name: "checksum", State: "ready", Data: v}}}}}}}
	raw, _ := json.Marshal(r)
	if !validRecord(raw, id, 42) || validRecord(raw, id, 43) || validRecord(raw, "other", 42) {
		t.Fatal("checksum audit mismatch")
	}
}
func TestHistogram(t *testing.T) {
	var h Histogram
	for _, v := range []time.Duration{0, time.Microsecond, 2 * time.Microsecond, 4 * time.Microsecond} {
		h.add(v)
	}
	if h.Count != 4 || h.MaxNS != 4000 || h.quantile(.95) != 8000 || h.quantile(.5) != 2000 {
		t.Fatalf("unexpected histogram: %+v", h)
	}
}
func TestConfigRequiresOptIn(t *testing.T) {
	t.Setenv("PULSE_DISTRIBUTED_TEST_ALLOW", "")
	c := Default()
	c.Namespace = "fresh"
	if c.Validate() == nil {
		t.Fatal("missing opt-in accepted")
	}
}
func TestGraphConstruction(t *testing.T) {
	c := Default()
	c.Namespace = "fresh"
	if _, e := task(t.Context(), c, "task-1"); e != nil {
		t.Fatal(e)
	}
	if _, e := assembly(1, 0); e != nil {
		t.Fatal(e)
	}
}
