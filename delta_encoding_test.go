package durable

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func wireTestBatch(t *testing.T) ChangeBatch {
	t.Helper()
	tx := newRecordTransaction(7)
	if err := tx.putAttempt("a", nil, AttemptRecord{Parent: "p", NodeID: "n", Index: 1, Limit: 1, State: AttemptActive}); err != nil {
		t.Fatal(err)
	}
	if err := tx.putPending("s", ^uint64(0), nil, &StreamMessageRecord{Sequence: ^uint64(0), Value: ValueRecord{Name: "v", State: "ready", Data: []byte{1, 2}}}); err != nil {
		t.Fatal(err)
	}
	return ChangeBatch{header: ChangeHeader{BatchFormat: 1, LogicalFormat: 2, RunID: "r", BusinessVersion: "v", DefinitionDigest: "d", InputsDigest: "i", RootScope: "root", ExpectedRevision: 7, Revision: 8}, tx: tx}
}

func TestChangeBatchWireCanonicalRoundTrip(t *testing.T) {
	b := wireTestBatch(t)
	data, err := MarshalChangeBatch(b)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte(`"sequence":18446744073709551615`)) {
		t.Fatal("nested uint64 encoded as JSON number")
	}
	decoded, err := UnmarshalChangeBatch(data)
	if err != nil {
		t.Fatal(err)
	}
	again, err := MarshalChangeBatch(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, again) {
		t.Fatalf("not canonical:\n%s\n%s", data, again)
	}
	d1, _ := ChangeBatchDigest(b)
	d2, _ := ChangeBatchDigest(decoded)
	if d1 != d2 {
		t.Fatal("digest changed")
	}
}

func TestChangeBatchWireRejectsDuplicateTrailingAndDepth(t *testing.T) {
	b, _ := MarshalChangeBatch(wireTestBatch(t))
	cases := []string{
		`{"version":1,"version":1,"header":{},"ops":[]}`,
		string(b) + ` {}`,
		`{"version":1,"header":{"batch_format":"1","logical_format":"2","run_id":"r","business_version":"v","definition_digest":"d","inputs_digest":"i","root_scope":"r","expected_revision":"0","revision":"1"},"ops":[{"kind":"attempt","key":"a","expected":null,"value":{"parent":"p","node_id":"n","boundary":"","index":1,"limit":1,"state":"active"},"bogus":1}]}`,
	}
	for i, data := range cases {
		if _, err := UnmarshalChangeBatch([]byte(data)); err == nil {
			t.Fatalf("case %d accepted", i)
		}
	}
	deep := `{"x":` + strings.Repeat(`[`, MaxChangeBatchDepth+2) + `0` + strings.Repeat(`]`, MaxChangeBatchDepth+2) + `}`
	if err := rejectJSON([]byte(deep)); err == nil {
		t.Fatal("deep JSON accepted")
	}
}

func TestWireScannerRejectsNestedDuplicate(t *testing.T) {
	data := []byte(`{"version":1,"header":{"batch_format":"1","logical_format":"2","run_id":"r","business_version":"v","definition_digest":"d","inputs_digest":"i","root_scope":"r","expected_revision":"0","revision":"1"},"ops":[{"kind":"scope","key":"x","expected":null,"value":{"graph":"g","path":"p","inputs":"i","nodes":{},"nodes":{}}}]}`)
	if _, err := UnmarshalChangeBatch(data); err == nil {
		t.Fatal("nested duplicate accepted")
	}
}

func TestWirePreservesNilAndEmptyData(t *testing.T) {
	tx1 := newRecordTransaction(1)
	_ = tx1.putAttempt("a", nil, AttemptRecord{Parent: "p", NodeID: "n", Index: 1, Limit: 1, State: AttemptActive})
	tx2 := newRecordTransaction(1)
	_ = tx2.putAttempt("a", nil, AttemptRecord{Parent: "p", NodeID: "n", Index: 1, Limit: 1, State: AttemptActive})
	h := ChangeHeader{BatchFormat: 1, LogicalFormat: 2, RunID: "r", BusinessVersion: "v", DefinitionDigest: "d", InputsDigest: "i", RootScope: "r", ExpectedRevision: 1, Revision: 2}
	_ = tx1.putNode("s", "n", nil, NodeRecord{NodeID: "n", Outputs: []ValueRecord{{Name: "x", State: "ready", Data: nil}}})
	_ = tx2.putNode("s", "n", nil, NodeRecord{NodeID: "n", Outputs: []ValueRecord{{Name: "x", State: "ready", Data: []byte{}}}})
	a, e := MarshalChangeBatch(ChangeBatch{header: h, tx: tx1})
	if e != nil {
		t.Fatal(e)
	}
	b, e := MarshalChangeBatch(ChangeBatch{header: h, tx: tx2})
	if e != nil {
		t.Fatal(e)
	}
	if bytes.Equal(a, b) {
		t.Fatal("nil and empty data collapsed")
	}
}

func TestWireUintFieldShape(t *testing.T) {
	v := map[string]any{"sequence": json.Number("18446744073709551615"), "nested": map[string]any{"generation": json.Number("9")}}
	raw := raw(v)
	if !bytes.Contains(raw, []byte(`"sequence":"18446744073709551615"`)) || !bytes.Contains(raw, []byte(`"generation":"9"`)) {
		t.Fatalf("uint shape: %s", raw)
	}
}
