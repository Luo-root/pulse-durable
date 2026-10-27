package durable

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestWireP1ExactSchemaAndIntegers(t *testing.T) {
	b := wireTestBatch(t)
	data, e := MarshalChangeBatch(b)
	if e != nil {
		t.Fatal(e)
	}
	var w wireBatch
	if json.Unmarshal(data, &w) != nil {
		t.Fatal("fixture")
	}
	for _, bad := range []string{`null`, `{}`, `{"parent":"p","node_id":"n","boundary":"","index":1,"limit":1,"state":"active","STATE":"active"}`} {
		x := w
		x.Ops = append([]wireOp(nil), w.Ops...)
		x.Ops[0].Value = json.RawMessage(bad)
		p, _ := json.Marshal(x)
		if _, e := UnmarshalChangeBatch(p); e == nil {
			t.Fatalf("accepted attempt %s", bad)
		}
	}
	for _, bad := range []string{`null`, `0`, `"+0"`, `"00"`, `"18446744073709551616"`} {
		p := bytes.ReplaceAll(data, []byte(`"sequence":"18446744073709551615"`), []byte(`"sequence":`+bad))
		if _, e := UnmarshalChangeBatch(p); e == nil {
			t.Fatalf("accepted integer %s", bad)
		}
	}
	if _, ok := parseU64("+0"); ok {
		t.Fatal("plus accepted")
	}
}

func TestWireP1TupleCollisionAndPrefix(t *testing.T) {
	tx := newRecordTransaction(7)
	for _, k := range []nodeRecordKey{{"a", "b/c"}, {"a/b", "c"}, {"a", "x"}, {"a", "x!"}} {
		if e := tx.putNode(k.scope, k.node, nil, NodeRecord{NodeID: k.node}); e != nil {
			t.Fatal(e)
		}
	}
	b := wireTestBatch(t)
	b.tx = tx
	p, e := MarshalChangeBatch(b)
	if e != nil {
		t.Fatal(e)
	}
	d, e := UnmarshalChangeBatch(p)
	if e != nil || d.OperationCount() != 4 {
		t.Fatalf("tuple roundtrip count=%d err=%v", d.OperationCount(), e)
	}
	var w wireBatch
	_ = json.Unmarshal(p, &w)
	w.Ops[0].Key = "forged"
	p, _ = json.Marshal(w)
	if _, e := UnmarshalChangeBatch(p); e == nil {
		t.Fatal("forged compound key accepted")
	}
}

func TestWireP1MarshalRejectsInvalidMetadata(t *testing.T) {
	b := wireTestBatch(t)
	b.header.RunID = string([]byte{255})
	if _, e := MarshalChangeBatch(b); e == nil {
		t.Fatal("invalid header UTF8 accepted")
	}
	b = wireTestBatch(t)
	v := b.tx.attempts["a"]
	v.value.NodeID = string([]byte{255})
	b.tx.attempts["a"] = v
	if _, e := MarshalChangeBatch(b); e == nil {
		t.Fatal("invalid nested UTF8 accepted")
	}
}

func TestWireP1ScopeExpectedMustBeNull(t *testing.T) {
	b := wireTestBatch(t)
	b.tx = newRecordTransaction(7)
	_ = b.tx.createScope("s", ScopeRecord{})
	p, e := MarshalChangeBatch(b)
	if e != nil {
		t.Fatal(e)
	}
	var w wireBatch
	_ = json.Unmarshal(p, &w)
	w.Ops[0].Expected = json.RawMessage(`{}`)
	p, _ = json.Marshal(w)
	if _, e := UnmarshalChangeBatch(p); e == nil {
		t.Fatal("scope non-null expected accepted")
	}
}
