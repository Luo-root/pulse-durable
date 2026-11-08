package durable

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// 本文件相当于 wire 层的容量回归与基准夹具：使用与 PostgreSQL 容量验收
// 相同的 768 KiB Data，约束公开编解码入口；不连接数据库或改变持久协议。
func largePayloadBatch(tb testing.TB) ChangeBatch {
	tb.Helper()
	tx := newRecordTransaction(1)
	node := NodeRecord{NodeID: "n", Attempt: "a", Reason: "completed", Outputs: []ValueRecord{{Name: "out", TypeID: "bytes", Version: "v1", State: "ready", Digest: "d", Data: make([]byte, 768<<10)}}}
	for i := range node.Outputs[0].Data {
		node.Outputs[0].Data[i] = byte(i)
	}
	if err := tx.putNode("r", "n", nil, node); err != nil {
		tb.Fatal(err)
	}
	return ChangeBatch{header: ChangeHeader{RunID: "r", BusinessVersion: "v", DefinitionDigest: "d", InputsDigest: "i", RootScope: "r", ExpectedRevision: 1, Revision: 2}, tx: tx}
}

func TestChangeBatchLargePayloadRoundTrip(t *testing.T) {
	batch := largePayloadBatch(t)
	wire, err := MarshalChangeBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalChangeBatch(wire)
	if err != nil {
		t.Fatal(err)
	}
	got := decoded.tx.nodes[nodeRecordKey{"r", "n"}].value.Outputs[0].Data
	want := batch.tx.nodes[nodeRecordKey{"r", "n"}].value.Outputs[0].Data
	if !bytes.Equal(got, want) {
		t.Fatal("large Data changed during wire round trip")
	}
	again, err := MarshalChangeBatch(decoded)
	if err != nil || !bytes.Equal(wire, again) {
		t.Fatalf("large wire is not canonical: %v", err)
	}
}

func TestWireJSONStructuralValidation(t *testing.T) {
	// 合法转义键与其字面写法应视为同一个键；原始身份键不做大小写折叠。
	valid := []string{
		`null`, `[]`, `{}`, `[0,-1,1.5,1e3,true,false,null,"x"]`, `1e-1000`,
		`{"x":[{"a":"\\\"/\b\f\n\r\t\u4e2d\ud83d\ude00"}],"X":1}`,
		strings.Repeat(`[`, MaxChangeBatchDepth) + `0` + strings.Repeat(`]`, MaxChangeBatchDepth),
	}
	invalid := []string{
		``, `01`, `[1,]`, `{"x":}`, `{} {}`, `{"a":1,"a":2}`,
		`{"a":1,"\u0061":2}`, `{"x":[{"a":0,"a":1}]}`,
		`"\ud800"`, `"\udc00"`, `"\ud800\u0041"`, `1e1000`,
		strings.Repeat(`[`, MaxChangeBatchDepth+1) + `0` + strings.Repeat(`]`, MaxChangeBatchDepth+1),
	}
	for _, data := range valid {
		if err := rejectJSON([]byte(data)); err != nil {
			t.Errorf("valid JSON rejected: %q: %v", data, err)
		}
	}
	for _, data := range invalid {
		if err := rejectJSON([]byte(data)); err == nil {
			t.Errorf("invalid JSON accepted: %q", data)
		}
	}
}

func TestWireByteArrayShape(t *testing.T) {
	for _, data := range []string{`null`, `[]`, `[0,1,255]`, ` [ 0, 255 ] `} {
		var got ValueRecord
		wire := []byte(`{"name":"x","type_id":"bytes","version":"v1","state":"ready","data":` + data + `,"digest":"d","stream":null}`)
		if err := unmarshalRecord(wire, &got); err != nil {
			t.Errorf("valid byte array %s rejected: %v", data, err)
		}
	}
	for _, data := range []string{`"AA=="`, `[null]`, `[-0]`, `[-1]`, `[256]`, `[1.0]`, `[1e0]`, `[true]`, `[[]]`} {
		var got ValueRecord
		wire := []byte(`{"name":"x","type_id":"bytes","version":"v1","state":"ready","data":` + data + `,"digest":"d","stream":null}`)
		if err := unmarshalRecord(wire, &got); err == nil {
			t.Errorf("invalid byte array %s accepted", data)
		}
	}
}

func BenchmarkChangeBatchLargePayload(b *testing.B) {
	batch := largePayloadBatch(b)
	wire, err := MarshalChangeBatch(batch)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("Marshal", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(wire)))
		for b.Loop() {
			if _, err := MarshalChangeBatch(batch); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Unmarshal", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(wire)))
		for b.Loop() {
			if _, err := UnmarshalChangeBatch(wire); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("ValidateJSON", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(wire)))
		for b.Loop() {
			if err := rejectJSON(wire); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("StandardJSONSyntax", func(b *testing.B) {
		b.SetBytes(int64(len(wire)))
		for b.Loop() {
			if !json.Valid(wire) {
				b.Fatal("fixture JSON is invalid")
			}
		}
	})
}
