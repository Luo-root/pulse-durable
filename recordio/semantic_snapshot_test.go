package recordio_test

import (
	"bytes"
	"strings"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
)

func TestLegacySnapshotRejectedWithoutChangingInput(t *testing.T) {
	// 冻结旧记录字节，不通过当前编码器生成。新领域对象没有 format 字段，
	// 旧 envelope 必须明确拒绝，不能丢掉字段后把新编码当作原历史承诺。
	frozen := []byte(`{"record":{"attempts":{},"definition":"definition","format":2,"inputs":"inputs","managed":null,"messages":null,"revision":2,"root_scope":"root","run_id":"legacy","scopes":{},"streams":null,"version":"v1"},"version":1}`)
	before := append([]byte(nil), frozen...)
	if _, err := recordio.DecodeSnapshot(frozen); err == nil {
		t.Fatal("legacy snapshot silently accepted by the new schema")
	}
	if _, err := recordio.Decode(frozen); err == nil {
		t.Fatal("legacy envelope downgraded to bare record JSON")
	}
	if !bytes.Equal(frozen, before) {
		t.Fatal("rejected legacy bytes changed")
	}
}

func TestSnapshotSemanticEvidenceShapesAndMetadata(t *testing.T) {
	for _, payload := range [][]byte{nil, {}, {0xff, 0, 1}} {
		r := durable.RunRecord{
			HistorySummaries: []durable.HistorySummary{{Kind: "kind", ID: "id", Payload: payload}},
			CommitIndex:      []durable.CommitIndexEntry{},
			Provenance:       &durable.Provenance{},
		}
		data := snapshotRoundTrip(t, r)
		for _, field := range []string{`"history_summaries":`, `"commit_index":[]`, `"provenance":`} {
			if !bytes.Contains(data, []byte(field)) {
				t.Fatalf("semantic evidence shape missing %s", field)
			}
		}
	}
	bad := string([]byte{0xff})
	for _, r := range []durable.RunRecord{
		{HistorySummaries: []durable.HistorySummary{{ID: bad}}},
		{CommitIndex: []durable.CommitIndexEntry{{BatchDigest: bad}}},
		{Provenance: &durable.Provenance{OriginRunID: bad}},
	} {
		if _, err := recordio.Encode(r); err == nil {
			t.Fatal("semantic metadata was silently repaired by JSON encoding")
		}
		if _, err := recordio.EncodeSnapshot(r); err == nil {
			t.Fatal("snapshot accepted invalid semantic metadata")
		}
	}
	data, err := recordio.EncodeSnapshot(durable.RunRecord{})
	if err != nil {
		t.Fatal(err)
	}
	// 仅新增的显式 optional 字段可缺省；原必填字段仍须拒绝缺失。
	missing := strings.Replace(string(data), `"revision":0,`, "", 1)
	if _, err := recordio.DecodeSnapshot([]byte(missing)); err == nil {
		t.Fatal("required legacy field became optional")
	}
}
