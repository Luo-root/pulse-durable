package recordio_test

import (
	"bytes"
	"strings"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
)

func TestSnapshotFormat2FrozenBytes(t *testing.T) {
	// 固定新增证据字段前的版本 1 快照；不能由当前编码器生成期望值。
	frozen := []byte(`{"record":{"attempts":{},"definition":"definition","format":2,"inputs":"inputs","managed":null,"messages":null,"revision":2,"root_scope":"root","run_id":"legacy","scopes":{},"streams":null,"version":"v1"},"version":1}`)
	r, err := recordio.DecodeSnapshot(frozen)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := recordio.EncodeSnapshot(r)
	if err != nil || !bytes.Equal(frozen, encoded) {
		t.Fatalf("format 2 snapshot bytes changed: %v", err)
	}
}

func TestSnapshotSemanticEvidenceShapesAndMetadata(t *testing.T) {
	for _, payload := range [][]byte{nil, {}, {0xff, 0, 1}} {
		r := durable.RunRecord{
			Format:           durable.SemanticRecordFormat,
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
