// 本文件守住领域对象与固定快照 DTO 的映射边界。
// 字段清单与完整编码双向检查，避免领域新增字段被静默漏存，或预算计数
// 继续使用已经漂移的领域形状。测试不解释业务语义、不连接后端。
package recordio

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/internal/snapshotfmt"
)

func TestSnapshotDTOFieldCoverage(t *testing.T) {
	pairs := [][2]reflect.Type{
		{reflect.TypeFor[durable.RunRecord](), reflect.TypeFor[snapshotRecord]()},
		{reflect.TypeFor[durable.ScopeRecord](), reflect.TypeFor[snapshotScope]()},
		{reflect.TypeFor[durable.AttemptRecord](), reflect.TypeFor[snapshotAttempt]()},
		{reflect.TypeFor[durable.NodeRecord](), reflect.TypeFor[snapshotNode]()},
		{reflect.TypeFor[durable.ValueRecord](), reflect.TypeFor[snapshotValue]()},
		{reflect.TypeFor[durable.StreamIDRecord](), reflect.TypeFor[snapshotStreamID]()},
		{reflect.TypeFor[durable.StreamDescriptorRecord](), reflect.TypeFor[snapshotStreamDescriptor]()},
		{reflect.TypeFor[durable.StreamMessageRecord](), reflect.TypeFor[snapshotStreamMessage]()},
		{reflect.TypeFor[durable.StreamRecord](), reflect.TypeFor[snapshotStream]()},
		{reflect.TypeFor[durable.MessageAttemptRecord](), reflect.TypeFor[snapshotMessageAttempt]()},
		{reflect.TypeFor[durable.ManagedRecord](), reflect.TypeFor[snapshotManaged]()},
		{reflect.TypeFor[durable.HistorySummary](), reflect.TypeFor[snapshotHistorySummary]()},
		{reflect.TypeFor[durable.CommitIndexEntry](), reflect.TypeFor[snapshotCommitIndex]()},
		{reflect.TypeFor[durable.Provenance](), reflect.TypeFor[snapshotProvenance]()},
	}
	for _, pair := range pairs {
		domain, dto := pair[0], pair[1]
		if domain.NumField() != dto.NumField() {
			t.Fatalf("%s and %s field counts differ: update the explicit mapping", domain, dto)
		}
		for i := 0; i < domain.NumField(); i++ {
			field := domain.Field(i)
			mapped, ok := dto.FieldByName(field.Name)
			if !ok || snapshotfmt.FieldName(field) != snapshotfmt.FieldName(mapped) || field.Type.Kind() != mapped.Type.Kind() {
				t.Fatalf("unmapped field %s.%s", domain, field.Name)
			}
		}
	}
}

func TestSnapshotDTOEncodingMatchesCountedRepresentation(t *testing.T) {
	for _, data := range [][]byte{nil, {}, {0, 255, 1}, bytes.Repeat([]byte{0xff}, 1024)} {
		value := durable.ValueRecord{Name: "中文<&>", Data: data, Stream: &durable.StreamDescriptorRecord{ID: durable.StreamIDRecord{Generation: ^uint64(0)}}}
		for _, empty := range []bool{false, true} {
			record := durable.RunRecord{RunID: "dto", Revision: ^uint64(0), Scopes: map[string]durable.ScopeRecord{"s": {Nodes: map[string]durable.NodeRecord{"n": {Outputs: []durable.ValueRecord{value}}}}},
				Streams: map[string]durable.StreamRecord{"s": {Pending: []durable.StreamMessageRecord{{Sequence: 1, Value: value}}, Cursor: &value}},
				Managed: map[string]durable.ManagedRecord{"m": {State: []durable.ValueRecord{value}, Values: []durable.ValueRecord{}}}}
			if empty {
				record.HistorySummaries = []durable.HistorySummary{}
				record.CommitIndex = []durable.CommitIndexEntry{}
				record.Provenance = &durable.Provenance{}
			}
			wire, err := EncodeSnapshot(record)
			if err != nil {
				t.Fatal(err)
			}
			count, err := snapshotfmt.RecordSize(record)
			if err != nil || count != int64(len(wire)) {
				t.Fatalf("count=%d bytes=%d err=%v", count, len(wire), err)
			}
			domainWire, err := snapshotfmt.Encode(record)
			if err != nil || !bytes.Equal(wire, domainWire) {
				t.Fatalf("DTO and counted field representation diverged: %v", err)
			}
			decoded, err := DecodeSnapshot(wire)
			if err != nil || !reflect.DeepEqual(decoded, record) {
				t.Fatalf("DTO did not preserve field shapes: %v", err)
			}
		}
	}
}

func TestSnapshotRejectsLegacyScopeBindingsWithoutRewriting(t *testing.T) {
	record := durable.RunRecord{RunID: "scope", Scopes: map[string]durable.ScopeRecord{"child": {Graph: "child", Path: "root/sub", Inputs: "fingerprint"}}}
	current, err := EncodeSnapshot(record)
	if err != nil {
		t.Fatal(err)
	}
	needle := []byte(`"graph":"child"`)
	if !bytes.Contains(current, needle) {
		t.Fatal("legacy scope probe missed current DTO")
	}
	for _, value := range []string{"true", "false"} {
		legacy := bytes.Replace(current, needle, []byte(`"bindings":`+value+`,`+string(needle)), 1)
		before := bytes.Clone(legacy)
		if _, err := DecodeSnapshot(legacy); err == nil {
			t.Fatalf("legacy scope bindings=%s accepted", value)
		}
		if !bytes.Equal(legacy, before) {
			t.Fatal("legacy scope rejection rewrote input")
		}
	}
}

func TestBareDecoderRejectsEnvelopeAndLegacyFormat(t *testing.T) {
	for _, input := range []string{
		`{"run_id":"r","format":2}`,
		`{"run_id":"r","format":3,"history_summaries":[]}`,
		`{"version":999,"record":{"run_id":"r"}}`,
		`{"run_id":"r","version":"v1","record":{"run_id":"shadow"}}`,
		`{"run_id":"r","unknown_required_component":{}}`,
		`{"run_id":"a","run_id":"b"}`,
		`{"run_id":"a","run\u005fid":"b"}`,
		`{"scopes":{"s":{},"s":{}}}`,
		`{"history_summaries":[{"kind":"required"}],"History_Summaries":null}`,
		`{"commit_index":[{"revision":1}],"Commit_Index":null}`,
		`{"scopes":{"s":{"nodes":{},"Nodes":null}}}`,
		`{"scopes":{"s":{"bindings":true}}}`,
		`{"scopes":{"s":{"bindings":false}}}`,
		`{"Run_ID":"case-alias"}`,
		`null`,
	} {
		before := []byte(input)
		if _, err := Decode(before); err == nil || strings.Contains(err.Error(), "shadow") {
			t.Fatalf("accepted unknown encoding or disclosed input: %v", err)
		}
		if string(before) != input {
			t.Fatal("decoder changed rejected input")
		}
	}
}
