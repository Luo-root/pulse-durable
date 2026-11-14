// 本文件验证只读物化标记与可压缩前缀能够组合。
// 标记只证明来源链，不消费 Retry/Message 预算，也不允许专用压缩批次改写它。
package durable

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestHistoryCompressionPreservesReadOnlyEvidenceMarker(t *testing.T) {
	p, _ := compressionRetryCommitRecord(t)
	record := CloneRecord(p.s.record)
	markerPayload := []byte(`{"schema":"history-evidence/v1","source":"committed-chain"}`)
	marker := HistorySummary{Kind: historyEvidenceSummaryKind, ID: "origin", SourceRevision: 1,
		SourceDigest: record.CommitIndex[0].BatchDigest, Payload: markerPayload, Digest: digest(markerPayload)}
	record.HistorySummaries = append(record.HistorySummaries, marker)
	before := CloneRecord(record)
	request := HistoryCompressionRequest{RunID: record.RunID, ThroughRevision: record.Revision, MaxObjects: 100, MaxBytes: 1 << 20}
	plan, err := PlanHistoryCompression(record, request)
	if err != nil || plan.Rows != 2 || len(plan.SummaryDigests) != 2 {
		t.Fatalf("mixed evidence planning rows=%d summaries=%d: %v", plan.Rows, len(plan.SummaryDigests), err)
	}
	protectedKey := CompressionObjectRef{Kind: "history_summary", ID: `{"kind":"history_evidence","id":"origin"}`}
	protected := false
	for _, object := range plan.ProtectedObjects {
		protected = protected || object == protectedKey
	}
	if !protected {
		t.Fatal("read-only evidence marker is not protected")
	}
	batch, err := BuildHistoryCompressionBatch(record, plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, changed := batch.tx.summaries[historySummaryKey{kind: marker.Kind, id: marker.ID}]; changed {
		t.Fatal("read-only marker entered the mutable summary set")
	}
	wire, err := MarshalChangeBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	roundtrip, err := UnmarshalChangeBatch(wire)
	if err != nil {
		t.Fatal(err)
	}
	after := CloneRecord(record)
	if _, err := roundtrip.ApplyTo(&after); err != nil {
		t.Fatal(err)
	}
	got := after.HistorySummaries[summaryIndex(after.HistorySummaries, marker.key())]
	if !reflect.DeepEqual(got, marker) || after.Revision != record.Revision+1 || len(after.CommitIndex) != len(record.CommitIndex)+1 {
		t.Fatal("compression changed the marker or failed to append its commit position")
	}
	if !reflect.DeepEqual(record, before) {
		t.Fatal("planning or construction mutated source evidence")
	}
	// 在仍然完整的压缩批次中额外塞入 marker 替换，不能借同批索引绕过只读边界。
	forged := batch
	forged.tx = batch.tx.copy()
	replacement := copyHistorySummary(marker)
	replacement.Payload = bytes.Clone(marker.Payload)
	replacement.Payload[0] = '['
	replacement.Digest = digest(replacement.Payload)
	if err := forged.tx.putHistorySummary(marker.key(), &marker, &replacement); err != nil {
		t.Fatal(err)
	}
	untouched := CloneRecord(record)
	if _, err := forged.ApplyTo(&untouched); err == nil || !reflect.DeepEqual(untouched, record) {
		t.Fatal("compression accepted a read-only marker mutation")
	}
}

func TestHistoryCompressionDoesNotTreatUnknownKindAsReadOnly(t *testing.T) {
	// 用 revision >= 2 的真实记录，让未知 Kind 的 SourceRevision 严格早于 head：
	// 只读 marker 允许早于 head，未知 Kind 不能因此获得同样的跳过待遇。
	p, _ := compressionRetryCommitRecord(t)
	record := CloneRecord(p.s.record)
	if record.Revision < 2 {
		t.Fatalf("fixture revision must be >= 2, got %d", record.Revision)
	}
	payload := []byte(`{}`)
	record.HistorySummaries = append(record.HistorySummaries, HistorySummary{Kind: "future_kind", ID: "opaque",
		SourceRevision: 1, SourceDigest: record.CommitIndex[0].BatchDigest, Payload: payload, Digest: digest(payload)})
	request := HistoryCompressionRequest{RunID: record.RunID, ThroughRevision: record.Revision, MaxObjects: 100, MaxBytes: 1 << 20}
	if _, err := PlanHistoryCompression(record, request); !errors.Is(err, ErrUnsupportedCapability) {
		t.Fatalf("unknown Kind before head bypassed planning through read-only fallback: %v", err)
	}
}

func TestHistoryCompressionRejectsCorruptReadOnlyMarker(t *testing.T) {
	// 已注册 Kind 但 payload 损坏时必须失败，不能因为「不可压缩」就被保护并跳过。
	p, _ := compressionRetryCommitRecord(t)
	record := CloneRecord(p.s.record)
	bad := []byte(`{"schema":"history-evidence/v9","source":"committed-chain"}`)
	record.HistorySummaries = append(record.HistorySummaries, HistorySummary{Kind: historyEvidenceSummaryKind, ID: "origin",
		SourceRevision: 1, SourceDigest: record.CommitIndex[0].BatchDigest, Payload: bad, Digest: digest(bad)})
	request := HistoryCompressionRequest{RunID: record.RunID, ThroughRevision: record.Revision, MaxObjects: 100, MaxBytes: 1 << 20}
	if _, err := PlanHistoryCompression(record, request); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("corrupt read-only marker accepted: %v", err)
	}
}

func TestHistoryCompressionRejectsMarkerRemovalAndUnknownSummaryOp(t *testing.T) {
	p, _ := compressionRetryCommitRecord(t)
	record := CloneRecord(p.s.record)
	markerPayload := []byte(`{"schema":"history-evidence/v1","source":"committed-chain"}`)
	marker := HistorySummary{Kind: historyEvidenceSummaryKind, ID: "origin", SourceRevision: 1,
		SourceDigest: record.CommitIndex[0].BatchDigest, Payload: markerPayload, Digest: digest(markerPayload)}
	record.HistorySummaries = append(record.HistorySummaries, marker)
	request := HistoryCompressionRequest{RunID: record.RunID, ThroughRevision: record.Revision, MaxObjects: 100, MaxBytes: 1 << 20}
	plan, err := PlanHistoryCompression(record, request)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := BuildHistoryCompressionBatch(record, plan)
	if err != nil {
		t.Fatal(err)
	}
	// 删除只读 marker：形状校验必须拒绝，且源记录不变。
	removal := batch
	removal.tx = batch.tx.copy()
	if err := removal.tx.putHistorySummary(marker.key(), &marker, nil); err != nil {
		t.Fatal(err)
	}
	untouched := CloneRecord(record)
	if _, err := removal.ApplyTo(&untouched); err == nil || !reflect.DeepEqual(untouched, record) {
		t.Fatal("compression accepted read-only marker removal")
	}
	// 注入未知 Kind 摘要 op：同样必须拒绝。
	injected := batch
	injected.tx = batch.tx.copy()
	unknownPayload := []byte(`{}`)
	unknown := HistorySummary{Kind: "future_kind", ID: "injected", SourceRevision: record.Revision,
		SourceDigest: compressionSourceDigest(record), Payload: unknownPayload, Digest: digest(unknownPayload)}
	if err := injected.tx.putHistorySummary(unknown.key(), nil, &unknown); err != nil {
		t.Fatal(err)
	}
	untouched = CloneRecord(record)
	if _, err := injected.ApplyTo(&untouched); err == nil || !reflect.DeepEqual(untouched, record) {
		t.Fatal("compression accepted an unknown-Kind summary operation")
	}
}
