package durable

import (
	"errors"
	"reflect"
	"testing"

	"github.com/Luo-root/pulse-durable/internal/snapshotfmt"
)

// TestBuildHistoryCompressionBatchBindsDeleteSummaryAndIndex 验证压缩批次的
// 三个不可拆分部分：对象精确删除、摘要应用位置和下一条 CommitIndex。
// 任一部分缺失，批次都不应被当成已完成的语义压缩。
func TestBuildHistoryCompressionBatchBindsDeleteSummaryAndIndex(t *testing.T) {
	before := compressionPlanRecord(t)
	request := HistoryCompressionRequest{RunID: before.RunID, ThroughRevision: before.Revision, MaxObjects: 100, MaxBytes: 1 << 20}
	plan, err := PlanHistoryCompression(before, request)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := BuildHistoryCompressionBatch(before, plan)
	if err != nil {
		t.Fatal(err)
	}
	if !batch.tx.historyCompression || batch.tx.compressionPlanDigest != plan.PlanDigest {
		t.Fatalf("compression transaction lost operation binding: plan=%s tx=%+v", plan.PlanDigest, batch.tx)
	}
	untouched := CloneRecord(before)
	after := CloneRecord(before)
	if _, err := batch.ApplyTo(&after); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, untouched) {
		t.Fatal("batch construction or application mutated the source record")
	}
	for _, object := range plan.DeleteObjects {
		if _, exists := after.Attempts[object.ID]; exists {
			t.Fatalf("planned attempt %q was retained", object.ID)
		}
	}
	if after.Revision != before.Revision+1 || len(after.CommitIndex) != len(before.CommitIndex)+1 {
		t.Fatalf("compression revision/index mismatch: before=%d/%d after=%d/%d", before.Revision, len(before.CommitIndex), after.Revision, len(after.CommitIndex))
	}
	var payload retryPrefixSummaryPayload
	if err := unmarshalRecord(after.HistorySummaries[0].Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.AppliedRevision != after.Revision {
		t.Fatalf("summary applied revision=%d want=%d", payload.AppliedRevision, after.Revision)
	}
	entry := after.CommitIndex[len(after.CommitIndex)-1]
	if entry.Revision != after.Revision || entry.ParentRevision != before.Revision {
		t.Fatalf("compression commit index does not close the revision edge: %+v", entry)
	}
	if err := ValidateSemanticEvidence(after); err != nil {
		t.Fatal(err)
	}
}

// TestHistoryCompressionBatchWireRoundTrip 保证压缩操作标记和 null 删除
// 形状经过后端 wire 仍保持同一批次语义，解码批次不能退化成普通元数据写入。
func TestHistoryCompressionBatchWireRoundTrip(t *testing.T) {
	before := compressionPlanRecord(t)
	request := HistoryCompressionRequest{RunID: before.RunID, ThroughRevision: before.Revision, MaxObjects: 100, MaxBytes: 1 << 20}
	plan, err := PlanHistoryCompression(before, request)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := BuildHistoryCompressionBatch(before, plan)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := MarshalChangeBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if !containsBytes(wire, []byte(`"operation":"history_compression"`)) || !containsBytes(wire, []byte(`"value":null`)) {
		t.Fatalf("wire lost compression operation or null deletion: %s", wire)
	}
	decoded, err := UnmarshalChangeBatch(wire)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Header() != batch.Header() {
		t.Fatalf("decoded compression header differs: got=%+v want=%+v", decoded.Header(), batch.Header())
	}
	want := CloneRecord(before)
	got := CloneRecord(before)
	if _, err := batch.ApplyTo(&want); err != nil {
		t.Fatal(err)
	}
	if _, err := decoded.ApplyTo(&got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatal("wire round-trip changed compression projection")
	}
}

// TestHistoryCompressionBatchRejectsStaleDeleteAtomically 验证删除使用完整旧值
// CAS。对象在构造后发生变化时，ApplyTo 必须在修改任何字段前失败。
func TestHistoryCompressionBatchRejectsStaleDeleteAtomically(t *testing.T) {
	before := compressionPlanRecord(t)
	request := HistoryCompressionRequest{RunID: before.RunID, ThroughRevision: before.Revision, MaxObjects: 100, MaxBytes: 1 << 20}
	plan, err := PlanHistoryCompression(before, request)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := BuildHistoryCompressionBatch(before, plan)
	if err != nil {
		t.Fatal(err)
	}
	stale := CloneRecord(before)
	id := plan.DeleteObjects[0].ID
	value := stale.Attempts[id]
	value.Limit++
	stale.Attempts[id] = value
	untouched := CloneRecord(stale)
	if _, err := batch.ApplyTo(&stale); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale compression delete accepted: %v", err)
	}
	if !reflect.DeepEqual(stale, untouched) {
		t.Fatal("stale delete partially changed the record")
	}
}

func TestHistoryCompressionBatchRejectsPlanDigestWithPartialOperations(t *testing.T) {
	before := compressionPlanRecord(t)
	request := HistoryCompressionRequest{RunID: before.RunID, ThroughRevision: before.Revision, MaxObjects: 100, MaxBytes: 1 << 20}
	plan, err := PlanHistoryCompression(before, request)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := BuildHistoryCompressionBatch(before, plan)
	if err != nil {
		t.Fatal(err)
	}
	for id := range batch.tx.attempts {
		delete(batch.tx.attempts, id)
		break
	}
	proposal := CloneRecord(before)
	if _, err := batch.ApplyTo(&proposal); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("partial compression operation set accepted: %v", err)
	}
	if !reflect.DeepEqual(proposal, before) {
		t.Fatal("partial compression failure changed the source projection")
	}
}

// TestHistoryCompressionMessageDeleteAndSnapshotDelta 使用消息前缀夹具，
// 同时检查 Message 删除与有符号快照差额。差额必须等于完整编码前后长度，
// 不能把删除误算成零字节替换。
func TestHistoryCompressionMessageDeleteAndSnapshotDelta(t *testing.T) {
	before := compressionMessageRecord(t)
	request := HistoryCompressionRequest{RunID: before.RunID, ThroughRevision: before.Revision, MaxObjects: 100, MaxBytes: 1 << 20}
	plan, err := PlanHistoryCompression(before, request)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := BuildHistoryCompressionBatch(before, plan)
	if err != nil {
		t.Fatal(err)
	}
	beforeBytes, err := snapshotfmt.Encode(before)
	if err != nil {
		t.Fatal(err)
	}
	delta, err := batch.SnapshotSizeDelta(before)
	if err != nil {
		t.Fatal(err)
	}
	after := CloneRecord(before)
	if _, err := batch.ApplyTo(&after); err != nil {
		t.Fatal(err)
	}
	afterBytes, err := snapshotfmt.Encode(after)
	if err != nil {
		t.Fatal(err)
	}
	if delta != int64(len(afterBytes)-len(beforeBytes)) {
		t.Fatalf("message compression snapshot delta=%d want=%d", delta, len(afterBytes)-len(beforeBytes))
	}
	for _, object := range plan.DeleteObjects {
		if object.Kind == "message" {
			if _, exists := after.Messages[object.ID]; exists {
				t.Fatalf("planned message %q was retained", object.ID)
			}
		}
	}
}

func TestMessageIndexRetainsSuffixAfterCompressionDelete(t *testing.T) {
	input := StreamIDRecord{Scope: "root", NodeID: "source", Key: "items", Group: "g", Generation: 1}
	first := MessageAttemptRecord{Scope: "root", Parent: "proc", NodeID: "consumer", Input: input, Sequence: 1, Index: 1, Limit: 3, State: AttemptFailed}
	latest := MessageAttemptRecord{Scope: "root", Parent: "proc", NodeID: "consumer", Input: input, Sequence: 1, Index: 2, Limit: 3, State: AttemptActive}
	key := messageGroup(latest)
	state := &runState{
		record:       RunRecord{Messages: map[string]MessageAttemptRecord{"first": first, "latest": latest}},
		messageIndex: map[messageGroupKey]messageIndexEntry{key: {id: "latest", index: latest.Index}}, messageIndexReady: true,
	}
	tx := newRecordTransaction(1)
	if err := tx.deleteMessage("latest", &latest); err != nil {
		t.Fatal(err)
	}
	tx.apply(&state.record)
	state.updateMessageIndex(tx)
	entry, ok := state.messageIndex[key]
	if !ok || entry.id != "first" || entry.index != first.Index {
		t.Fatalf("message suffix disappeared from index: %+v, ok=%v", entry, ok)
	}
}

func containsBytes(data, needle []byte) bool {
	for i := 0; i+len(needle) <= len(data); i++ {
		if reflect.DeepEqual(data[i:i+len(needle)], needle) {
			return true
		}
	}
	return false
}
