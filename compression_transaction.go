// 本文件定义语义历史压缩的根包删除事务提案。
// 它像数据库层的“构造事务”而不是第二套执行器：这里只把已经由只读
// 计划证明的 Attempt/Message 精确 CAS 删除、已应用摘要和下一条
// CommitIndex 放进现有 recordTransaction；真正的持久提交仍经过
// ChangeBatch.ApplyTo、Session.Apply 与确认后 Runtime 发布。
package durable

import (
	"context"
	"fmt"
	"reflect"
)

// BuildHistoryCompressionBatch 根据一个只读计划构造带语义证据的专用压缩批次。
//
// 构造过程会再次从 record 事实规划，并要求 PlanDigest 与当前事实一致；
// 调用方即使能够重新计算 PlanDigest，也不能借此替换删除集合。每个
// Attempt/Message 都保存完整旧值作为 CAS 前置条件。每条已知摘要的
// Payload 会在同一批次中写入 AppliedRevision=record.Revision+1，随后由
// 同一批次追加下一条 CommitIndex。函数不写 Store、不修改 record；返回的
// ChangeBatch 可以交给现有 Session.Apply，也可以先用 ApplyTo 做隔离投影。
//
// 这里不把 Provenance 当成权限凭据，也不修改它：Provenance 描述记录的
// 来源链，压缩提交位置由摘要 SourceRevision、AppliedRevision 和
// CommitIndex 共同见证。
func BuildHistoryCompressionBatch(record RunRecord, plan HistoryCompressionPlan) (ChangeBatch, error) {
	if err := validateHistoryCompressionSource(record, plan); err != nil {
		return ChangeBatch{}, err
	}
	if record.Revision == ^uint64(0) {
		return ChangeBatch{}, fmt.Errorf("%w: compression revision exhausted", ErrConflict)
	}
	appliedRevision := record.Revision + 1
	tx := newHistoryCompressionTransaction(record.Revision, plan.PlanDigest, plan.Request)

	// 删除集合来自重新验证过的计划，但旧值始终重新从当前权威记录读取。
	// 计划中的 ID 不能替代 CAS 旧值，也不能让缺失对象被当成幂等成功。
	for _, object := range plan.DeleteObjects {
		switch object.Kind {
		case "attempt":
			expected, ok := record.Attempts[object.ID]
			if !ok {
				return ChangeBatch{}, fmt.Errorf("%w: compression attempt %q disappeared", ErrConflict, object.ID)
			}
			if err := tx.deleteAttempt(object.ID, &expected); err != nil {
				return ChangeBatch{}, err
			}
		case "message":
			expected, ok := record.Messages[object.ID]
			if !ok {
				return ChangeBatch{}, fmt.Errorf("%w: compression message %q disappeared", ErrConflict, object.ID)
			}
			if err := tx.deleteMessage(object.ID, &expected); err != nil {
				return ChangeBatch{}, err
			}
		default:
			// PlanHistoryCompression 目前只允许这两类可恢复对象；显式保留
			// 失败关闭，避免未来新增 Kind 时误删未知持久对象。
			return ChangeBatch{}, fmt.Errorf("%w: compression object kind %q has no delete transaction", ErrUnsupportedCapability, object.Kind)
		}
	}

	// 计划覆盖当前全部摘要。摘要不删除，而是把候选事实推进为“已应用”
	// 见证；删除对象与这个推进必须共用同一个 recordTransaction。
	for _, summary := range record.HistorySummaries {
		if !defaultHistorySummaryRegistry.compressionEnabled(summary.Kind) {
			continue
		}
		value, err := appliedCompressionSummary(summary, appliedRevision)
		if err != nil {
			return ChangeBatch{}, err
		}
		expected := copyHistorySummary(summary)
		if err := tx.putHistorySummary(summary.key(), &expected, &value); err != nil {
			return ChangeBatch{}, err
		}
	}

	// ensureSemanticCommitIndex 先对“去掉自身 commit_index 操作”的
	// canonical batch 求摘要，再追加唯一的下一条索引，避免自引用。
	if err := tx.ensureSemanticCommitIndex(record); err != nil {
		return ChangeBatch{}, err
	}
	batch := ChangeBatch{header: changeHeaderFor(record, tx), tx: tx}

	// 在隔离副本上执行一次完整 ApplyTo。这样构造器返回的批次已经同时
	// 通过 CAS、语义转移和恢复投影检查；原 record 在任何失败路径都不变。
	proposal := CloneRecord(record)
	if _, err := batch.ApplyTo(&proposal); err != nil {
		return ChangeBatch{}, err
	}
	if err := validateHistoryCompressionCommit(record, proposal, plan, batch); err != nil {
		return ChangeBatch{}, err
	}
	return batch, nil
}

// validateHistoryCompressionSource 把“计划可重签”与“计划仍绑定当前 head”
// 分开处理。后端执行前应在自己的权威锁内重新 Load record，再调用构造器。
func validateHistoryCompressionSource(record RunRecord, plan HistoryCompressionPlan) error {
	if err := plan.Validate(plan.Request); err != nil {
		return err
	}
	if record.RunID != plan.RunID || !hasCompleteSemanticEvidence(record) ||
		record.Revision != plan.HeadRevision || recordDigest(record) != plan.HeadDigest {
		return fmt.Errorf("%w: compression plan or source record mismatch", ErrConflict)
	}
	fresh, err := PlanHistoryCompression(record, plan.Request)
	if err != nil {
		return err
	}
	if fresh.PlanDigest != plan.PlanDigest {
		return fmt.Errorf("%w: compression plan differs from source facts", ErrConflict)
	}
	return nil
}

// appliedCompressionSummary 只修改已知摘要的应用位置，并重新计算 Payload
// Digest。它拒绝拿已经应用的摘要再次充当候选，避免一次提交重复消费同一
// 历史前缀或覆盖其高水位。
func appliedCompressionSummary(summary HistorySummary, appliedRevision uint64) (HistorySummary, error) {
	if err := summary.validate(); err != nil {
		return HistorySummary{}, err
	}
	if appliedRevision == 0 {
		return HistorySummary{}, fmt.Errorf("%w: invalid compression application revision", ErrInvalid)
	}
	value := copyHistorySummary(summary)
	payload, err := defaultHistorySummaryRegistry.decode(summary)
	if err != nil {
		return HistorySummary{}, err
	}
	if payload.applicationRevision() != 0 {
		return HistorySummary{}, fmt.Errorf("%w: summary is not an unapplied candidate", ErrConflict)
	}
	encoded, err := payload.withApplication(appliedRevision)
	if err != nil {
		return HistorySummary{}, err
	}
	value.Payload = append([]byte(nil), encoded...)
	value.Digest = digest(value.Payload)
	if err := value.validate(); err != nil {
		return HistorySummary{}, err
	}
	return value, nil
}

// validateHistoryCompressionCommit 检查构造出的 proposal 与批次的精确结果，
// 并把恢复等价约束投影到“删除对象之外的业务事实”。语义证据本身通过
// expected ApplyTo 逐字比较，因此 AppliedRevision、CommitIndex 和 CAS 删除
// 不会被恢复投影比较掩盖。
func validateHistoryCompressionCommit(before, after RunRecord, plan HistoryCompressionPlan, batch ChangeBatch) error {
	if before.Revision == ^uint64(0) || after.Revision != before.Revision+1 ||
		!hasCompleteSemanticEvidence(after) || batch.Header().ExpectedRevision != before.Revision ||
		batch.Header().Revision != after.Revision {
		return fmt.Errorf("%w: invalid compression commit revision", ErrInvalidRecord)
	}
	if err := ValidateSemanticEvidence(after); err != nil {
		return err
	}
	expected := CloneRecord(before)
	if _, err := batch.ApplyTo(&expected); err != nil {
		return err
	}
	if !reflect.DeepEqual(expected, after) {
		return fmt.Errorf("%w: compression batch result differs from its atomic projection", ErrInvalidRecord)
	}

	// 只比较删除前缀之外的业务投影。Revision 和三类语义证据属于本次
	// 提交本身，已经由上面的完整 batch projection 独立核对。
	want := CloneRecord(before)
	for _, object := range plan.DeleteObjects {
		switch object.Kind {
		case "attempt":
			delete(want.Attempts, object.ID)
		case "message":
			delete(want.Messages, object.ID)
		default:
			return fmt.Errorf("%w: unsupported compression object kind %q", ErrUnsupportedCapability, object.Kind)
		}
	}
	want.Revision = after.Revision
	want.HistorySummaries, want.CommitIndex, want.Provenance = nil, nil, nil
	got := CloneRecord(after)
	got.HistorySummaries, got.CommitIndex, got.Provenance = nil, nil, nil
	if !reflect.DeepEqual(want, got) {
		return fmt.Errorf("%w: compressed recovery projection differs from retained facts", ErrInvalidRecord)
	}
	return nil
}

// commitHistoryCompression 在 Runtime 的独占锁内重新构造并提交压缩批次。
// 它不绕过普通 recordTransaction：锁内重规划、CAS 检查、Store.Apply、确认
// 后私有记录发布仍然复用同一条路径。后端独立维护入口可以直接使用
// BuildHistoryCompressionBatch + Session.Apply，获得相同的 ChangeBatch 语义。
func (s *runState) commitHistoryCompression(ctx context.Context, plan HistoryCompressionPlan) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped != nil {
		return s.stopped
	}
	if err := checkCheckpointContext(ctx); err != nil {
		return err
	}
	batch, err := BuildHistoryCompressionBatch(s.record, plan)
	if err != nil {
		return err
	}
	return s.commitRecordTransactionPrepared(ctx, batch.tx, nil)
}

func (b ChangeBatch) validateHistoryCompressionTransition(record RunRecord) error {
	if err := validateHistoryCompressionOperationShape("history_compression", b.tx.compressionPlanDigest, b.tx, b.header.Revision); err != nil {
		return err
	}
	if !hasCompleteSemanticEvidence(record) {
		return fmt.Errorf("%w: history compression requires a complete evidence component", ErrConflict)
	}
	if b.tx.compressionPlanDigest == "" {
		return fmt.Errorf("%w: history compression plan digest is missing", ErrConflict)
	}
	request := b.tx.compressionRequest
	if request.RunID != record.RunID || request.ThroughRevision != record.Revision {
		return fmt.Errorf("%w: history compression request binding mismatch", ErrConflict)
	}
	fresh, err := PlanHistoryCompression(record, request)
	if err != nil {
		return err
	}
	if fresh.PlanDigest != b.tx.compressionPlanDigest {
		return fmt.Errorf("%w: history compression plan no longer matches source facts", ErrConflict)
	}
	if err := validateHistoryCompressionBatchOperations(record, fresh, b.tx, b.header.Revision); err != nil {
		return err
	}
	// 压缩属于有界维护路径，允许在隔离副本上验证恢复闭包；不能只证明
	// 删除集合相同，就把缺父身份或 Managed 见证的结果发布为可恢复状态。
	proposal := CloneRecord(record)
	b.tx.apply(&proposal)
	proposal.Revision = b.header.Revision
	if _, err := buildRecoverySummaryIndex(proposal); err != nil {
		return err
	}
	return nil
}

// validateHistoryCompressionBatchOperations 把不可信 ChangeBatch 的每个
// 删除和摘要替换与当前 head 重新生成的计划逐项对齐。PlanDigest 只是完整性
// 摘要，不能授权调用方自行删减操作集合；否则一个“合法计划摘要 + 部分删除”
// 可能绕过原子恢复等价校验，把持久记录留在不可恢复的中间状态。
func validateHistoryCompressionBatchOperations(record RunRecord, plan HistoryCompressionPlan, tx *recordTransaction, appliedRevision uint64) error {
	if tx == nil || len(tx.attempts)+len(tx.messages) != len(plan.DeleteObjects) {
		return fmt.Errorf("%w: compression delete set differs from plan", ErrInvalidRecord)
	}
	deletes := make(map[CompressionObjectRef]struct{}, len(plan.DeleteObjects))
	for _, object := range plan.DeleteObjects {
		deletes[object] = struct{}{}
		switch object.Kind {
		case "attempt":
			expected, ok := record.Attempts[object.ID]
			op, staged := tx.attempts[object.ID]
			if !ok || !staged || op.absent || !op.remove || op.expected != expected {
				return fmt.Errorf("%w: compression attempt operation differs from plan", ErrInvalidRecord)
			}
		case "message":
			expected, ok := record.Messages[object.ID]
			op, staged := tx.messages[object.ID]
			if !ok || !staged || op.absent || !op.remove || op.expected != expected {
				return fmt.Errorf("%w: compression message operation differs from plan", ErrInvalidRecord)
			}
		default:
			return fmt.Errorf("%w: unsupported compression object kind %q", ErrUnsupportedCapability, object.Kind)
		}
	}
	for id, op := range tx.attempts {
		if _, ok := deletes[CompressionObjectRef{Kind: "attempt", ID: id}]; !ok || !op.remove {
			return fmt.Errorf("%w: compression contains an unplanned attempt delete", ErrInvalidRecord)
		}
	}
	for id, op := range tx.messages {
		if _, ok := deletes[CompressionObjectRef{Kind: "message", ID: id}]; !ok || !op.remove {
			return fmt.Errorf("%w: compression contains an unplanned message delete", ErrInvalidRecord)
		}
	}
	compressibleSummaries := 0
	for _, summary := range record.HistorySummaries {
		if defaultHistorySummaryRegistry.compressionEnabled(summary.Kind) {
			compressibleSummaries++
		}
	}
	if len(tx.summaries) != compressibleSummaries {
		return fmt.Errorf("%w: compression summary set differs from plan", ErrInvalidRecord)
	}
	for _, summary := range record.HistorySummaries {
		if !defaultHistorySummaryRegistry.compressionEnabled(summary.Kind) {
			continue
		}
		key := historySummaryKey{kind: summary.Kind, id: summary.ID}
		op, ok := tx.summaries[key]
		if !ok || op.absent || op.remove || !reflect.DeepEqual(op.expected, summary) {
			return fmt.Errorf("%w: compression summary operation differs from plan", ErrInvalidRecord)
		}
		want, err := appliedCompressionSummary(summary, appliedRevision)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(op.value, want) {
			return fmt.Errorf("%w: compression summary application differs from plan", ErrInvalidRecord)
		}
	}
	return nil
}

func validateHistoryCompressionOperationShape(operation, planDigest string, tx *recordTransaction, appliedRevision uint64) error {
	if tx == nil || !tx.historyCompression || operation != "history_compression" || !validDigest(planDigest) {
		return fmt.Errorf("%w: invalid history compression operation header", ErrConflict)
	}
	if len(tx.attempts)+len(tx.messages) == 0 || len(tx.summaries) == 0 || len(tx.commitIndex) != 1 || tx.provenance != nil ||
		len(tx.scopes) != 0 || len(tx.nodes) != 0 || len(tx.streams) != 0 || len(tx.pending) != 0 || len(tx.managed) != 0 || len(tx.managedEntries) != 0 {
		return fmt.Errorf("%w: invalid history compression operation set", ErrInvalidRecord)
	}
	for _, op := range tx.attempts {
		if !op.remove || op.absent {
			return fmt.Errorf("%w: compression attempts must be exact deletes", ErrInvalidRecord)
		}
	}
	for _, op := range tx.messages {
		if !op.remove || op.absent {
			return fmt.Errorf("%w: compression messages must be exact deletes", ErrInvalidRecord)
		}
	}
	for _, op := range tx.summaries {
		if !defaultHistorySummaryRegistry.compressionEnabled(op.value.Kind) {
			return fmt.Errorf("%w: non-compressible summary entered compression transaction", ErrInvalidRecord)
		}
		if op.absent || op.remove || op.value.SourceRevision >= appliedRevision || op.value.SourceRevision != op.expected.SourceRevision || op.value.SourceDigest != op.expected.SourceDigest {
			return fmt.Errorf("%w: compression summary source changed", ErrInvalidRecord)
		}
		payload, err := defaultHistorySummaryRegistry.decode(op.value)
		if err != nil {
			return err
		}
		if payload.applicationRevision() != appliedRevision {
			return fmt.Errorf("%w: summary application position is invalid", ErrInvalidRecord)
		}
	}
	index, ok := tx.commitIndex[appliedRevision]
	if !ok || !index.absent || index.remove || index.value.validate(appliedRevision) != nil {
		return fmt.Errorf("%w: compression must append its next commit index", ErrInvalidRecord)
	}
	return nil
}
