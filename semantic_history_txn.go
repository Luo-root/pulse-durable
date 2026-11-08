// 本文件把语义历史证据接入现有 recordTransaction 覆盖层。
// 与 attempt/stream 操作共用 CAS 与确认后发布；这里只暂存证据，不执行压缩或后端 I/O。
package durable

import (
	"fmt"
	"reflect"
	"sort"
)

// putHistorySummary 暂存摘要的精确 CAS。expected 为 nil 表示键不存在，
// value 为 nil 表示删除已有摘要；两者不能同时为 nil。
func (tx *recordTransaction) putHistorySummary(key HistorySummaryKey, expected, value *HistorySummary) error {
	if tx.err != nil {
		return tx.err
	}
	if !key.valid() || (expected == nil && value == nil) {
		tx.err = fmt.Errorf("%w: invalid history summary operation", ErrConflict)
		return tx.err
	}
	op := putHistorySummary{absent: expected == nil, remove: value == nil}
	if expected != nil {
		if expected.key() != key {
			tx.err = fmt.Errorf("%w: history summary expected key mismatch", ErrConflict)
			return tx.err
		}
		if err := expected.validate(); err != nil {
			tx.err = err
			return err
		}
		op.expected = copyHistorySummary(*expected)
	}
	if value != nil {
		if value.key() != key {
			tx.err = fmt.Errorf("%w: history summary value key mismatch", ErrConflict)
			return tx.err
		}
		if err := value.validate(); err != nil {
			tx.err = err
			return err
		}
		op.value = copyHistorySummary(*value)
	}
	stable := historySummaryKey{kind: key.Kind, id: key.ID}
	if old, ok := tx.summaries[stable]; ok &&
		(old.absent != op.absent || old.remove != op.remove || !reflect.DeepEqual(old.expected, op.expected)) {
		tx.err = fmt.Errorf("%w: conflicting history summary precondition", ErrConflict)
		return tx.err
	}
	if tx.summaries == nil {
		tx.summaries = make(map[historySummaryKey]putHistorySummary)
	}
	tx.summaries[stable] = op
	return nil
}

// putCommitIndex 暂存因果索引的精确 CAS。revision 是持久键，不能从 value
// 推导；value 为 nil 表示删除。
func (tx *recordTransaction) putCommitIndex(revision uint64, expected, value *CommitIndexEntry) error {
	if tx.err != nil {
		return tx.err
	}
	if revision == 0 || (expected == nil && value == nil) {
		tx.err = fmt.Errorf("%w: invalid commit index operation", ErrConflict)
		return tx.err
	}
	op := putCommitIndex{absent: expected == nil, remove: value == nil}
	if expected != nil {
		if err := expected.validate(revision); err != nil {
			tx.err = err
			return err
		}
		op.expected = *expected
	}
	if value != nil {
		if err := value.validate(revision); err != nil {
			tx.err = err
			return err
		}
		op.value = *value
	}
	if old, ok := tx.commitIndex[revision]; ok &&
		(old.absent != op.absent || old.remove != op.remove || old.expected != op.expected) {
		tx.err = fmt.Errorf("%w: conflicting commit index precondition", ErrConflict)
		return tx.err
	}
	if tx.commitIndex == nil {
		tx.commitIndex = make(map[uint64]putCommitIndex)
	}
	tx.commitIndex[revision] = op
	return nil
}

// putProvenance 暂存唯一来源证明的精确 CAS。来源证明不是权限凭据；它只是
// 让恢复和后续压缩执行能够核对来源记录。
func (tx *recordTransaction) putProvenance(expected, value *Provenance) error {
	if tx.err != nil {
		return tx.err
	}
	if expected == nil && value == nil {
		tx.err = fmt.Errorf("%w: invalid provenance operation", ErrConflict)
		return tx.err
	}
	op := provenanceOperation{absent: expected == nil, remove: value == nil}
	if expected != nil {
		if err := expected.validate(); err != nil {
			tx.err = err
			return err
		}
		op.expected = *expected
	}
	if value != nil {
		if err := value.validate(); err != nil {
			tx.err = err
			return err
		}
		op.value = *value
	}
	if tx.provenance != nil && (tx.provenance.absent != op.absent || tx.provenance.remove != op.remove || tx.provenance.expected != op.expected) {
		tx.err = fmt.Errorf("%w: conflicting provenance precondition", ErrConflict)
		return tx.err
	}
	tx.provenance = &op
	return nil
}

func (tx *recordTransaction) hasSemanticEvidence() bool {
	return len(tx.summaries) != 0 || len(tx.commitIndex) != 0 || tx.provenance != nil
}

func findHistorySummary(values []HistorySummary, key HistorySummaryKey) (HistorySummary, bool) {
	index := summaryIndex(values, key)
	if index < 0 {
		return HistorySummary{}, false
	}
	return values[index], true
}

// checkSemanticEvidence 只检查本批涉及的精确旧值。普通事务要求来源已经
// 携带完整语义证据组件；首次物化由显式边界协议单独完成。
func (tx *recordTransaction) checkSemanticEvidence(record RunRecord) error {
	if !tx.hasSemanticEvidence() {
		return nil
	}
	if !hasAnySemanticEvidence(record) {
		return fmt.Errorf("%w: semantic evidence requires an existing complete evidence component", ErrConflict)
	}
	if !hasCompleteSemanticEvidence(record) {
		return validateSemanticEvidence(record)
	}
	for key, op := range tx.summaries {
		current, exists := findHistorySummary(record.HistorySummaries, HistorySummaryKey{Kind: key.kind, ID: key.id})
		if exists == op.absent || (exists && !reflect.DeepEqual(current, op.expected)) {
			return fmt.Errorf("%w: history summary precondition", ErrConflict)
		}
		// 已应用高水位只能由独立语义压缩事务推进；普通事务
		// 不能删除、改写或伪造它们。
		if !tx.historyCompression {
			if err := checkOrdinarySummaryOperation(op); err != nil {
				return err
			}
		}
	}
	for revision, op := range tx.commitIndex {
		index := commitIndexPosition(record.CommitIndex, revision)
		exists := index >= 0
		var current CommitIndexEntry
		if exists {
			current = record.CommitIndex[index]
		}
		if exists == op.absent || (exists && current != op.expected) {
			return fmt.Errorf("%w: commit index precondition", ErrConflict)
		}
	}
	if tx.provenance != nil {
		op := *tx.provenance
		exists := record.Provenance != nil
		if exists == op.absent || (exists && *record.Provenance != op.expected) {
			return fmt.Errorf("%w: provenance precondition", ErrConflict)
		}
	}
	return nil
}

// applySemanticEvidence 只应用已经通过 checkSemanticEvidence 的提案；它不独立
// 做权限或恢复校验。摘要按稳定键更新，索引保持 revision 升序。
func (tx *recordTransaction) applySemanticEvidence(record *RunRecord) {
	for _, key := range sortedSemanticTransactionKeys(tx.summaries) {
		op := tx.summaries[key]
		stable := HistorySummaryKey{Kind: key.kind, ID: key.id}
		index := summaryIndex(record.HistorySummaries, stable)
		if op.remove {
			if index >= 0 {
				record.HistorySummaries = append(record.HistorySummaries[:index], record.HistorySummaries[index+1:]...)
			}
			if len(record.HistorySummaries) == 0 {
				record.HistorySummaries = nil
			}
			continue
		}
		if index >= 0 {
			record.HistorySummaries[index] = copyHistorySummary(op.value)
		} else {
			record.HistorySummaries = append(record.HistorySummaries, copyHistorySummary(op.value))
		}
	}
	for _, revision := range sortedCommitTransactionRevisions(tx.commitIndex) {
		op := tx.commitIndex[revision]
		index := commitIndexPosition(record.CommitIndex, revision)
		if op.remove {
			if index >= 0 {
				record.CommitIndex = append(record.CommitIndex[:index:index], record.CommitIndex[index+1:]...)
			}
			if len(record.CommitIndex) == 0 {
				record.CommitIndex = nil
			}
			continue
		}
		if index >= 0 {
			record.CommitIndex[index] = op.value
		} else {
			record.CommitIndex = append(record.CommitIndex, op.value)
		}
	}
	// 索引是连续不可变前缀；按序处理本批键后直接追加即可，不扫描或排序旧历史。
	if tx.provenance != nil {
		if tx.provenance.remove {
			record.Provenance = nil
		} else {
			value := tx.provenance.value
			record.Provenance = &value
		}
	}
}

func sortedSemanticTransactionKeys(values map[historySummaryKey]putHistorySummary) []historySummaryKey {
	keys := make([]historySummaryKey, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].kind != keys[j].kind {
			return keys[i].kind < keys[j].kind
		}
		return keys[i].id < keys[j].id
	})
	return keys
}

func sortedCommitTransactionRevisions(values map[uint64]putCommitIndex) []uint64 {
	revisions := make([]uint64, 0, len(values))
	for revision := range values {
		revisions = append(revisions, revision)
	}
	sort.Slice(revisions, func(i, j int) bool { return revisions[i] < revisions[j] })
	return revisions
}

// ensureSemanticCommitIndex 为已有完整语义证据记录的每次普通业务提交
// 补齐下一条因果索引。索引摘要绑定去掉 commit_index 操作后的 canonical
// batch，避免 CommitIndexEntry.BatchDigest 与它自身循环引用。
func (tx *recordTransaction) ensureSemanticCommitIndex(record RunRecord) error {
	if !hasCompleteSemanticEvidence(record) {
		return nil
	}
	if record.Revision == ^uint64(0) {
		return fmt.Errorf("%w: revision exhausted", ErrConflict)
	}
	revision := record.Revision + 1
	if _, exists := tx.commitIndex[revision]; exists {
		return nil
	}
	provisional := ChangeBatch{header: changeHeaderFor(record, tx), tx: tx}
	batchDigest, err := semanticBatchContentDigest(provisional)
	if err != nil {
		return err
	}
	entry := CommitIndexEntry{Revision: revision, ParentRevision: record.Revision, BatchDigest: batchDigest}
	return tx.putCommitIndex(revision, nil, &entry)
}
