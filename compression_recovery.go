// 本文件把已应用语义摘要接入 Runtime 的只读恢复索引。
// 类似 messageIndex：RunRecord 是事实源，索引只加速预算/前沿查询；它不提交删除。
package durable

import (
	"fmt"
	"math"
	"sort"

	"github.com/Luo-root/pulse-durable/internal/snapshotfmt"
)

type retrySummaryGroup struct{ parent, node, boundary string }
type messageSummaryGroup struct {
	scope, parent, node, stream string
}

type recoverySummaryIndex struct {
	retries  map[retrySummaryGroup]retryPrefixSummaryPayload
	messages map[messageSummaryGroup]messagePrefixSummaryPayload
	managed  map[string]bool
	reserved map[string]bool
}

func compressionManagedDigest(managed ManagedRecord) string {
	// 快照编码保留 Data/State 的 nil/empty 形状，不能用 omitempty 归一化。
	encoded, err := snapshotfmt.Encode(managed)
	if err != nil {
		return ""
	}
	return digest(encoded)
}

// buildRecoverySummaryIndex 仅在完整恢复校验时读取摘要与当前事实。
// applied=source+1 是摘要与删除同一逻辑提交的必要关系，不能用 head 最新值
// 代替来源；提交链真实性仍由后端校验，字段自洽不等于授权。
func buildRecoverySummaryIndex(record RunRecord) (*recoverySummaryIndex, error) {
	index := &recoverySummaryIndex{retries: make(map[retrySummaryGroup]retryPrefixSummaryPayload), messages: make(map[messageSummaryGroup]messagePrefixSummaryPayload), managed: make(map[string]bool), reserved: make(map[string]bool)}
	if !hasAnySemanticEvidence(record) {
		return index, nil
	}
	if !hasCompleteSemanticEvidence(record) {
		return nil, validateSemanticEvidence(record)
	}
	if err := ValidateSemanticEvidence(record); err != nil {
		return nil, err
	}
	applied := func(summary HistorySummary, revision uint64) bool {
		return revision != 0 && summary.SourceRevision != math.MaxUint64 && revision == summary.SourceRevision+1 && revision <= record.Revision && commitIndexPosition(record.CommitIndex, revision) >= 0
	}
	for _, summary := range record.HistorySummaries {
		payload, err := defaultHistorySummaryRegistry.decode(summary)
		if err != nil {
			return nil, err
		}
		if err := payload.validateRecovery(); err != nil {
			return nil, err
		}
		if payload.applicationRevision() == 0 {
			continue
		}
		if !applied(summary, payload.applicationRevision()) {
			return nil, fmt.Errorf("%w: summary has no committed application position", ErrInvalidRecord)
		}
		if err := payload.restore(record, index); err != nil {
			return nil, err
		}
	}
	// 当前保留对象不能与已删除前缀重叠，也不能把剩余 Retry 预算改成另一档。
	for _, attempt := range record.Attempts {
		if p, ok := index.retries[retrySummaryGroup{attempt.Parent, attempt.NodeID, attempt.Boundary}]; ok && (attempt.Index <= p.ThroughIndex || attempt.Limit != p.Limit) {
			return nil, fmt.Errorf("%w: retained retry contradicts compressed prefix", ErrInvalidRecord)
		}
	}
	confirmedIDs := make(map[string][]string)
	for messageID, message := range record.Messages {
		if index.reserved[messageID] {
			return nil, fmt.Errorf("%w: pruned retry identity reused by a message", ErrInvalidRecord)
		}
		key := messageSummaryGroup{message.Scope, message.Parent, message.NodeID, streamKey(message.Input)}
		if p, ok := index.messages[key]; ok && message.Sequence <= p.ThroughSequence {
			return nil, fmt.Errorf("%w: retained message contradicts compressed prefix", ErrInvalidRecord)
		}
		if message.State == AttemptConfirmed {
			key := managedKey(message.Scope, message.Parent, message.NodeID)
			confirmedIDs[key] = append(confirmedIDs[key], messageID)
		}
	}
	managedKeys := make(map[string]bool)
	for _, prefix := range index.messages {
		key := managedKey(prefix.ProcessingScope, prefix.ProcessingAttempt, prefix.NodeID)
		managedKeys[key] = true
	}
	// 每个 Managed 至少要有一份仍解释当前状态的摘要：它的生成时确认集合
	// 之外出现后续确认，或当前值仍等于该摘要绑定值。其他输入组的旧摘要
	// 可继续维持前沿，不要求其历史 State 与当前 State 永远相等。
	for _, p := range index.messages {
		key := managedKey(p.ProcessingScope, p.ProcessingAttempt, p.NodeID)
		later := false
		for _, id := range confirmedIDs[key] {
			position := sort.SearchStrings(p.ConfirmationIDs, id)
			if position == len(p.ConfirmationIDs) || p.ConfirmationIDs[position] != id {
				later = true
				break
			}
		}
		if later || compressionManagedDigest(record.Managed[key]) == p.ManagedDigest {
			index.managed[key] = true
		}
	}
	for key := range managedKeys {
		if !index.managed[key] {
			return nil, fmt.Errorf("%w: compressed managed state changed without a later confirmation", ErrInvalidRecord)
		}
	}
	return index, nil
}

// checkOrdinarySummaryOperation 将已应用摘要保留给专门压缩协议。
// 未注册 Kind 只允许通过底层记录编码保存；修改入口不能把 opaque 证据
// 当成未应用候选，更不能因此绕过未知必需能力的拒绝。
func checkOrdinarySummaryOperation(op putHistorySummary) error {
	check := func(summary HistorySummary) error {
		payload, err := defaultHistorySummaryRegistry.decode(summary)
		if err != nil {
			return err
		}
		if err := payload.validateRecovery(); err != nil {
			return err
		}
		if payload.applicationRevision() != 0 {
			return fmt.Errorf("%w: applied summary requires a dedicated compression transaction", ErrConflict)
		}
		return nil
	}
	if !op.absent {
		if err := check(op.expected); err != nil {
			return err
		}
	}
	if !op.remove {
		return check(op.value)
	}
	return nil
}

func (s *runState) recoverySummaries() (*recoverySummaryIndex, error) {
	if s.summaryIndex != nil {
		return s.summaryIndex, nil
	}
	index, err := buildRecoverySummaryIndex(s.record)
	if err != nil {
		return nil, err
	}
	s.summaryIndex = index
	return index, nil
}
