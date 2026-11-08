// 本文件是 Retry 前缀摘要的独立 handler 实现。它解释失败次数与稳定身份，
// 为通用规划/恢复接口提供行为；不操作 Store，不决定全局保留策略。
package durable

import "fmt"

const RetryPrefixSummaryKind = "retry_prefix"

type retryPrefixSummaryPayload struct {
	AppliedRevision  uint64 `json:"applied_revision"`
	LastPrunedID     string `json:"last_pruned_id"`
	Parent           string `json:"parent"`
	NodeID           string `json:"node_id"`
	Boundary         string `json:"boundary"`
	ThroughIndex     int    `json:"through_index"`
	Limit            int    `json:"limit"`
	LastPrunedDigest string `json:"last_pruned_digest"`
}

type retryPrefixSummaryHandler struct{}

func (retryPrefixSummaryHandler) decode(data []byte) (decodedHistorySummary, error) {
	var payload retryPrefixSummaryPayload
	if err := unmarshalRecord(data, &payload); err != nil {
		return nil, fmt.Errorf("%w: invalid retry summary payload", ErrInvalidRecord)
	}
	return payload, nil
}

func (p retryPrefixSummaryPayload) applicationRevision() uint64 { return p.AppliedRevision }

func (p retryPrefixSummaryPayload) withApplication(revision uint64) ([]byte, error) {
	p.AppliedRevision = revision
	encoded := raw(p)
	if len(encoded) == 0 {
		return nil, fmt.Errorf("%w: retry summary payload cannot be encoded", ErrInvalidRecord)
	}
	return encoded, nil
}

// 候选可用只读合成身份，恢复必须检查实际持久身份；这个检查只在恢复或
// 普通修改时调用，不把它提前成只读规划入口的隐式新要求。
func (p retryPrefixSummaryPayload) validateRecovery() error {
	if !validRetryRecoveryPayload(p) {
		return fmt.Errorf("%w: invalid retry recovery summary", ErrInvalidRecord)
	}
	return nil
}

func validRetryRecoveryPayload(p retryPrefixSummaryPayload) bool {
	return validIdentity(p.Parent) && validIdentity(p.LastPrunedID) && validSemanticText(p.NodeID) && validSemanticText(p.Boundary) && p.ThroughIndex >= 1 && p.Limit >= p.ThroughIndex && validDigest(p.LastPrunedDigest)
}

func (p retryPrefixSummaryPayload) candidates(record RunRecord, index *compressionCandidateIndex) ([]CompressionObjectRef, error) {
	if p.AppliedRevision != 0 || !validSemanticText(p.Parent) || !validSemanticText(p.NodeID) || !validSemanticText(p.Boundary) || !validDigest(p.LastPrunedDigest) || !validIdentity(p.LastPrunedID) {
		return nil, fmt.Errorf("%w: invalid retry prefix summary payload", ErrInvalid)
	}
	key := retrySummaryGroup{p.Parent, p.NodeID, p.Boundary}
	if index.seenRetries[key] {
		return nil, fmt.Errorf("%w: overlapping retry summary group", ErrConflict)
	}
	index.seenRetries[key] = true
	group := index.retries[key]
	derived, err := retryPrefixForCompression(record, p, group)
	if err != nil {
		return nil, err
	}
	if derived.LastPrunedDigest != p.LastPrunedDigest {
		return nil, fmt.Errorf("%w: retry prefix summary does not match record", ErrConflict)
	}
	rows := make([]CompressionObjectRef, 0, p.ThroughIndex)
	lastID := ""
	for id, attempt := range group {
		if attempt.Parent == p.Parent && attempt.NodeID == p.NodeID && attempt.Boundary == p.Boundary && attempt.Index <= p.ThroughIndex {
			if index.refs[id] {
				return nil, fmt.Errorf("%w: retry attempt remains in reference closure", ErrConflict)
			}
			rows = append(rows, CompressionObjectRef{Kind: "attempt", ID: id})
			if attempt.Index == p.ThroughIndex {
				lastID = id
			}
		}
	}
	if p.LastPrunedID != lastID {
		return nil, fmt.Errorf("%w: retry prefix last identity changed", ErrConflict)
	}
	return rows, nil
}

// restore 只在公共入口已验证提交位置与 Payload 后调用。它登记不可复用的
// 最后删除身份与 Retry 高水位；任何碰撞都在执行任何业务节点之前拒绝。
func (p retryPrefixSummaryPayload) restore(record RunRecord, index *recoverySummaryIndex) error {
	parent, exists := record.Attempts[p.Parent]
	if !exists || parent.NodeID != p.NodeID {
		return fmt.Errorf("%w: retry summary parent changed", ErrInvalidRecord)
	}
	if _, exists := record.Attempts[p.LastPrunedID]; exists {
		return fmt.Errorf("%w: pruned retry identity reappeared", ErrInvalidRecord)
	}
	key := retrySummaryGroup{p.Parent, p.NodeID, p.Boundary}
	if _, duplicate := index.retries[key]; duplicate {
		return fmt.Errorf("%w: duplicate retry recovery summary", ErrInvalidRecord)
	}
	_, scopeCollision := record.Scopes[p.LastPrunedID]
	if index.reserved[p.LastPrunedID] || p.LastPrunedID == record.RootScope || scopeCollision {
		return fmt.Errorf("%w: pruned retry identity collides with retained evidence", ErrInvalidRecord)
	}
	index.reserved[p.LastPrunedID] = true
	index.retries[key] = p
	return nil
}
