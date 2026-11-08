// 本文件是 Message 前缀摘要的独立 handler 实现。它把 ACK、Managed 与
// 确认集合解释为可恢复的消息前沿；不确认消息、不改变预算或授权历史删除。
package durable

import (
	"fmt"
	"slices"
)

const MessagePrefixSummaryKind = "message_prefix"

type messagePrefixSummaryPayload struct {
	AppliedRevision   uint64         `json:"applied_revision"`
	ProcessingScope   string         `json:"processing_scope"`
	ProcessingAttempt string         `json:"processing_attempt"`
	NodeID            string         `json:"node_id"`
	Input             StreamIDRecord `json:"input"`
	ThroughSequence   uint64         `json:"through_sequence"`
	BudgetDigest      string         `json:"budget_digest"`
	ManagedDigest     string         `json:"managed_digest"`
	ConfirmationIDs   []string       `json:"confirmation_ids"`
}

type messagePrefixSummaryHandler struct{}

func (messagePrefixSummaryHandler) decode(data []byte) (decodedHistorySummary, error) {
	var payload messagePrefixSummaryPayload
	if err := unmarshalRecord(data, &payload); err != nil {
		return nil, fmt.Errorf("%w: invalid message summary payload", ErrInvalidRecord)
	}
	return payload, nil
}

func (p messagePrefixSummaryPayload) applicationRevision() uint64 { return p.AppliedRevision }

func (p messagePrefixSummaryPayload) withApplication(revision uint64) ([]byte, error) {
	p.AppliedRevision = revision
	encoded := raw(p)
	if len(encoded) == 0 {
		return nil, fmt.Errorf("%w: message summary payload cannot be encoded", ErrInvalidRecord)
	}
	return encoded, nil
}

func (p messagePrefixSummaryPayload) validateRecovery() error {
	if !validMessageRecoveryPayload(p) {
		return fmt.Errorf("%w: invalid message recovery summary", ErrInvalidRecord)
	}
	return nil
}

func validMessageRecoveryPayload(p messagePrefixSummaryPayload) bool {
	if !validIdentity(p.ProcessingScope) || !validIdentity(p.ProcessingAttempt) || !validSemanticText(p.NodeID) || !validIdentity(p.Input.Scope) || !validSemanticText(p.Input.NodeID) || !validSemanticText(p.Input.Key) || !validSemanticText(p.Input.Group) || p.Input.Generation == 0 || p.ThroughSequence == 0 || !validDigest(p.BudgetDigest) || !validDigest(p.ManagedDigest) || len(p.ConfirmationIDs) == 0 {
		return false
	}
	for i, id := range p.ConfirmationIDs {
		if !validIdentity(id) || i > 0 && p.ConfirmationIDs[i-1] >= id {
			return false
		}
	}
	return true
}

func (p messagePrefixSummaryPayload) candidates(record RunRecord, index *compressionCandidateIndex) ([]CompressionObjectRef, error) {
	if p.AppliedRevision != 0 || !validSemanticText(p.ProcessingScope) || !validSemanticText(p.ProcessingAttempt) || !validSemanticText(p.NodeID) || p.Input.Generation == 0 || p.ThroughSequence == 0 || !validDigest(p.BudgetDigest) || !validDigest(p.ManagedDigest) || len(p.ConfirmationIDs) == 0 {
		return nil, fmt.Errorf("%w: invalid message prefix summary payload", ErrInvalid)
	}
	key := messageSummaryGroup{p.ProcessingScope, p.ProcessingAttempt, p.NodeID, streamKey(p.Input)}
	if index.seenMessages[key] {
		return nil, fmt.Errorf("%w: overlapping message summary group", ErrConflict)
	}
	index.seenMessages[key] = true
	rows, budget, err := messagePrefixForCompression(record, p, index)
	if err != nil {
		return nil, err
	}
	if budget != p.BudgetDigest || p.ManagedDigest != compressionManagedDigest(record.Managed[managedKey(p.ProcessingScope, p.ProcessingAttempt, p.NodeID)]) || !slices.Equal(p.ConfirmationIDs, index.confirmations[managedKey(p.ProcessingScope, p.ProcessingAttempt, p.NodeID)]) {
		return nil, fmt.Errorf("%w: message prefix summary does not match record", ErrConflict)
	}
	return rows, nil
}

func (p messagePrefixSummaryPayload) restore(record RunRecord, index *recoverySummaryIndex) error {
	stream, exists := record.Streams[streamKey(p.Input)]
	if !exists || p.Input.Generation == 0 || p.Input.Generation > stream.Stream.ID.Generation || p.ThroughSequence > stream.AckedThrough || stream.AckedThrough > stream.LastSequence {
		return fmt.Errorf("%w: message recovery summary exceeds ACK frontier", ErrInvalidRecord)
	}
	managedKey := managedKey(p.ProcessingScope, p.ProcessingAttempt, p.NodeID)
	managed, exists := record.Managed[managedKey]
	if !exists || managed.Scope != p.ProcessingScope || managed.Parent != p.ProcessingAttempt || managed.NodeID != p.NodeID {
		return fmt.Errorf("%w: message summary lost managed state", ErrInvalidRecord)
	}
	key := messageSummaryGroup{p.ProcessingScope, p.ProcessingAttempt, p.NodeID, streamKey(p.Input)}
	if _, duplicate := index.messages[key]; duplicate {
		return fmt.Errorf("%w: duplicate message recovery summary", ErrInvalidRecord)
	}
	index.messages[key] = p
	return nil
}
