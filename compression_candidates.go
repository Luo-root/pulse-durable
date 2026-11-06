// 本文件解析两种已知摘要并独立核对候选事实。
// 候选只用于只读规划；来源、ACK、预算和引用闭包任一不闭合都失败，不推断可删。
package durable

import (
	"fmt"
	"slices"
)

const (
	RetryPrefixSummaryKind   = "retry_prefix"
	MessagePrefixSummaryKind = "message_prefix"
)

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

func candidateObjectsFromSummary(record RunRecord, summary HistorySummary) ([]CompressionObjectRef, error) {
	return candidateObjectsFromVerifiedSource(record, summary, indexCompressionCandidates(record))
}

func candidateObjectsFromVerifiedSource(record RunRecord, summary HistorySummary, index *compressionCandidateIndex) ([]CompressionObjectRef, error) {
	if err := summary.validate(); err != nil {
		return nil, err
	}
	if summary.SourceRevision != record.Revision || summary.SourceDigest != index.sourceDigest {
		return nil, fmt.Errorf("%w: compression summary source changed", ErrConflict)
	}
	switch summary.Kind {
	case RetryPrefixSummaryKind:
		var payload retryPrefixSummaryPayload
		if err := unmarshalRecord(summary.Payload, &payload); err != nil || payload.AppliedRevision != 0 || !validSemanticText(payload.Parent) || !validSemanticText(payload.NodeID) || !validSemanticText(payload.Boundary) || !validDigest(payload.LastPrunedDigest) {
			return nil, fmt.Errorf("%w: invalid retry prefix summary payload", ErrInvalid)
		}
		key := retrySummaryGroup{payload.Parent, payload.NodeID, payload.Boundary}
		if index.seenRetries[key] {
			return nil, fmt.Errorf("%w: overlapping retry summary group", ErrConflict)
		}
		index.seenRetries[key] = true
		group := index.retries[key]
		derived, err := retryPrefixForCompression(record, payload, group)
		if err != nil {
			return nil, err
		}
		if derived.LastPrunedDigest != payload.LastPrunedDigest {
			return nil, fmt.Errorf("%w: retry prefix summary does not match record", ErrConflict)
		}
		rows := make([]CompressionObjectRef, 0, payload.ThroughIndex)
		lastID := ""
		for id, attempt := range group {
			if attempt.Parent == payload.Parent && attempt.NodeID == payload.NodeID && attempt.Boundary == payload.Boundary && attempt.Index <= payload.ThroughIndex {
				if index.refs[id] {
					return nil, fmt.Errorf("%w: retry attempt remains in reference closure", ErrConflict)
				}
				rows = append(rows, CompressionObjectRef{Kind: "attempt", ID: id})
				if attempt.Index == payload.ThroughIndex {
					lastID = id
				}
			}
		}
		if payload.LastPrunedID != lastID {
			return nil, fmt.Errorf("%w: retry prefix last identity changed", ErrConflict)
		}
		return rows, nil
	case MessagePrefixSummaryKind:
		var payload messagePrefixSummaryPayload
		if err := unmarshalRecord(summary.Payload, &payload); err != nil || payload.AppliedRevision != 0 || !validSemanticText(payload.ProcessingScope) || !validSemanticText(payload.ProcessingAttempt) || !validSemanticText(payload.NodeID) || payload.Input.Generation == 0 || payload.ThroughSequence == 0 || !validDigest(payload.BudgetDigest) {
			return nil, fmt.Errorf("%w: invalid message prefix summary payload", ErrInvalid)
		}
		key := messageSummaryGroup{payload.ProcessingScope, payload.ProcessingAttempt, payload.NodeID, streamKey(payload.Input)}
		if index.seenMessages[key] {
			return nil, fmt.Errorf("%w: overlapping message summary group", ErrConflict)
		}
		index.seenMessages[key] = true
		rows, budget, err := messagePrefixForCompression(record, payload, index)
		if err != nil {
			return nil, err
		}
		if budget != payload.BudgetDigest || payload.ManagedDigest != compressionManagedDigest(record.Managed[managedKey(payload.ProcessingScope, payload.ProcessingAttempt, payload.NodeID)]) || !slices.Equal(payload.ConfirmationIDs, index.confirmations[managedKey(payload.ProcessingScope, payload.ProcessingAttempt, payload.NodeID)]) {
			return nil, fmt.Errorf("%w: message prefix summary does not match record", ErrConflict)
		}
		return rows, nil
	default:
		return nil, fmt.Errorf("%w: unsupported compression summary kind %q", ErrUnsupportedCapability, summary.Kind)
	}
}
