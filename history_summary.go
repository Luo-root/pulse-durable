package durable

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
)

var (
	// ErrInvalid 表示摘要请求或记录违反契约。
	ErrInvalid = errors.New("durable: invalid history summary")
	// ErrUnknown 表示摘要引用了未知的持久身份或类型。
	ErrUnknown = errors.New("durable: unknown history summary type")
)

// RetrySummary 是 Retry 失败前缀的只读契约摘要。
type RetrySummary struct {
	Parent             string
	NodeID             string
	Boundary           string
	PrunedThroughIndex int
	Limit              int
	FailedCount        int
	LastPrunedDigest   string
	NextIndex          int
	IdentityDigest     string
	Revision           uint64
}

// MessageSummary 是已结算消息前缀的只读契约摘要。
type MessageSummary struct {
	ProcessingScope        string
	ProcessingAttempt      string
	NodeID                 string
	Input                  StreamIDRecord
	SettledThroughSequence uint64
	FinalizedBudgetDigest  string
	IdentityDigest         string
	Revision               uint64
}

// RetryPrefix 返回仅由 index=1 起连续 AttemptFailed 记录组成的前缀。
// 它只读记录，不执行 Retry，不删除历史，不改变 NextIndex，也不授予 GC、Pin 或 Replay 权限。
func (r RunRecord) RetryPrefix(parent, node, boundary string) (RetrySummary, error) {
	if err := validateSummaryRecord(r); err != nil {
		return RetrySummary{}, err
	}
	if parent == "" || node == "" || boundary == "" {
		return RetrySummary{}, fmt.Errorf("%w: retry identity is incomplete", ErrInvalid)
	}
	var rows []AttemptRecord
	for _, a := range r.Attempts {
		if a.Parent == parent && a.NodeID == node && a.Boundary == boundary {
			if a.Index < 1 || a.Limit < 1 || a.Index > a.Limit || !validAttemptState(a.State) {
				return RetrySummary{}, fmt.Errorf("%w: invalid retry record", ErrInvalid)
			}
			rows = append(rows, a)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Index < rows[j].Index })
	if len(rows) == 0 || rows[0].Index != 1 {
		return RetrySummary{}, fmt.Errorf("%w: retry prefix is empty or has a gap", ErrInvalid)
	}
	limit := rows[0].Limit
	failed := 0
	last := ""
	for i, a := range rows {
		if a.State == AttemptActive || a.State == AttemptConfirmed {
			break
		}
		if a.Index != i+1 || a.Limit != limit || a.State != AttemptFailed {
			return RetrySummary{}, fmt.Errorf("%w: retry prefix is not continuous, failed, or limit-consistent", ErrInvalid)
		}
		failed++
		last = summaryDigest("retry", r.RunID, r.Revision, parent, node, boundary, a.Index, a.Limit, string(a.State))
	}
	if failed == 0 {
		return RetrySummary{}, fmt.Errorf("%w: retry prefix has no failed entry", ErrInvalid)
	}
	next := failed + 1
	identity := summaryDigest("retry-identity", r.RunID, r.Revision, parent, node, boundary)
	if !validSummaryDigest(last) || !validSummaryDigest(identity) {
		return RetrySummary{}, fmt.Errorf("%w: generated retry digest is invalid", ErrInvalid)
	}
	return RetrySummary{Parent: parent, NodeID: node, Boundary: boundary, PrunedThroughIndex: failed,
		Limit: limit, FailedCount: failed, LastPrunedDigest: last, NextIndex: next,
		IdentityDigest: identity, Revision: r.Revision}, nil
}

// MessagePrefix 返回同一 processing attempt/input stable stream/group 的连续已结算消息前缀。
// sequence 不得低于 ACK 前沿；结果只表达读取事实，不确认、执行、删除或重置预算。
func (r RunRecord) MessagePrefix(scope, attempt, node string, input StreamIDRecord, sequence uint64) (MessageSummary, error) {
	if err := validateSummaryRecord(r); err != nil {
		return MessageSummary{}, err
	}
	if scope == "" || attempt == "" || node == "" || input.Scope == "" || input.NodeID == "" || input.Key == "" || input.Group == "" || input.Generation == 0 || sequence == 0 {
		return MessageSummary{}, fmt.Errorf("%w: message identity is incomplete", ErrInvalid)
	}
	stream, ok := r.Streams[streamKey(input)]
	if !ok {
		return MessageSummary{}, fmt.Errorf("%w: input stream is unknown", ErrUnknown)
	}
	if sequence <= stream.AckedThrough {
		return MessageSummary{}, fmt.Errorf("%w: message sequence is at or behind ACK frontier", ErrConflict)
	}
	var rows []MessageAttemptRecord
	for _, m := range r.Messages {
		if m.Scope == scope && m.Parent == attempt && m.NodeID == node && m.Input == input {
			if m.Sequence == 0 || m.Index < 1 || m.Limit < 1 || m.Index > m.Limit || !validAttemptState(m.State) {
				return MessageSummary{}, fmt.Errorf("%w: invalid message attempt", ErrInvalid)
			}
			rows = append(rows, m)
		}
	}
	if len(rows) == 0 {
		return MessageSummary{}, fmt.Errorf("%w: message history is unknown", ErrUnknown)
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Sequence < rows[j].Sequence })
	if rows[0].Sequence != sequence {
		return MessageSummary{}, fmt.Errorf("%w: message sequence is not the prefix boundary", ErrConflict)
	}
	for i, m := range rows {
		if m.Sequence != sequence+uint64(i) || m.State != AttemptConfirmed {
			return MessageSummary{}, fmt.Errorf("%w: message prefix is not continuously settled", ErrInvalid)
		}
	}
	for _, pending := range stream.Pending {
		if pending.Sequence >= sequence && pending.Sequence <= rows[len(rows)-1].Sequence {
			return MessageSummary{}, fmt.Errorf("%w: message prefix input remains pending", ErrInvalid)
		}
	}
	last := rows[len(rows)-1]
	budget := summaryDigest("message-budget", r.RunID, r.Revision, scope, attempt, node, input, last.Limit, last.Index, last.Sequence)
	identity := summaryDigest("message-identity", r.RunID, r.Revision, scope, attempt, node, input)
	if !validSummaryDigest(budget) || !validSummaryDigest(identity) {
		return MessageSummary{}, fmt.Errorf("%w: generated message digest is invalid", ErrInvalid)
	}
	return MessageSummary{ProcessingScope: scope, ProcessingAttempt: attempt, NodeID: node, Input: input,
		SettledThroughSequence: last.Sequence,
		FinalizedBudgetDigest:  budget, IdentityDigest: identity, Revision: r.Revision}, nil
}

func validateSummaryRecord(r RunRecord) error {
	if r.Revision == 0 || r.RunID == "" || (r.Format != RecordFormat && r.Format != SemanticRecordFormat) || r.Attempts == nil {
		return fmt.Errorf("%w: invalid record identity or revision", ErrInvalid)
	}
	if err := validateSemanticEvidence(r); err != nil {
		return err
	}
	if r.Messages == nil && r.Streams == nil {
		return fmt.Errorf("%w: missing message history", ErrUnknown)
	}
	return nil
}

func validAttemptState(s AttemptState) bool {
	return s == AttemptActive || s == AttemptFailed || s == AttemptConfirmed
}

func summaryDigest(domain string, values ...any) string {
	b := []byte(domain)
	for _, v := range values {
		b = append(b, 0)
		b = append(b, []byte(fmt.Sprint(v))...)
	}
	d := sha256.Sum256(b)
	return hex.EncodeToString(d[:])
}

func validSummaryDigest(s string) bool {
	if len(s) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
