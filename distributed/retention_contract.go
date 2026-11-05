package distributed

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"time"
)

var (
	// ErrConflict 表示 retention 计划或权威候选发生冲突。
	ErrConflict = errors.New("distributed: retention conflict")
	// ErrPinned 表示候选受到永久保护，不能删除。
	ErrPinned = errors.New("distributed: retention candidate pinned")
	// ErrHistoryExpired 表示所需历史已越过保留边界。
	ErrHistoryExpired = errors.New("distributed: retention history expired")
)

const (
	maxRetentionCandidates = 1000000
	maxRetentionRuns       = 1000000
	maxRetentionRows       = uint64(1 << 50)
	maxRetentionBytes      = uint64(1 << 60)
)

// RetentionCursor 明确区分首页（Set=false）与已提供的分页游标；空游标不能冒充首页。
type RetentionCursor struct {
	Set   bool   `json:"set"`
	Value string `json:"value"`
}

func (c RetentionCursor) valid() bool {
	if !c.Set {
		return c.Value == ""
	}
	return text(c.Value)
}

// RetentionCandidate 是稳定排序和执行绑定所使用的候选身份及摘要，不是删除命令。
type RetentionCandidate struct {
	Namespace string                     `json:"namespace"`
	Kind      string                     `json:"kind"`
	ID        string                     `json:"id"`
	Revision  uint64                     `json:"revision"`
	Rows      uint64                     `json:"rows"`
	Bytes     uint64                     `json:"bytes"`
	Protected []RetentionProtectedReason `json:"protected,omitempty"`
}

// RetentionProtectedReason 表示候选受保护，受保护候选不可删除。
type RetentionProtectedReason string

const (
	RetentionProtectedPin             RetentionProtectedReason = "pin"
	RetentionProtectedBuildPin        RetentionProtectedReason = "build_pin"
	RetentionProtectedGuard           RetentionProtectedReason = "guard"
	RetentionProtectedTombstone       RetentionProtectedReason = "tombstone"
	RetentionProtectedHeadChanged     RetentionProtectedReason = "head_changed"
	RetentionProtectedPolicyChanged   RetentionProtectedReason = "policy_changed"
	RetentionProtectedHistoryRetained RetentionProtectedReason = "history_retained"
)

func (r RetentionProtectedReason) valid() bool {
	switch r {
	case RetentionProtectedPin, RetentionProtectedBuildPin, RetentionProtectedGuard, RetentionProtectedTombstone, RetentionProtectedHeadChanged, RetentionProtectedPolicyChanged, RetentionProtectedHistoryRetained:
		return true
	}
	return false
}

func retentionCandidateLess(a, b RetentionCandidate) bool {
	if a.Namespace != b.Namespace {
		return a.Namespace < b.Namespace
	}
	if a.Kind != b.Kind {
		return a.Kind < b.Kind
	}
	if a.ID != b.ID {
		return a.ID < b.ID
	}
	return a.Revision < b.Revision
}
func cloneRetentionCandidates(in []RetentionCandidate) []RetentionCandidate {
	out := make([]RetentionCandidate, len(in))
	for i, c := range in {
		out[i] = c
		out[i].Protected = append([]RetentionProtectedReason(nil), c.Protected...)
	}
	return out
}
func retentionCandidateKey(c RetentionCandidate) string {
	return c.Namespace + "\x1f" + c.Kind + "\x1f" + c.ID + "\x1f" + strconv.FormatUint(c.Revision, 10)
}

// RetentionCandidateSummary 是 dry-run 对候选的只读摘要。
type RetentionCandidateSummary struct {
	Candidates   []RetentionCandidate                  `json:"candidates"`
	Protected    map[string][]RetentionProtectedReason `json:"protected"`
	LogicalRows  uint64                                `json:"logical_rows"`
	LogicalBytes uint64                                `json:"logical_bytes"`
}

// RetentionDryRun 请求只读预览，完整绑定命名空间、策略版本、策略和观察窗口。
type RetentionDryRun struct {
	Namespace       string          `json:"namespace"`
	PolicyVersion   uint64          `json:"policy_version"`
	Policy          RetentionPolicy `json:"policy"`
	ObservedFrom    time.Time       `json:"observed_from"`
	ObservedThrough time.Time       `json:"observed_through"`
	Cursor          RetentionCursor `json:"cursor"`
	Limit           uint64          `json:"limit"`
}

// RetentionDryRunResult 是不可直接执行的预览结果。
type RetentionDryRunResult struct {
	DryRunID        string                    `json:"dry_run_id"`
	Namespace       string                    `json:"namespace"`
	PolicyVersion   uint64                    `json:"policy_version"`
	Policy          RetentionPolicy           `json:"policy"`
	PolicyDigest    string                    `json:"policy_digest"`
	ObservedFrom    time.Time                 `json:"observed_from"`
	ObservedThrough time.Time                 `json:"observed_through"`
	Summary         RetentionCandidateSummary `json:"summary"`
	Cursor          RetentionCursor           `json:"cursor"`
	NextCursor      RetentionCursor           `json:"next_cursor"`
	ReceiptDigest   string                    `json:"receipt_digest"`
}

// RetentionExecute 请求执行绑定的 dry-run 身份、策略摘要、游标和有界预算；本包不执行删除。
type RetentionExecute struct {
	DryRunID      string          `json:"dry_run_id"`
	Namespace     string          `json:"namespace"`
	PolicyVersion uint64          `json:"policy_version"`
	PolicyDigest  string          `json:"policy_digest"`
	Cursor        RetentionCursor `json:"cursor"`
	MaxRuns       uint64          `json:"max_runs"`
	MaxRows       uint64          `json:"max_rows"`
	MaxBytes      uint64          `json:"max_bytes"`
}

// RetentionExecuteResult 是后端执行后的契约结果，不代表本包执行了任何删除。
type RetentionExecuteResult struct {
	ReceiptDigest string          `json:"receipt_digest"`
	PlanDigest    string          `json:"plan_digest"`
	DryRunID      string          `json:"dry_run_id"`
	Namespace     string          `json:"namespace"`
	PolicyVersion uint64          `json:"policy_version"`
	Cursor        RetentionCursor `json:"cursor"`
	Runs          uint64          `json:"runs"`
	Rows          uint64          `json:"rows"`
	Bytes         uint64          `json:"bytes"`
}

// RetentionReceipt 是永久边界所需的只读摘要；receipt/guards 不因策略过期而成为删除授权。
type RetentionReceipt struct {
	DryRunID      string               `json:"dry_run_id"`
	PlanDigest    string               `json:"plan_digest"`
	Namespace     string               `json:"namespace"`
	PolicyVersion uint64               `json:"policy_version"`
	PolicyDigest  string               `json:"policy_digest"`
	Cursor        RetentionCursor      `json:"cursor"`
	Candidates    []RetentionCandidate `json:"candidates"`
	Guards        []string             `json:"guards"`
}

// RetentionPreviewer 只生成预览，不执行删除。
type RetentionPreviewer interface {
	PreviewRetention(context.Context, RetentionDryRun) (RetentionDryRunResult, error)
}

// RetentionExecutor 仅接收显式绑定的执行请求；后端必须逐候选重新锁定、采样和复验。
type RetentionExecutor interface {
	ExecuteRetention(context.Context, RetentionExecute) (RetentionExecuteResult, error)
}

func retentionJSON(v any) ([]byte, error) { return json.Marshal(v) }
func retentionDigest(v any) (string, error) {
	b, e := retentionJSON(v)
	if e != nil {
		return "", e
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}
func retentionPolicyDigest(p RetentionPolicy) (string, error) {
	q, e := p.Validate()
	if e != nil {
		return "", e
	}
	// 使用按 key 排序的对象，而不是依赖结构体字段顺序；PostgreSQL jsonb
	// 的文本摘要也按同一稳定表示计算，避免跨后端摘要不一致。
	return retentionDigest(map[string]any{
		"Enabled":            q.Enabled,
		"EventTTL":           q.EventTTL,
		"ReceiptTTL":         q.ReceiptTTL,
		"ReplayHistoryTTL":   q.ReplayHistoryTTL,
		"TerminalPayloadTTL": q.TerminalPayloadTTL,
	})
}

func (q RetentionDryRun) Validate() error {
	if !text(q.Namespace) || q.PolicyVersion == 0 || q.ObservedFrom.IsZero() || q.ObservedThrough.IsZero() || q.ObservedThrough.Before(q.ObservedFrom) || !q.Cursor.valid() || q.Limit == 0 || q.Limit > maxRetentionCandidates {
		return ErrInvalid
	}
	if _, e := q.Policy.Validate(); e != nil {
		return e
	}
	return nil
}
func (r RetentionDryRunResult) Validate(q RetentionDryRun) error {
	if q.Validate() != nil || !text(r.DryRunID) || r.Namespace != q.Namespace || r.PolicyVersion != q.PolicyVersion || r.ObservedFrom.Equal(q.ObservedFrom) == false || r.ObservedThrough.Equal(q.ObservedThrough) == false || r.Cursor.Set != q.Cursor.Set || r.Cursor.Value != q.Cursor.Value || !r.Cursor.valid() || !r.NextCursor.valid() || !fingerprint(r.PolicyDigest) || !fingerprint(r.ReceiptDigest) {
		return ErrInvalid
	}
	qd, e := retentionPolicyDigest(q.Policy)
	if e != nil || qd != r.PolicyDigest {
		return ErrInvalid
	}
	if e := r.Summary.validate(); e != nil {
		return e
	}
	for _, c := range r.Summary.Candidates {
		if c.Namespace != r.Namespace {
			return ErrInvalid
		}
	}
	return nil
}
func (s RetentionCandidateSummary) validate() error {
	if s.Candidates == nil || len(s.Candidates) > maxRetentionCandidates {
		return ErrInvalid
	}
	candidateKeys := make(map[string]bool, len(s.Candidates))
	candidateProtected := make(map[string][]RetentionProtectedReason, len(s.Candidates))
	for _, c := range s.Candidates {
		key := retentionCandidateKey(c)
		candidateKeys[key] = true
		candidateProtected[key] = append([]RetentionProtectedReason(nil), c.Protected...)
	}
	for key, reasons := range s.Protected {
		if !candidateKeys[key] {
			return ErrInvalid
		}
		for _, reason := range reasons {
			if !reason.valid() {
				return ErrInvalid
			}
		}
		if !sameRetentionReasons(reasons, candidateProtected[key]) {
			return ErrInvalid
		}
	}
	for key, reasons := range candidateProtected {
		if len(reasons) > 0 && !sameRetentionReasons(s.Protected[key], reasons) {
			return ErrInvalid
		}
	}
	sorted := cloneRetentionCandidates(s.Candidates)
	sort.Slice(sorted, func(i, j int) bool { return retentionCandidateLess(sorted[i], sorted[j]) })
	seen := map[string]bool{}
	var rows, bytes uint64
	for _, c := range sorted {
		if !text(c.Namespace) || !text(c.Kind) || !text(c.ID) || c.Revision == 0 || c.Bytes > maxRetentionBytes || c.Rows > maxRetentionRows {
			return ErrInvalid
		}
		if seen[retentionCandidateKey(c)] {
			return ErrInvalid
		}
		seen[retentionCandidateKey(c)] = true
		for _, p := range c.Protected {
			if !p.valid() {
				return ErrInvalid
			}
		}
		if ^uint64(0)-rows < c.Rows || ^uint64(0)-bytes < c.Bytes {
			return ErrInvalid
		}
		rows += c.Rows
		bytes += c.Bytes
	}
	if rows != s.LogicalRows || bytes != s.LogicalBytes {
		return ErrInvalid
	}
	return nil
}
func (q RetentionExecute) Validate() error {
	if !text(q.DryRunID) || !text(q.Namespace) || q.PolicyVersion == 0 || !fingerprint(q.PolicyDigest) || !q.Cursor.valid() || q.MaxRuns == 0 || q.MaxRuns > maxRetentionRuns || q.MaxRows == 0 || q.MaxRows > maxRetentionRows || q.MaxBytes == 0 || q.MaxBytes > maxRetentionBytes {
		return ErrInvalid
	}
	return nil
}
func (r RetentionExecuteResult) Validate(q RetentionExecute) error {
	if q.Validate() != nil || r.DryRunID != q.DryRunID || r.Namespace != q.Namespace || r.PolicyVersion != q.PolicyVersion || r.Cursor.Set != q.Cursor.Set || r.Cursor.Value != q.Cursor.Value || !fingerprint(r.ReceiptDigest) || !fingerprint(r.PlanDigest) || r.Runs > q.MaxRuns || r.Rows > q.MaxRows || r.Bytes > q.MaxBytes {
		return ErrInvalid
	}
	return nil
}
func (r RetentionReceipt) Validate() error {
	if !text(r.DryRunID) || !text(r.Namespace) || r.PolicyVersion == 0 || !fingerprint(r.PlanDigest) || !fingerprint(r.PolicyDigest) || !r.Cursor.valid() || r.Candidates == nil || r.Guards == nil {
		return ErrInvalid
	}
	summary := r.CandidatesSummary()
	if e := summary.validate(); e != nil {
		return e
	}
	for _, c := range summary.Candidates {
		if c.Namespace != r.Namespace {
			return ErrInvalid
		}
	}
	for _, g := range r.Guards {
		if !text(g) {
			return ErrInvalid
		}
	}
	return nil
}
func sameRetentionReasons(a, b []RetentionProtectedReason) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[RetentionProtectedReason]int, len(a))
	for _, reason := range a {
		seen[reason]++
	}
	for _, reason := range b {
		seen[reason]--
	}
	for _, count := range seen {
		if count != 0 {
			return false
		}
	}
	return true
}

func (r RetentionReceipt) CandidatesSummary() RetentionCandidateSummary {
	var rows, bytes uint64
	protected := make(map[string][]RetentionProtectedReason)
	for _, c := range r.Candidates {
		rows += c.Rows
		bytes += c.Bytes
		if len(c.Protected) > 0 {
			protected[retentionCandidateKey(c)] = append([]RetentionProtectedReason(nil), c.Protected...)
		}
	}
	return RetentionCandidateSummary{Candidates: cloneRetentionCandidates(r.Candidates), Protected: protected, LogicalRows: rows, LogicalBytes: bytes}
}
func (r RetentionDryRunResult) Clone() RetentionDryRunResult {
	r.Summary.Candidates = cloneRetentionCandidates(r.Summary.Candidates)
	r.Summary.Protected = cloneRetentionReasons(r.Summary.Protected)
	return r
}
func cloneRetentionReasons(in map[string][]RetentionProtectedReason) map[string][]RetentionProtectedReason {
	if in == nil {
		return nil
	}
	out := make(map[string][]RetentionProtectedReason, len(in))
	for k, v := range in {
		out[k] = append([]RetentionProtectedReason(nil), v...)
	}
	return out
}
