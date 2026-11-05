// 本文件定义历史批次回收的独立能力，相当于后端适配器之间的协议层。
// 它只校验计划和回执，不访问存储、不判断 TTL、不替调用方取得删除权限。
// 物理快照合成、语义历史压缩和终态载荷裁剪各自保持独立责任。
package distributed

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"
)

const (
	// HistoryGCMaxRows 与 HistoryGCMaxBytes 是计划 v1 的单事务资源上限，
	// 不是吞吐保证或保留政策。调用方仍必须显式提供不超过上限的预算。
	// 一个最大 16 MiB 批次可单独回收；大历史由多个计划逐次处理。
	HistoryGCMaxRows  = uint64(1024)
	HistoryGCMaxBytes = uint64(16 << 20)
)

// HistoryGCRequest 请求登记一个 Run 的不可变回收计划。
// PlanID 在 Identity 内永久保留；同 ID 不同请求必须冲突。登记会写计划，
// 不属于只读候选预览，也不授权修改策略。策略必须已显式启用 ReplayHistoryTTL。
// MaxRows/MaxBytes 单位分别为批次行数、实际 wire 字节，零值无效。
type HistoryGCRequest struct {
	Identity      Identity `json:"identity"`
	PlanID        string   `json:"plan_id"`
	PolicyVersion uint64   `json:"policy_version"`
	PolicyDigest  string   `json:"policy_digest"`
	MaxRows       uint64   `json:"max_rows"`
	MaxBytes      uint64   `json:"max_bytes"`
}

// Validate 只检查请求形状；后端仍须在可信 session_user 下鉴权并锁定权威策略。
func (q HistoryGCRequest) Validate() error {
	if !historyGCIdentity(q.Identity) || !historyGCText(q.PlanID) || q.PolicyVersion == 0 || !fingerprint(q.PolicyDigest) || q.MaxRows == 0 || q.MaxRows > HistoryGCMaxRows || q.MaxBytes == 0 || q.MaxBytes > HistoryGCMaxBytes {
		return ErrInvalid
	}
	return nil
}

// Digest 使用固定顺序的字符串数组，避免 JSON 对象字段顺序、uint64 浮点
// 转换和分隔符碰撞。后端必须独立重算，摘要不构成任何访问凭据。
func (q HistoryGCRequest) Digest() string {
	return historyGCDigest("pulse-durable/history-gc/request/v1", q.Identity.Namespace, q.Identity.RunID, q.PlanID, historyGCUint(q.PolicyVersion), q.PolicyDigest, historyGCUint(q.MaxRows), historyGCUint(q.MaxBytes))
}

// HistoryGCPlan 是准备阶段的不可变结果，不是当前数据库状态的替代品。
//
// 首片仅允许删除已被发布快照覆盖的历史前缀，且保留快照所在修订的边界
// batch 和全部后缀。BeforeFloor/AfterFloor 是最低承诺可重放修订；新 floor
// 等于快照修订，不承诺更早的历史仍可使用。执行时须重新检查整个失效前缀
// 的 TTL、所有有效历史 pin/build pin、head/代际、摘要与实际删除行数/字节。
// 不删除 run、首快照、已发布快照、guard、receipt、pin、event 或 lease。
type HistoryGCPlan struct {
	Request            HistoryGCRequest `json:"request"`
	RequestDigest      string           `json:"request_digest"`
	PlanDigest         string           `json:"plan_digest"`
	HeadRevision       uint64           `json:"head_revision"`
	HeadChain          string           `json:"head_chain"`
	SnapshotGeneration uint64           `json:"snapshot_generation"`
	SnapshotRevision   uint64           `json:"snapshot_revision"`
	SnapshotChain      string           `json:"snapshot_chain"`
	SnapshotDigest     string           `json:"snapshot_digest"`
	BeforeFloor        uint64           `json:"before_floor"`
	AfterFloor         uint64           `json:"after_floor"`
	FirstRevision      uint64           `json:"first_revision"`
	ThroughRevision    uint64           `json:"through_revision"`
	PrefixDigest       string           `json:"prefix_digest"`
	Rows               uint64           `json:"rows"`
	Bytes              uint64           `json:"bytes"`
	PreparedAt         time.Time        `json:"prepared_at"`
}

// Digest 重算计划 v1 的完整绑定；PlanDigest 自身不参与摘要。
// PreparedAt 使用 UTC Unix 微秒，避免时区和 PostgreSQL 文本格式差异。
func (p HistoryGCPlan) Digest() string {
	return historyGCDigest("pulse-durable/history-gc/plan/v1", p.RequestDigest,
		historyGCUint(p.HeadRevision), p.HeadChain, historyGCUint(p.SnapshotGeneration),
		historyGCUint(p.SnapshotRevision), p.SnapshotChain, p.SnapshotDigest,
		historyGCUint(p.BeforeFloor), historyGCUint(p.AfterFloor), historyGCUint(p.FirstRevision),
		historyGCUint(p.ThroughRevision), p.PrefixDigest, historyGCUint(p.Rows), historyGCUint(p.Bytes),
		strconv.FormatInt(p.PreparedAt.UnixMicro(), 10))
}

// Validate 绑定完整请求并检查范围，不证明计划权威性或仍满足删除前提。
// 后端收到合法计划也不能绕过锁后策略/pin/历史复验。
func (p HistoryGCPlan) Validate(q HistoryGCRequest) error {
	if q.Validate() != nil || p.Request != q || p.RequestDigest != q.Digest() || !fingerprint(p.PlanDigest) || p.PlanDigest != p.Digest() || !fingerprint(p.HeadChain) || !fingerprint(p.SnapshotChain) || !fingerprint(p.SnapshotDigest) || !fingerprint(p.PrefixDigest) || p.SnapshotGeneration < 2 || p.SnapshotGeneration > 9223372036854775807 || p.SnapshotRevision < 2 || p.HeadRevision < p.SnapshotRevision || p.BeforeFloor == 0 || p.BeforeFloor > p.SnapshotRevision || p.AfterFloor != p.SnapshotRevision || p.FirstRevision < 2 || p.ThroughRevision < p.FirstRevision || p.ThroughRevision >= p.SnapshotRevision || p.Rows == 0 || p.Rows != p.ThroughRevision-p.FirstRevision+1 || p.Rows > q.MaxRows || p.Bytes < p.Rows || p.Bytes > q.MaxBytes || !historyGCTime(p.PreparedAt) {
		return ErrInvalid
	}
	return nil
}

// HistoryGCExecute 只能执行已登记的精确计划，不接受临时替换的候选或预算。
// 并发重复执行必须返回同一永久回执；计划 ID/摘要冲突不可复用。
type HistoryGCExecute struct {
	Identity   Identity `json:"identity"`
	PlanID     string   `json:"plan_id"`
	PlanDigest string   `json:"plan_digest"`
}

func (q HistoryGCExecute) Validate() error {
	if !historyGCIdentity(q.Identity) || !historyGCText(q.PlanID) || !fingerprint(q.PlanDigest) {
		return ErrInvalid
	}
	return nil
}

// HistoryGCReceipt 是已确认回收的永久结果，和实际删除在同一数据库事务提交。
// ReceiptDigest 校验完整性，不授予重复删除、重放或恢复旧身份的权限。
type HistoryGCReceipt struct {
	Identity      Identity  `json:"identity"`
	PlanID        string    `json:"plan_id"`
	PlanDigest    string    `json:"plan_digest"`
	ReceiptDigest string    `json:"receipt_digest"`
	Rows          uint64    `json:"rows"`
	Bytes         uint64    `json:"bytes"`
	HistoryFloor  uint64    `json:"history_floor"`
	OccurredAt    time.Time `json:"occurred_at"`
}

func (r HistoryGCReceipt) Digest() string {
	return historyGCDigest("pulse-durable/history-gc/receipt/v1", r.Identity.Namespace, r.Identity.RunID, r.PlanID,
		r.PlanDigest, historyGCUint(r.Rows), historyGCUint(r.Bytes), historyGCUint(r.HistoryFloor), strconv.FormatInt(r.OccurredAt.UnixMicro(), 10))
}

func (r HistoryGCReceipt) Validate(p HistoryGCPlan) error {
	if p.Validate(p.Request) != nil || r.Identity != p.Request.Identity || r.PlanID != p.Request.PlanID || r.PlanDigest != p.PlanDigest || r.Rows != p.Rows || r.Bytes != p.Bytes || r.HistoryFloor != p.AfterFloor || !historyGCTime(r.OccurredAt) || r.OccurredAt.Before(p.PreparedAt) || !fingerprint(r.ReceiptDigest) || r.ReceiptDigest != r.Digest() {
		return ErrInvalid
	}
	return nil
}

// HistoryGCState 的 absent 只表示本次观察未找到计划，不证明先前操作未提交。
// 超时/断线后应查询同一身份，不能把 absent 转成一个新 PlanID 盲重试。
type HistoryGCState string

const (
	HistoryGCAbsent   HistoryGCState = "absent"
	HistoryGCPrepared HistoryGCState = "prepared"
	HistoryGCExecuted HistoryGCState = "executed"
)

// HistoryGCResult 查询完整的不可变计划与回执；零值不是合法结果。
// 返回指针必须是隔离副本，不能让调用方改写后端权威状态。
type HistoryGCResult struct {
	State   HistoryGCState    `json:"state"`
	Plan    *HistoryGCPlan    `json:"plan"`
	Receipt *HistoryGCReceipt `json:"receipt"`
}

func (r HistoryGCResult) Validate(q HistoryGCExecute) error {
	if q.Validate() != nil {
		return ErrInvalid
	}
	if r.State == HistoryGCAbsent {
		if r.Plan != nil || r.Receipt != nil {
			return ErrInvalid
		}
		return nil
	}
	if r.Plan == nil || r.Plan.Validate(r.Plan.Request) != nil || r.Plan.Request.Identity != q.Identity || r.Plan.Request.PlanID != q.PlanID || r.Plan.PlanDigest != q.PlanDigest {
		return ErrInvalid
	}
	switch r.State {
	case HistoryGCPrepared:
		if r.Receipt != nil {
			return ErrInvalid
		}
	case HistoryGCExecuted:
		if r.Receipt == nil || r.Receipt.Validate(*r.Plan) != nil {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}

// HistoryGCPlanner 显式登记单 Run 的删除计划，不承担候选只读预览职责。
type HistoryGCPlanner interface {
	PrepareHistoryGC(context.Context, HistoryGCRequest) (HistoryGCPlan, error)
}

// HistoryGCExecutor 只执行精确计划，结果未知必须保留原 PlanID/PlanDigest。
type HistoryGCExecutor interface {
	ExecuteHistoryGC(context.Context, HistoryGCExecute) (HistoryGCReceipt, error)
}

// HistoryGCQuery 用调用方在准备前已知的请求摘要核对同一计划。
// 准备响应丢失时尚不知道 PlanDigest，因此查询不依赖执行端摘要。
// RequestDigest 仅用于匹配，不替代 namespace 与可信主体鉴权。
type HistoryGCQuery struct {
	Identity      Identity `json:"identity"`
	PlanID        string   `json:"plan_id"`
	RequestDigest string   `json:"request_digest"`
}

func (q HistoryGCQuery) Validate() error {
	if !historyGCIdentity(q.Identity) || !historyGCText(q.PlanID) || !fingerprint(q.RequestDigest) {
		return ErrInvalid
	}
	return nil
}

// ValidateQuery 检查未知准备结果的查询绑定，absent 仍不是未提交证明。
func (r HistoryGCResult) ValidateQuery(q HistoryGCQuery) error {
	if q.Validate() != nil {
		return ErrInvalid
	}
	if r.State == HistoryGCAbsent {
		if r.Plan != nil || r.Receipt != nil {
			return ErrInvalid
		}
		return nil
	}
	if r.Plan == nil || r.Plan.RequestDigest != q.RequestDigest {
		return ErrInvalid
	}
	return r.Validate(HistoryGCExecute{Identity: q.Identity, PlanID: q.PlanID, PlanDigest: r.Plan.PlanDigest})
}

// HistoryGCReader 核对同一计划是否已执行，不取得业务执行权或删除权限。
type HistoryGCReader interface {
	FindHistoryGC(context.Context, HistoryGCQuery) (HistoryGCResult, error)
}

func historyGCDigest(fields ...string) string {
	b, _ := json.Marshal(fields)
	return hash(b)
}
func historyGCUint(n uint64) string { return strconv.FormatUint(n, 10) }
func historyGCIdentity(i Identity) bool {
	return historyGCText(i.Namespace) && historyGCText(i.RunID)
}
func historyGCText(s string) bool { return text(s) && !strings.ContainsRune(s, 0) }
func historyGCTime(t time.Time) bool {
	return !t.IsZero() && t.Year() >= 1 && t.Year() <= 9999 && t.Equal(t.Truncate(time.Microsecond))
}
