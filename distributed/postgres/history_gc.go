// 本文件是历史 GC 公共协议的 PostgreSQL 适配层，相当于 Go 接口的传输实现。
// 它负责固定 SQL 调用、规范十进制字符串编解码、完整响应绑定和错误脱敏；
// 不负责策略决策、加锁、删除或重试，这些事务不变量由独立 SQL 入口保证。
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/jackc/pgx/v5/pgconn"
)

var _ d.HistoryGCPlanner = (*Coordinator)(nil)
var _ d.HistoryGCExecutor = (*Coordinator)(nil)
var _ d.HistoryGCReader = (*Coordinator)(nil)

// historyGCError 只暴露公共错误身份，不回传可能包含凭据或业务载荷的 SQL 文本。
func historyGCError(err error, write bool) error {
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		switch pe.Code {
		case "HG001":
			return d.ErrInvalid
		case "HG002":
			return d.ErrConflict
		case "HG003":
			return d.ErrPinned
		case "HG004":
			return d.ErrHistoryExpired
		case "HG005":
			return d.ErrUnknown
		case "HG006":
			return d.ErrNotFound
		}
	}
	return scrub(err, write)
}

// historyGCCall 的三个分支是完整 SQL 白名单；操作名不进入 SQL 拼接。
func (c *Coordinator) historyGCCall(ctx context.Context, op string, a map[string]any, write bool) ([]byte, error) {
	var sql string
	switch op {
	case "prepare":
		sql = "SELECT pulse_durable_distributed.history_gc_prepare($1::jsonb)"
	case "execute":
		sql = "SELECT pulse_durable_distributed.history_gc_execute($1::jsonb)"
	case "find":
		sql = "SELECT pulse_durable_distributed.history_gc_find($1::jsonb)"
	default:
		return nil, d.ErrInvalid
	}
	ctx, cancel, err := bounded(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if c == nil || c.pool == nil {
		return nil, d.ErrInvalid
	}
	b, err := json.Marshal(a)
	if err != nil {
		return nil, d.ErrInvalid
	}
	var out []byte
	err = c.pool.QueryRow(ctx, sql, b).Scan(&out)
	return out, historyGCError(err, write)
}

// 在线整数必须是字符串；不用 float64 中转，以保留完整 uint64 精度。
type historyGCWireRequest struct {
	Identity      d.Identity `json:"identity"`
	PlanID        string     `json:"plan_id"`
	PolicyVersion string     `json:"policy_version"`
	PolicyDigest  string     `json:"policy_digest"`
	MaxRows       string     `json:"max_rows"`
	MaxBytes      string     `json:"max_bytes"`
}
type historyGCWirePlan struct {
	Request            historyGCWireRequest `json:"request"`
	RequestDigest      string               `json:"request_digest"`
	PlanDigest         string               `json:"plan_digest"`
	HeadRevision       string               `json:"head_revision"`
	HeadChain          string               `json:"head_chain"`
	SnapshotGeneration string               `json:"snapshot_generation"`
	SnapshotRevision   string               `json:"snapshot_revision"`
	SnapshotChain      string               `json:"snapshot_chain"`
	SnapshotDigest     string               `json:"snapshot_digest"`
	BeforeFloor        string               `json:"before_floor"`
	AfterFloor         string               `json:"after_floor"`
	FirstRevision      string               `json:"first_revision"`
	ThroughRevision    string               `json:"through_revision"`
	PrefixDigest       string               `json:"prefix_digest"`
	Rows               string               `json:"rows"`
	Bytes              string               `json:"bytes"`
	PreparedAt         time.Time            `json:"prepared_at"`
}
type historyGCWireReceipt struct {
	Identity      d.Identity `json:"identity"`
	PlanID        string     `json:"plan_id"`
	PlanDigest    string     `json:"plan_digest"`
	ReceiptDigest string     `json:"receipt_digest"`
	Rows          string     `json:"rows"`
	Bytes         string     `json:"bytes"`
	HistoryFloor  string     `json:"history_floor"`
	OccurredAt    time.Time  `json:"occurred_at"`
}
type historyGCWireResult struct {
	State   d.HistoryGCState      `json:"state"`
	Plan    *historyGCWirePlan    `json:"plan"`
	Receipt *historyGCWireReceipt `json:"receipt"`
}

// historyGCNumbers 同时执行规范编码检查；任一字段异常都不返回半有效对象。
func historyGCNumbers(fields ...struct {
	s string
	n *uint64
}) bool {
	for _, f := range fields {
		n, ok := positiveDecimal(f.s)
		if !ok {
			return false
		}
		*f.n = n
	}
	return true
}
func (w historyGCWirePlan) decode() (d.HistoryGCPlan, error) {
	p := d.HistoryGCPlan{
		Request:       d.HistoryGCRequest{Identity: w.Request.Identity, PlanID: w.Request.PlanID, PolicyDigest: w.Request.PolicyDigest},
		RequestDigest: w.RequestDigest, PlanDigest: w.PlanDigest, HeadChain: w.HeadChain,
		SnapshotChain: w.SnapshotChain, SnapshotDigest: w.SnapshotDigest, PrefixDigest: w.PrefixDigest, PreparedAt: w.PreparedAt,
	}
	if !historyGCNumbers(
		struct {
			s string
			n *uint64
		}{w.Request.PolicyVersion, &p.Request.PolicyVersion},
		struct {
			s string
			n *uint64
		}{w.Request.MaxRows, &p.Request.MaxRows},
		struct {
			s string
			n *uint64
		}{w.Request.MaxBytes, &p.Request.MaxBytes},
		struct {
			s string
			n *uint64
		}{w.HeadRevision, &p.HeadRevision},
		struct {
			s string
			n *uint64
		}{w.SnapshotGeneration, &p.SnapshotGeneration},
		struct {
			s string
			n *uint64
		}{w.SnapshotRevision, &p.SnapshotRevision},
		struct {
			s string
			n *uint64
		}{w.BeforeFloor, &p.BeforeFloor},
		struct {
			s string
			n *uint64
		}{w.AfterFloor, &p.AfterFloor},
		struct {
			s string
			n *uint64
		}{w.FirstRevision, &p.FirstRevision},
		struct {
			s string
			n *uint64
		}{w.ThroughRevision, &p.ThroughRevision},
		struct {
			s string
			n *uint64
		}{w.Rows, &p.Rows},
		struct {
			s string
			n *uint64
		}{w.Bytes, &p.Bytes},
	) || p.Validate(p.Request) != nil {
		return d.HistoryGCPlan{}, d.ErrUnknown
	}
	return p, nil
}
func (w historyGCWireReceipt) decode(p d.HistoryGCPlan) (d.HistoryGCReceipt, error) {
	r := d.HistoryGCReceipt{Identity: w.Identity, PlanID: w.PlanID, PlanDigest: w.PlanDigest, ReceiptDigest: w.ReceiptDigest, OccurredAt: w.OccurredAt}
	if !historyGCNumbers(struct {
		s string
		n *uint64
	}{w.Rows, &r.Rows}, struct {
		s string
		n *uint64
	}{w.Bytes, &r.Bytes}, struct {
		s string
		n *uint64
	}{w.HistoryFloor, &r.HistoryFloor}) || r.Validate(p) != nil {
		return d.HistoryGCReceipt{}, d.ErrUnknown
	}
	return r, nil
}
func decodeHistoryGCResult(b []byte) (d.HistoryGCResult, error) {
	var w historyGCWireResult
	if strictPinJSON(b, &w) != nil {
		return d.HistoryGCResult{}, d.ErrUnknown
	}
	out := d.HistoryGCResult{State: w.State}
	if w.Plan != nil {
		p, err := w.Plan.decode()
		if err != nil {
			return d.HistoryGCResult{}, err
		}
		out.Plan = &p
	}
	if w.Receipt != nil {
		if out.Plan == nil {
			return d.HistoryGCResult{}, d.ErrUnknown
		}
		r, err := w.Receipt.decode(*out.Plan)
		if err != nil {
			return d.HistoryGCResult{}, err
		}
		out.Receipt = &r
	}
	return out, nil
}

// PrepareHistoryGC 登记精确请求，不把预览当作删除授权。请求在调用前校验，
// 响应必须绑定完整请求及摘要；提交可能成功但响应丢失/损坏时返回 ErrUnknown，
// 调用方应以同一 RequestDigest 查询，不生成新 PlanID 自动重试。
func (c *Coordinator) PrepareHistoryGC(ctx context.Context, q d.HistoryGCRequest) (d.HistoryGCPlan, error) {
	if err := q.Validate(); err != nil {
		return d.HistoryGCPlan{}, err
	}
	a := args(q.Identity)
	a["plan_id"] = q.PlanID
	a["policy_version"] = strconv.FormatUint(q.PolicyVersion, 10)
	a["policy_digest"] = q.PolicyDigest
	a["max_rows"] = strconv.FormatUint(q.MaxRows, 10)
	a["max_bytes"] = strconv.FormatUint(q.MaxBytes, 10)
	a["request_digest"] = q.Digest()
	b, err := c.historyGCCall(ctx, "prepare", a, true)
	if err != nil {
		return d.HistoryGCPlan{}, err
	}
	var w historyGCWirePlan
	if strictPinJSON(b, &w) != nil {
		return d.HistoryGCPlan{}, d.ErrUnknown
	}
	p, err := w.decode()
	if err != nil || p.Validate(q) != nil {
		return d.HistoryGCPlan{}, d.ErrUnknown
	}
	return p, nil
}

// ExecuteHistoryGC 只发送登记计划的身份与摘要，不接受替换预算或 worker token。
// 只有 executed 状态及完整计划/回执均通过输入绑定时才返回成功；通信或
// 响应不确定性保持 ErrUnknown，不在适配器内重试或推断删除未提交。
func (c *Coordinator) ExecuteHistoryGC(ctx context.Context, q d.HistoryGCExecute) (d.HistoryGCReceipt, error) {
	if err := q.Validate(); err != nil {
		return d.HistoryGCReceipt{}, err
	}
	a := args(q.Identity)
	a["plan_id"] = q.PlanID
	a["plan_digest"] = q.PlanDigest
	b, err := c.historyGCCall(ctx, "execute", a, true)
	if err != nil {
		return d.HistoryGCReceipt{}, err
	}
	out, err := decodeHistoryGCResult(b)
	if err != nil || out.State != d.HistoryGCExecuted || out.Validate(q) != nil || out.Receipt == nil {
		return d.HistoryGCReceipt{}, d.ErrUnknown
	}
	return *out.Receipt, nil
}

// FindHistoryGC 使用准备前已知的 RequestDigest 恢复观察结果；absent 仅代表
// 当前未观察到计划，不证明未提交、更不授权换 ID 删除。返回值均为本次解码
// 的隔离对象；异常响应返回 ErrUnknown，查询错误按只读语义脱敏传播。
func (c *Coordinator) FindHistoryGC(ctx context.Context, q d.HistoryGCQuery) (d.HistoryGCResult, error) {
	if err := q.Validate(); err != nil {
		return d.HistoryGCResult{}, err
	}
	a := args(q.Identity)
	a["plan_id"] = q.PlanID
	a["request_digest"] = q.RequestDigest
	b, err := c.historyGCCall(ctx, "find", a, false)
	if err != nil {
		return d.HistoryGCResult{}, err
	}
	out, err := decodeHistoryGCResult(b)
	if err != nil || out.ValidateQuery(q) != nil {
		return d.HistoryGCResult{}, d.ErrUnknown
	}
	return out, nil
}
