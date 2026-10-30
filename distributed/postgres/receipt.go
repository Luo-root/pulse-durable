package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"time"
	"unicode/utf8"

	d "github.com/Luo-root/pulse-durable/distributed"
)

// FindMutation 仅解析精确的已提交协调器变更。不存在
// 或过期绝不授权重放。覆盖范围由数据库在运行锁内采样，
// 独立于事件保留及调用方时钟。
func (c *Coordinator) FindMutation(ctx context.Context, q d.MutationQuery) (d.MutationQueryResult, error) {
	if q.Validate() != nil {
		return d.MutationQueryResult{}, d.ErrInvalid
	}
	a := args(q.Identity)
	mutate(a, q.Mutation)
	a["operation"] = q.Operation
	a["window_from"] = q.RequestedWindow.From
	a["window_through"] = q.RequestedWindow.Through
	if q.ExpectedEpoch != nil {
		a["expected_epoch"] = *q.ExpectedEpoch
	}
	if q.ExpectedRevision != nil {
		a["expected_revision"] = strconv.FormatUint(*q.ExpectedRevision, 10)
	}
	b, err := c.call(ctx, "receipt", a, false)
	if err != nil {
		return d.MutationQueryResult{}, err
	}
	return decodeMutationResult(b, q)
}

func decodeMutationResult(b []byte, q d.MutationQuery) (d.MutationQueryResult, error) {
	var w struct {
		State   d.MutationQueryState `json:"state"`
		From    time.Time            `json:"coverage_from"`
		Through time.Time            `json:"coverage_through"`
		Receipt *struct {
			Operation string    `json:"operation"`
			Epoch     int64     `json:"epoch"`
			Revision  string    `json:"revision"`
			ID        string    `json:"mutation_id"`
			Digest    string    `json:"mutation_digest"`
			At        time.Time `json:"occurred_at"`
		} `json:"receipt"`
	}
	if !receiptUnicode(b) || strictMaintenanceJSON(b, &w) != nil || !receiptRequiredShape(b, w.State) {
		return d.MutationQueryResult{}, d.ErrInvalid
	}
	out := d.MutationQueryResult{State: w.State, Coverage: d.MutationWindow{From: w.From, Through: w.Through}}
	if v := w.Receipt; v != nil {
		revision, err := strconv.ParseUint(v.Revision, 10, 64)
		if err != nil || strconv.FormatUint(revision, 10) != v.Revision {
			return d.MutationQueryResult{}, d.ErrInvalid
		}
		out.Receipt = &d.MutationReceipt{Identity: q.Identity, Mutation: d.Mutation{ID: v.ID, Digest: v.Digest}, Operation: v.Operation, Epoch: v.Epoch, Revision: revision, OccurredAt: v.At}
	}
	if out.Validate(q) != nil {
		return d.MutationQueryResult{}, d.ErrInvalid
	}
	return out, nil
}

// 必需字段有意按回执专门检查：strictMaintenanceJSON 拒绝
// 重复键、未知键、大小写别名，但不要求字段存在，也不拒绝 null
// 标量值。在接受 Go 零值默认值前先检查这些条件。
func receiptRequiredShape(b []byte, state d.MutationQueryState) bool {
	var obj map[string]json.RawMessage
	if json.Unmarshal(b, &obj) != nil || len(obj) != 4 {
		return false
	}
	for _, key := range []string{"state", "coverage_from", "coverage_through", "receipt"} {
		v, ok := obj[key]
		if !ok || (key != "receipt" && bytes.Equal(bytes.TrimSpace(v), []byte("null"))) {
			return false
		}
	}
	if state != d.Found {
		return bytes.Equal(bytes.TrimSpace(obj["receipt"]), []byte("null"))
	}
	var receipt map[string]json.RawMessage
	if json.Unmarshal(obj["receipt"], &receipt) != nil || len(receipt) != 6 {
		return false
	}
	for _, key := range []string{"operation", "epoch", "revision", "mutation_id", "mutation_digest", "occurred_at"} {
		v, ok := receipt[key]
		if !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
			return false
		}
	}
	return true
}

// encoding/json 会修复畸形 UTF-8 和孤立 UTF-16 代理项。回执
// 必须拒绝两者，而非比较修复后的身份字符串。
func receiptUnicode(b []byte) bool {
	if !utf8.Valid(b) {
		return false
	}
	for i := 0; i < len(b); i++ {
		if b[i] != '\\' {
			continue
		}
		i++
		if i >= len(b) {
			return false
		}
		if b[i] != 'u' {
			continue
		}
		if i+4 >= len(b) {
			return false
		}
		v, err := strconv.ParseUint(string(b[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if v >= 0xdc00 && v <= 0xdfff {
			return false
		}
		if v < 0xd800 || v > 0xdbff {
			continue
		}
		if i+6 >= len(b) || b[i+1] != '\\' || b[i+2] != 'u' {
			return false
		}
		low, err := strconv.ParseUint(string(b[i+3:i+7]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return false
		}
		i += 6
	}
	return true
}
