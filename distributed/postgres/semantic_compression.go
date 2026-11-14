package postgres

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"strconv"
	"time"

	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/Luo-root/pulse-durable/recordio"
)

// semanticCompressionWireReceipt 是 SQL 回执的传输形状。
// 在线整数一律是规范十进制字符串（不经过 float64），mutation 用小写键；
// epoch/revision 供既有 FindMutation 回执查询交叉核对，不进入业务回执摘要。
type semanticCompressionWireReceipt struct {
	Identity d.Identity `json:"identity"`
	Mutation struct {
		ID     string `json:"id"`
		Digest string `json:"digest"`
	} `json:"mutation"`
	ExpectedRevision  string    `json:"expected_revision"`
	AppliedRevision   string    `json:"applied_revision"`
	ExpectedHeadChain string    `json:"expected_head_chain"`
	AppliedHeadChain  string    `json:"applied_head_chain"`
	DeletedObjects    string    `json:"deleted_objects"`
	SourceBytes       string    `json:"source_bytes"`
	OccurredAt        time.Time `json:"occurred_at"`
	ReceiptDigest     string    `json:"receipt_digest"`
	Epoch             int64     `json:"epoch"`
	Revision          string    `json:"revision"`
}

// decode 把传输形状收敛为公共回执，并逐字段拒绝非规范整数。
func (w semanticCompressionWireReceipt) decode() (d.SemanticCompressionReceipt, error) {
	out := d.SemanticCompressionReceipt{
		Identity:          w.Identity,
		Mutation:          d.Mutation{ID: w.Mutation.ID, Digest: w.Mutation.Digest},
		ExpectedHeadChain: w.ExpectedHeadChain,
		AppliedHeadChain:  w.AppliedHeadChain,
		OccurredAt:        w.OccurredAt,
		ReceiptDigest:     w.ReceiptDigest,
	}
	for _, field := range []struct {
		text string
		dst  *uint64
	}{
		{w.ExpectedRevision, &out.ExpectedRevision},
		{w.AppliedRevision, &out.AppliedRevision},
		{w.DeletedObjects, &out.DeletedObjects},
		{w.SourceBytes, &out.SourceBytes},
	} {
		n, ok := positiveDecimal(field.text)
		if !ok {
			return d.SemanticCompressionReceipt{}, d.ErrUnknown
		}
		*field.dst = n
	}
	// guard/receipt 的 revision 必须与回执 applied_revision 一致。
	if w.Revision != w.AppliedRevision {
		return d.SemanticCompressionReceipt{}, d.ErrUnknown
	}
	return out, nil
}

// semanticCompressionWireResult 是 SQL 入口的严格响应 DTO。
// prepare 只返回锁内读取的权威恢复 image；commit/found 才返回永久回执。
type semanticCompressionWireResult struct {
	State             string                          `json:"state"`
	Image             json.RawMessage                 `json:"image"`
	SourceImageDigest string                          `json:"source_image_digest"`
	Receipt           *semanticCompressionWireReceipt `json:"receipt"`
}

func (c *Coordinator) semanticCompressionCall(ctx context.Context, a map[string]any, write bool) ([]byte, error) {
	return c.storageCall(ctx, "semantic_compress", a, write)
}

// rawJSONPresent 判断一个 RawMessage 是否携带真实值。
// encoding/json 会把 JSON null 原样保留成 4 字节的 "null"，
// 因此不能用 len 判断字段是否缺省。
func rawJSONPresent(b json.RawMessage) bool {
	trimmed := bytes.TrimSpace(b)
	return len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null"))
}

func decodeSemanticCompressionResult(b []byte, q d.SemanticCompressionRequest) (semanticCompressionWireResult, d.SemanticCompressionReceipt, error) {
	var out semanticCompressionWireResult
	if !receiptUnicode(b) || strictMaintenanceJSON(b, &out) != nil {
		return semanticCompressionWireResult{}, d.SemanticCompressionReceipt{}, d.ErrUnknown
	}
	switch out.State {
	case "prepared":
		if !rawJSONPresent(out.Image) || out.Receipt != nil || !fingerprint(out.SourceImageDigest) {
			return semanticCompressionWireResult{}, d.SemanticCompressionReceipt{}, d.ErrUnknown
		}
		return out, d.SemanticCompressionReceipt{}, nil
	case "committed", "found":
		if rawJSONPresent(out.Image) || out.Receipt == nil {
			return semanticCompressionWireResult{}, d.SemanticCompressionReceipt{}, d.ErrUnknown
		}
		receipt, err := out.Receipt.decode()
		if err != nil || receipt.Validate(q) != nil {
			return semanticCompressionWireResult{}, d.SemanticCompressionReceipt{}, d.ErrUnknown
		}
		return out, receipt, nil
	default:
		return semanticCompressionWireResult{}, d.SemanticCompressionReceipt{}, d.ErrUnknown
	}
}

// CompressSemanticHistory 从数据库锁内的权威 image 重建根包压缩批次。
//
// prepare 阶段只登记永久 mutation guard，并返回当前完整恢复 image；它不
// 接收调用方的删除集合。根包据 image 运行 PlanHistoryCompression 和
// BuildHistoryCompressionBatch，commit 阶段只把这份受信批次交给 SQL 做
// 锁后身份、head、wire、预算和 CAS 复验。响应丢失时重复同一个请求会先
// 找到永久 receipt，绝不换 mutation ID，也不把未知结果推断成未提交。
func (c *Coordinator) CompressSemanticHistory(ctx context.Context, q d.SemanticCompressionRequest) (d.SemanticCompressionReceipt, error) {
	if q.Validate() != nil {
		return d.SemanticCompressionReceipt{}, d.ErrInvalid
	}
	prepare := args(q.Identity)
	prepare["phase"] = "prepare"
	prepare["expected_revision"] = strconv.FormatUint(q.ExpectedRevision, 10)
	prepare["expected_head_chain"] = q.ExpectedHeadChain
	prepare["max_objects"] = strconv.FormatUint(q.MaxObjects, 10)
	prepare["max_bytes"] = strconv.FormatUint(q.MaxBytes, 10)
	mutate(prepare, q.Mutation)

	b, err := c.semanticCompressionCall(ctx, prepare, true)
	if err != nil {
		return d.SemanticCompressionReceipt{}, err
	}
	prepared, preparedReceipt, err := decodeSemanticCompressionResult(b, q)
	if err != nil {
		return d.SemanticCompressionReceipt{}, err
	}
	if prepared.State == "found" || prepared.State == "committed" {
		return preparedReceipt, nil
	}

	// storage_image 的 JSON 是后端在 prepare 的 run 锁内生成的；这里仅做
	// 完整历史链重放和根包语义校验，不接受调用方另传的 RunRecord。
	request := CompactStorageRequest{Identity: q.Identity, ThroughRevision: q.ExpectedRevision}
	record, chain, err := buildMaintenanceImage(ctx, request, prepared.Image)
	if err != nil {
		return d.SemanticCompressionReceipt{}, err
	}
	if record.Revision != q.ExpectedRevision || chain != q.ExpectedHeadChain {
		return d.SemanticCompressionReceipt{}, d.ErrRevisionConflict
	}
	planRequest := durable.HistoryCompressionRequest{
		RunID:           q.Identity.RunID,
		ThroughRevision: q.ExpectedRevision,
		MaxObjects:      q.MaxObjects,
		MaxBytes:        q.MaxBytes,
	}
	plan, err := durable.PlanHistoryCompression(record, planRequest)
	if err != nil {
		return d.SemanticCompressionReceipt{}, err
	}
	batch, err := durable.BuildHistoryCompressionBatch(record, plan)
	if err != nil {
		return d.SemanticCompressionReceipt{}, err
	}
	proposed := durable.CloneRecord(record)
	if _, err = batch.ApplyTo(&proposed); err != nil {
		return d.SemanticCompressionReceipt{}, err
	}
	wire, err := durable.MarshalChangeBatch(batch)
	if err != nil {
		return d.SemanticCompressionReceipt{}, d.ErrInvalid
	}
	recordWire, err := recordio.EncodeSnapshot(proposed)
	if err != nil {
		return d.SemanticCompressionReceipt{}, d.ErrInvalid
	}

	commit := args(q.Identity)
	commit["phase"] = "commit"
	commit["expected_revision"] = strconv.FormatUint(q.ExpectedRevision, 10)
	commit["expected_head_chain"] = q.ExpectedHeadChain
	commit["max_objects"] = strconv.FormatUint(q.MaxObjects, 10)
	commit["max_bytes"] = strconv.FormatUint(q.MaxBytes, 10)
	commit["source_image_digest"] = prepared.SourceImageDigest
	commit["plan_digest"] = plan.PlanDigest
	commit["batch"] = base64.StdEncoding.EncodeToString(wire)
	commit["batch_digest"] = digest(wire)
	commit["record"] = base64.StdEncoding.EncodeToString(recordWire)
	commit["record_digest"] = digest(recordWire)
	mutate(commit, q.Mutation)

	b, err = c.semanticCompressionCall(ctx, commit, true)
	if err != nil {
		return d.SemanticCompressionReceipt{}, err
	}
	_, committedReceipt, err := decodeSemanticCompressionResult(b, q)
	if err != nil {
		return d.SemanticCompressionReceipt{}, err
	}
	return committedReceipt, nil
}

// SemanticCompressionDigestFromSnapshot 绑定根包生成的精确 snapshot envelope
// 字节，供上层审计或测试保存来源证据；它不作为 SQL 授权凭据。
func SemanticCompressionDigestFromSnapshot(record durable.RunRecord) (string, error) {
	b, err := recordio.EncodeSnapshot(record)
	if err != nil {
		return "", err
	}
	return digest(b), nil
}

var _ d.SemanticCompressionExecutor = (*Coordinator)(nil)
