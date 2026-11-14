// 本文件定义 distributed PostgreSQL 语义压缩维护协议的纯契约部分。
// 它只绑定请求、计划与永久回执的摘要，不读取数据库或授予删除权限；
// SQL 入口仍须在 session_user 与 run 行锁内重新验证完整证据闭包。
package distributed

import (
	"context"
	"encoding/json"
	"strconv"
	"time"
)

const SemanticCompressionMaxObjects = uint64(100000)
const SemanticCompressionMaxBytes = uint64(64 << 20)

type SemanticCompressionRequest struct {
	Identity          Identity `json:"identity"`
	ExpectedRevision  uint64   `json:"expected_revision"`
	ExpectedHeadChain string   `json:"expected_head_chain"`
	Mutation          Mutation `json:"mutation"`
	MaxObjects        uint64   `json:"max_objects"`
	MaxBytes          uint64   `json:"max_bytes"`
}

func (q SemanticCompressionRequest) Validate() error {
	if !validSemanticCompressionIdentity(q.Identity) || q.ExpectedRevision == 0 || !validSemanticCompressionFingerprint(q.ExpectedHeadChain) || !validSemanticCompressionMutation(q.Mutation) || q.MaxObjects == 0 || q.MaxObjects > SemanticCompressionMaxObjects || q.MaxBytes == 0 || q.MaxBytes > SemanticCompressionMaxBytes {
		return ErrInvalid
	}
	return nil
}

func validSemanticCompressionIdentity(i Identity) bool {
	return validSemanticCompressionText(i.Namespace) && validSemanticCompressionText(i.RunID)
}
func validSemanticCompressionMutation(m Mutation) bool {
	return validSemanticCompressionText(m.ID) && validSemanticCompressionFingerprint(m.Digest)
}
func validSemanticCompressionText(s string) bool        { return s != "" && len(s) <= MaxTextBytes }
func validSemanticCompressionFingerprint(s string) bool { return len(s) == 64 && isLowerHex(s) }
func isLowerHex(s string) bool {
	for _, r := range s {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}
func (q SemanticCompressionRequest) Digest() string {
	return hashJSON("pulse-durable/distributed/semantic-compression/request/v1", q.Identity.Namespace, q.Identity.RunID, strconv.FormatUint(q.ExpectedRevision, 10), q.ExpectedHeadChain, q.Mutation.ID, q.Mutation.Digest, strconv.FormatUint(q.MaxObjects, 10), strconv.FormatUint(q.MaxBytes, 10))
}

type SemanticCompressionReceipt struct {
	Identity          Identity  `json:"identity"`
	Mutation          Mutation  `json:"mutation"`
	ExpectedRevision  uint64    `json:"expected_revision"`
	AppliedRevision   uint64    `json:"applied_revision"`
	ExpectedHeadChain string    `json:"expected_head_chain"`
	AppliedHeadChain  string    `json:"applied_head_chain"`
	DeletedObjects    uint64    `json:"deleted_objects"`
	SourceBytes       uint64    `json:"source_bytes"`
	OccurredAt        time.Time `json:"occurred_at"`
	ReceiptDigest     string    `json:"receipt_digest"`
}

func (r SemanticCompressionReceipt) Digest() string {
	return hashJSON("pulse-durable/distributed/semantic-compression/receipt/v1", r.Identity.Namespace, r.Identity.RunID, r.Mutation.ID, r.Mutation.Digest, strconv.FormatUint(r.ExpectedRevision, 10), strconv.FormatUint(r.AppliedRevision, 10), r.ExpectedHeadChain, r.AppliedHeadChain, strconv.FormatUint(r.DeletedObjects, 10), strconv.FormatUint(r.SourceBytes, 10), strconv.FormatInt(r.OccurredAt.UnixMicro(), 10))
}

func (r SemanticCompressionReceipt) Validate(q SemanticCompressionRequest) error {
	if q.Validate() != nil || r.Identity != q.Identity || r.Mutation != q.Mutation || r.ExpectedRevision != q.ExpectedRevision || r.AppliedRevision != q.ExpectedRevision+1 || r.ExpectedHeadChain != q.ExpectedHeadChain || !validSemanticCompressionFingerprint(r.AppliedHeadChain) || r.OccurredAt.IsZero() || r.OccurredAt.UnixNano()%1000 != 0 || r.ReceiptDigest != r.Digest() || r.DeletedObjects == 0 || r.SourceBytes == 0 || r.DeletedObjects > q.MaxObjects || r.SourceBytes > q.MaxBytes {
		return ErrInvalid
	}
	return nil
}

// SemanticCompressionExecutor 是专用维护入口；它不复用 Acquire/owner token，
// 也不允许调用方上传删除集合。后端从当前权威记录重规划并以 Mutation 做永久去重。
type SemanticCompressionExecutor interface {
	CompressSemanticHistory(context.Context, SemanticCompressionRequest) (SemanticCompressionReceipt, error)
}

func hashJSON(domain string, values ...string) string {
	b, _ := json.Marshal(append([]string{domain}, values...))
	return hash(b)
}
