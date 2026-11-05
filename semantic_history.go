// 本文件定义逻辑历史证据，类似记录层的一组值对象与公共校验函数。
// 它负责格式闭合和批次承诺，不读取后端、不决定历史保留，也不授予删除或回放权限。
package durable

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

// RecordFormat3 是携带语义历史证据的逻辑记录格式。
// SemanticRecordFormat 是同一格式的语义别名，代码中优先使用它表达用途。
const (
	RecordFormat3        = 3
	SemanticRecordFormat = RecordFormat3
)

// HistorySummaryKey 标识一条由语义历史压缩产生的摘要。
// Kind 用来区分摘要算法；ID 是该算法下的稳定业务对象身份。
// 两个字段共同构成 RunRecord.HistorySummaries 的键，避免不同摘要类型
// 共享短 ID 时互相覆盖。
type HistorySummaryKey struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// HistorySummary 是已被压缩的历史事实摘要。
// Payload 由 Kind 解释，Digest 必须等于 Payload 的 SHA-256；摘要本身不
// 授予删除、重放或访问权限。SourceRevision/SourceDigest 记录生成它时
// 观察到的逻辑记录位置，后续执行片会再用 CommitIndex 校验来源闭包。
type HistorySummary struct {
	Kind           string `json:"kind"`
	ID             string `json:"id"`
	SourceRevision uint64 `json:"source_revision"`
	SourceDigest   string `json:"source_digest"`
	Payload        []byte `json:"payload"`
	Digest         string `json:"digest"`
}

// CommitIndexEntry 是一个已提交逻辑 revision 的因果索引。
// ParentRevision 必须严格指向前一条索引（revision=1 时为 0）；BatchDigest
// 绑定该 revision 去掉 commit_index 操作后的 canonical ChangeBatch，避免
// 自引用。它不替代数据库 head、session owner 或权限校验；消息因果引用另行补齐。
type CommitIndexEntry struct {
	Revision       uint64 `json:"revision"`
	ParentRevision uint64 `json:"parent_revision"`
	BatchDigest    string `json:"batch_digest"`
}

// Provenance 描述当前 Format3 记录由哪一条逻辑来源链产生。
// Digest 是固定字段的 canonical 摘要，用于防止调用方提交互相矛盾的
// 来源元数据。它不是签名，也不是授权凭据；OriginRunID 可以指向另一个
// Run，这为后续指定位置派生回放保留来源边界。
type Provenance struct {
	OriginRunID        string `json:"origin_run_id"`
	OriginRevision     uint64 `json:"origin_revision"`
	OriginRecordDigest string `json:"origin_record_digest"`
	CompressionID      string `json:"compression_id"`
	Digest             string `json:"digest"`
}

type historySummaryKey struct {
	kind string
	id   string
}

type putHistorySummary struct {
	expected HistorySummary
	value    HistorySummary
	absent   bool
	remove   bool
}

type putCommitIndex struct {
	expected CommitIndexEntry
	value    CommitIndexEntry
	absent   bool
	remove   bool
}

type provenanceOperation struct {
	expected Provenance
	value    Provenance
	absent   bool
	remove   bool
}

func (k HistorySummaryKey) valid() bool {
	return validSemanticText(k.Kind) && validSemanticText(k.ID)
}

func (s HistorySummary) key() HistorySummaryKey { return HistorySummaryKey{Kind: s.Kind, ID: s.ID} }

func (s HistorySummary) validate() error {
	if !s.key().valid() || s.SourceRevision == 0 || !validDigest(s.SourceDigest) ||
		!validDigest(s.Digest) || digest(s.Payload) != s.Digest {
		return fmt.Errorf("%w: invalid history summary", ErrInvalid)
	}
	return nil
}

func (e CommitIndexEntry) validate(revision uint64) error {
	if revision == 0 || e.Revision != revision || e.ParentRevision+1 != e.Revision ||
		!validDigest(e.BatchDigest) {
		return fmt.Errorf("%w: invalid commit index entry", ErrInvalid)
	}
	return nil
}

func (p Provenance) validate() error {
	if !validSemanticText(p.OriginRunID) || p.OriginRevision == 0 ||
		!validDigest(p.OriginRecordDigest) || !validSemanticText(p.CompressionID) ||
		!validDigest(p.Digest) || p.Digest != provenanceDigest(p.OriginRunID, p.OriginRevision, p.OriginRecordDigest, p.CompressionID) {
		return fmt.Errorf("%w: invalid provenance", ErrInvalid)
	}
	return nil
}

func copyHistorySummary(s HistorySummary) HistorySummary {
	out := s
	if s.Payload != nil {
		out.Payload = make([]byte, len(s.Payload))
		copy(out.Payload, s.Payload)
	}
	return out
}

func copyProvenance(p Provenance) Provenance { return p }

func provenanceDigest(runID string, revision uint64, recordDigest, compressionID string) string {
	return semanticDigest("provenance/v1", runID, revision, recordDigest, compressionID)
}

func semanticDigest(domain string, values ...any) string {
	payload, err := json.Marshal(struct {
		Domain string `json:"domain"`
		Values []any  `json:"values"`
	}{Domain: domain, Values: values})
	if err != nil {
		return ""
	}
	h := sha256.Sum256(payload)
	return hex.EncodeToString(h[:])
}

func validSemanticText(s string) bool {
	return s != "" && utf8.ValidString(s) && validUTF8Text(s)
}

func validUTF8Text(s string) bool {
	for _, r := range s {
		if r == '\x00' {
			return false
		}
	}
	return true
}

// ValidateSemanticEvidence 检查记录的逻辑格式与语义历史证据是否闭合。
// 它只负责 Format1/2/3 与摘要、CommitIndex、Provenance 的公共结构关系；
// 不检查图定义、Codec、运行所有权、物理链或数据库 head。各后端在解码
// 记录后调用它，Runtime 还会继续执行图和恢复投影校验。
func ValidateSemanticEvidence(r RunRecord) error {
	return validateSemanticEvidence(r)
}

func validateSemanticEvidence(r RunRecord) error {
	if !IsLogicalRecordFormat(r.Format) {
		return fmt.Errorf("%w: unsupported logical record format", ErrInvalidRecord)
	}
	if r.Format != SemanticRecordFormat {
		if r.HistorySummaries != nil || r.CommitIndex != nil || r.Provenance != nil {
			return fmt.Errorf("%w: format 2 record contains format 3 evidence", ErrInvalidRecord)
		}
		return nil
	}
	if len(r.HistorySummaries) == 0 || r.CommitIndex == nil || r.Provenance == nil {
		return fmt.Errorf("%w: format 3 record lacks semantic evidence", ErrInvalidRecord)
	}
	if err := r.Provenance.validate(); err != nil {
		return err
	}
	seen := make(map[HistorySummaryKey]bool, len(r.HistorySummaries))
	for _, summary := range r.HistorySummaries {
		key := summary.key()
		if !key.valid() || summary.SourceRevision == 0 || summary.SourceRevision > r.Revision || seen[key] {
			return fmt.Errorf("%w: duplicate or invalid history summary key", ErrInvalidRecord)
		}
		seen[key] = true
		if err := summary.validate(); err != nil {
			return err
		}
	}
	if len(r.CommitIndex) == 0 || uint64(len(r.CommitIndex)) != r.Revision {
		return fmt.Errorf("%w: commit index is not complete", ErrInvalidRecord)
	}
	for i, entry := range r.CommitIndex {
		revision := uint64(i + 1)
		if err := entry.validate(revision); err != nil {
			return err
		}
		if i > 0 && r.CommitIndex[i-1].Revision+1 != entry.Revision {
			return fmt.Errorf("%w: commit index is not ordered", ErrInvalidRecord)
		}
	}
	return nil
}

func cloneSemanticEvidence(r *RunRecord) {
	if r.HistorySummaries != nil {
		summaries := r.HistorySummaries
		r.HistorySummaries = make([]HistorySummary, len(summaries))
		for i, summary := range summaries {
			r.HistorySummaries[i] = copyHistorySummary(summary)
		}
	}
	if r.CommitIndex != nil {
		entries := r.CommitIndex
		r.CommitIndex = make([]CommitIndexEntry, len(entries))
		copy(r.CommitIndex, entries)
	}
	if r.Provenance != nil {
		value := copyProvenance(*r.Provenance)
		r.Provenance = &value
	}
}

func summaryIndex(values []HistorySummary, key HistorySummaryKey) int {
	for i := range values {
		if values[i].Kind == key.Kind && values[i].ID == key.ID {
			return i
		}
	}
	return -1
}

func commitIndexPosition(values []CommitIndexEntry, revision uint64) int {
	// 已加载记录的索引严格按 revision=1..H 排列，可直接定位；长度比较先于
	// uint64→int 转换，避免来自损坏记录的超大 revision 发生截断。
	if revision > 0 && revision <= uint64(len(values)) && values[int(revision-1)].Revision == revision {
		return int(revision - 1)
	}
	return -1
}

// semanticBatchContentDigest 计算 Format3 批次的内容摘要。
// CommitIndexEntry 自身不参与这个摘要，避免“索引记录自身的 Digest
// 又依赖包含该索引的 wire”形成循环；它绑定的是同一 header 和同一业务
// 操作集合、但去掉 commit_index 操作后的 canonical wire。
func semanticBatchContentDigest(b ChangeBatch) (string, error) {
	if b.tx == nil {
		return "", fmt.Errorf("%w: semantic batch is missing transaction", ErrConflict)
	}
	wire, err := marshalChangeBatch(b, false)
	if err != nil {
		return "", err
	}
	return digest(wire), nil
}

// validateSemanticTransition 在写入前校验本批受影响的证据，不物化完整提案。
// 输入必须来自 Load/快照边界的完整校验；这里维护闭合不变量：历史索引只读、
// 每批恰好追加下一条索引、摘要至少保留一条、来源不能消失。所有可能失败的
// 检查先于 tx.apply，因此失败不会污染调用方拥有的映射或业务 Data。
func (b ChangeBatch) validateSemanticTransition(record RunRecord) error {
	if b.header.LogicalFormat != SemanticRecordFormat {
		return nil
	}
	if record.Format != SemanticRecordFormat || len(record.HistorySummaries) == 0 ||
		uint64(len(record.CommitIndex)) != record.Revision || record.Revision == 0 || record.Provenance == nil {
		return fmt.Errorf("%w: source semantic evidence is incomplete", ErrInvalidRecord)
	}
	if err := record.CommitIndex[len(record.CommitIndex)-1].validate(record.Revision); err != nil {
		return fmt.Errorf("%w: invalid source commit index", ErrInvalidRecord)
	}
	if err := record.Provenance.validate(); err != nil {
		return fmt.Errorf("%w: invalid source provenance", ErrInvalidRecord)
	}
	if len(b.tx.commitIndex) != 1 {
		return fmt.Errorf("%w: each format 3 batch must append exactly one commit index", ErrInvalidRecord)
	}
	op, exists := b.tx.commitIndex[b.header.Revision]
	if !exists || !op.absent || op.remove || op.value.validate(b.header.Revision) != nil {
		return fmt.Errorf("%w: historical commit indexes are immutable", ErrInvalidRecord)
	}
	contentDigest, err := semanticBatchContentDigest(b)
	if err != nil {
		return err
	}
	if op.value.BatchDigest != contentDigest {
		return fmt.Errorf("%w: commit index batch digest mismatch", ErrInvalidRecord)
	}
	count := len(record.HistorySummaries)
	for key, op := range b.tx.summaries {
		if op.remove {
			count--
			continue
		}
		if op.value.key() != (HistorySummaryKey{Kind: key.kind, ID: key.id}) || op.value.validate() != nil || op.value.SourceRevision > b.header.Revision {
			return fmt.Errorf("%w: invalid proposed history summary", ErrInvalidRecord)
		}
		if op.absent {
			count++
		}
	}
	if count < 1 {
		return fmt.Errorf("%w: format 3 record must retain semantic summaries", ErrInvalidRecord)
	}
	if op := b.tx.provenance; op != nil && (op.remove || op.value.validate() != nil) {
		return fmt.Errorf("%w: format 3 record must retain valid provenance", ErrInvalidRecord)
	}
	return nil
}
