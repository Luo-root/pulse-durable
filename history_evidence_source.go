// 本文件相当于历史读取层的顺序校验器：后端提供已在其锁/事务中读取并
// 校验过的原始首快照与批次，校验器验证连续投影和原始提交链，生成首次
// 证据物化所需的索引。它不写 Store、不授予权限、不把摘要当作可信主体。
package durable

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

const (
	// HistoryEvidenceMaxBatches/Bytes 是历史证据构建的初始工作预算，
	// 不代表保留政策或业务记录格式。超限必须停止，不能截断历史再补索引。
	HistoryEvidenceMaxBatches = 100000
	HistoryEvidenceMaxBytes   = 64 << 20
)

// HistoryEvidenceBounds 限制完整来源链的批数与原始字节总量。
// MaxBytes 包含首快照和每份原始 wire；零值无效，不提供隐式无限预算。
type HistoryEvidenceBounds struct {
	MaxBatches uint64
	MaxBytes   uint64
}

func (b HistoryEvidenceBounds) validate() error {
	if b.MaxBatches == 0 || b.MaxBatches > HistoryEvidenceMaxBatches || b.MaxBytes == 0 || b.MaxBytes > HistoryEvidenceMaxBytes {
		return fmt.Errorf("%w: invalid history evidence bounds", ErrInvalid)
	}
	return nil
}

// HistoryEvidenceVerifier 是一个单次、顺序、非并发安全的来源校验器。
// 必须通过 NewHistoryEvidenceVerifier 创建，任何失败都会使它永久停止；
// 已消费的部分不能在出错后继续拼成另一条来源。该类型不持有后端锁。
// 后端负责先验证 snapshot 原始字节的摘要、编码与 Record 一致性，并在
// 原子发布时重验其权威 head；仅构造本对象不代表历史已授权或持久提交。
type HistoryEvidenceVerifier struct {
	expected StorageState
	bounds   HistoryEvidenceBounds
	record   RunRecord
	chain    [32]byte
	entries  []CommitIndexEntry
	bytes    uint64
	err      error
	finished bool
}

// VerifiedHistoryEvidence 是通过完整链检查的隔离结果，字段保持私有以免
// 调用方修改其中的记录/索引后仍沿用验证结论。它只证明给定输入自洽，
// 不替代后端的来源认证、独占权、Generation/CAS 或发布事务。
type VerifiedHistoryEvidence struct {
	state   StorageState
	record  RunRecord
	entries []CommitIndexEntry
	bytes   uint64
}

// NewHistoryEvidenceVerifier 从 revision1 原始首快照开始；originDigest 必须
// 是后端已经核对过的真实持久字节 SHA-256，绝不能用重编码的新快照代替。
// expected 由同一个权威视图读取。源已存在语义证据时拒绝，首次物化不
// 重建或覆盖既有证据；从较晚合成快照猜造前缀也明确拒绝。
func NewHistoryEvidenceVerifier(ctx context.Context, expected StorageState, origin RunRecord, originDigest [32]byte, originBytes uint64, bounds HistoryEvidenceBounds) (*HistoryEvidenceVerifier, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := bounds.validate(); err != nil {
		return nil, err
	}
	if expected.RunID != origin.RunID || expected.Generation == 0 || expected.HeadRevision == 0 || expected.BaseRevision == 0 || expected.BaseRevision > expected.HeadRevision || originDigest == ([32]byte{}) || originBytes == 0 {
		return nil, fmt.Errorf("%w: incomplete history evidence source", ErrInvalidRecord)
	}
	if originBytes > bounds.MaxBytes || expected.HeadRevision-1 > bounds.MaxBatches {
		return nil, ErrLimitExceeded
	}
	if hasAnySemanticEvidence(origin) {
		return nil, fmt.Errorf("%w: history evidence already exists", ErrConflict)
	}
	if err := ValidateInitialRecord(origin, expected.RunID); err != nil {
		return nil, fmt.Errorf("%w: invalid history evidence origin", ErrInvalidRecord)
	}
	chain := SeedChain(originDigest)
	if expected.BaseRevision == 1 && expected.BaseChain != chain {
		return nil, fmt.Errorf("%w: history evidence origin anchor mismatch", ErrInvalidRecord)
	}
	return &HistoryEvidenceVerifier{expected: expected, bounds: bounds, record: CloneRecord(origin), chain: chain,
		entries: []CommitIndexEntry{{Revision: 1, ParentRevision: 0, BatchDigest: hex.EncodeToString(originDigest[:])}}, bytes: originBytes}, nil
}

// Append 验证一份原始持久 wire 及其独立存储的 digest/parent/next。
// 链只使用原 wire 的字节摘要，不通过重新编码“修复”旧历史。输入不会
// 被修改或保留别名；失败之后 Finish 也失败，不能返回部分索引。
func (v *HistoryEvidenceVerifier) Append(ctx context.Context, wire []byte, expectedDigest, parent, next [32]byte) error {
	if v == nil {
		return fmt.Errorf("%w: history evidence verifier is missing", ErrInvalid)
	}
	if v.err != nil {
		return v.err
	}
	fail := func(err error) error { v.err = err; return err }
	if err := checkContext(ctx); err != nil {
		return fail(err)
	}
	if v.finished || v.record.Revision >= v.expected.HeadRevision {
		return fail(fmt.Errorf("%w: history evidence path exceeds captured head", ErrConflict))
	}
	if uint64(len(wire)) > v.bounds.MaxBytes-v.bytes || uint64(len(v.entries)-1) >= v.bounds.MaxBatches {
		return fail(ErrLimitExceeded)
	}
	digest := sha256.Sum256(wire)
	if digest != expectedDigest || parent != v.chain {
		return fail(fmt.Errorf("%w: history evidence batch digest or parent mismatch", ErrInvalidRecord))
	}
	batch, err := UnmarshalChangeBatch(wire)
	if err != nil || batch.Header().ExpectedRevision != v.record.Revision || batch.Header().Revision != v.record.Revision+1 || batch.tx.hasSemanticEvidence() || batch.tx.historyCompression {
		return fail(fmt.Errorf("%w: invalid ordinary history evidence batch", ErrInvalidRecord))
	}
	calculated, err := NextChain(parent, digest, batch.Header().Revision)
	if err != nil || calculated != next {
		return fail(fmt.Errorf("%w: history evidence next anchor mismatch", ErrInvalidRecord))
	}
	if _, err := batch.ApplyTo(&v.record); err != nil {
		return fail(fmt.Errorf("%w: history evidence batch projection mismatch", ErrInvalidRecord))
	}
	if v.record.Revision == v.expected.BaseRevision && calculated != v.expected.BaseChain {
		return fail(fmt.Errorf("%w: history evidence retained base mismatch", ErrInvalidRecord))
	}
	v.entries = append(v.entries, CommitIndexEntry{Revision: v.record.Revision, ParentRevision: v.record.Revision - 1, BatchDigest: hex.EncodeToString(digest[:])})
	v.chain, v.bytes = calculated, v.bytes+uint64(len(wire))
	return nil
}

// Finish 仅在恰好达到捕获的 head/revision/chain 时返回证据。结果不与
// 校验器或调用方输入共享可变内存；未完成、失败、取消均返回零结果。
func (v *HistoryEvidenceVerifier) Finish(ctx context.Context) (VerifiedHistoryEvidence, error) {
	if v == nil {
		return VerifiedHistoryEvidence{}, fmt.Errorf("%w: history evidence verifier is missing", ErrInvalid)
	}
	if v.err != nil {
		return VerifiedHistoryEvidence{}, v.err
	}
	if err := checkContext(ctx); err != nil {
		v.err = err
		return VerifiedHistoryEvidence{}, err
	}
	if v.finished || v.record.Revision != v.expected.HeadRevision || v.chain != v.expected.HeadChain {
		v.err = fmt.Errorf("%w: history evidence path is incomplete or changed", ErrInvalidRecord)
		return VerifiedHistoryEvidence{}, v.err
	}
	v.finished = true
	return VerifiedHistoryEvidence{state: v.expected, record: CloneRecord(v.record), entries: append([]CommitIndexEntry(nil), v.entries...), bytes: v.bytes}, nil
}

// Record/CommitIndex 返回隔离副本；它们不授予普通 Apply 首次物化权限。
func (e VerifiedHistoryEvidence) Record() RunRecord { return CloneRecord(e.record) }
func (e VerifiedHistoryEvidence) CommitIndex() []CommitIndexEntry {
	return append([]CommitIndexEntry(nil), e.entries...)
}
func (e VerifiedHistoryEvidence) StorageState() StorageState { return e.state }
func (e VerifiedHistoryEvidence) SourceBytes() uint64        { return e.bytes }
