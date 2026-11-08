package durable

import (
	"context"
	"fmt"
)

// HistoryEvidenceSource 是后端提供的受权威锁/事务保护的连续历史视图。
// Source 返回 revision1 的原始 snapshot bytes 摘要及其解码记录；Batch 从
// expected revision+1 逐批返回原 wire 与数据库已保存的 parent/next chain。
// 它不接受调用方提交的摘要，也不在实现中读取业务外部系统。
type HistoryEvidenceSource interface {
	HistoryEvidenceState(context.Context, string) (StorageState, error)
	HistoryEvidenceOrigin(context.Context, string) (record RunRecord, digest [32]byte, bytes uint64, err error)
	HistoryEvidenceBatch(context.Context, string, uint64) (wire []byte, digest, parent, next [32]byte, err error)
}

// MaterializeHistoryEvidence 先在后端提供的一次权威边界上捕获 head，
// 再在锁外验证完整链并构造不可执行的证据候选。它不会修改 Store；发布
// 必须由后端用 ExpectedRevision/Generation/CAS 原子接入专用入口。
// 源记录已有任一证据、head 变化、缺批、改写或预算超限均失败关闭。
func MaterializeHistoryEvidence(ctx context.Context, source HistoryEvidenceSource, runID string, bounds HistoryEvidenceBounds) (VerifiedHistoryEvidence, error) {
	if source == nil || runID == "" {
		return VerifiedHistoryEvidence{}, fmt.Errorf("%w: history evidence source is missing", ErrInvalid)
	}
	state, err := source.HistoryEvidenceState(ctx, runID)
	if err != nil {
		return VerifiedHistoryEvidence{}, err
	}
	record, digest, bytes, err := source.HistoryEvidenceOrigin(ctx, runID)
	if err != nil {
		return VerifiedHistoryEvidence{}, err
	}
	verifier, err := NewHistoryEvidenceVerifier(ctx, state, record, digest, bytes, bounds)
	if err != nil {
		return VerifiedHistoryEvidence{}, err
	}
	for revision := uint64(2); revision <= state.HeadRevision; revision++ {
		wire, wireDigest, parent, next, err := source.HistoryEvidenceBatch(ctx, runID, revision)
		if err != nil {
			return VerifiedHistoryEvidence{}, err
		}
		if err := verifier.Append(ctx, wire, wireDigest, parent, next); err != nil {
			return VerifiedHistoryEvidence{}, err
		}
	}
	return verifier.Finish(ctx)
}
