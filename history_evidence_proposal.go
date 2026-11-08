package durable

import (
	"context"
	"fmt"
)

// HistoryEvidenceMaterialization 是一次已经从完整历史链验证出的首条证据
// 提案。它携带来源 head 与完整记录，发布方必须在自己的权威锁/事务内
// 再次比较 ExpectedRevision/HeadChain；这里的结果本身不是授权令牌。
type HistoryEvidenceMaterialization struct {
	State       StorageState
	Record      RunRecord
	CommitIndex []CommitIndexEntry
	SourceBytes uint64
}

// MaterializeHistoryEvidenceProposal 从 VerifiedHistoryEvidence 构造三项
// 闭合组件。首版只把既有连续 revision 作为 CommitIndex 的内容承诺，
// Provenance 明确标识“首次证据物化”及来源记录摘要；不凭空生成 Retry/
// Message 摘要，不把全历史重命名成语义压缩，也不改变业务投影。
func MaterializeHistoryEvidenceProposal(ctx context.Context, verified VerifiedHistoryEvidence) (HistoryEvidenceMaterialization, error) {
	if err := checkContext(ctx); err != nil {
		return HistoryEvidenceMaterialization{}, err
	}
	record := verified.Record()
	if hasAnySemanticEvidence(record) || record.Revision != verified.StorageState().HeadRevision || record.RunID != verified.StorageState().RunID {
		return HistoryEvidenceMaterialization{}, fmt.Errorf("%w: evidence materialization source is not an ordinary head", ErrConflict)
	}
	entries := verified.CommitIndex()
	if len(entries) == 0 || uint64(len(entries)) != record.Revision {
		return HistoryEvidenceMaterialization{}, fmt.Errorf("%w: evidence materialization index is incomplete", ErrInvalidRecord)
	}
	for i, entry := range entries {
		if err := entry.validate(uint64(i + 1)); err != nil {
			return HistoryEvidenceMaterialization{}, err
		}
	}
	originDigest := entries[0].BatchDigest
	p := Provenance{OriginRunID: record.RunID, OriginRevision: 1, OriginRecordDigest: originDigest, CompressionID: "history-evidence-materialization/v1"}
	p.Digest = provenanceDigest(p.OriginRunID, p.OriginRevision, p.OriginRecordDigest, p.CompressionID)
	record.HistorySummaries = []HistorySummary{{Kind: historyEvidenceSummaryKind, ID: "origin", SourceRevision: record.Revision, SourceDigest: recordDigest(record), Payload: []byte(`{"schema":"history-evidence/v1","source":"committed-chain"}`)}}
	record.HistorySummaries[0].Digest = digest(record.HistorySummaries[0].Payload)
	record.CommitIndex = entries
	record.Provenance = &p
	if err := ValidateSemanticEvidence(record); err != nil {
		return HistoryEvidenceMaterialization{}, err
	}
	return HistoryEvidenceMaterialization{State: verified.StorageState(), Record: CloneRecord(record), CommitIndex: append([]CommitIndexEntry(nil), entries...), SourceBytes: verified.SourceBytes()}, nil
}
