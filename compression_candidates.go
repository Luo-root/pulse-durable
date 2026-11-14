// 本文件是摘要候选的公共调度入口，类似只读查询服务。
// 来源位置先由通用证据验证，Kind 的 Payload 与候选事实再交给只读注册表；
// 本入口不解释具体算法，不执行删除，也不推断缺失的引用闭包。
package durable

import "fmt"

func candidateObjectsFromSummary(record RunRecord, summary HistorySummary) ([]CompressionObjectRef, error) {
	return candidateObjectsFromVerifiedSource(record, summary, indexCompressionCandidates(record))
}

func candidateObjectsFromVerifiedSource(record RunRecord, summary HistorySummary, index *compressionCandidateIndex) ([]CompressionObjectRef, error) {
	if err := summary.validate(); err != nil {
		return nil, err
	}
	if summary.SourceRevision != record.Revision || summary.SourceDigest != index.sourceDigest {
		return nil, fmt.Errorf("%w: compression summary source changed", ErrConflict)
	}
	payload, err := defaultHistorySummaryRegistry.decode(summary)
	if err != nil {
		return nil, err
	}
	if !defaultHistorySummaryRegistry.compressionEnabled(summary.Kind) {
		return nil, fmt.Errorf("%w: summary kind %q has no deletion semantics", ErrUnsupportedCapability, summary.Kind)
	}
	return payload.candidates(record, index)
}
