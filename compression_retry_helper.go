// 本文件重新推导 Retry 失败前缀的事实承诺，供压缩规划与恢复校验共享。
// 它只检查数据，不信任摘要里自报的 digest，也不执行删除或重置预算。
package durable

import "fmt"

func retryPrefixForCompression(record RunRecord, payload retryPrefixSummaryPayload, group map[string]AttemptRecord) (RetrySummary, error) {
	// ThroughIndex 来自持久 Payload；先用真实对象数限制分配与循环次数，
	// 防止极大整数把一个很小的损坏记录放大为无界规划工作。
	if payload.ThroughIndex < 1 || payload.ThroughIndex > len(group) || payload.Limit < payload.ThroughIndex {
		return RetrySummary{}, fmt.Errorf("%w: retry prefix object closure is incomplete", ErrInvalid)
	}
	byIndex := make(map[int]AttemptRecord, payload.ThroughIndex)
	for _, attempt := range group {
		if attempt.Parent != payload.Parent || attempt.NodeID != payload.NodeID || attempt.Boundary != payload.Boundary {
			continue
		}
		if attempt.Index < 1 || attempt.Index > attempt.Limit || attempt.Limit != payload.Limit || !validAttemptState(attempt.State) {
			return RetrySummary{}, fmt.Errorf("%w: invalid retry budget", ErrConflict)
		}
		if attempt.Index > payload.ThroughIndex {
			continue
		}
		if _, duplicate := byIndex[attempt.Index]; duplicate || attempt.State != AttemptFailed {
			return RetrySummary{}, fmt.Errorf("%w: retry prefix is active or duplicated", ErrConflict)
		}
		byIndex[attempt.Index] = attempt
	}
	if len(byIndex) != payload.ThroughIndex {
		return RetrySummary{}, fmt.Errorf("%w: retry prefix has a gap", ErrInvalid)
	}
	// 与只读 RetryPrefix 使用同一末尾事实摘要规则；每一项必须在上面独立验证。
	last := byIndex[payload.ThroughIndex]
	lastDigest := summaryDigest("retry", record.RunID, record.Revision, payload.Parent, payload.NodeID, payload.Boundary, last.Index, last.Limit, string(last.State))
	return RetrySummary{Parent: payload.Parent, NodeID: payload.NodeID, Boundary: payload.Boundary,
		PrunedThroughIndex: payload.ThroughIndex, Limit: payload.Limit, FailedCount: payload.ThroughIndex,
		LastPrunedDigest: lastDigest, Revision: record.Revision}, nil
}
