// 本文件在一次规划中预先按业务身份分组。它只保存原记录值的只读视图，
// 不克隆 Payload，不构成第二份事实源；调用期间原记录必须由调用方独占。
package durable

import "sort"

type compressionCandidateIndex struct {
	sourceDigest  string
	refs          map[string]bool
	retries       map[retrySummaryGroup]map[string]AttemptRecord
	messages      map[messageSummaryGroup]map[string]MessageAttemptRecord
	confirmations map[string][]string
	seenRetries   map[retrySummaryGroup]bool
	seenMessages  map[messageSummaryGroup]bool
}

func indexCompressionCandidates(record RunRecord) *compressionCandidateIndex {
	index := &compressionCandidateIndex{sourceDigest: compressionSourceDigest(record), refs: compressionReferenceClosure(record),
		retries: make(map[retrySummaryGroup]map[string]AttemptRecord), messages: make(map[messageSummaryGroup]map[string]MessageAttemptRecord), confirmations: make(map[string][]string), seenRetries: make(map[retrySummaryGroup]bool), seenMessages: make(map[messageSummaryGroup]bool)}
	for id, attempt := range record.Attempts {
		key := retrySummaryGroup{attempt.Parent, attempt.NodeID, attempt.Boundary}
		if index.retries[key] == nil {
			index.retries[key] = make(map[string]AttemptRecord)
		}
		index.retries[key][id] = attempt
	}
	for id, message := range record.Messages {
		key := messageSummaryGroup{message.Scope, message.Parent, message.NodeID, streamKey(message.Input)}
		if index.messages[key] == nil {
			index.messages[key] = make(map[string]MessageAttemptRecord)
		}
		index.messages[key][id] = message
		if message.State == AttemptConfirmed {
			managed := managedKey(message.Scope, message.Parent, message.NodeID)
			index.confirmations[managed] = append(index.confirmations[managed], id)
		}
	}
	for key := range index.confirmations {
		sort.Strings(index.confirmations[key])
	}
	return index
}
