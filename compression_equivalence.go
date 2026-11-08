package durable

import (
	"fmt"
	"reflect"
	"sort"
)

// ValidateHistoryCompressionEquivalence 检查候选投影只移除了重新推导的
// attempt/message，并完整保留其他记录字段。这是恢复等价的必要预检，
// 尚不证明 Runtime 的 Retry 高水位、Managed来源或业务可复用成果等价。
// 因而只接受 Executable=false；不写 Store、不授予后端删除权限。
// 调用期间输入不得并发修改；失败不修改任一输入。
func ValidateHistoryCompressionEquivalence(before, after RunRecord, plan HistoryCompressionPlan) error {
	if err := plan.Validate(plan.Request); err != nil {
		return err
	}
	if before.RunID != plan.RunID || after.RunID != plan.RunID || recordDigest(before) != plan.HeadDigest {
		return fmt.Errorf("%w: compression plan or source record mismatch", ErrConflict)
	}
	// Digest 可由任何调用方重算；必须从原记录重新推导全部候选与保护集合。
	// 该预检仍不证明 Runtime 高水位恢复等价，也不允许后端执行删除。
	fresh, err := PlanHistoryCompression(before, plan.Request)
	if err != nil {
		return err
	}
	if fresh.PlanDigest != plan.PlanDigest {
		return fmt.Errorf("%w: compression plan differs from source facts", ErrInvalid)
	}
	if before.Revision != plan.HeadRevision || after.Revision != before.Revision || !hasCompleteSemanticEvidence(before) || !hasCompleteSemanticEvidence(after) {
		return fmt.Errorf("%w: compression requires an unchanged head with complete semantic evidence", ErrConflict)
	}
	if err := ValidateSemanticEvidence(before); err != nil {
		return err
	}
	if err := ValidateSemanticEvidence(after); err != nil {
		return err
	}
	expected := CloneRecord(before)
	for _, object := range plan.DeleteObjects {
		switch object.Kind {
		case "attempt":
			if _, ok := expected.Attempts[object.ID]; !ok {
				return fmt.Errorf("%w: plan deletes an unknown attempt", ErrInvalid)
			}
			delete(expected.Attempts, object.ID)
		case "message":
			if _, ok := expected.Messages[object.ID]; !ok {
				return fmt.Errorf("%w: plan deletes an unknown message", ErrInvalid)
			}
			delete(expected.Messages, object.ID)
		default:
			return fmt.Errorf("%w: compression object kind %q has no recovery projection rule", ErrUnsupportedCapability, object.Kind)
		}
	}
	normalizeCompressionProjection(&expected)
	actual := CloneRecord(after)
	normalizeCompressionProjection(&actual)
	if !reflect.DeepEqual(expected, actual) {
		return fmt.Errorf("%w: compressed recovery projection differs from retained facts", ErrInvalidRecord)
	}
	return nil
}

func normalizeCompressionProjection(record *RunRecord) {
	sort.Slice(record.HistorySummaries, func(i, j int) bool {
		if record.HistorySummaries[i].Kind != record.HistorySummaries[j].Kind {
			return record.HistorySummaries[i].Kind < record.HistorySummaries[j].Kind
		}
		return record.HistorySummaries[i].ID < record.HistorySummaries[j].ID
	})
	sort.Slice(record.CommitIndex, func(i, j int) bool { return record.CommitIndex[i].Revision < record.CommitIndex[j].Revision })
}
