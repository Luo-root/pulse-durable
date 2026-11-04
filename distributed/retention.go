package distributed

import "time"

// RetentionPolicy 需显式启用。TTL 为 nil 表示无限期保留，即使策略已启用也如此。
// 守卫、运行身份和墓碑不受这些 TTL 约束。
// 终态载荷的保留时长从 finished_at 起算，历史记录从修订提交时起算，
// 事件和回执从 occurred_at 起算。固定保留仍优先于到期规则。
type RetentionPolicy struct {
	Enabled            bool
	TerminalPayloadTTL *time.Duration
	ReplayHistoryTTL   *time.Duration
	EventTTL           *time.Duration
	ReceiptTTL         *time.Duration
}

// Validate 返回独立副本，包括为每个时长指针复制其指向的值。
// Duration 已经是以纳秒为单位的 int64；适配器解析更大的值时，
// 必须在转换前拒绝溢出。此方法绝不计算截止时间，也不执行删除。
func (p RetentionPolicy) Validate() (RetentionPolicy, error) {
	out := p
	for _, field := range []**time.Duration{&out.TerminalPayloadTTL, &out.ReplayHistoryTTL, &out.EventTTL, &out.ReceiptTTL} {
		if *field == nil {
			continue
		}
		v := **field
		if v < time.Hour || v%time.Second != 0 {
			return RetentionPolicy{}, ErrInvalid
		}
		*field = &v
	}
	return out, nil
}

// RequiresDryRun 报告保留范围是否收缩，并不授予应用该变更的权限。
// 即使策略未启用，对它的变更也会保守地检查是否缩短了 TTL。
func (p RetentionPolicy) RequiresDryRun(next RetentionPolicy) (bool, error) {
	if _, err := p.Validate(); err != nil {
		return false, err
	}
	if _, err := next.Validate(); err != nil {
		return false, err
	}
	if !p.Enabled && next.Enabled {
		return true, nil
	}
	before := []*time.Duration{p.TerminalPayloadTTL, p.ReplayHistoryTTL, p.EventTTL, p.ReceiptTTL}
	after := []*time.Duration{next.TerminalPayloadTTL, next.ReplayHistoryTTL, next.EventTTL, next.ReceiptTTL}
	for i, v := range after {
		if v != nil && (before[i] == nil || *v < *before[i]) {
			return true, nil
		}
	}
	return false, nil
}

// RetentionPolicyUpdate 是限定于命名空间的 CAS 请求。DryRunID 引用独立的只读预览，
// 不是布尔绕过开关，也不构成删除授权。适配器必须认证调用方，
// 并验证预览真实有效，且绑定了此命名空间、预期版本和完全一致的拟议策略。
type RetentionPolicyUpdate struct {
	Namespace       string
	ExpectedVersion uint64
	Policy          RetentionPolicy
	DryRunID        string
}

// Validate 只检查结构和已观察到的版本。适配器必须针对权威数据原子地重新执行 CAS；
// 此纯检查既不应用策略，也不验证试运行凭证。
// 版本为零绝不表示隐式的插入或更新。
func (q RetentionPolicyUpdate) Validate(currentVersion uint64, current RetentionPolicy) (RetentionPolicy, error) {
	if !text(q.Namespace) || q.ExpectedVersion == 0 || currentVersion == 0 || (q.DryRunID != "" && !text(q.DryRunID)) {
		return RetentionPolicy{}, ErrInvalid
	}
	if q.ExpectedVersion != currentVersion {
		return RetentionPolicy{}, ErrRevisionConflict
	}
	shrinks, err := current.RequiresDryRun(q.Policy)
	if err != nil {
		return RetentionPolicy{}, err
	}
	if shrinks && q.DryRunID == "" {
		return RetentionPolicy{}, ErrInvalid
	}
	return q.Policy.Validate()
}

// MutationWindow 是时间点的闭区间。接受固定时区的时间戳；
// 比较依据是时间点，绝不依据 time.Location 指针是否相同。
type MutationWindow struct {
	From    time.Time
	Through time.Time
}

func (w MutationWindow) valid() bool {
	return !w.From.IsZero() && !w.Through.IsZero() && !w.Through.Before(w.From)
}

// MutationQuery 不改变 Mutation 或现有的 Coordinator 接口。
// epoch/revision 为 nil 表示未知；非 nil 时要求精确匹配，包括零值。
// 已知值（例如 Initialize/Apply 中的值）应显式提供。
// Cancel(identity, mutation) 可以将它们保留为 nil，并从回执读取已提交的值；
// 不得根据后续 Get 的结果猜测这些值。Epoch 不得为负。
type MutationQuery struct {
	Identity         Identity
	Mutation         Mutation
	Operation        string
	ExpectedEpoch    *int64
	ExpectedRevision *uint64
	RequestedWindow  MutationWindow
}

func (q MutationQuery) Validate() error {
	if !text(q.Identity.Namespace) || !text(q.Identity.RunID) || !text(q.Mutation.ID) || !fingerprint(q.Mutation.Digest) || (q.ExpectedEpoch != nil && *q.ExpectedEpoch < 0) || !q.RequestedWindow.valid() {
		return ErrInvalid
	}
	switch q.Operation {
	case "initialize", "apply", "finish", "release", "block", "cancel":
		return nil
	default:
		return ErrInvalid
	}
}

type MutationQueryState string

const (
	Found              MutationQueryState = "found"
	AbsentWithinWindow MutationQueryState = "absent_within_window"
	Expired            MutationQueryState = "expired"
)

// MutationReceipt 只描述已提交的协调器变更，不描述外部副作用。
// 它与现有的后端专用回执类型相互独立。
type MutationReceipt struct {
	Identity   Identity
	Mutation   Mutation
	Operation  string
	Epoch      int64
	Revision   uint64
	OccurredAt time.Time
}

// MutationQueryResult 携带权威且连续的回执覆盖窗口。适配器必须在同一次一致性读取中，
// 根据数据库时间和持久化的保留水位确定该窗口，绝不能使用客户端时钟。
// Coverage.From 界定连续完整覆盖的下界，而不是所有保留回执的下界：
// Found 回执可以更早（例如已固定保留或尚未清理的回执）。
// Coverage.Through 是本次读取的权威观测上界；返回的任何回执都不得晚于它。
// AbsentWithinWindow 要求完整覆盖请求窗口；Expired 表示请求的部分历史已不再受覆盖。
// 不存在或过期都不能证明未提交，也不授予业务重放权限。
// 即使是 Found，也只确定该精确变更的情况，不确定外部副作用或所有权。
type MutationQueryResult struct {
	State    MutationQueryState
	Coverage MutationWindow
	Receipt  *MutationReceipt
}

// Validate 检查内部一致性，不验证覆盖窗口或回执的权威性。
// 此方法的任何结果都不构成执行、租约或重放授权。
func (r MutationQueryResult) Validate(q MutationQuery) error {
	if q.Validate() != nil || !r.Coverage.valid() {
		return ErrInvalid
	}
	covered := !q.RequestedWindow.From.Before(r.Coverage.From) && !q.RequestedWindow.Through.After(r.Coverage.Through)
	switch r.State {
	case Found:
		v := r.Receipt
		if v == nil || v.Identity != q.Identity || v.Mutation != q.Mutation || v.Operation != q.Operation || v.Epoch < 0 || (q.ExpectedEpoch != nil && v.Epoch != *q.ExpectedEpoch) || (q.ExpectedRevision != nil && v.Revision != *q.ExpectedRevision) || v.OccurredAt.IsZero() || v.OccurredAt.Before(q.RequestedWindow.From) || v.OccurredAt.After(q.RequestedWindow.Through) || v.OccurredAt.After(r.Coverage.Through) {
			return ErrInvalid
		}
	case AbsentWithinWindow:
		if r.Receipt != nil || !covered {
			return ErrInvalid
		}
	case Expired:
		// 对未来时间的请求不属于已过期历史。只有覆盖下界处的历史丢失才能认定过期，
		// 而且读取范围必须已达到请求的上界。
		if r.Receipt != nil || !q.RequestedWindow.From.Before(r.Coverage.From) || q.RequestedWindow.Through.After(r.Coverage.Through) {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}
