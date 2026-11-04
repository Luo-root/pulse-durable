package distributed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var (
	ErrPinInvalid  = fmt.Errorf("distributed: invalid history pin: %w", ErrInvalid)
	ErrPinConflict = errors.New("distributed: history pin conflict")
	ErrPinExpired  = errors.New("distributed: history pin expired")
	ErrPinNotFound = fmt.Errorf("distributed: history pin not found: %w", ErrNotFound)
	ErrPinUnknown  = fmt.Errorf("distributed: history pin result unknown: %w", ErrUnknown)
)

type PinStatus string

const (
	PinAbsent   PinStatus = "absent"
	PinActive   PinStatus = "active"
	PinExpired  PinStatus = "expired"
	PinReleased PinStatus = "released"
	PinConflict PinStatus = "conflict"
	PinUnknown  PinStatus = "unknown"
)

type PinOperation string

const (
	PinCreateOperation  PinOperation = "create"
	PinRenewOperation   PinOperation = "renew"
	PinReleaseOperation PinOperation = "release"
)

// PinRequest 的 PinID 在 Identity(namespace,run_id) 内永久不复用。
// 创建绑定 revision、chain、创建 Mutation、holder 和 TTL；同一规范请求返回原操作结果，
// 不同绑定返回 ErrPinConflict。TTL=nil 仅在创建时表示长期，需后端 maintain 授权。
// HolderDigest 仅是不可转移的绑定摘要，不是凭证或 owner 正文。
type PinRequest struct {
	Identity     Identity
	PinID        string
	Mutation     Mutation
	Revision     uint64
	Chain        string
	TTL          *time.Duration
	HolderDigest string
}

// RenewPinRequest 的 nil TTL 保持原策略，不改变 LongLived。后端必须持久原 TTL，
// 有限 pin 使用 max(现 expires_at, 权威 DB now+原 TTL) 续期；显式 TTL 使用新有限时长。
// 有限转长期暂不支持；expired/released 不得复活。任何续期都不得回退到期点。
type RenewPinRequest struct {
	Identity        Identity
	PinID           string
	Mutation        Mutation
	ExpectedVersion uint64
	TTL             *time.Duration
	HolderDigest    string
}

// ReleasePinRequest 必须对现有 holder 和版本 CAS；maintain 可代释放但不能跳过版本 CAS。
type ReleasePinRequest struct {
	Identity        Identity
	PinID           string
	Mutation        Mutation
	ExpectedVersion uint64
	HolderDigest    string
}

// PinQuery 只读取当前 pin，不证明任何先前写操作是否提交，不改变业务状态。
type PinQuery struct {
	Identity Identity
	PinID    string
}

// PinState 是当前状态或操作后快照。Mutation 始终是不可变的创建 Mutation，
// 不随续期/释放变化；每次写操作的 Mutation 在独立 receipt 中。
type PinState struct {
	PinID    string
	Identity Identity
	Revision uint64
	Chain    string
	Version  uint64
	// LongLived 与 ExpiresAt.IsZero() 必须一致；非零时间由后端返回，不能由本地时钟判态。
	LongLived    bool
	ExpiresAt    time.Time
	HolderDigest string
	Mutation     Mutation
}

// PinResult 的 conflict/unknown 只允许伴随对应 error，且不得携带 State。
// Unknown 表示可能已提交，禁止按未发生重放或更换幂等键；用 FindPinMutation 裁决。
// FindPin 仅返回 absent/active/expired/released；不确定则 ErrPinUnknown+零值结果。
// 创建幂等重试返回原 receipt 的 Result，不表示 pin 现在仍 active；需另查 FindPin。
type PinResult struct {
	Status PinStatus
	State  *PinState
}

// HistoryPinner 独立于 Coordinator/maintenance，只保护历史引用，不是物理快照 build pin，
// 不负责 retention、GC、replay。所有 Validate/Digest 都是纯函数，不授权、不读 DB/本地时间。
// 每次调用（包括幂等和回执读取）先检查当前认证身份权限，再查精确回执，再对新操作 CAS。
// 后端从认证 session_user 注入 holder 主体并匹配，绝不能用请求 HolderDigest 作为权限。
// 原 receipt 重放不得绕过撤权。长期创建仅 maintain；holder 创建后不能转移。
// Mutation.ID 的唯一作用域是 Identity+Mutation.ID，跨所有 pin 写操作共享；
// operation、PinID、各参数均进入规范摘要。相同键同参数返回原 receipt，异参数冲突。
// 权威行、永久 mutation guard、不可变 receipt 和状态必须在同一事务中发布。
// 新写操作目标缺失返回 ErrPinNotFound；过期返回 ErrPinExpired；版本/绑定/幂等键冲突
// 返回 ErrPinConflict；结果不确定返回 ErrPinUnknown。Released 不允许续期，返回 ErrPinConflict。
// 新 pin 的版本从 1 开始，每次新成功 renew/release 恰好 +1，禁止回绕。
// 精确旧 receipt 的解析先于新操作的 CAS/版本耗尽判断；当前版本为 MaxUint64 时，
// 新 renew/release 必须返回 ErrPinConflict，零发布；纯请求/状态校验仍允许该版本。
// TTL 的形状合法不保证截止时间可表示：后端必须持锁后使用权威 DB now 检查
// now+TTL 的算术溢出和数据库时间范围，失败返回 ErrPinInvalid，零发布，不能截断/夹取。
// 不使用客户端时间、UTC/local 转换猜测数据库范围或据此裁决状态。
// FindPin 的 absent 是当前不存在的观测，不授权重放。
type HistoryPinner interface {
	PinHistory(context.Context, PinRequest) (PinResult, error)
	RenewPin(context.Context, RenewPinRequest) (PinResult, error)
	ReleasePin(context.Context, ReleasePinRequest) error
	FindPin(context.Context, PinQuery) (PinResult, error)
}

// HistoryPinMutationReader 与当前 pin 查询独立；后续续期 B 不覆盖续期 A 或 release 回执。
// guard 与 receipt 永久保留，直到未来显式 retention 契约另行定义，不能静默过期或删除。
// 已知 guard 却缺 receipt 是损坏/未知，返回 ErrPinUnknown 而不能报 absent。
// 同 Identity+Mutation.ID 已绑定其他 PinID/Operation/Digest 时返回 ErrPinConflict，不能报 absent。
// Absent 不证明未来不会提交，也不授权重放；unknown 必须返回 ErrPinUnknown。
type HistoryPinMutationReader interface {
	FindPinMutation(context.Context, PinMutationQuery) (PinMutationResult, error)
}

type PinMutationQuery struct {
	Identity  Identity
	PinID     string
	Mutation  Mutation
	Operation PinOperation
}

type PinMutationStatus string

const (
	PinMutationFound   PinMutationStatus = "found"
	PinMutationAbsent  PinMutationStatus = "absent"
	PinMutationUnknown PinMutationStatus = "unknown"
)

// PinMutationReceipt 是不可变已提交操作的证据，不代表当前 pin 仍有效。
// RequestDigest 是完整规范请求摘要；Mutation 是该操作而非创建 Mutation。
// Result 是操作当时的后状态：create/renew 为 active，release 为 released。
type PinMutationReceipt struct {
	Identity      Identity
	PinID         string
	Mutation      Mutation
	Operation     PinOperation
	Version       uint64
	OccurredAt    time.Time
	RequestDigest string
	Result        PinResult
}

type PinMutationResult struct {
	Status  PinMutationStatus
	Receipt *PinMutationReceipt
}

const (
	PinTTLMin = time.Second
	// PinTTLMax 是 time.Duration 能表示的最大整秒值，不额外施加业务上限。
	// MaxInt64 纳秒本身不是整秒，必须被拒绝，不能舍入或截断为此值。
	PinTTLMax = time.Duration(1<<63-1) / time.Second * time.Second
)

// pinDigestInput 的字段顺序与 JSON 编码构成摘要格式 v1；无 map，无 omitempty，
// TTL 为纳秒整数或 null，区分 none 与有限值。Mutation 自身 Digest 不入摘要。
type pinDigestInput struct {
	Format          string
	Operation       PinOperation
	Identity        Identity
	PinID           string
	MutationID      string
	Revision        uint64
	Chain           string
	HolderDigest    string
	ExpectedVersion uint64
	TTL             *time.Duration
}

func (r PinRequest) Digest() string {
	return digestPin(pinDigestInput{"history-pin-v1", PinCreateOperation, r.Identity, r.PinID, r.Mutation.ID, r.Revision, r.Chain, r.HolderDigest, 0, r.TTL})
}
func (r RenewPinRequest) Digest() string {
	return digestPin(pinDigestInput{"history-pin-v1", PinRenewOperation, r.Identity, r.PinID, r.Mutation.ID, 0, "", r.HolderDigest, r.ExpectedVersion, r.TTL})
}
func (r ReleasePinRequest) Digest() string {
	return digestPin(pinDigestInput{"history-pin-v1", PinReleaseOperation, r.Identity, r.PinID, r.Mutation.ID, 0, "", r.HolderDigest, r.ExpectedVersion, nil})
}
func digestPin(v pinDigestInput) string { b, _ := json.Marshal(v); return hash(b) }
func validatePinTTL(v *time.Duration) bool {
	return v == nil || *v >= PinTTLMin && *v <= PinTTLMax && *v%time.Second == 0
}

// NextPinVersion 仅检查新操作的版本算术。后端必须先授权并解析精确旧 receipt，
// 仅对新操作在权威版本 CAS 后调用；它不访问数据库，也不保证 CAS 的原子性。
func NextPinVersion(current uint64) (uint64, error) {
	if current == 0 {
		return 0, ErrPinInvalid
	}
	if current == ^uint64(0) {
		return 0, ErrPinConflict
	}
	return current + 1, nil
}
func validatePinIdentity(id Identity) bool { return text(id.Namespace) && text(id.RunID) }
func validatePinMutation(m Mutation) bool  { return text(m.ID) && fingerprint(m.Digest) }
func validatePinBinding(id Identity, p string, m Mutation, h string) bool {
	return validatePinIdentity(id) && text(p) && validatePinMutation(m) && fingerprint(h)
}
func (r PinRequest) Validate() (PinRequest, error) {
	if !validatePinBinding(r.Identity, r.PinID, r.Mutation, r.HolderDigest) || r.Revision == 0 || !fingerprint(r.Chain) || !validatePinTTL(r.TTL) || r.Mutation.Digest != r.Digest() {
		return PinRequest{}, ErrPinInvalid
	}
	if r.TTL != nil {
		v := *r.TTL
		r.TTL = &v
	}
	return r, nil
}
func (r RenewPinRequest) Validate() (RenewPinRequest, error) {
	if !validatePinBinding(r.Identity, r.PinID, r.Mutation, r.HolderDigest) || r.ExpectedVersion == 0 || !validatePinTTL(r.TTL) || r.Mutation.Digest != r.Digest() {
		return RenewPinRequest{}, ErrPinInvalid
	}
	if r.TTL != nil {
		v := *r.TTL
		r.TTL = &v
	}
	return r, nil
}
func (r ReleasePinRequest) Validate() error {
	if !validatePinBinding(r.Identity, r.PinID, r.Mutation, r.HolderDigest) || r.ExpectedVersion == 0 || r.Mutation.Digest != r.Digest() {
		return ErrPinInvalid
	}
	return nil
}
func (q PinQuery) Validate() error {
	if !validatePinIdentity(q.Identity) || !text(q.PinID) {
		return ErrPinInvalid
	}
	return nil
}
func (s PinState) Validate() error {
	if !validatePinBinding(s.Identity, s.PinID, s.Mutation, s.HolderDigest) || s.Revision == 0 || !fingerprint(s.Chain) || s.Version == 0 || s.LongLived != s.ExpiresAt.IsZero() {
		return ErrPinInvalid
	}
	return nil
}
func (r PinResult) Validate() error {
	switch r.Status {
	case PinAbsent, PinConflict, PinUnknown:
		if r.State != nil {
			return ErrPinInvalid
		}
	case PinActive, PinExpired, PinReleased:
		if r.State == nil || r.State.Validate() != nil || r.Status == PinExpired && r.State.LongLived {
			return ErrPinInvalid
		}
	default:
		return ErrPinInvalid
	}
	return nil
}

// ValidateOutcome 检查写操作结果与 error 配对，不证明提交或授予权限。
func (r PinResult) ValidateOutcome(err error) error {
	if err != nil {
		if r.Status == "" && r.State == nil {
			return nil
		}
		if errors.Is(err, ErrPinConflict) && r.Status == PinConflict || errors.Is(err, ErrPinUnknown) && r.Status == PinUnknown {
			return r.Validate()
		}
		return ErrPinInvalid
	}
	if r.Status == PinAbsent || r.Status == PinConflict || r.Status == PinUnknown {
		return ErrPinInvalid
	}
	return r.Validate()
}
func (r PinResult) ValidateFindOutcome(err error) error {
	if err != nil {
		if r.Status == "" && r.State == nil {
			return nil
		}
		return ErrPinInvalid
	}
	if r.Status == PinConflict || r.Status == PinUnknown {
		return ErrPinInvalid
	}
	return r.Validate()
}
func (q PinMutationQuery) Validate() error {
	if (PinQuery{q.Identity, q.PinID}).Validate() != nil || !validatePinMutation(q.Mutation) {
		return ErrPinInvalid
	}
	switch q.Operation {
	case PinCreateOperation, PinRenewOperation, PinReleaseOperation:
		return nil
	}
	return ErrPinInvalid
}

// Validate 核对精确操作及不可变后状态，不与当前 pin 比较，不判断当前有效性。
func (r PinMutationResult) Validate(q PinMutationQuery) error {
	if q.Validate() != nil {
		return ErrPinInvalid
	}
	switch r.Status {
	case PinMutationAbsent, PinMutationUnknown:
		if r.Receipt != nil {
			return ErrPinInvalid
		}
	case PinMutationFound:
		v := r.Receipt
		if v == nil || v.Identity != q.Identity || v.PinID != q.PinID || v.Mutation != q.Mutation || v.Operation != q.Operation || v.Version == 0 || v.OccurredAt.IsZero() || v.RequestDigest != q.Mutation.Digest || v.Result.Validate() != nil {
			return ErrPinInvalid
		}
		s := v.Result.State
		if s == nil || s.Identity != q.Identity || s.PinID != q.PinID || s.Version != v.Version {
			return ErrPinInvalid
		}
		if q.Operation == PinReleaseOperation {
			if v.Result.Status != PinReleased {
				return ErrPinInvalid
			}
		} else if v.Result.Status != PinActive {
			return ErrPinInvalid
		}
		if q.Operation == PinCreateOperation && (s.Mutation != q.Mutation || v.Version != 1) {
			return ErrPinInvalid
		}
	default:
		return ErrPinInvalid
	}
	return nil
}

// ValidateOutcome 要求 unknown 伴随 ErrPinUnknown；其他错误只返回零值结果。
func (r PinMutationResult) ValidateOutcome(q PinMutationQuery, err error) error {
	if err != nil {
		if r.Status == "" && r.Receipt == nil {
			return q.Validate()
		}
		if r.Status == PinMutationUnknown && errors.Is(err, ErrPinUnknown) {
			return r.Validate(q)
		}
		return ErrPinInvalid
	}
	if r.Status == PinMutationUnknown {
		return ErrPinInvalid
	}
	return r.Validate(q)
}
