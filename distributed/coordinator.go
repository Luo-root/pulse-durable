package distributed

import (
	"context"
	"time"

	durable "github.com/Luo-root/pulse-durable"
)

type Status string

const (
	Queued    Status = "queued"
	Running   Status = "running"
	Blocked   Status = "blocked"
	Succeeded Status = "succeeded"
	Failed    Status = "failed"
	Canceled  Status = "canceled"
)

// Snapshot 是权威观测结果，绝不是执行许可。
// Reason 是稳定且已清理的代码，绝不会包含 codec 错误或业务负载文本。
type Snapshot struct {
	Task       Task
	Status     Status
	Epoch      int64
	Revision   uint64
	LeaseUntil time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
	Reason     string
}
type SubmitResult struct {
	Snapshot     Snapshot
	Deduplicated bool
}
type Worker struct {
	ID     string
	BootID string
}

// Token 对每次 claim 都不可变：包含新的随机 >=128 位 LeaseID、boot，以及正向单调递增的 Epoch。
// Namespace 始终是所有权身份的一部分。
type Token struct {
	Identity Identity
	Worker   Worker
	LeaseID  string
	Epoch    int64
}
type ClaimRequest struct {
	Namespace    string
	Worker       Worker
	Capabilities []Route
	Limit        int
	TTL          time.Duration
	// LeaseIDs 由调用方生成，彼此独立且随机长度 >=128 位；每个请求槽位一个
	//（len 必须等于 Limit）。返回的行只使用其中一个 ID；未填充的 ID 保持未使用。
	// 提交结果未知时必须保留全部 ID。
	LeaseIDs []string
}
type Claim struct {
	Snapshot Snapshot
	Token    Token
}

// ClaimResolution 成功时恰好包含 Claim 或 Closed 之一。Closed 是持久墓碑：
// 任何延迟到达的 Claim 都不能再以该 namespace/ID 提交。
// ResolveClaim 会与进行中的 Claim 串行化；对于同一个 worker boot 它是幂等的，
// 且只返回当前仍有效的 claim。错误（包括未知结果或超时）并不等于 Closed；
// 必须保留容量预留，并解析同一个 ID。
// 开始任何执行前，Renew 仍必须确认所有权。FindClaim 只能用于观测，
// 不能证明不存在的 claim 之后绝不会提交。
type ClaimResolution struct {
	Claim  *Claim
	Closed bool
}

type Mutation struct {
	ID     string
	Digest string
}
type InitializeRequest struct {
	Token    Token
	Record   durable.RunRecord
	Mutation Mutation
}
type FinishRequest struct {
	Token            Token
	ExpectedRevision uint64
	Outcome          durable.Outcome
	Mutation         Mutation
}
type ListRequest struct {
	Namespace  string
	Status     Status
	Limit      int
	AfterRunID string
}

// Coordinator 是未来持久化后端的契约，不是实现。
// 所有所有权操作都必须原子锁定权威行，在获取锁之后采样数据库时间，检查
// running+boot+lease+epoch+未过期条件，并在同一事务中修改。客户端时钟和此前读取的 token
// 都不能授权写入。
//
// Submit 按 (namespace,run_id) 只插入一次 queued/epoch=0；冲突时通过 CompareSubmission
// 比较规范化不可变字段和 digest，并保留全部 status/record/lease 状态。
// RunID 和终态行不会自动回收。
// Claim 只接受精确 capability，数量最多为 Limit，目标为 queued 或已过期的 running；
// 它递增 epoch（拒绝溢出）、分配新 lease 并保留 record。
// 每个请求槽位在 Claim 前都提供自己的新 LeaseID。后端必须拒绝复用的 ID，包括重试；
// 结果不确定的请求不得重新 Claim。
// 不确定的 Claim 必须使用每个原始 lease ID 调用 FindClaim 查询；在 Renew 重新确认所有权前
// 不得启动执行。绝不能使用新 ID 重放。每次写入都可能返回 ErrUnknown：停止操作、读取权威状态，
// 不得推断业务失败，也不得自动重放。所有 mutation 都有稳定的 ID/digest。
//
// LoadOwned 只有在有效所有权下记录不存在时才返回 ErrNotFound。
// ErrInvalidRecord 只有在权威所有权/时钟检查成功后，才表示确定性的记录损坏；其他错误不允许 Block。
// InitializeOwned 只发布初始 revision=1 checkpoint。ApplyOwned 是唯一普通 checkpoint 转换：
// 发布 batch 前，原子地对所有权加 fencing，并匹配权威 revision 与 chain head。
// Finish 只接受 Runtime OutcomeCompleted/OutcomeFailed 及匹配的最终 revision；runtime 必须先确认
// 所有根输出/最终失败。它原子地设置不可变的 succeeded/failed。Cancel 与 Finish 在同一把锁下竞争，
// 并幂等地返回已有终态。Block 为不兼容/损坏的 claim 使用已清理的 reason，避免无限 reclaim。
// Release 保留 epoch/record，只有在本地执行和进行中的保存都退出后才重新排队。
// Close 只清理本地资源，绝不是隐式 Release。读取不会续租。
// 实现必须执行 namespace 授权、有界调用/记录、每次写入的 fencing 和 retention，
// 不能适配一个没有 fencing 的旧 Store。
type Coordinator interface {
	Submit(context.Context, Task) (SubmitResult, error)
	Get(context.Context, Identity) (Snapshot, error)
	List(context.Context, ListRequest) ([]Snapshot, error)
	Claim(context.Context, ClaimRequest) ([]Claim, error)
	FindClaim(context.Context, string, Worker, string) ([]Claim, error)
	ResolveClaim(context.Context, string, Worker, string) (ClaimResolution, error)
	Renew(context.Context, Token, time.Duration) (time.Time, error)
	// DropOwnedProjection 只丢弃与该精确 token 对应的本地缓存状态。
	// 它是幂等的，不执行数据库操作，也绝不会释放 lease。
	DropOwnedProjection(Token)
	LoadOwned(context.Context, Token) (durable.RunRecord, error)
	InitializeOwned(context.Context, InitializeRequest) error
	ApplyOwned(context.Context, Token, Mutation, durable.ChangeBatch) (durable.CommitAck, error)
	Finish(context.Context, FinishRequest) (Snapshot, error)
	Release(context.Context, Token, Mutation) error
	Block(context.Context, Token, string, Mutation) error
	Cancel(context.Context, Identity, Mutation) (Snapshot, error)
}

// PullPolicy 限制接纳，不限制数据库所有权。Active 包括待处理 claim、未确认的提交、
// 正在执行的任务，以及在所有保存操作退出前处于 draining 状态的工作。
// 必须在本地串行化 reservation + Claim：两个并发 poller 重新计算同一批空闲槽位不构成接纳。
// Paused 会停止新 claim，但仍续租已有工作；Draining 还表示关闭意图，绝不是业务取消。
type PullPolicy struct {
	Capacity int
	BatchMax int
	Paused   bool
	Draining bool
}

func (p PullPolicy) Limit(active int) (int, error) {
	if p.Capacity < 1 || p.BatchMax < 1 || p.BatchMax > 1024 || active < 0 || active > p.Capacity {
		return 0, ErrInvalid
	}
	if p.Paused || p.Draining {
		return 0, nil
	}
	free := p.Capacity - active
	if free > p.BatchMax {
		free = p.BatchMax
	}
	return free, nil
}
