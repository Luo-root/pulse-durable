package distributed

import (
	"context"
	"time"

	durable "github.com/Luo-root/pulse-durable"
)

// TaskCatalog 负责任务身份目录、状态观测及不依赖执行租约的取消操作，
// 不负责租约或 checkpoint 写入。
type TaskCatalog interface {
	Submit(context.Context, Task) (SubmitResult, error)
	Get(context.Context, Identity) (Snapshot, error)
	List(context.Context, ListRequest) ([]Snapshot, error)
	Cancel(context.Context, Identity, Mutation) (Snapshot, error)
}

// LeaseCoordinator 只负责领取、续租、核对和释放执行所有权。
type LeaseCoordinator interface {
	Claim(context.Context, ClaimRequest) ([]Claim, error)
	FindClaim(context.Context, string, Worker, string) ([]Claim, error)
	ResolveClaim(context.Context, string, Worker, string) (ClaimResolution, error)
	Renew(context.Context, Token, time.Duration) (time.Time, error)
	Release(context.Context, Token, Mutation) error
}

// OwnedCheckpointStore 只负责已领取运行的检查点和终态提交。
// 它不创建任务、不领取任务，也不自行续租。
type OwnedCheckpointStore interface {
	DropOwnedProjection(Token)
	LoadOwned(context.Context, Token) (durable.RunRecord, error)
	InitializeOwned(context.Context, InitializeRequest) error
	ApplyOwned(context.Context, Token, Mutation, durable.ChangeBatch) (durable.CommitAck, error)
	Finish(context.Context, FinishRequest) (Snapshot, error)
	Block(context.Context, Token, string, Mutation) error
}

// Coordinator 保留为兼容 facade；新实现和测试应优先依赖上面的最小责任接口。
