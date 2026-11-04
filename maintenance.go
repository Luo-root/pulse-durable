package durable

import (
	"context"
	"errors"
)

var (
	// ErrInvalidRecord 表示保留的记录或其历史损坏。
	ErrInvalidRecord = errors.New("durable: invalid record")
	// ErrUnsupportedCapability 表示后端缺少请求的能力。
	ErrUnsupportedCapability = errors.New("durable: unsupported capability")
	// ErrHistoryExpired 仅用于被显式保留操作移除的历史，
	// 绝不用于保留链中缺失或损坏的组件。
	ErrHistoryExpired = errors.New("durable: requested history expired")
	// ErrPinned 表示受保护的历史阻止回收。
	ErrPinned = errors.New("durable: history is pinned")
	// ErrLimitExceeded 表示超出配置的工作量或表示形式预算。
	ErrLimitExceeded = errors.New("durable: maintenance limit exceeded")
)

// Maintenance 描述存储级历史与物理快照合成。
// 这些操作绝不执行业务代码。普通业务写入仍使用
// Session。所有官方存储均实现这些语义，包括
// 不提供持久性的 MemoryStore 参考实现。第三方存储可不实现此接口。
// 分布式实现还要求在自身入口使用受命名空间限定的身份，
// 并校验数据库维护授权。
//
// 合成必须保留所有保留的历史修订及其链。
// 它不是保留操作：只有独立授权的保留操作可以
// 回收旧表示。发布后发生的错误可能意味着结果未知；
// 计划下一次调用前，检查代际及基线锚点。
type Maintenance interface {
	InspectStorage(context.Context, string) (StorageState, error)
	LoadRevision(context.Context, string, uint64) (RevisionImage, error)
	CompactStorage(context.Context, CompactStorageRequest) (StorageState, error)
}

// StorageState 是一次原子观测，不是对后续操作的许可。
// Generation 从 1 开始，仅在发布表示形式时改变。
// Apply 改变 HeadRevision 和 HeadChain，不改变 Generation。摘要验证
// 字节与顺序的真实性，但不授权调用方。
type StorageState struct {
	RunID        string
	Generation   uint64
	BaseRevision uint64
	BaseChain    [32]byte
	HeadRevision uint64
	HeadChain    [32]byte
}

// RevisionImage 包含请求的精确修订及其已提交链锚点的隔离投影，
// 绝不替换为更新记录。
// LoadRevision 是原子读取，不创建持久固定点，因此
// 不保证后续调用仍可加载相同历史。
type RevisionImage struct {
	Record RunRecord
	Chain  [32]byte
}

// CompactStorageRequest 在 ThroughRevision（大于 0）处合成快照。
// ExpectedGeneration 来自 InspectStorage，且必须为正数。
// 本地后端可在已有独占存储锁内合成，
// 会话活跃时返回 ErrBusy。分布式后端捕获一个
// 固定路径，在发布锁外构建，且仅在表示代际未变时通过 CAS 发布。
// MemoryStore 同样在锁外构建。
// 并发业务后缀保持完整。低于当前基线或高于当前头的修订号
// 属于冲突；历史读取使用 LoadRevision。
// 取消的构建绝不发布；发布后取消属于
// 结果未知。构建绝不隐式回收保留历史。
type CompactStorageRequest struct {
	RunID              string
	ThroughRevision    uint64
	ExpectedGeneration uint64
}
