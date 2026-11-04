package durable

import "context"

// RevisionReader 提供精确历史修订读取，不承担业务执行或保留授权。
// 后端不支持时应返回 ErrUnsupportedCapability，而不是返回更新修订或伪造空记录。
type RevisionReader interface {
	LoadRevision(context.Context, string, uint64) (RevisionImage, error)
}

// SnapshotInspector 提供当前物理表示的一次一致性观察。
// 返回值不构成后续操作的授权，调用方仍须使用 ExpectedGeneration 做 CAS。
type SnapshotInspector interface {
	InspectStorage(context.Context, string) (StorageState, error)
}

// SnapshotCompactor 只负责物理快照合成，不负责历史回收或业务执行。
type SnapshotCompactor interface {
	CompactStorage(context.Context, CompactStorageRequest) (StorageState, error)
}

// Pin、Retention 和 Derived Replay 的请求/结果类型，
// 必须在各自功能契约冻结后再进入根包。当前切片只拆分已有的物理历史能力，
// 不提前引入未冻结的业务类型。
