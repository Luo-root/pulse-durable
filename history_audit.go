package durable

import "context"

// HistoryAuditor 是可选的完整保留历史审计能力，不属于普通历史读取
// 或快照合成的必经路径。第三方及其他后端无须实现此接口。
// 审计只校验一次一致观察所引用的全部保留历史，不执行业务代码，
// 不创建 pin，不采纳孤立对象，也不回收或修复数据。
type HistoryAuditor interface {
	AuditHistory(context.Context, AuditHistoryRequest) (HistoryAuditResult, error)
}

// AuditHistoryRequest 选择需要完整审计的运行。
type AuditHistoryRequest struct {
	RunID string
}

// HistoryAuditResult 仅在成功时证明 State 所描述的保留历史已完整校验。
// 后续写入不包含在此结果中；计数不包括未提交孤立对象。
// BatchBytes 是保留批文件的总字节数，包含其寻址元数据。
type HistoryAuditResult struct {
	State      StorageState
	Snapshots  uint64
	Batches    uint64
	BatchBytes uint64
}
