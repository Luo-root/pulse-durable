package durable

import (
	"context"
	"errors"
)

// ErrNotFound 表示 RunID 尚无记录。
var ErrNotFound = errors.New("durable: run not found")

// ErrBusy 表示同一 RunID 已有活动会话；Acquire 不等待它释放。
var ErrBusy = errors.New("durable: run busy")

// ErrConflict 表示 CAS 修订号、记录身份或已有运行发生冲突。
var ErrConflict = errors.New("durable: revision or run conflict")

// RecordFormat 是新建 RunRecord 的格式版本。版本 2 包含增量流、消息预算和显式状态；
// Runtime 仍读取不含这些字段的普通版本 1，后续写入升级为 2。旧 Runtime 拒绝版本 2。
const RecordFormat = 2

// Store 为一个 RunID 提供独占会话。实现须并发安全，且不同 RunID 可独立使用。
// Acquire 不等待已占用的 RunID 释放，检测到占用时返回 ErrBusy；Close 释放所有权。
// 后端获取连接与网络检测可能受容量和 context 限制，具体等待边界由后端声明。
// 跨进程互斥和持久性由具体后端声明，MemoryStore 不提供这两项保证。
type Store interface {
	Acquire(context.Context, string) (Session, error)
}

// Session 是同一 RunID 的独占读写会话。
// Load 返回经完整性验证的隔离副本，无检查点时返回 ErrNotFound。
// Initialize 仅发布 Revision=1 的初始空执行状态，已有检查点时返回 ErrConflict。
// Apply 是唯一普通写入口，原子核对身份、连续修订号和精确对象前置条件。
// 写入错误可能表示已提交；调用方必须停止，从新会话 Load 裁决，不能盲重试。
// 物理链锚点由会话加载、校验和维护，不属于 ChangeHeader。
// Close 幂等并等待在途操作退出；关闭后拒绝新操作。
type Session interface {
	Load(context.Context) (RunRecord, error)
	Initialize(context.Context, RunRecord) error
	Apply(context.Context, ChangeBatch) (CommitAck, error)
	Close() error
}

// RunRecord 只含可序列化数据；后端维护基础快照、增量与权威 head。
// Definition 与 Inputs 是 SHA-256 指纹；记录不保存业务错误文本。
// Scopes/Attempts 中的键是持久随机身份，不能由运行路径或内存计数重新派生。
type RunRecord struct {
	Format     int                             `json:"format"`
	RunID      string                          `json:"run_id"`
	Revision   uint64                          `json:"revision"`
	Version    string                          `json:"version"`
	Definition string                          `json:"definition"`
	Inputs     string                          `json:"inputs"`
	RootScope  string                          `json:"root_scope"`
	Scopes     map[string]ScopeRecord          `json:"scopes"`
	Attempts   map[string]AttemptRecord        `json:"attempts"`
	Streams    map[string]StreamRecord         `json:"streams,omitempty"`
	Messages   map[string]MessageAttemptRecord `json:"messages,omitempty"`
	Managed    map[string]ManagedRecord        `json:"managed,omitempty"`
}

// ScopeRecord 保存一层图在某个持久作用域中的身份、输入指纹与完整节点成果。
// 子图的作用域直接使用其父节点当前最深 Attempt 的身份。
type ScopeRecord struct {
	Graph  string                `json:"graph"`
	Path   string                `json:"path"`
	Inputs string                `json:"inputs"`
	Nodes  map[string]NodeRecord `json:"nodes"`
	// Bindings 区分稳定 In 绑定指纹与格式 1 的已到达输入值指纹。
	Bindings bool `json:"bindings,omitempty"`
}

// AttemptState 是尝试的持久状态。
type AttemptState string

const (
	// AttemptActive 可在恢复时认回；内层成功在根提交之前仍处于此状态。
	AttemptActive AttemptState = "active"
	// AttemptFailed 使整个后代失效；记录保留但不能复用。
	AttemptFailed AttemptState = "failed"
	// AttemptConfirmed 与完整节点检查点在同一次 Save 中确认。
	AttemptConfirmed AttemptState = "confirmed"
)

// AttemptRecord 记录某节点根尝试或内置 Retry 层的游标。
// Boundary 为空且 Limit 为一表示节点根尝试；最终真实失败后不能通过 Resume 获得新预算。
// Retry 使用固定切面位置和正数 Limit，Parent 指向本层图 Scope 或外层 Attempt。
type AttemptRecord struct {
	Parent   string       `json:"parent"`
	NodeID   string       `json:"node_id"`
	Boundary string       `json:"boundary"`
	Index    int          `json:"index"`
	Limit    int          `json:"limit"`
	State    AttemptState `json:"state"`
}

// NodeRecord 是完整节点链确认后的终态，Attempt 必须指向该节点的已确认根尝试。
// Reason 仅为 completed/skipped；Outputs 恰好覆盖节点全部 Provides。
type NodeRecord struct {
	NodeID  string        `json:"node_id"`
	Attempt string        `json:"attempt"`
	Reason  string        `json:"reason"`
	Outputs []ValueRecord `json:"outputs"`
}

// ValueRecord 是一个非流式键的编码结果。State 仅为 ready/skipped。
// skipped 没有 Data/Digest；ready 的 Data 由 TypeID/Version 指定的 Codec 解释，
// Digest 是该字节串的 SHA-256 校验和，用于拒绝缺失或损坏的编码。
type ValueRecord struct {
	Name    string `json:"name"`
	TypeID  string `json:"type_id"`
	Version string `json:"version"`
	State   string `json:"state"`
	Digest  string `json:"digest,omitempty"`
	Data    []byte `json:"data,omitempty"`
	// Stream 引用独立流记录，与 Data/Digest 互斥；TypeID/Version 描述消息元素。
	Stream *StreamDescriptorRecord `json:"stream,omitempty"`
}

// CloneRecord returns an isolated deep copy, preserving nil versus empty maps,
// slices and encoded Data. Backends must not use a JSON round trip for copies:
// omitempty normalization would destroy exact ChangeBatch preconditions.
func CloneRecord(r RunRecord) RunRecord { return cloneRecord(r) }

func cloneRecord(r RunRecord) RunRecord {
	out := r
	if r.Scopes != nil {
		out.Scopes = make(map[string]ScopeRecord, len(r.Scopes))
		for id, s := range r.Scopes {
			s.Nodes = cloneNodes(s.Nodes)
			out.Scopes[id] = s
		}
	}
	if r.Attempts != nil {
		out.Attempts = make(map[string]AttemptRecord, len(r.Attempts))
		for id, a := range r.Attempts {
			out.Attempts[id] = a
		}
	}
	out.Streams = cloneStreams(r.Streams)
	if r.Messages != nil {
		out.Messages = make(map[string]MessageAttemptRecord, len(r.Messages))
		for id, message := range r.Messages {
			out.Messages[id] = message
		}
	}
	if r.Managed != nil {
		out.Managed = make(map[string]ManagedRecord, len(r.Managed))
		for id, managed := range r.Managed {
			managed.State = cloneValues(managed.State)
			managed.Values = cloneValues(managed.Values)
			out.Managed[id] = managed
		}
	}
	return out
}

func cloneNodes(nodes map[string]NodeRecord) map[string]NodeRecord {
	if nodes == nil {
		return nil
	}
	out := make(map[string]NodeRecord, len(nodes))
	for id, n := range nodes {
		n.Outputs = cloneValues(n.Outputs)
		out[id] = n
	}
	return out
}
