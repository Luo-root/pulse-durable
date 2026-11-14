// 本文件相当于摘要算法的接口与只读路由表。核心压缩/恢复流程只查询
// handler；每个 Kind 自己解释 Payload、推导候选与建立恢复索引。新增算法
// 只需提供一个实现并在构造处注册，不给 RunRecord 增加能力或格式版本号。
package durable

import "fmt"

// historySummaryHandler 只负责选择一种 Payload 解释，不能写 Store 或授予
// 删除权限。事实来源与事务 CAS 仍由公共规划/提交路径验证。
type historySummaryHandler interface {
	decode([]byte) (decodedHistorySummary, error)
}

// decodedHistorySummary 将算法差异组合进通用流程。候选阶段可以读取尚未
// 应用的合成事实；恢复阶段还必须验证稳定身份和保留见证，不能混用两者。
type decodedHistorySummary interface {
	applicationRevision() uint64
	withApplication(uint64) ([]byte, error)
	validateRecovery() error
	candidates(RunRecord, *compressionCandidateIndex) ([]CompressionObjectRef, error)
	restore(RunRecord, *recoverySummaryIndex) error
}

type historySummaryRegistration struct {
	kind         string
	handler      historySummaryHandler
	compressible bool
}

// historySummaryRegistry 创建时复制注册集合，之后只读。调用方不能拿注册
// 切片的别名更换算法；无可变全局 Register，避免运行中解释发生变化。
type historySummaryRegistry struct {
	handlers     map[string]historySummaryHandler
	compressible map[string]bool
}

func newHistorySummaryRegistry(registrations ...historySummaryRegistration) (*historySummaryRegistry, error) {
	r := &historySummaryRegistry{handlers: make(map[string]historySummaryHandler, len(registrations)), compressible: make(map[string]bool, len(registrations))}
	for _, registration := range registrations {
		if !validSemanticText(registration.kind) || registration.handler == nil || nilValue(registration.handler) {
			return nil, fmt.Errorf("%w: invalid summary handler registration", ErrInvalid)
		}
		if _, duplicate := r.handlers[registration.kind]; duplicate {
			return nil, fmt.Errorf("%w: duplicate summary handler registration", ErrConflict)
		}
		r.handlers[registration.kind] = registration.handler
		r.compressible[registration.kind] = registration.compressible
	}
	return r, nil
}

// compressionEnabled 只读注册表的能力标记。未注册 Kind 的零值不等于
// 只读证据：调用方仍须先 decode；未知或损坏 payload 必须继续失败。
// 历史物化 marker 注册为不可压缩，不能借这条分支获得删除或改写权限。
func (r *historySummaryRegistry) compressionEnabled(kind string) bool {
	return r != nil && r.compressible[kind]
}

func (r *historySummaryRegistry) decode(summary HistorySummary) (decodedHistorySummary, error) {
	if r == nil {
		return nil, fmt.Errorf("%w: summary handler registry is missing", ErrUnsupportedCapability)
	}
	handler, known := r.handlers[summary.Kind]
	if !known {
		return nil, fmt.Errorf("%w: summary kind %q has no registered semantics", ErrUnsupportedCapability, summary.Kind)
	}
	return handler.decode(summary.Payload)
}

// 固定的启动装配点；map 不导出、不在运行中增删。新增 Kind 只在这里注册，
// 规划、恢复、普通操作与专用提交共用同一实现，不维护平行的 Kind switch。
var defaultHistorySummaryRegistry = func() *historySummaryRegistry {
	r, err := newHistorySummaryRegistry(
		historySummaryRegistration{RetryPrefixSummaryKind, retryPrefixSummaryHandler{}, true},
		historySummaryRegistration{MessagePrefixSummaryKind, messagePrefixSummaryHandler{}, true},
		historySummaryRegistration{historyEvidenceSummaryKind, historyEvidenceSummaryHandler{}, false},
	)
	if err != nil {
		panic(err) // 仅静态内置注册表的编程错误；不处理来自持久数据的输入。
	}
	return r
}()
