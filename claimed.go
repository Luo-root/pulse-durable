package durable

import (
	"context"
	"errors"
	"fmt"

	"github.com/Luo-root/pulse"
)

// ClaimedSession 是调用方拥有的、已获取的 Session，并带有明确且不可变的 RunID 绑定。
// RunID 必须非空、为有效 UTF-8，且与 RunOptions 匹配。
// 调用方必须阻止同一 session 上的并发执行，直到执行返回并完成协调的 Finish/Release 前持续保有所有权，
// 然后再关闭它。分布式实现必须自行对 Load、Initialize 和 Apply 做栅栏保护；此接口不实现租约或分布式协调。
type ClaimedSession interface {
	Session
	RunID() string
}

// Outcome 是运行时的执行结论，不是协调器持久化的任务状态。
// 只有 Completed 和 Failed 允许执行业务终态 Finish；二者都不表示 Finish 已经执行。
// 随附错误保持既有 Runtime 错误契约。
type Outcome string

const (
	// OutcomeCompleted 表示 Wait 成功且每个根节点都已确认。
	OutcomeCompleted Outcome = "completed"
	// OutcomeFailed 表示最终根业务失败已被权威保存，
	// 包括因该失败已存在而在 Start 前被拒绝恢复。
	OutcomeFailed Outcome = "failed"
	// OutcomeInterrupted 表示未确认业务终态结论。宿主取消和 BeforeStart 失败可以通过恢复重试。
	OutcomeInterrupted Outcome = "interrupted"
	// OutcomeInfrastructure 包括检查点失败和写入结果不确定。
	// 在判断任何尝试写入是否提交前，先重新加载权威状态。
	OutcomeInfrastructure Outcome = "infrastructure"
	// OutcomeRejected 表示调用无效或不兼容；业务未启动。
	OutcomeRejected Outcome = "rejected"
)

// RunClaimed 使用已获取的 session 运行新的 RunID。它执行与 Run 相同的校验和检查点操作，但从不调用 Acquire 或 Close。
// 调用方在返回后继续保有 session，以便协调 Finish/Release。
func (r *Runtime) RunClaimed(ctx context.Context, session ClaimedSession, g *pulse.Graph, in pulse.Inputs, opts RunOptions) (*pulse.Execution, Outcome, error) {
	var outcome Outcome
	e, err := r.executeSession(ctx, g, in, opts, false, session, &outcome)
	return e, outcome, err
}

// ResumeClaimed 通过与 Resume 相同的校验和恢复路径进行恢复，但不获取或关闭 session。
// 已完成的检查点会被复用；最终业务失败不会获得新的重试预算。
// 应检查 Outcome，而不是 pending Snapshot 或 errors.Is(err, context.Canceled)，以决定是否 Finish。
func (r *Runtime) ResumeClaimed(ctx context.Context, session ClaimedSession, g *pulse.Graph, in pulse.Inputs, opts RunOptions) (*pulse.Execution, Outcome, error) {
	var outcome Outcome
	e, err := r.executeSession(ctx, g, in, opts, true, session, &outcome)
	return e, outcome, err
}

func claimedRunID(session ClaimedSession) (id string, err error) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			id, err = "", fmt.Errorf("durable: claimed session RunID panicked")
		}
	}()
	id = session.RunID()
	returned = true
	return id, nil
}

// checkpointOrInspectionFailure 将诊断不确定性视为可靠失败，但不改变返回给调用方的原始错误。
// 跟踪正常返回，使 panic(nil) 在兼容模式下也能被包含。
func checkpointOrInspectionFailure(err error) (failed bool) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			failed = true
		}
	}()
	var checkpoint *pulse.CheckpointError
	failed = errors.As(err, &checkpoint)
	returned = true
	return failed
}

// 仅在 Wait 已等待业务和可靠工作汇合后调用。即使某个兄弟节点的最终失败已保存，检查点失败仍优先。
func (s *runState) executionOutcome(e *pulse.Execution, err error) Outcome {
	// 错误检查可能调用任意用户 As/Unwrap 方法。绝不要在状态锁下运行这些方法，
	// 也绝不要将检查过程中的 panic 解释为已确认的业务结论。
	checkpoint := checkpointOrInspectionFailure(err)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped != nil || checkpoint {
		return OutcomeInfrastructure
	}
	if s.scopeFailure(s.record.RootScope) != nil {
		return OutcomeFailed
	}
	if err == nil && len(s.record.Scopes[s.record.RootScope].Nodes) == len(s.def.graphs[s.def.root].nodes) {
		return OutcomeCompleted
	}
	if e.Snapshot().Graph.Status == pulse.GraphStatusCanceled {
		return OutcomeInterrupted
	}
	// 没有已确认的终态事实：不要将无法解释的核心失败提升为业务最终失败，
	// 即使其错误看起来像取消。
	return OutcomeInfrastructure
}
