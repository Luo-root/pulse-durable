package durable

import (
	"context"
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/Luo-root/pulse"
)

// AttrRunID 是稳定 Durable 运行身份的观测属性键，值来自 RunOptions.RunID。
// 宿主在 Sink 装配时补充；不把 Pulse 的进程内 Execution ID 当作持久身份。
const AttrRunID = "durable.run_id"

// Runtime 将 Pulse 的可靠检查点桥接到 Store。可并发运行不同 RunID。
// 同一 RunID 的 Run/Resume 在整个 Start/Wait 期间持有独占会话。
type Runtime struct {
	store  Store
	codecs *Codecs
}

// New 创建 Runtime。Store 和 Codecs 必须非 nil；不触发存储或业务调用。
func New(store Store, codecs *Codecs) (*Runtime, error) {
	if nilValue(store) || codecs == nil {
		return nil, fmt.Errorf("durable: store and codecs are required")
	}
	return &Runtime{store: store, codecs: codecs}, nil
}

// RunOptions 是本次执行的宿主契约。RunID 和 Version 显式必填。
// RunID 和 Version 是有效 UTF-8 文本；非法文本在访问 Store 之前按参数错误拒绝。
// Version 由宿主定义，不要求 SemVer 格式。
// 函数、闭包、固定绑定值、Aspect 顺序/参数/语义变化由业务 Version 负责。
type RunOptions struct {
	RunID       string
	Version     string
	ExecOptions []pulse.ExecOption
	// OnEvent 接收恢复事实，不含值、编码字节或 Pulse 业务耗时。
	// 回调须并发安全；同步返回错误或 panic 会停止本次运行，已确认记录不回滚。
	// 恢复回调在相应图启动前发出，不得同步等待同一个执行完成。
	OnEvent func(Event) error
	// BeforeStart 在恢复计划装配后、Start 前调用。宿主可 SubscribeStream 并启动读取 goroutine；
	// goroutine 须等待执行启动后再 Receive，并在执行取消时退出。回调错误或 panic 会取消执行、释放会话。
	BeforeStart func(*pulse.Execution) error
	// RequireDelta explicitly selects DeltaSession before any business starts.
	// Missing capability is rejected; Apply errors never fall back to Save.
	// The initial seed uses one full Save. Final node validation may materialize
	// a complete proposal, while stream/message hot checkpoints use typed deltas.
	RequireDelta bool
}

// Event 是 Durable 自身的恢复事实，不伪造 Pulse Waiting/Running 或业务耗时。
type Event struct {
	RunID, Scope, Graph, Path, NodeID string
	Kind                              EventKind
}

// EventKind 是 Durable 事件种类。
type EventKind string

// EventRestored 表示已确认的节点成果被选入这次恢复计划。
const EventRestored EventKind = "restored"

// Run 为新 RunID 创建记录并阻塞到本次执行结束。已有记录返回 ErrConflict。
// 每次调用创建新 Execution；失败时尽可能返回该执行，供 Snapshot/Err 查询。
// 业务副作用仍需调用方幂等、事务或 outbox；本库不承诺 exactly-once。
func (r *Runtime) Run(ctx context.Context, g *pulse.Graph, in pulse.Inputs, opts RunOptions) (*pulse.Execution, error) {
	return r.execute(ctx, g, in, opts, false)
}

// Resume 从权威记录恢复，调用方须重新提供相同输入及业务版本。
// 仅中断、宿主取消或可靠失败留下的 active 尝试可继续；最终真实失败的根尝试
// 已耗尽该 RunID 的预算，Resume 不为它分配新预算。
// 根图已有最终业务失败时返回 *pulse.CheckpointScopeFailure；返回的 Execution
// 尚未 Start，不发恢复事件或调用 BeforeStart，ReadOutput 不提供结果。
func (r *Runtime) Resume(ctx context.Context, g *pulse.Graph, in pulse.Inputs, opts RunOptions) (*pulse.Execution, error) {
	return r.execute(ctx, g, in, opts, true)
}

func (r *Runtime) execute(ctx context.Context, g *pulse.Graph, in pulse.Inputs, opts RunOptions, resume bool) (execution *pulse.Execution, err error) {
	return r.executeSession(ctx, g, in, opts, resume, nil, nil)
}

func (r *Runtime) executeSession(ctx context.Context, g *pulse.Graph, in pulse.Inputs, opts RunOptions, resume bool, claimed ClaimedSession, result *Outcome) (execution *pulse.Execution, err error) {
	outcome := OutcomeRejected
	var state *runState
	started, waited := false, false
	defer func() {
		if result == nil {
			return
		}
		if state != nil && waited {
			outcome = state.executionOutcome(execution, err)
		} else if started {
			outcome = OutcomeInfrastructure
		}
		if result != nil {
			*result = outcome
		}
	}()
	if err := checkContext(ctx); err != nil {
		if ctx != nil && ctx.Err() != nil {
			outcome = OutcomeInterrupted
		}
		return nil, err
	}
	if r == nil || opts.RunID == "" || opts.Version == "" {
		return nil, fmt.Errorf("durable: runtime, run ID and business version are required")
	}
	if !utf8.ValidString(opts.RunID) {
		return nil, fmt.Errorf("durable: run ID must be valid UTF-8")
	}
	if !utf8.ValidString(opts.Version) {
		return nil, fmt.Errorf("durable: business version must be valid UTF-8")
	}
	if result != nil {
		if nilValue(claimed) {
			return nil, fmt.Errorf("durable: claimed session is required")
		}
		id, err := claimedRunID(claimed)
		if err != nil {
			return nil, err
		}
		if id == "" || !utf8.ValidString(id) || id != opts.RunID {
			return nil, fmt.Errorf("%w: claimed session run ID mismatch or invalid", ErrConflict)
		}
	}
	d, err := inspectDefinition(g, r.codecs.snapshot())
	if err != nil {
		return nil, err
	}
	var session Session = claimed
	if result == nil {
		session, err = acquire(r.store, ctx, opts.RunID)
		if err != nil {
			if errors.Is(err, ErrBusy) {
				return nil, err
			}
			return nil, checkpointError("acquire run", err)
		}
		defer func() { err = errors.Join(err, checkpointError("close session", closeSession(session))) }()
	}
	var delta DeltaSession
	if opts.RequireDelta {
		var ok bool
		delta, ok = session.(DeltaSession)
		if !ok || nilValue(delta) {
			return nil, ErrUnsupportedCapability
		}
	}
	outcome = OutcomeInfrastructure
	stored, loadErr := load(session, ctx)
	if resume {
		if loadErr != nil {
			if errors.Is(loadErr, ErrNotFound) {
				outcome = OutcomeRejected
				return nil, loadErr
			}
			return nil, checkpointError("load run", loadErr)
		}
		if stored.RunID != opts.RunID || stored.Version != opts.Version || stored.Definition != d.fingerprint {
			outcome = OutcomeRejected
			return nil, fmt.Errorf("%w: run, business version or definition mismatch", ErrConflict)
		}
	} else {
		if loadErr == nil {
			outcome = OutcomeRejected
			return nil, fmt.Errorf("%w: run already exists", ErrConflict)
		}
		if !errors.Is(loadErr, ErrNotFound) {
			return nil, checkpointError("load run", loadErr)
		}
		id, idErr := randomID()
		if idErr != nil {
			return nil, idErr
		}
		stored = RunRecord{Format: RecordFormat, RunID: opts.RunID, Version: opts.Version,
			Definition: d.fingerprint, RootScope: id, Scopes: make(map[string]ScopeRecord), Attempts: make(map[string]AttemptRecord)}
	}
	state = &runState{session: session, delta: delta, def: d, record: cloneRecord(stored), event: opts.OnEvent}
	execOptions := append([]pulse.ExecOption(nil), opts.ExecOptions...)
	execOptions = append(execOptions, pulse.WithExecCheckpoint(stored.RootScope, state.hooks()))
	outcome = OutcomeRejected
	execution, err = g.NewExecution(ctx, in, execOptions...)
	if err != nil {
		return nil, err
	}
	defer execution.Cancel()
	outcome = OutcomeInfrastructure
	inputHash, err := d.inputsFingerprint(d.root, execution.CheckpointInputs())
	if err != nil {
		return execution, checkpointError("encode inputs", err)
	}
	if resume {
		if stored.Inputs != inputHash {
			outcome = OutcomeRejected
			return execution, fmt.Errorf("%w: input fingerprint mismatch", ErrConflict)
		}
		if err := state.validateLoaded(); err != nil {
			outcome = OutcomeRejected
			return execution, checkpointError("validate restore", err)
		}
		if err := state.reconcileFailedStreams(ctx); err != nil {
			return execution, checkpointError("reconcile failed streams", err)
		}
		// No restore events, host registrations or Start for a scope whose
		// final business failure is already authoritative. This Execution
		// remains pending; the returned error, not Snapshot, is the verdict.
		if err := state.scopeFailure(stored.RootScope); err != nil {
			outcome = OutcomeFailed
			return execution, err
		}
		records, err := state.decodeScope(stored.RootScope)
		if err != nil {
			return execution, checkpointError("decode root", err)
		}
		if err := execution.RestoreNodes(records); err != nil {
			return execution, checkpointError("restore root", err)
		}
		if err := state.restored(stored.RootScope, records); err != nil {
			return execution, checkpointError("recovery event", err)
		}
	} else {
		proposed := cloneRecord(stored)
		proposed.Inputs = inputHash
		proposed.Scopes[stored.RootScope] = ScopeRecord{Graph: d.root.graph, Path: d.root.path, Inputs: inputHash, Nodes: make(map[string]NodeRecord)}
		if err := state.save(ctx, proposed); err != nil {
			return execution, checkpointError("initialize run", err)
		}
	}
	outcome = OutcomeInterrupted
	if opts.BeforeStart != nil {
		if err := beforeStart(opts.BeforeStart, execution); err != nil {
			return execution, err
		}
	}
	if err := execution.Start(); err != nil {
		if result != nil && checkpointOrInspectionFailure(err) {
			outcome = OutcomeInfrastructure
		}
		return execution, err
	}
	started = true
	err = execution.Wait()
	waited = true
	return execution, err
}

func beforeStart(callback func(*pulse.Execution) error, execution *pulse.Execution) (err error) {
	// 按正常返回识别兼容模式的 panic(nil)；operationPanic 保留 error 原因并脱敏正文。
	returned := false
	defer func() {
		if !returned {
			err = operationPanic("before start callback", recover())
		}
	}()
	err = callback(execution)
	returned = true
	if err != nil {
		return &operationError{op: "before start callback", cause: err}
	}
	return nil
}

// checkpointError 只在可靠操作边界调用；参数、版本和运行身份冲突仍返回原分类。
func checkpointError(op string, err error) error {
	if err == nil {
		return nil
	}
	return &pulse.CheckpointError{Operation: op, Err: err}
}

func acquire(store Store, ctx context.Context, runID string) (session Session, err error) {
	// 按正常返回识别兼容模式的 panic(nil)；Store panic 沿原契约不保留原值。
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			session, err = nil, fmt.Errorf("durable: store acquire panicked")
		}
	}()
	session, err = store.Acquire(ctx, runID)
	returned = true
	if err != nil {
		return nil, &operationError{op: "store acquire", cause: err}
	}
	if nilValue(session) {
		return nil, fmt.Errorf("durable: store returned a nil session")
	}
	return session, nil
}

func load(session Session, ctx context.Context) (record RunRecord, err error) {
	// 按正常返回识别兼容模式的 panic(nil)；Store panic 沿原契约不保留原值。
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			record, err = RunRecord{}, fmt.Errorf("durable: store load panicked")
		}
	}()
	record, err = session.Load(ctx)
	returned = true
	if err != nil {
		return RunRecord{}, &operationError{op: "store load", cause: err}
	}
	return cloneRecord(record), nil
}

func closeSession(session Session) (err error) {
	// 按正常返回识别兼容模式的 panic(nil)；Store panic 沿原契约不保留原值。
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			err = fmt.Errorf("durable: session close panicked")
		}
	}()
	err = session.Close()
	returned = true
	if err != nil {
		return &operationError{op: "session close", cause: err}
	}
	return nil
}
