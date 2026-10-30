// worker 包运行实验性的、有容量上限的分布式 Durable 工作进程。
// 协调器必须对每个所有者操作实施栅栏保护。本包单元测试
// 不能证明数据库原子性或跨进程安全。
package worker

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"
	"unicode/utf8"

	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
)

// Renewer 应使用独立于 Claim 和检查点流量的专用后端连接池或
// 连接预算。仅靠 goroutine 无法预留数据库容量。
type Renewer interface {
	Renew(context.Context, d.Token, time.Duration) (time.Time, error)
}

// Event 包含身份及固定原因码，绝不包含载荷或原始错误。
// Observe 同步执行，必须快速且并发安全，不得同步调用
// Drain。不安装指标或高基数指标标签。
type Event struct {
	Kind, Reason string
	Worker       d.Worker
	Token        d.Token
	Revision     uint64
	Mutation     d.Mutation
	Duration     time.Duration
}
type Config struct {
	Coordinator                                        d.Coordinator
	Renewer                                            Renewer
	Registry                                           *d.Registry
	Namespace, WorkerID                                string
	Capacity, BatchMax                                 int
	TTL, RenewInterval, OperationTimeout, RenewTimeout time.Duration
	PollMin, PollMax                                   time.Duration
	Observe                                            func(Event)
}

// Worker 只能使用一次。New 创建独立随机的 128 位启动标识。
// Pause/Resume 仅影响准入。Drain 绝不发出业务 Cancel。
type Worker struct {
	cfg                       Config
	identity                  d.Worker
	mu                        sync.Mutex
	started, paused, draining bool
	active                    int
	changed                   chan struct{}
	// pollTimer 是内部确定性测试接口；nil 使用真实定时器。
	pollTimer func(time.Duration) (<-chan time.Time, func())
}

func validText(s string) bool { return len(s) > 0 && len(s) <= d.MaxTextBytes && utf8.ValidString(s) }

func nilCoordinator(c d.Coordinator) bool {
	if c == nil {
		return true
	}
	v := reflect.ValueOf(c)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}

func New(c Config) (*Worker, error) {
	if c.Coordinator == nil || c.Renewer == nil || c.Registry == nil || !validText(c.Namespace) || !validText(c.WorkerID) {
		return nil, d.ErrInvalid
	}
	if nilCoordinator(c.Coordinator) {
		return nil, d.ErrInvalid
	}
	if _, err := (d.PullPolicy{Capacity: c.Capacity, BatchMax: c.BatchMax}).Limit(0); err != nil {
		return nil, err
	}
	if c.TTL <= 0 || c.RenewInterval <= 0 || c.OperationTimeout <= 0 || c.RenewTimeout <= 0 || c.RenewInterval >= c.TTL || c.RenewTimeout >= c.TTL-c.RenewInterval || c.PollMin <= 0 || c.PollMax < c.PollMin || c.PollMax > time.Hour {
		return nil, d.ErrInvalid
	}
	boot, err := randomID()
	if err != nil {
		return nil, err
	}
	return &Worker{cfg: c, identity: d.Worker{ID: c.WorkerID, BootID: boot}, changed: make(chan struct{}, 1)}, nil
}
func (w *Worker) Identity() d.Worker { return w.identity }
func (w *Worker) signal() {
	select {
	case w.changed <- struct{}{}:
	default:
	}
}
func (w *Worker) Pause() { w.mu.Lock(); w.paused = true; w.mu.Unlock(); w.signal() }

// Resume 不能撤销 Drain，也不能在 Run 返回后让 Worker 复用。
func (w *Worker) Resume() {
	w.mu.Lock()
	if !w.draining {
		w.paused = false
	}
	w.mu.Unlock()
	w.signal()
}
func (w *Worker) Active() int { w.mu.Lock(); defer w.mu.Unlock(); return w.active }

// Drain 停止准入、保持续租，并等待所有本地预留结束。
// 截止时间到期返回错误但不取消任务。未解决的领取及
// 不配合取消的代码可无限期占用预留容量；绝不复用它们。
func (w *Worker) Drain(ctx context.Context) error {
	w.mu.Lock()
	w.draining = true
	w.mu.Unlock()
	w.signal()
	// 不消耗 Run 的通知：独立的有界检查避免丢失唤醒。
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if w.Active() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
func (w *Worker) emit(e Event) {
	if w.cfg.Observe != nil {
		e.Worker = w.identity
		func() { defer func() { _ = recover() }(); w.cfg.Observe(e) }()
	}
}
func reason(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, d.ErrUnknown):
		return "unknown"
	case errors.Is(err, d.ErrLeaseLost):
		return "lease_lost"
	case errors.Is(err, d.ErrTerminal):
		return "terminal"
	case errors.Is(err, d.ErrClockRegression):
		return "clock_regression"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "interrupted"
	default:
		return "infrastructure"
	}
}

// adjustSlots 是同步的准入记账操作，不是唤醒事件。
func (w *Worker) adjustSlots(n int) { w.mu.Lock(); w.active -= n; w.mu.Unlock() }

// 只有容量实际变化时，异步完成才唤醒轮询器。
func (w *Worker) releaseSlots(n int) {
	if n == 0 {
		return
	}
	w.adjustSlots(n)
	w.signal()
}
func (w *Worker) reserve() (int, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, _ := (d.PullPolicy{Capacity: w.cfg.Capacity, BatchMax: w.cfg.BatchMax, Paused: w.paused, Draining: w.draining}).Limit(w.active)
	w.active += n
	return n, w.draining && w.active == 0
}
func (w *Worker) delay() time.Duration {
	// 加密随机字节避免共享伪随机数生成器状态及进程间相关轮询。
	id, err := randomID()
	if err != nil {
		return w.cfg.PollMax
	}
	var n uint64
	for i := 0; i < 12; i++ {
		n = n*33 + uint64(id[i])
	}
	return w.cfg.PollMin + time.Duration(n%uint64(w.cfg.PollMax-w.cfg.PollMin+1))
}
func (w *Worker) wait(ctx context.Context) bool {
	var tick <-chan time.Time
	var stop func()
	if w.pollTimer != nil {
		tick, stop = w.pollTimer(w.delay())
	} else {
		t := time.NewTimer(w.delay())
		tick = t.C
		stop = func() { t.Stop() }
	}
	defer stop()
	select {
	case <-ctx.Done():
		return false
	case <-tick:
		return true
	case <-w.changed:
		return true
	}
}

// Run 阻塞直至排空或取消，且只能调用一次。取消
// 通过本地上下文封闭会话，而非 Coordinator.Cancel。Run 可在
// 不配合取消的业务代码退出前返回；Active 保留这些预留容量，
// 清理仅在执行和所有保存确实返回后释放它们。
func (w *Worker) Run(ctx context.Context) error {
	if ctx == nil {
		return d.ErrInvalid
	}
	w.mu.Lock()
	if w.started {
		w.mu.Unlock()
		return d.ErrInvalid
	}
	w.started = true
	w.mu.Unlock()
	defer func() { w.mu.Lock(); w.draining = true; w.mu.Unlock(); w.signal() }()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		n, drained := w.reserve()
		if drained {
			return nil
		}
		if n > 0 {
			w.claim(ctx, n)
		}
		if !w.wait(ctx) {
			return ctx.Err()
		}
	}
}
func (w *Worker) claim(ctx context.Context, n int) {
	ids := make([]string, n)
	for i := range ids {
		id, err := randomID()
		if err != nil {
			w.adjustSlots(n)
			w.emit(Event{Kind: "claim", Reason: "random_failed"})
			return
		}
		ids[i] = id
	}
	req := d.ClaimRequest{Namespace: w.cfg.Namespace, Worker: w.identity, Capabilities: w.cfg.Registry.Capabilities(), Limit: n, TTL: w.cfg.TTL, LeaseIDs: ids}
	op, cancel := context.WithTimeout(ctx, w.cfg.OperationTimeout)
	start := time.Now()
	claims, err := w.cfg.Coordinator.Claim(op, req)
	cancel()
	w.emit(Event{Kind: "claim", Reason: reason(err), Duration: time.Since(start)})
	if err != nil {
		// 任何传输失败都可能掩盖提交，并非只有显式 ErrUnknown 如此。
		for _, id := range ids {
			go w.resolve(ctx, id)
		}
		return
	}
	byID := make(map[string]d.Claim, len(claims))
	valid := len(claims) <= n
	allowed := make(map[string]bool, n)
	for _, id := range ids {
		allowed[id] = true
	}
	for _, c := range claims {
		if !allowed[c.Token.LeaseID] || byID[c.Token.LeaseID].Token.LeaseID != "" || !w.validClaim(c) {
			valid = false
		}
		byID[c.Token.LeaseID] = c
	}
	if !valid {
		w.emit(Event{Kind: "claim", Reason: "invalid_reply"})
		for _, id := range ids {
			go w.resolve(ctx, id)
		}
		return
	}
	w.adjustSlots(n - len(claims))
	for _, c := range claims {
		go w.execute(ctx, c)
	}
}
func (w *Worker) validClaim(c d.Claim) bool {
	return c.Token.Worker == w.identity && c.Token.Identity.Namespace == w.cfg.Namespace && c.Token.Identity == c.Snapshot.Task.Identity() && c.Token.Epoch > 0 && c.Token.LeaseID != "" && c.Snapshot.Status == d.Running
}
func (w *Worker) resolve(ctx context.Context, id string) {
	for ctx.Err() == nil {
		op, cancel := context.WithTimeout(ctx, w.cfg.OperationTimeout)
		resolution, err := w.cfg.Coordinator.ResolveClaim(op, w.cfg.Namespace, w.identity, id)
		cancel()
		if err == nil {
			if resolution.Closed && resolution.Claim == nil {
				w.emit(Event{Kind: "resolve_claim", Reason: "closed"})
				w.releaseSlots(1)
				return
			}
			if !resolution.Closed && resolution.Claim != nil && resolution.Claim.Token.LeaseID == id && w.validClaim(*resolution.Claim) {
				w.emit(Event{Kind: "resolve_claim", Token: resolution.Claim.Token, Reason: "claimed"})
				w.execute(ctx, *resolution.Claim)
				return
			}
		}
		w.emit(Event{Kind: "resolve_claim", Reason: "unresolved"})
		// 未知或畸形响应不能证明已持久关闭。保留
		// 预留容量，仅解析此原始 ID；绝不重放 Claim。
		t := time.NewTimer(w.delay())
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
	// 未知令牌：无论 Release 还是释放此预留容量都不安全。
}
func (w *Worker) renew(ctx context.Context, token d.Token) error {
	op, cancel := context.WithTimeout(ctx, w.cfg.RenewTimeout)
	defer cancel()
	start := time.Now()
	_, err := w.cfg.Renewer.Renew(op, token, w.cfg.TTL)
	w.emit(Event{Kind: "renew", Token: token, Reason: reason(err), Duration: time.Since(start)})
	return err
}
func (w *Worker) execute(parent context.Context, c d.Claim) {
	defer w.releaseSlots(1)
	w.emit(Event{Kind: "owned", Token: c.Token, Revision: c.Snapshot.Revision, Reason: "ok"})
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	s := &session{c: w.cfg.Coordinator, token: c.Token, timeout: w.cfg.OperationTimeout, ctx: ctx, cancel: cancel, observe: w.emit}
	var claimed durable.ClaimedSession = s
	// 即使 Claim/ResolveClaim 成功，也要求权威续租。
	renewSession := func() bool {
		start := time.Now()
		if w.renew(ctx, c.Token) != nil || !s.confirmed(start, w.cfg.TTL, w.cfg.RenewTimeout) {
			s.seal(true)
			return false
		}
		return true
	}
	if !renewSession() {
		return
	}
	stop := make(chan struct{})
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		ticker := time.NewTicker(w.cfg.RenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				s.seal(true)
				return
			case <-ticker.C:
				if !renewSession() {
					return
				}
			}
		}
	}()
	var stopOnce sync.Once
	stopRenew := func() { stopOnce.Do(func() { close(stop) }); <-joined }
	defer func() { stopRenew(); _ = s.Close() }()
	a, in, err := w.cfg.Registry.Resolve(ctx, c.Snapshot.Task)
	outcome := durable.OutcomeRejected
	// Registry 对载荷解码及组装验证暴露 ErrInvalid，
	// 对精确路由、定义或输入不兼容暴露 ErrMismatch。
	blockReason := "incompatible"
	if errors.Is(err, d.ErrInvalid) {
		blockReason = "invalid_payload"
	}
	if _, parseErr := d.ParseTask(c.Snapshot.Task.Bytes()); parseErr != nil {
		blockReason = "invalid_task"
	}
	if err == nil {
		blockReason = "invalid_record"
		rt, newErr := durable.New(claimedOnlyStore{}, a.Codecs)
		if newErr != nil {
			err = newErr
		} else {
			_, loadErr := s.Load(ctx)
			opts := durable.RunOptions{RunID: c.Token.Identity.RunID, Version: c.Snapshot.Task.Route().BusinessVersion}
			switch {
			case errors.Is(loadErr, durable.ErrNotFound):
				_, outcome, err = rt.RunClaimed(ctx, claimed, a.Graph, in, opts)
			case errors.Is(loadErr, d.ErrInvalidRecord):
				outcome = durable.OutcomeRejected
				err = loadErr
			case loadErr == nil:
				_, outcome, err = rt.ResumeClaimed(ctx, claimed, a.Graph, in, opts)
			default:
				outcome = durable.OutcomeInfrastructure
				err = loadErr
			}
		}
	}
	// 在判定终态前，等待所有已进行中的续租。迟来的
	// 未知续租不得与下方本地不确定性检查竞争。
	stopRenew()
	// Runtime Wait 已等待业务和检查点工作结束。封闭新的本地操作，
	// 并在任何协调终态或释放前显式等待进行中的调用。
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.inflight.Wait()
	w.emit(Event{Kind: "execution", Token: c.Token, Reason: string(outcome)})
	if s.uncertain() || ctx.Err() != nil {
		// 停机中断可在静止后 Release，但所有权不确定
		// 或任意 Save 失败时必须等待过期。绝不凭空生成终态。
		if !s.uncertain() {
			w.release(c.Token)
		}
		return
	}
	switch outcome {
	case durable.OutcomeCompleted, durable.OutcomeFailed:
		op, done := context.WithTimeout(ctx, w.cfg.OperationTimeout)
		record, loadErr := w.cfg.Coordinator.LoadOwned(op, c.Token)
		done()
		if loadErr != nil {
			w.emit(Event{Kind: "finish", Token: c.Token, Reason: reason(loadErr)})
			return
		}
		req := d.FinishRequest{Token: c.Token, ExpectedRevision: record.Revision, Outcome: outcome}
		req.Mutation, err = mutation(req)
		if err != nil {
			return
		}
		op, done = context.WithTimeout(ctx, w.cfg.OperationTimeout)
		_, err = w.cfg.Coordinator.Finish(op, req)
		done()
		w.emit(Event{Kind: "finish", Token: c.Token, Revision: record.Revision, Mutation: req.Mutation, Reason: reason(err)})
	case durable.OutcomeRejected:
		m, merr := mutation(struct {
			Token  d.Token
			Reason string
		}{c.Token, blockReason})
		if merr != nil {
			return
		}
		op, done := context.WithTimeout(ctx, w.cfg.OperationTimeout)
		err = w.cfg.Coordinator.Block(op, c.Token, blockReason, m)
		done()
		w.emit(Event{Kind: "block", Token: c.Token, Mutation: m, Reason: reason(err)})
	default:
		w.release(c.Token)
	}
}
func (w *Worker) release(token d.Token) {
	m, err := mutation(token)
	if err != nil {
		return
	}
	op, cancel := context.WithTimeout(context.Background(), w.cfg.OperationTimeout)
	defer cancel()
	err = w.cfg.Coordinator.Release(op, token, m)
	w.emit(Event{Kind: "release", Token: token, Mutation: m, Reason: reason(err)})
}
