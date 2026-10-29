// systemtest 包为 #6、#88 和 #90 提供显式启用的进程测试框架。
// 它是测试基础设施，不是生产工作进程启动器。绝不迁移数据库
// 或授予权限；环境准备由测试协调器负责。
package systemtest

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/Luo-root/pulse-durable/distributed/postgres"
	"github.com/Luo-root/pulse-durable/distributed/worker"
)

// Config 不含凭据。DSN 通过环境单独提供。
// 不同主机可用 CLI 指定相同命名空间和版本，但各自使用
// 唯一 ID。Duration 是硬上限，不是耐久负载运行的证据。
type Config struct {
	Namespace  string        `json:"namespace"`
	ID         string        `json:"worker_id"`
	Version    string        `json:"version"`
	Capacity   int           `json:"capacity"`
	Duration   time.Duration `json:"duration"`
	HoldFinish bool          `json:"hold_finish"`
	// JSON 中的时间值单位为纳秒；零值选择本地测试默认值。
	TTL              time.Duration `json:"ttl,omitempty"`
	RenewInterval    time.Duration `json:"renew_interval,omitempty"`
	RenewTimeout     time.Duration `json:"renew_timeout,omitempty"`
	OperationTimeout time.Duration `json:"operation_timeout,omitempty"`
	PollMin          time.Duration `json:"poll_min,omitempty"`
	PollMax          time.Duration `json:"poll_max,omitempty"`
}

// workerConfig 仅为省略的时间值设置默认值；保留负数及非法
// 组合，让 worker.New 拒绝它们，而不是将其掩盖。
func (c Config) workerConfig(backend d.Coordinator, renew worker.Renewer, reg *d.Registry, observe func(worker.Event)) worker.Config {
	value := func(v, fallback time.Duration) time.Duration {
		if v == 0 {
			return fallback
		}
		return v
	}
	return worker.Config{Coordinator: backend, Renewer: renew, Registry: reg,
		Namespace: c.Namespace, WorkerID: c.ID, Capacity: c.Capacity, BatchMax: c.Capacity,
		TTL: value(c.TTL, 2*time.Second), RenewInterval: value(c.RenewInterval, 300*time.Millisecond),
		RenewTimeout: value(c.RenewTimeout, 400*time.Millisecond), OperationTimeout: value(c.OperationTimeout, time.Second),
		PollMin: value(c.PollMin, 20*time.Millisecond), PollMax: value(c.PollMax, 50*time.Millisecond), Observe: observe}
}

// Event 是允许列表内的 JSON 行结果。有意省略原始后端错误、载荷及 DSN。
// 业务计数描述尝试次数，不代表恰好一次的 I/O。
type Event struct {
	Kind   string   `json:"kind"`
	PID    int      `json:"pid"`
	Worker d.Worker `json:"worker"`
	RunID  string   `json:"run_id,omitempty"`
	Epoch  int64    `json:"epoch,omitempty"`
	Reason string   `json:"reason,omitempty"`
	Active int      `json:"active,omitempty"`
}

type emitter struct {
	mu       sync.Mutex
	out      io.Writer
	identity d.Worker
}

func (e *emitter) emit(v Event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	v.PID = os.Getpid()
	v.Worker = e.identity
	_ = json.NewEncoder(e.out).Encode(v)
}

// 提交方与工作进程共享 Assembly；构建图时绝不调用
// 回调。第二个节点依赖已持久化的第一个节点输出。
func Assembly(body func(*pulse.RunCtx, string, string) error) (d.Assembly, error) {
	g, err := pulse.New("distributed-systemtest", "checkpoint process recovery")
	if err != nil {
		return d.Assembly{}, err
	}
	id := pulse.NewKey[string]("run_id")
	prefix := pulse.NewKey[int]("prefix")
	result := pulse.NewKey[int]("result")
	if err = g.DeclareInput(pulse.NewPort(id)); err != nil {
		return d.Assembly{}, err
	}
	if err = g.Add(pulse.NewNode("prefix", "", pulse.Requires(id), pulse.Provides(prefix), func(rc *pulse.RunCtx) error {
		run, err := pulse.Get(rc, id)
		if err != nil {
			return err
		}
		if body != nil {
			if err = body(rc, run, "prefix"); err != nil {
				return err
			}
		}
		return pulse.Set(rc, prefix, 41)
	})); err != nil {
		return d.Assembly{}, err
	}
	if err = g.Add(pulse.NewNode("suffix", "", append(pulse.Requires(id), pulse.Requires(prefix)...), pulse.Provides(result), func(rc *pulse.RunCtx) error {
		run, err := pulse.Get(rc, id)
		if err != nil {
			return err
		}
		if body != nil {
			if err = body(rc, run, "suffix"); err != nil {
				return err
			}
		}
		v, err := pulse.Get(rc, prefix)
		if err != nil {
			return err
		}
		return pulse.Set(rc, result, v+1)
	})); err != nil {
		return d.Assembly{}, err
	}
	if err = g.DeclareOutput(pulse.NewPort(result)); err != nil {
		return d.Assembly{}, err
	}
	keys := pulse.NewRegistry()
	if err = pulse.RegisterKey(keys, id); err != nil {
		return d.Assembly{}, err
	}
	return d.Assembly{Graph: g, Codecs: durable.NewCodecs(), Keys: keys}, nil
}
func Task(ctx context.Context, ns, id, version string) (d.Task, error) {
	a, err := Assembly(nil)
	if err != nil {
		return d.Task{}, err
	}
	var in pulse.Inputs
	if err = pulse.Seed(&in, pulse.NewKey[string]("run_id"), id); err != nil {
		return d.Task{}, err
	}
	return d.NewTask(ctx, d.Identity{Namespace: ns, RunID: id}, "systemtest", version, a.Graph, in, a.Codecs)
}

type observed struct {
	d.Coordinator
	emit       *emitter
	gate       <-chan struct{}
	hold       bool
	mu         sync.Mutex
	tokens     map[string]d.Token
	finishHeld *d.Token
}

func (o *observed) Claim(ctx context.Context, r d.ClaimRequest) ([]d.Claim, error) {
	rows, err := o.Coordinator.Claim(ctx, r)
	if err == nil {
		o.mu.Lock()
		for _, c := range rows {
			o.tokens[c.Token.Identity.RunID] = c.Token
			o.emit.emit(Event{Kind: "owned", RunID: c.Token.Identity.RunID, Epoch: c.Token.Epoch})
		}
		o.mu.Unlock()
	}
	return rows, err
}
func (o *observed) token(id string) d.Token { o.mu.Lock(); defer o.mu.Unlock(); return o.tokens[id] }
func (o *observed) Finish(ctx context.Context, r d.FinishRequest) (d.Snapshot, error) {
	if o.hold {
		o.mu.Lock()
		token := r.Token
		o.finishHeld = &token
		o.mu.Unlock()
		o.emit.emit(Event{Kind: "before_finish", RunID: token.Identity.RunID, Epoch: token.Epoch})
		// 有意不配合取消的故障注入：操作截止时间不得
		// 移除终止窗口。只有终止进程（或单元测试关卡）
		// 才会释放它。父进程清理始终终止并等待此进程。
		<-o.gate
		o.mu.Lock()
		o.finishHeld = nil
		o.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return d.Snapshot{}, err
		}
	}
	return o.Coordinator.Finish(ctx, r)
}

type drainer interface {
	Drain(context.Context) error
	Active() int
}

func sealDrain(ctx context.Context, w drainer, emit *emitter) {
	seal, cancel := context.WithCancel(ctx)
	cancel()
	_ = w.Drain(seal) // 先设置 draining，再检查已取消的等待上下文
	emit.emit(Event{Kind: "draining", Active: w.Active()})
}

// Run 接收标准输入 JSON 字符串："start"、"release"、"drain"、"probe_finish"。EOF 取消
// 进程。Drain 停止准入，并在活跃任务退出前保持续租。
// 必须同时设置 PULSE_DISTRIBUTED_TEST_DSN 和 PULSE_DISTRIBUTED_TEST_ALLOW=yes。
func Run(parent context.Context, c Config, in io.Reader, out io.Writer) error {
	if os.Getenv("PULSE_DISTRIBUTED_TEST_ALLOW") != "yes" || os.Getenv("PULSE_DISTRIBUTED_TEST_DSN") == "" {
		return errors.New("systemtest: explicit opt-in required")
	}
	if c.Namespace == "" || c.ID == "" || c.Version == "" || c.Capacity < 1 || c.Duration <= 0 {
		return errors.New("systemtest: invalid configuration")
	}
	ctx, cancel := context.WithTimeout(parent, c.Duration)
	defer cancel()
	backend, err := postgres.New(ctx, os.Getenv("PULSE_DISTRIBUTED_TEST_DSN"))
	if err != nil {
		return errors.New("systemtest: coordinator unavailable")
	}
	defer backend.Close()
	renew, err := postgres.New(ctx, os.Getenv("PULSE_DISTRIBUTED_TEST_DSN"))
	if err != nil {
		return errors.New("systemtest: renewer unavailable")
	}
	defer renew.Close()
	emit := &emitter{out: out}
	gate := make(chan struct{})
	start := make(chan struct{})
	var releaseOnce, startOnce sync.Once
	finishGate := make(chan struct{}) // 有意永不打开：控制器在此窗口内终止进程
	o := &observed{Coordinator: backend, emit: emit, gate: finishGate, hold: c.HoldFinish, tokens: map[string]d.Token{}}
	reg := &d.Registry{}
	_, err = reg.Register("systemtest", c.Version, func() (d.Assembly, error) {
		return Assembly(func(rc *pulse.RunCtx, id, step string) error {
			tok := o.token(id)
			emit.emit(Event{Kind: step, RunID: id, Epoch: tok.Epoch})
			if step == "suffix" {
				select {
				case <-gate:
				case <-rc.Context().Done():
					return rc.Context().Err()
				}
			}
			return nil
		})
	})
	if err != nil {
		return err
	}
	w, err := worker.New(c.workerConfig(o, renew, reg, func(e worker.Event) {
		emit.emit(Event{Kind: e.Kind, RunID: e.Token.Identity.RunID, Epoch: e.Token.Epoch, Reason: e.Reason})
	}))
	if err != nil {
		return err
	}
	emit.identity = w.Identity()
	emit.emit(Event{Kind: "ready"})
	drainDone := make(chan struct{})
	var drainOnce sync.Once
	go func() {
		s := bufio.NewScanner(in)
		for s.Scan() {
			var cmd string
			if json.Unmarshal(s.Bytes(), &cmd) != nil {
				continue
			}
			switch cmd {
			case "start":
				startOnce.Do(func() { close(start) })
			case "release":
				releaseOnce.Do(func() { close(gate) })
			case "probe_finish":
				o.mu.Lock()
				if token := o.finishHeld; token != nil {
					emit.emit(Event{Kind: "finish_held", RunID: token.Identity.RunID, Epoch: token.Epoch})
				}
				o.mu.Unlock()
			case "drain":
				drainOnce.Do(func() {
					// 即使等待上下文已过期，Drain 仍同步封闭准入。
					// 仅在调用返回后确认，绝不在收到请求时确认。
					sealDrain(ctx, w, emit)
					go func() {
						err := w.Drain(ctx)
						reason := "ok"
						if err != nil {
							reason = "timeout"
						}
						emit.emit(Event{Kind: "drained", Reason: reason})
						close(drainDone)
					}()
				})
			}
		}
		cancel()
	}()
	select {
	case <-start:
	case <-ctx.Done():
		return ctx.Err()
	}
	err = w.Run(ctx)
	if err == nil { // Run 可能在 Drain goroutine 发出结果前返回。
		select {
		case <-drainDone:
		case <-ctx.Done():
		}
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("systemtest: worker stopped")
	}
	return nil
}
