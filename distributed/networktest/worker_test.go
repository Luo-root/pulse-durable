package networktest

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/Luo-root/pulse-durable/distributed/internal/faultproxy"
	"github.com/Luo-root/pulse-durable/distributed/worker"
)

// 仅包装观察操作：所有所有权、检查点和终态操作
// 仍访问真实 PostgreSQL 协调器。不模拟任何后端结果。
type observedCoordinator struct {
	d.Coordinator
	finishes, releases atomic.Int32
}

func (c *observedCoordinator) Finish(ctx context.Context, r d.FinishRequest) (d.Snapshot, error) {
	c.finishes.Add(1)
	return c.Coordinator.Finish(ctx, r)
}
func (c *observedCoordinator) Release(ctx context.Context, token d.Token, m d.Mutation) error {
	c.releases.Add(1)
	return c.Coordinator.Release(ctx, token, m)
}
func receive[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-ctx.Done():
		t.Fatal("worker business/event barrier timed out")
		var zero T
		return zero
	}
}

func TestWorkerRenewBlackholeRecovery(t *testing.T) {
	f := setup(t)
	entered := make(chan struct{})
	exit := make(chan struct{})
	var exitOnce sync.Once
	unblock := func() { exitOnce.Do(func() { close(exit) }) }
	// 断言失败时，也始终等待不配合取消的业务回调结束。
	t.Cleanup(unblock)
	var executions atomic.Int32
	factory := func() (d.Assembly, error) {
		g, err := pulse.New("network-worker", "renewal fencing recovery")
		if err != nil {
			return d.Assembly{}, err
		}
		err = g.Add(pulse.NewNode("business", "business", nil, nil, func(*pulse.RunCtx) error {
			if executions.Add(1) == 1 {
				close(entered)
				<-exit
			}
			return nil
		}))
		return d.Assembly{Graph: g, Codecs: durable.NewCodecs(), Keys: pulse.NewRegistry()}, err
	}
	registry := &d.Registry{}
	if _, err := registry.Register("network-worker", "v1", factory); err != nil {
		t.Fatal("registry setup failed")
	}
	a, err := factory()
	if err != nil {
		t.Fatal("assembly failed")
	}
	task, err := d.NewTask(f.ctx, d.Identity{Namespace: f.ns, RunID: opaqueID(t)}, "network-worker", "v1", a.Graph, pulse.Inputs{}, a.Codecs)
	if err != nil {
		t.Fatal("worker task preparation failed")
	}
	if _, err = f.direct.Submit(f.ctx, task); err != nil {
		t.Fatalf("worker submit failed: %v", err)
	}
	observed := &observedCoordinator{Coordinator: f.direct}
	unknown := make(chan worker.Event, 16)
	executed := make(chan worker.Event, 16)
	// 使用宽裕的本地租约预算：续租传输超时远早于
	// 本地过期。后续重新领取由数据库时钟而非本地定时器授权。
	cfg := worker.Config{Coordinator: observed, Renewer: f.remote, Registry: registry, Namespace: f.ns, WorkerID: "old-worker", Capacity: 1, BatchMax: 1, TTL: 6 * time.Second, RenewInterval: time.Second, RenewTimeout: 500 * time.Millisecond, OperationTimeout: 2 * time.Second, PollMin: 20 * time.Millisecond, PollMax: 40 * time.Millisecond,
		Observe: func(e worker.Event) {
			if e.Kind == "renew" && e.Reason == "unknown" {
				unknown <- e
			}
			if e.Kind == "execution" {
				executed <- e
			}
		}}
	old, err := worker.New(cfg)
	if err != nil {
		t.Fatal("worker configuration failed")
	}
	ctx, cancel := context.WithCancel(f.ctx)
	done := make(chan error, 1)
	go func() { done <- old.Run(ctx) }()
	t.Cleanup(func() { unblock(); cancel(); receive(t, f.ctx, done) })
	receive(t, f.ctx, entered)
	old.Pause() // 首次执行静止后，阻止任何第二次准入。
	after, err := f.proxy.Blackhole(faultproxy.ClientToServer, true)
	if err != nil {
		t.Fatal(err)
	}
	barrierCtx, barrierCancel := context.WithTimeout(f.ctx, 3*time.Second)
	_, err = f.proxy.WaitBarrier(barrierCtx, faultproxy.ClientToServer, after)
	barrierCancel()
	if err != nil {
		t.Fatal("renewal request never reached blackhole")
	}
	failed := receive(t, f.ctx, unknown)
	if failed.Token.Worker != old.Identity() {
		t.Fatal("unexpected unknown renewal identity")
	}
	// Run 有意不配合取消：即使会话已自行封闭，预留容量仍须
	// 持续占用，直到业务静止。
	if old.Active() != 1 {
		t.Fatal("uncertain renewal freed executing capacity")
	}
	unblock()
	receive(t, f.ctx, executed)
	drainCtx, drainCancel := context.WithTimeout(f.ctx, 2*time.Second)
	err = old.Drain(drainCtx)
	drainCancel()
	if err != nil {
		t.Fatal("sealed execution failed to quiesce")
	}
	if observed.finishes.Load() != 0 || observed.releases.Load() != 0 {
		t.Fatal("uncertain worker called Finish or Release")
	}
	// 后端仍须显示 running，而非 queued、failed 或 succeeded。
	s, err := f.direct.Get(f.ctx, task.Identity())
	if err != nil || s.Status != d.Running || s.Epoch != 1 {
		t.Fatal("unknown renewal manufactured a terminal/released state")
	}
	if _, err = f.proxy.Blackhole(faultproxy.ClientToServer, false); err != nil {
		t.Fatal(err)
	}
	restore(t, f)
	// await 默认的 3 秒不足以覆盖有意放宽至 6 秒的租约。
	expiryCtx, expiryCancel := context.WithTimeout(f.ctx, 9*time.Second)
	defer expiryCancel()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		var expired bool
		if err = f.admin.QueryRow(expiryCtx, "SELECT lease_until<=clock_timestamp() FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2", f.ns, task.Identity().RunID).Scan(&expired); err != nil {
			t.Fatal("DB lease expiry query failed")
		}
		if expired {
			break
		}
		select {
		case <-expiryCtx.Done():
			t.Fatal("DB lease never expired")
		case <-tick.C:
		}
	}
	// 新的真实 Worker 恢复持久化记录，重新执行被中断的节点，
	// 并以新启动标识和纪元通过权威协调器完成任务。
	recovered := make(chan worker.Event, 16)
	cfg.Coordinator = f.direct
	cfg.Renewer = f.direct
	cfg.WorkerID = "new-worker"
	cfg.Observe = func(e worker.Event) {
		if e.Kind == "finish" {
			recovered <- e
		}
	}
	next, err := worker.New(cfg)
	if err != nil {
		t.Fatal("recovery worker configuration failed")
	}
	nextCtx, nextCancel := context.WithCancel(f.ctx)
	nextDone := make(chan error, 1)
	go func() { nextDone <- next.Run(nextCtx) }()
	t.Cleanup(func() { nextCancel(); receive(t, f.ctx, nextDone) })
	finish := receive(t, f.ctx, recovered)
	if finish.Reason != "ok" || finish.Token.Epoch != 2 || finish.Token.Worker != next.Identity() {
		t.Fatal("new owner did not finish recovery")
	}
	s, err = f.direct.Get(f.ctx, task.Identity())
	if err != nil || s.Status != d.Succeeded || s.Epoch != 2 || executions.Load() != 2 {
		t.Fatal("recovered task not authoritatively succeeded")
	}
	if observed.finishes.Load() != 0 || observed.releases.Load() != 0 {
		t.Fatal("old worker issued late Finish or Release")
	}
}
