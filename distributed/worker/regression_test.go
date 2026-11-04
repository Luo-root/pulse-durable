package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
)

// 定时器绝不自行推进。每个信号恰好允许一次新轮询；
// 在创建定时器时检查待处理唤醒，可确定性检测旧的同步信号，
// 无须休眠或吞吐量、时序阈值。
func TestEmptyClaimHonorsPollTimer(t *testing.T) {
	w, f := fixture(t, func(*pulse.RunCtx) error { return nil })
	f.claims = 1 // 模拟实现在后续每个 Claim 中返回空结果
	type poll struct {
		count    int
		wakeups  int
		duration time.Duration
		tick     chan time.Time
	}
	polls := make(chan poll, 4)
	w.pollTimer = func(delay time.Duration) (<-chan time.Time, func()) {
		f.mu.Lock()
		count := f.claims
		f.mu.Unlock()
		tick := make(chan time.Time, 1)
		select {
		case polls <- poll{count: count, wakeups: len(w.changed), duration: delay, tick: tick}:
		default: // 错误的忙轮询器在测试清理时也必须可取消。
		}
		return tick, func() {}
	}
	cancel, _ := runWorker(t, w)
	for i := 0; i < 3; i++ {
		p := await(t, polls)
		if p.count != i+2 || p.wakeups != 0 {
			t.Fatalf("poll bypass: count=%d pending wakeups=%d", p.count, p.wakeups)
		}
		if p.duration < w.cfg.PollMin || p.duration > w.cfg.PollMax {
			t.Fatal("unbounded poll delay")
		}
		if w.Active() != 0 {
			t.Fatal("empty Claim retained reservation")
		}
		if i < 2 {
			p.tick <- time.Now()
		}
	}
	cancel()
}
func TestZeroSlotReleaseDoesNotWakePoller(t *testing.T) {
	w, _ := fixture(t, func(*pulse.RunCtx) error { return nil })
	w.releaseSlots(0)
	if len(w.changed) != 0 {
		t.Fatal("zero slot release signaled")
	}
	w.active = 1
	w.releaseSlots(1)
	if w.Active() != 0 || len(w.changed) != 1 {
		t.Fatal("async completion did not wake poller")
	}
}

type commitPanic struct {
	*fake
	value any
	saves atomic.Int32
}

func (f *commitPanic) InitializeOwned(ctx context.Context, req d.InitializeRequest) error {
	if err := f.fake.InitializeOwned(ctx, req); err != nil {
		return err
	}
	f.saves.Add(1)
	panic(f.value)
}

// 不检查或格式化 panic 值，包括通过 errors.Is 链。
type untrustedPanic struct{}

func (untrustedPanic) Error() string { panic("panic value was formatted") }
func (untrustedPanic) Is(error) bool { panic("panic value was inspected") }

func TestSavePanicSealsSession(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
	}{{"commit_then_panic", untrustedPanic{}}, {"panic_nil", nil}} {
		t.Run(tc.name, func(t *testing.T) {
			_, f := fixture(t, func(*pulse.RunCtx) error { return nil })
			backend := &commitPanic{fake: f, value: tc.value}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := &session{c: backend, ctx: ctx, cancel: cancel, timeout: time.Second}
			if err := s.Initialize(ctx, durable.RunRecord{Revision: 1}); !errors.Is(err, d.ErrUnknown) {
				t.Fatalf("not unknown: %v", err)
			}
			if !s.uncertain() || ctx.Err() == nil {
				t.Fatal("panic did not seal and cancel")
			}
			if err := s.Initialize(context.Background(), durable.RunRecord{Revision: 2}); !errors.Is(err, d.ErrLeaseLost) {
				t.Fatal("sealed session allowed new save")
			}
			closed := make(chan struct{})
			go func() { _ = s.Close(); close(closed) }()
			await(t, closed)
			if backend.saves.Load() != 1 {
				t.Fatal("Save replayed")
			}
		})
	}
}
func TestSavePanicNeverFinishesOrReleases(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value any
	}{{"commit_then_panic", untrustedPanic{}}, {"panic_nil", nil}} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			w, f := fixture(t, func(*pulse.RunCtx) error { calls.Add(1); return nil })
			backend := &commitPanic{fake: f, value: tc.value}
			w.cfg.Coordinator = backend
			n, _ := w.reserve()
			if n != 1 {
				t.Fatal("reservation")
			}
			req := d.ClaimRequest{Namespace: w.cfg.Namespace, Worker: w.identity, Capabilities: w.cfg.Registry.Capabilities(), Limit: 1, TTL: w.cfg.TTL, LeaseIDs: []string{"original-lease"}}
			claims, err := f.Claim(contextWithDeadline(t), req)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() { w.execute(context.Background(), claims[0]); close(done) }()
			await(t, done)
			if w.Active() != 0 || calls.Load() != 0 || backend.saves.Load() != 1 {
				t.Fatal("execution did not quiesce at initial uncertain checkpoint")
			}
			f.mu.Lock()
			releases := f.releases
			record := f.record
			f.mu.Unlock()
			if releases != 0 || record == nil {
				t.Fatal("commit-then-panic released or was not committed")
			}
			select {
			case <-f.finish:
				t.Fatal("uncertain Save finished")
			default:
			}
		})
	}
}
func contextWithDeadline(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	return ctx
}
