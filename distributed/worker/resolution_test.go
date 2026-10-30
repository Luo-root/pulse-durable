package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Luo-root/pulse"
	d "github.com/Luo-root/pulse-durable/distributed"
)

// orderedClaim 模拟每个 ID 的永久串行化契约，而非 SQL。
// 服务端决策仍阻塞在屏障后时，Claim 返回未知。
type orderedClaim struct {
	*fake
	mu                              sync.Mutex
	committed, closed               bool
	req                             d.ClaimRequest
	lateReady, lateCommit, lateDone chan struct{}
	resolveEntered, resolveProceed  chan struct{}
}

func (f *orderedClaim) Claim(ctx context.Context, r d.ClaimRequest) ([]d.Claim, error) {
	f.fake.mu.Lock()
	f.fake.claims++
	count := f.fake.claims
	f.fake.mu.Unlock()
	if count > 1 {
		return nil, nil
	}
	f.mu.Lock()
	f.req = r
	f.mu.Unlock()
	close(f.lateReady)
	go func() {
		<-f.lateCommit
		f.mu.Lock()
		if !f.closed {
			f.committed = true
			f.fake.mu.Lock()
			f.fake.token = d.Token{Identity: f.task.Identity(), Worker: r.Worker, LeaseID: r.LeaseIDs[0], Epoch: 1}
			f.fake.mu.Unlock()
		}
		f.mu.Unlock()
		close(f.lateDone)
	}()
	return nil, d.ErrUnknown
}
func (f *orderedClaim) ResolveClaim(ctx context.Context, ns string, w d.Worker, id string) (d.ClaimResolution, error) {
	if _, ok := ctx.Deadline(); !ok {
		panic("unbounded ResolveClaim")
	}
	select {
	case f.resolveEntered <- struct{}{}:
	default:
	}
	select {
	case <-f.resolveProceed:
	case <-ctx.Done():
		return d.ClaimResolution{}, ctx.Err()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if ns != f.req.Namespace || w != f.req.Worker || id != f.req.LeaseIDs[0] {
		panic("changed identity")
	}
	if !f.committed {
		f.closed = true
		return d.ClaimResolution{Closed: true}, nil
	}
	f.fake.mu.Lock()
	token := f.fake.token
	f.fake.mu.Unlock()
	return d.ClaimResolution{Claim: &d.Claim{Token: token, Snapshot: d.Snapshot{Task: f.task, Status: d.Running}}}, nil
}
func TestDelayedClaimResolveOrdering(t *testing.T) {
	for _, claimFirst := range []bool{true, false} {
		name := "resolve_first"
		if claimFirst {
			name = "claim_first"
		}
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			w, f := fixture(t, func(*pulse.RunCtx) error { calls.Add(1); return nil })
			model := &orderedClaim{fake: f, lateReady: make(chan struct{}), lateCommit: make(chan struct{}), lateDone: make(chan struct{}), resolveEntered: make(chan struct{}, 1), resolveProceed: make(chan struct{})}
			w.cfg.Coordinator = model
			runWorker(t, w)
			await(t, model.lateReady)
			await(t, model.resolveEntered)
			// 解析已有预留槽位时停止新的准入。
			w.Pause()
			if claimFirst {
				close(model.lateCommit)
				await(t, model.lateDone)
				close(model.resolveProceed)
				await(t, f.finish)
			} else {
				close(model.resolveProceed)
				until(t, func() bool { return w.Active() == 0 })
				close(model.lateCommit)
				await(t, model.lateDone)
			}
			until(t, func() bool { return w.Active() == 0 })
			model.mu.Lock()
			committed, closed := model.committed, model.closed
			model.mu.Unlock()
			if claimFirst {
				if !committed || closed || calls.Load() != 1 {
					t.Fatal("committed claim not executed exactly once")
				}
			} else {
				if committed || !closed || calls.Load() != 0 {
					t.Fatal("late claim escaped durable closure")
				}
				// Closed 已恢复容量：准入可发起一次独立的新轮询。
				w.Resume()
				until(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.claims >= 2 })
				w.Pause()
			}
		})
	}
}
func TestUnknownThenClosedRecoversCapacityWithoutOverclaim(t *testing.T) {
	w, f := fixture(t, func(*pulse.RunCtx) error { t.Error("closed lease executed"); return nil })
	f.claimUnknown = true
	f.resolveUnknown = true
	runWorker(t, w)
	until(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return len(f.finds) >= 3 })
	w.Pause()
	if w.Active() != 1 {
		t.Fatal("unknown resolution freed reservation")
	}
	f.mu.Lock()
	if f.claims != 1 {
		t.Fatal("overclaimed unresolved slot")
	}
	original := f.token.LeaseID
	f.resolveUnknown = false
	f.resolveClosed = true
	f.mu.Unlock()
	until(t, func() bool { return w.Active() == 0 })
	f.mu.Lock()
	for _, id := range f.finds {
		if id != original {
			t.Fatal("resolution changed lease identity")
		}
	}
	f.mu.Unlock()
	w.Resume()
	until(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.claims >= 2 })
}
