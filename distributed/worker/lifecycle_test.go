package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)

func TestUnknownResolutionCannotFreeReservation(t *testing.T) {
	w, f := fixture(t, func(*pulse.RunCtx) error { return nil })
	f.claimUnknown = true
	f.resolveUnknown = true
	cancel, _ := runWorker(t, w)
	until(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return len(f.finds) >= 3 })
	if w.Active() != 1 {
		t.Fatal("unknown claim freed reservation")
	}
	f.mu.Lock()
	if f.claims != 1 {
		t.Fatal("unknown claim replayed")
	}
	f.mu.Unlock()
	cancel()
	ctx, done := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer done()
	if !errors.Is(w.Drain(ctx), context.DeadlineExceeded) {
		t.Fatal("drain ignored unresolved ownership")
	}
}
func TestFailedRuntimeOnlyUsesFailedFinish(t *testing.T) {
	w, f := fixture(t, func(*pulse.RunCtx) error { return errors.New("secret business error") })
	runWorker(t, w)
	req := await(t, f.finish)
	if req.Outcome != durable.OutcomeFailed {
		t.Fatalf("outcome=%v", req.Outcome)
	}
}

// 此示例仅展示生命周期配置，不连接
// 数据库。真实宿主提供受栅栏保护的 Coordinator 及专用 Renewer。
func ExampleWorker_Pause() {
	var w *Worker // 通过 New(Config{Coordinator: ..., Renewer: ...}) 获取
	if w != nil {
		w.Pause()
		w.Resume()
	}
	// Output:
}
