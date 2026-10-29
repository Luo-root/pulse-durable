package systemtest

import (
	"bytes"
	"context"
	d "github.com/Luo-root/pulse-durable/distributed"
	"testing"
	"time"
)

// 此测试会使旧的 ctx.Done select 实现失败：过去一秒截止时间会在
// 控制器完成权威查询前移除故障窗口。
func TestFinishGateIgnoresOperationDeadline(t *testing.T) {
	gate := make(chan struct{})
	o := &observed{emit: &emitter{out: &bytes.Buffer{}}, gate: gate, hold: true}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_, _ = o.Finish(ctx, d.FinishRequest{Token: d.Token{Identity: d.Identity{RunID: "one"}, Epoch: 1}})
		close(done)
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		o.mu.Lock()
		held := o.finishHeld != nil
		o.mu.Unlock()
		if held {
			break
		}
		time.Sleep(time.Millisecond)
	}
	o.mu.Lock()
	held := o.finishHeld != nil
	o.mu.Unlock()
	if !held {
		t.Fatal("Finish did not enter gate")
	}
	cancel()
	select {
	case <-done:
		t.Fatal("operation cancellation removed kill window")
	case <-time.After(30 * time.Millisecond):
	}
	o.mu.Lock()
	held = o.finishHeld != nil
	o.mu.Unlock()
	if !held {
		t.Fatal("Finish gate lost after cancellation")
	}
	// 仅在检查后释放；已取消上下文绝不进入数据库变更。
	close(gate)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("gate did not release")
	}
}

type fakeDrainer struct {
	sealed bool
	active int
}

func (f *fakeDrainer) Drain(ctx context.Context) error { f.sealed = true; return ctx.Err() }
func (f *fakeDrainer) Active() int                     { return f.active }
func TestDrainACKSealsBeforeEmission(t *testing.T) {
	f := &fakeDrainer{active: 1}
	var b bytes.Buffer
	sealDrain(context.Background(), f, &emitter{out: &b})
	if !f.sealed || !bytes.Contains(b.Bytes(), []byte(`"kind":"draining"`)) || !bytes.Contains(b.Bytes(), []byte(`"active":1`)) {
		t.Fatal("drain ACK did not follow sealing with active reservation")
	}
}
