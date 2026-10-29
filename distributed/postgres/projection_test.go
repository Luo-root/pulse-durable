package postgres

import (
	"context"
	"errors"
	d "github.com/Luo-root/pulse-durable/distributed"
	"testing"
	"time"
)

func TestProjectionGatesIndependentAndCancelable(t *testing.T) {
	c := &Coordinator{}
	a := d.Token{Identity: d.Identity{Namespace: "ns", RunID: "a"}, Epoch: 1}
	b := a
	b.Identity.RunID = "b"
	_, release, e := c.projectionGate(context.Background(), a)
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, _, e = c.projectionGate(ctx, a); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatalf("same-token gate ignored cancellation: %v", e)
	}
	ctxB, cancelB := context.WithTimeout(context.Background(), time.Second)
	defer cancelB()
	_, done, e := c.projectionGate(ctxB, b)
	if e != nil {
		t.Fatalf("slow A blocked independent B: %v", e)
	}
	done()
	// 撤销绝不需要繁忙的令牌关卡，也不能删除新纪元。
	newer := a
	newer.Epoch = 2
	p, done, e := c.projectionGate(context.Background(), newer)
	if e != nil {
		t.Fatal(e)
	}
	defer done()
	c.discardProjection(a)
	if p.revoked.Load() {
		t.Fatal("old token invalidation revoked new epoch")
	}
	c.projectionMu.Lock()
	_, old := c.projections[a]
	_, fresh := c.projections[newer]
	c.projectionMu.Unlock()
	if old || !fresh {
		t.Fatal("cache cleanup crossed token identity")
	}
}
