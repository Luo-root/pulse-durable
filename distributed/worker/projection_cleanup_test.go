package worker

import (
	"context"
	d "github.com/Luo-root/pulse-durable/distributed"
	"sync"
	"testing"
	"time"
)

type localProjectionCoordinator struct {
	d.Coordinator
	mu     sync.Mutex
	cached map[d.Token]bool
}

func (c *localProjectionCoordinator) DropOwnedProjection(token d.Token) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.cached, token)
}
func TestSessionCloseDropsProjectionAfterLocalLeaseFailure(t *testing.T) {
	for _, mode := range []string{"timer", "late-successful-renew"} {
		t.Run(mode, func(t *testing.T) {
			old := d.Token{Identity: d.Identity{Namespace: "ns", RunID: "run"}, Epoch: 1}
			newer := old
			newer.Epoch = 2
			c := &localProjectionCoordinator{cached: map[d.Token]bool{old: true, newer: true}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			s := &session{c: c, token: old, ctx: ctx, cancel: cancel, timeout: time.Second}
			if mode == "timer" {
				if !s.confirmed(time.Now(), 30*time.Millisecond, 0) {
					t.Fatal("initial budget")
				}
				select {
				case <-ctx.Done():
				case <-time.After(time.Second):
					t.Fatal("lease timer did not seal")
				}
			} else {
				// DB Renew can succeed after the conservative request-start budget elapsed.
				if s.confirmed(time.Now().Add(-time.Second), 100*time.Millisecond, 0) {
					t.Fatal("late renewal accepted")
				}
				s.seal(true)
			}
			if e := s.Close(); e != nil {
				t.Fatal(e)
			}
			if e := s.Close(); e != nil {
				t.Fatal(e)
			}
			c.mu.Lock()
			defer c.mu.Unlock()
			if c.cached[old] || !c.cached[newer] {
				t.Fatal("local close leaked old cache or removed new epoch")
			}
		})
	}
}
