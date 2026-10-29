package filestore

import (
	"context"
	"encoding/json"
	"errors"
	durable "github.com/Luo-root/pulse-durable"
	"os"
	"sync"
	"testing"
	"time"
)

type parkedContext struct {
	context.Context
	once             sync.Once
	entered, release chan struct{}
}

func (c *parkedContext) Err() error {
	c.once.Do(func() { close(c.entered); <-c.release })
	return c.Context.Err()
}
func TestCloseWaitsForInflightApply(t *testing.T) {
	st := newTestStore(t)
	ss := acquireTestSession(t, st, "run")
	r := incrementalSeed("run")
	if err := ss.Initialize(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	ctx := &parkedContext{Context: t.Context(), entered: make(chan struct{}), release: make(chan struct{})}
	applied := make(chan error, 1)
	go func() { _, err := ss.Apply(ctx, revisionBatch(t, r)); applied <- err }()
	<-ctx.entered
	closed := make(chan error, 1)
	go func() { closed <- ss.Close() }()
	select {
	case err := <-closed:
		t.Fatal("Close returned during Apply", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(ctx.release)
	if err := <-applied; err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if _, err := ss.Load(t.Context()); !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
	fresh := acquireTestSession(t, st, "run")
	got, err := fresh.Load(t.Context())
	if err != nil || got.Revision != 2 {
		t.Fatal("inflight commit lost", err)
	}
}
func TestApplyRejectsChangedHeadAndChainBudget(t *testing.T) {
	for _, mode := range []string{"anchor", "budget"} {
		t.Run(mode, func(t *testing.T) {
			st := newTestStore(t)
			ss := acquireTestSession(t, st, "run")
			r := incrementalSeed("run")
			if err := ss.Initialize(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			inner := ss.(*session)
			h := inner.head
			if mode == "anchor" {
				h.HeadChain[0] ^= 1
			} else {
				h.BatchBytes = maxChainBytes
				h.HistoryBytes = maxChainBytes
			}
			data, err := json.Marshal(h)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(inner.path, data, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err = ss.Apply(t.Context(), revisionBatch(t, r)); !errors.Is(err, durable.ErrConflict) {
				t.Fatal("changed head accepted", err)
			}
			after, err := os.ReadFile(inner.path)
			if err != nil || string(after) != string(data) {
				t.Fatal("CAS changed authority", err)
			}
		})
	}
}
