package soaktest

import (
	"context"
	"errors"
	"testing"
	"time"
)

type heldClaimWorker struct {
	entered   chan context.Context
	draining  chan struct{}
	claimDone chan struct{}
}

func (w *heldClaimWorker) Run(ctx context.Context) error {
	w.entered <- ctx
	select {
	case <-w.claimDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-w.draining:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (w *heldClaimWorker) Drain(ctx context.Context) error {
	close(w.draining)
	select {
	case <-w.claimDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func TestShutdownDrainsInFlightClaimBeforeCancel(t *testing.T) {
	w := &heldClaimWorker{make(chan context.Context, 1), make(chan struct{}), make(chan struct{})}
	stop := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- runLifecycle(t.Context(), stop, w, func(string) {}) }()
	ctx := <-w.entered
	close(stop)
	select {
	case <-w.draining:
	case <-time.After(time.Second):
		t.Fatal("admission not sealed")
	}
	if ctx.Err() != nil {
		t.Fatal("in-flight claim canceled instead of drained")
	}
	select {
	case e := <-done:
		t.Fatalf("returned before claim finished: %v", e)
	default:
	}
	close(w.claimDone)
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("drained worker not stopped")
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("context not canceled after orderly exit")
	}
}
