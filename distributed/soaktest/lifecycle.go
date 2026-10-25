package soaktest

import (
	"context"
	"errors"
	"time"
)

type lifecycleWorker interface {
	Run(context.Context) error
	Drain(context.Context) error
}

// Graceful shutdown never cancels an in-flight Claim/ResolveClaim/renewal.
// Drain seals admission and waits for every capacity reservation; Run must then
// exit before its context is canceled. Timeouts remain failures, not exceptions
// to event classification.
func runLifecycle(parent context.Context, shutdown <-chan struct{}, w lifecycleWorker, emit func(string)) error {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case e := <-done:
		if e != nil {
			return errors.New("worker run failed")
		}
		return errors.New("worker exited before shutdown")
	case <-parent.Done():
		return errors.New("worker parent canceled before drain")
	case <-shutdown:
	}
	emit("drain_started")
	drainCtx, stop := context.WithTimeout(ctx, 8*time.Second)
	defer stop()
	if w.Drain(drainCtx) != nil {
		return errors.New("worker drain failed")
	}
	select {
	case e := <-done:
		if e != nil {
			return errors.New("worker run failed during drain")
		}
	case <-drainCtx.Done():
		return errors.New("worker did not exit after drain")
	}
	emit("drained")
	return nil
}
