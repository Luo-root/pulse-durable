package durable

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
)

func TestCompletedWriterTerminalSurvivesLaterStopResume(t *testing.T) {
	base := NewMemoryStore()
	sourceDone, stopped := make(chan struct{}), make(chan struct{})
	var sourceOnce, stopOnce sync.Once
	var calls atomic.Int32
	observed := &streamObservationStore{Store: base, afterSave: func(record RunRecord) {
		if recordHasNode(record, "terminal-history", "source") {
			sourceOnce.Do(func() { close(sourceDone) })
		}
	}}
	build := func() *pulse.Graph {
		g := streamAliasGraph(t, "terminal-history", stopped, func(rc *pulse.RunCtx, stream pulse.Stream[int]) error {
			if _, err := stream.Receive(rc); err != nil {
				return err
			}
			select {
			case <-sourceDone:
			case <-rc.Context().Done():
				return rc.Context().Err()
			}
			if err := stream.Stop(rc); err != nil {
				return err
			}
			stopOnce.Do(func() { close(stopped) })
			return nil
		}, &calls)
		if err := g.DeclareOutput(pulse.NewPort(pulse.NewKey[pulse.Stream[int]]("original"))); err != nil {
			t.Fatal(err)
		}
		return g
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	runtime := testRuntime(t, observed, NewCodecs())
	opts := testOptions("terminal-history")
	for _, resume := range []bool{false, true} {
		var e *pulse.Execution
		var err error
		if resume {
			e, err = runtime.Resume(ctx, build(), pulse.Inputs{}, opts)
		} else {
			e, err = runtime.Run(ctx, build(), pulse.Inputs{}, opts)
		}
		if err != nil {
			t.Fatal(err)
		}
		stream, state, err := pulse.ReadOutput(e, pulse.NewKey[pulse.Stream[int]]("original"))
		if err != nil || state != pulse.SlotStateReady {
			t.Fatalf("stream output after resume=%v: state=%s err=%v", resume, state, err)
		}
		snapshot := stream.Snapshot()
		if snapshot.Terminal != pulse.StreamCompleted || !snapshot.Stopped {
			t.Fatalf("resume=%v rewrote completed writer after later reader Stop: %+v", resume, snapshot)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("completed source reran %d times", calls.Load())
	}
}
