package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/filestore"
)

func TestStreamRecoveryAcrossProcessExit(t *testing.T) {
	for _, scenario := range []string{"append-before", "append-after", "commit-before", "commit-after", "finish-before", "finish-after", "completed-tail", "uncertain-append", "uncertain-commit", "message-failed", "message-budget", "nested-stream", "ordinary-commit-after", "ordinary-first-write"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			err := streamRecoveryProcess(t, dir, scenario, "run")
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != crashExit {
				t.Fatalf("first process did not exit at durable boundary: %v", err)
			}
			before := loadStreamRecord(t, dir)
			assertStreamTransactions(t, before)
			if strings.HasPrefix(scenario, "ordinary-") {
				if _, complete := before.Scopes[before.RootScope].Nodes["processor"]; complete {
					t.Fatal("ordinary-value crash occurred after complete node confirmation")
				}
				var persisted string
				for _, managed := range before.Managed {
					if managed.NodeID == "processor" {
						for _, value := range managed.Values {
							if value.Name == "ordinary" {
								persisted = string(value.Data)
							}
						}
					}
				}
				if want := fmt.Sprint(ordinaryRecoveryValue(scenario)); persisted != want {
					t.Fatalf("confirmed ordinary value=%q, want effective first write %q", persisted, want)
				}
			}
			if err := streamRecoveryProcess(t, dir, scenario, "resume"); err != nil {
				t.Fatalf("resume: %v", err)
			}
			after := loadStreamRecord(t, dir)
			assertStreamTransactions(t, after)
			if after.RootScope != before.RootScope || after.Revision <= before.Revision || after.Format != 2 {
				t.Fatal("lost run identity, revision or format")
			}
			if scenario == "completed-tail" || scenario == "finish-after" {
				if got := callCount(dir, "source"); got != 1 {
					t.Fatalf("completed source body ran %d times", got)
				}
			}
			trace, err := os.ReadFile(filepath.Join(dir, "processor.calls"))
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "message-failed" && string(trace) != "1\n2\n2\n3\n" {
				t.Fatalf("wrong input replay order: %q", trace)
			}
			if strings.HasPrefix(scenario, "ordinary-") && string(trace) != "1\n2\n3\n" {
				t.Fatalf("restoring ordinary values replayed confirmed input: %q", trace)
			}
			if scenario == "message-budget" {
				if strings.Count(string(trace), "2\n") != 2 {
					t.Fatalf("restart replenished message budget: %q", trace)
				}
				if _, ok := after.Scopes[after.RootScope].Nodes["processor"]; ok {
					t.Fatal("exhausted processor committed a complete node")
				}
				return
			}
			for _, stream := range after.Streams {
				if stream.LastSequence != 3 || stream.AckedThrough != 3 || len(stream.Pending) != 0 || stream.Stream.Terminal != "completed" {
					t.Fatalf("lost or unconfirmed stream tail: %+v", stream)
				}
			}
			if value := managedSum(t, after); value != 6 {
				t.Fatalf("restored explicit state=%d, want 6", value)
			}
			t.Logf("real exit/restart: input trace=%q, revision=%d→%d", trace, before.Revision, after.Revision)
		})
	}
}

func TestManagedHistoryRecoveryAcrossProcessExit(t *testing.T) {
	for _, scenario := range []string{"managed-healthy", "managed-missing", "managed-wrong-owner"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			err := streamRecoveryProcess(t, dir, scenario, "run")
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != crashExit {
				t.Fatalf("first process did not exit after confirming message 1: %v", err)
			}
			before := loadStreamRecord(t, dir)
			assertStreamTransactions(t, before)
			if len(before.Messages) != 1 || len(before.Managed) != 1 || managedSum(t, before) != 1 {
				t.Fatal("crash did not retain exactly one confirmed message and sum=1")
			}
			for _, message := range before.Messages {
				if message.State != durable.AttemptConfirmed || message.Sequence != 1 {
					t.Fatal("crash did not retain the first confirmation")
				}
			}
			for _, stream := range before.Streams {
				if stream.Stream.ID.NodeID == "source" && stream.AckedThrough != 1 {
					t.Fatal("crash did not retain input Ack=1")
				}
			}
			if _, ready := before.Scopes[before.RootScope].Nodes["source-ready"]; !ready {
				t.Fatal("test lacks a complete node for observing premature restore events")
			}
			if scenario != "managed-healthy" {
				store, err := filestore.New(dir)
				if err != nil {
					t.Fatal(err)
				}
				session, err := store.Acquire(context.Background(), before.RunID)
				if err != nil {
					t.Fatal(err)
				}
				// Deliberately persist an incomplete record; a healthy filestore did not lose data.
				for key, managed := range before.Managed {
					if scenario == "managed-missing" {
						delete(before.Managed, key)
					} else {
						managed.Parent = before.RootScope
						before.Managed[key] = managed
					}
				}
				expected := before.Revision
				before.Revision++
				if err := session.Save(context.Background(), before, expected); err != nil {
					t.Fatal(err)
				}
				if err := session.Close(); err != nil {
					t.Fatal(err)
				}
				before = loadStreamRecord(t, dir)
			}
			trace, err := os.ReadFile(filepath.Join(dir, "processor.calls"))
			if err != nil || string(trace) != "1\n" {
				t.Fatalf("wrong pre-resume trace: %q %v", trace, err)
			}
			sourceCalls := callCount(dir, "source")
			if err := streamRecoveryProcess(t, dir, scenario, "resume"); err != nil {
				t.Fatalf("resume verification: %v", err)
			}
			after := loadStreamRecord(t, dir)
			if scenario == "managed-healthy" {
				assertStreamTransactions(t, after)
				trace, err := os.ReadFile(filepath.Join(dir, "processor.calls"))
				if err != nil || string(trace) != "1\n2\n3\n" || managedSum(t, after) != 6 {
					t.Fatalf("healthy resume lost or replayed a confirmed input: trace=%q sum=%d err=%v", trace, managedSum(t, after), err)
				}
				t.Logf("healthy exit=%d, resume=0, sum=6, revision=%d→%d", exit.ExitCode(), before.Revision, after.Revision)
				return
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("rejected restore advanced revision, generation or persisted history")
			}
			afterTrace, err := os.ReadFile(filepath.Join(dir, "processor.calls"))
			if err != nil || string(afterTrace) != string(trace) || callCount(dir, "source") != sourceCalls {
				t.Fatalf("rejected restore ran business: trace=%q source calls=%d err=%v", afterTrace, callCount(dir, "source"), err)
			}
			t.Logf("damaged history rejected before business/events/Save; exit=%d, resume verifier=0, revision=%d unchanged", exit.ExitCode(), after.Revision)
		})
	}
}

func TestConfirmedManagedValuesAcrossProcessExit(t *testing.T) {
	for _, scenario := range []string{"healthy", "node-skipped", "managed-skipped", "before-reconcile", "reconcile-control"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			name := "confirmed-values-" + scenario
			if err := streamRecoveryProcess(t, dir, name, "run"); err != nil {
				t.Fatalf("healthy seed process: %v", err)
			}
			before := loadStreamRecord(t, dir)
			node := before.Scopes[before.RootScope].Nodes["processor"]
			if len(before.Managed) != 1 || len(node.Outputs) != 1 || string(node.Outputs[0].Data) != "42" || node.Outputs[0].State != "ready" {
				t.Fatal("seed did not confirm the ordinary value")
			}
			for _, managed := range before.Managed {
				if managed.Parent != node.Attempt || !reflect.DeepEqual(managed.Values, node.Outputs) {
					t.Fatal("healthy seed has inconsistent owners or values")
				}
			}
			if scenario != "healthy" {
				store, err := filestore.New(dir)
				if err != nil {
					t.Fatal(err)
				}
				session, err := store.Acquire(t.Context(), before.RunID)
				if err != nil {
					t.Fatal(err)
				}
				record, err := session.Load(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				// Deliberate, individually valid record edits through public CAS; a
				// healthy Runtime is not claimed to produce contradictory values.
				scope := record.Scopes[record.RootScope]
				if scenario == "node-skipped" || scenario == "before-reconcile" {
					value := scope.Nodes["processor"]
					value.Outputs[0].State, value.Outputs[0].Data, value.Outputs[0].Digest = "skipped", nil, ""
					scope.Nodes["processor"] = value
				}
				if scenario == "managed-skipped" {
					for key, managed := range record.Managed {
						managed.Values[0].State, managed.Values[0].Data, managed.Values[0].Digest = "skipped", nil, ""
						record.Managed[key] = managed
					}
				}
				if strings.Contains(scenario, "reconcile") {
					// Legacy format-2 shape: producer failed after its consumer
					// confirmed, but its root failure still needs one reconcile Save.
					source := scope.Nodes["source"].Attempt
					attempt := record.Attempts[source]
					attempt.State = durable.AttemptActive
					record.Attempts[source] = attempt
					delete(scope.Nodes, "source")
					for key, stream := range record.Streams {
						stream.Stream.Terminal = string(pulse.StreamFailed)
						record.Streams[key] = stream
					}
				}
				record.Scopes[record.RootScope] = scope
				expected := record.Revision
				record.Revision++
				if err := session.Save(t.Context(), record, expected); err != nil {
					t.Fatal(err)
				}
				if err := session.Close(); err != nil {
					t.Fatal(err)
				}
				before = loadStreamRecord(t, dir)
			}
			if err := streamRecoveryProcess(t, dir, name, "resume"); err != nil {
				t.Fatalf("new-process verification: %v", err)
			}
			after := loadStreamRecord(t, dir)
			if scenario != "healthy" && scenario != "reconcile-control" && !reflect.DeepEqual(before, after) {
				t.Fatalf("rejected restore changed history: revision %d->%d", before.Revision, after.Revision)
			}
			if scenario == "reconcile-control" && after.Revision != before.Revision+1 {
				t.Fatal("control did not exercise the pending reconcile Save")
			}
			if callCount(dir, "source") != 1 || callCount(dir, "processor") != 1 {
				t.Fatal("completed business was replayed")
			}
			t.Logf("real seed/Resume processes: %s, revision=%d->%d, source/processor calls=1/1", scenario, before.Revision, after.Revision)
		})
	}
}

func confirmedManagedValuesProcess(t *testing.T, store durable.Store, dir, scenario, phase string) {
	t.Helper()
	g, err := pulse.New("confirmed-values", "")
	if err != nil {
		t.Fatal(err)
	}
	input, result := pulse.NewKey[pulse.Stream[int]]("input"), pulse.NewKey[int]("result")
	if err := g.DeclareOutput(pulse.NewPort(result)); err != nil {
		t.Fatal(err)
	}
	if err := pulse.ProduceStream(g, "source", "", input, func(_ *pulse.RunCtx, writer *pulse.StreamWriter[int]) error {
		logCall(dir, "source")
		return writer.Send(42)
	}, pulse.WithBuffer(1), pulse.WithStreamSource("one-value", true)); err != nil {
		t.Fatal(err)
	}
	if err := g.Add(pulse.NewNode("processor", "", pulse.Requires(input), pulse.Provides(result), func(rc *pulse.RunCtx) error {
		logCall(dir, "processor")
		stream, err := pulse.Get(rc, input)
		if err != nil {
			return err
		}
		for {
			delivery, err := stream.Receive(rc)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			if err := pulse.HandleMessage(rc, delivery, func(rc *pulse.RunCtx, value int) error { return pulse.Set(rc, result, value) }); err != nil {
				return err
			}
		}
	})); err != nil {
		t.Fatal(err)
	}
	var events, beforeStarts, starts, saves atomic.Int32
	rt, err := durable.New(&streamInjectedStore{Store: store, saves: &saves}, durable.NewCodecs())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	opts := durable.RunOptions{RunID: "stream-run", Version: "confirmed-values/1"}
	if phase == "run" {
		if _, err := rt.Run(ctx, g, pulse.Inputs{}, opts); err != nil {
			t.Fatal(err)
		}
		return
	}
	opts.OnEvent = func(durable.Event) error { events.Add(1); return nil }
	opts.BeforeStart = func(*pulse.Execution) error { beforeStarts.Add(1); return nil }
	opts.ExecOptions = []pulse.ExecOption{pulse.WithExecObserver(pulse.ObserverFunc{GraphStarted: func(pulse.Site) { starts.Add(1) }})}
	e, err := rt.Resume(ctx, g, pulse.Inputs{}, opts)
	if scenario == "confirmed-values-healthy" {
		if err != nil {
			t.Fatal(err)
		}
		value, state, err := pulse.ReadOutput(e, result)
		if err != nil || value != 42 || state != pulse.SlotStateReady || events.Load() != 2 || beforeStarts.Load() != 1 || starts.Load() != 1 {
			t.Fatalf("healthy control: value=%d state=%s events=%d BeforeStart=%d Start=%d err=%v", value, state, events.Load(), beforeStarts.Load(), starts.Load(), err)
		}
		return
	}
	if scenario == "confirmed-values-reconcile-control" {
		var scopeFailure *pulse.CheckpointScopeFailure
		if !errors.As(err, &scopeFailure) || saves.Load() != 1 || events.Load()+beforeStarts.Load()+starts.Load() != 0 {
			t.Fatalf("legacy control: err=%v saves=%d events=%d BeforeStart=%d Start=%d", err, saves.Load(), events.Load(), beforeStarts.Load(), starts.Load())
		}
		return
	}
	var checkpoint *pulse.CheckpointError
	if !errors.As(err, &checkpoint) || checkpoint.Operation != "validate restore" || saves.Load()+events.Load()+beforeStarts.Load()+starts.Load() != 0 {
		t.Fatalf("conflict reached reconcile/events/BeforeStart/Start/Save: err=%v saves=%d events=%d BeforeStart=%d Start=%d", err, saves.Load(), events.Load(), beforeStarts.Load(), starts.Load())
	}
}

// Each phase is a fresh process using the real filestore. Save may fail before
// writing or after its real CAS succeeds; neither case may lose reliable classification.
func TestSharedRestoreFailureAcrossProcesses(t *testing.T) {
	for _, scenario := range []string{"store-error", "cancel-error", "committed-cancel-error"} {
		t.Run(scenario, func(t *testing.T) {
			dir, name := t.TempDir(), "shared-restore-"+scenario
			if err := streamRecoveryProcess(t, dir, name, "seed"); err != nil {
				t.Fatal(err)
			}
			before := loadStreamRecord(t, dir)
			if len(before.Streams) != 1 || callCount(dir, "source") != 1 {
				t.Fatal("seed did not confirm one source")
			}
			for _, stream := range before.Streams {
				if stream.Stream.ID.Generation != 1 || stream.Stream.Terminal != "completed" {
					t.Fatal("seed was not complete generation 1")
				}
			}
			// Continue to recovery even on a classification failure so the old-Core
			// counterexample cannot be mistaken for record loss or an unjoined Save.
			if err := streamRecoveryProcess(t, dir, name, "resume"); err != nil {
				t.Errorf("classification: %v", err)
			}
			after := loadStreamRecord(t, dir)
			if scenario == "committed-cancel-error" {
				if after.Revision != before.Revision+1 {
					t.Fatal("real Save did not advance exactly one revision")
				}
				for _, stream := range after.Streams {
					if stream.Stream.ID.Generation != 2 {
						t.Fatal("real Save did not retain generation 2")
					}
				}
			} else if !reflect.DeepEqual(before, after) {
				t.Fatal("uncommitted Save changed authoritative record")
			}
			if err := streamRecoveryProcess(t, dir, name, "recover"); err != nil {
				t.Fatal(err)
			}
			recovered := loadStreamRecord(t, dir)
			generation := uint64(2)
			if scenario == "committed-cancel-error" {
				generation = 3
			}
			for _, stream := range recovered.Streams {
				if stream.Stream.ID.Generation != generation || stream.Stream.Terminal != "completed" {
					t.Fatal("recovery lost authoritative generation/terminal")
				}
			}
			if recovered.RootScope != before.RootScope || recovered.Revision != after.Revision+1 || callCount(dir, "source") != 1 {
				t.Fatal("recovery changed run identity, revision or confirmed source calls")
			}
			t.Logf("three independent processes: generation=1→%d→%d revision=%d→%d→%d source_calls=1", generation-1, generation, before.Revision, after.Revision, recovered.Revision)
		})
	}
}

type sharedRestoreStore struct {
	durable.Store
	outer                context.Context
	entered              chan context.Context
	reply, settled       chan struct{}
	once                 sync.Once
	inflight, earlyClose atomic.Bool
	committed            bool
}

type sharedRestoreSession struct {
	durable.Session
	owner *sharedRestoreStore
}

func (s *sharedRestoreStore) Acquire(ctx context.Context, id string) (durable.Session, error) {
	session, err := s.Store.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	return &sharedRestoreSession{session, s}, nil
}

func (s *sharedRestoreSession) Save(ctx context.Context, record durable.RunRecord, revision uint64) error {
	o := s.owner
	for _, stream := range record.Streams {
		if stream.Stream.ID.Generation != 2 {
			continue
		}
		held := false
		o.once.Do(func() { held = true })
		if !held {
			break
		}
		o.inflight.Store(true)
		defer func() { o.inflight.Store(false); close(o.settled) }()
		if o.committed {
			if err := s.Session.Save(ctx, record, revision); err != nil {
				return err
			}
		}
		o.entered <- ctx
		select {
		case <-o.reply:
		case <-o.outer.Done():
			return o.outer.Err()
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return errors.New("accepted restore Save failed")
	}
	return s.Session.Save(ctx, record, revision)
}

func (s *sharedRestoreSession) Close() error {
	o := s.owner
	if o.inflight.Load() {
		o.earlyClose.Store(true)
		select {
		case <-o.settled:
		case <-o.outer.Done():
			return o.outer.Err()
		}
	}
	return s.Session.Close()
}

func sharedRestoreAwait[T any](t *testing.T, ctx context.Context, signal <-chan T) T {
	t.Helper()
	select {
	case value := <-signal:
		return value
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		var zero T
		return zero
	}
}

func sharedRestoreFailureProcess(t *testing.T, fs durable.Store, dir, scenario, phase string) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	g, err := pulse.New("shared-restore", "")
	if err != nil {
		t.Fatal(err)
	}
	key := pulse.NewKey[pulse.Stream[int]]("output")
	if err := g.DeclareOutput(pulse.NewPort(key)); err != nil {
		t.Fatal(err)
	}
	if err := pulse.ProduceStream(g, "source", "empty confirmed source", key, func(*pulse.RunCtx, *pulse.StreamWriter[int]) error {
		logCall(dir, "source")
		return nil
	}, pulse.WithStreamSource("empty", true)); err != nil {
		t.Fatal(err)
	}
	opts := durable.RunOptions{RunID: "stream-run", Version: "shared-restore/1"}
	var execution *pulse.Execution
	var sub *pulse.StreamSubscription[int]
	opts.BeforeStart = func(e *pulse.Execution) error {
		execution = e
		var err error
		sub, err = pulse.SubscribeStream(e, key)
		return err
	}
	if phase != "resume" {
		received := make(chan error, 1)
		opts.ExecOptions = []pulse.ExecOption{pulse.WithExecObserver(pulse.ObserverFunc{GraphStarted: func(pulse.Site) {
			go func() { _, err := sub.Receive(ctx); received <- err }()
		}})}
		rt, err := durable.New(fs, durable.NewCodecs())
		if err != nil {
			t.Fatal(err)
		}
		if phase == "seed" {
			_, err = rt.Run(ctx, g, pulse.Inputs{}, opts)
		} else {
			_, err = rt.Resume(ctx, g, pulse.Inputs{}, opts)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err := sharedRestoreAwait(t, ctx, received); err != io.EOF {
			t.Fatalf("healthy Receive=%v", err)
		}
		return
	}
	store := &sharedRestoreStore{Store: fs, outer: ctx, entered: make(chan context.Context, 1), reply: make(chan struct{}), settled: make(chan struct{}), committed: scenario == "shared-restore-committed-cancel-error"}
	var release sync.Once
	defer release.Do(func() { close(store.reply) })
	rt, err := durable.New(store, durable.NewCodecs())
	if err != nil {
		t.Fatal(err)
	}
	cancellationFirst := scenario != "shared-restore-store-error"
	ready, sourceCanceled := make(chan struct{}), make(chan struct{})
	var graphReason pulse.NodeFinishReason
	var graphErr error
	var graphFinished atomic.Int32
	opts.ExecOptions = []pulse.ExecOption{pulse.WithExecObserver(pulse.ObserverFunc{
		GraphStarted: func(pulse.Site) {
			go func() { _, _ = sub.Receive(ctx) }()
			saveCtx := sharedRestoreAwait(t, ctx, store.entered)
			if cancellationFirst {
				execution.Cancel()
				sharedRestoreAwait(t, ctx, saveCtx.Done())
			}
			close(ready)
		},
		Finished: func(_ pulse.Site, node string, reason pulse.NodeFinishReason, _ error) {
			if node == "source" && reason == pulse.NodeCanceled {
				close(sourceCanceled)
			}
		},
		GraphFinished: func(_ pulse.Site, reason pulse.NodeFinishReason, err error) {
			graphReason, graphErr = reason, err
			graphFinished.Add(1)
		},
	})}
	result := make(chan error, 1)
	go func() { _, err := rt.Resume(ctx, g, pulse.Inputs{}, opts); result <- err }()
	sharedRestoreAwait(t, ctx, ready)
	if cancellationFirst {
		sharedRestoreAwait(t, ctx, sourceCanceled)
		if !errors.Is(execution.Err(), context.Canceled) {
			t.Fatal("source cancellation was not first")
		}
	}
	select {
	case err := <-result:
		t.Fatalf("Runtime returned before Save reply: %v", err)
	default:
	}
	release.Do(func() { close(store.reply) })
	err = sharedRestoreAwait(t, ctx, result)
	sharedRestoreAwait(t, ctx, store.settled)
	var checkpoint, finalCheckpoint *pulse.CheckpointError
	final := execution.Wait()
	// Runtime joins its session-close result; compare the reliable error identity
	// within that chain and keep Core's own terminal values exactly identical.
	if !errors.As(err, &checkpoint) || !errors.As(final, &finalCheckpoint) || checkpoint != finalCheckpoint || execution.Err() != final || execution.Snapshot().Graph.Err != final || execution.Snapshot().Graph.Status != pulse.GraphStatusFailed || graphReason != pulse.NodeFailed || graphErr != final || graphFinished.Load() != 1 {
		t.Errorf("accepted Save classification/observation lost: checkpoint=%t result=%v status=%s GraphFinished=%s/%d", checkpoint != nil, err, execution.Snapshot().Graph.Status, graphReason, graphFinished.Load())
	}
	if store.earlyClose.Load() {
		t.Error("Runtime closed session before admitted Save returned")
	}
	t.Logf("Save admitted and joined: checkpoint=%t status=%s source_calls=%d", checkpoint != nil, execution.Snapshot().Graph.Status, callCount(dir, "source"))
}

func streamRecoveryProcess(t *testing.T, dir, scenario, phase string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStreamRecoveryProcessHelper$", "-test.count=1")
	cmd.Env = append(os.Environ(), "PULSE_STREAM_DIR="+dir, "PULSE_STREAM_CASE="+scenario, "PULSE_STREAM_PHASE="+phase)
	output, err := cmd.CombinedOutput()
	if len(output) > 0 {
		t.Logf("%s/%s: %s", scenario, phase, output)
	}
	if ctx.Err() != nil {
		t.Fatalf("helper deadline: %v", ctx.Err())
	}
	return err
}

func loadStreamRecord(t *testing.T, dir string) durable.RunRecord {
	t.Helper()
	store, err := filestore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.Acquire(context.Background(), "stream-run")
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	record, err := session.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func managedSum(t *testing.T, record durable.RunRecord) int {
	t.Helper()
	for _, managed := range record.Managed {
		if managed.NodeID == "processor" {
			for _, state := range managed.State {
				if state.Name == "sum" {
					var value int
					if err := json.Unmarshal(state.Data, &value); err != nil {
						t.Fatal(err)
					}
					return value
				}
			}
		}
	}
	return 0
}

func assertStreamTransactions(t *testing.T, record durable.RunRecord) {
	t.Helper()
	var input, output durable.StreamRecord
	for _, stream := range record.Streams {
		if stream.Stream.ID.NodeID == "source" {
			input = stream
		}
		if stream.Stream.ID.NodeID == "processor" {
			output = stream
		}
	}
	var sum int
	for _, attempt := range record.Messages {
		if attempt.State == durable.AttemptConfirmed {
			sum += int(attempt.Sequence)
			for _, pending := range input.Pending {
				if pending.Sequence == attempt.Sequence {
					t.Fatal("confirmed message retained pending input")
				}
			}
		}
	}
	if managedSum(t, record) != sum {
		t.Fatal("managed state and message confirmations diverged")
	}
	var confirmed int
	for _, attempt := range record.Messages {
		if attempt.State == durable.AttemptConfirmed {
			confirmed++
		}
	}
	if output.LastSequence != uint64(confirmed) {
		t.Fatalf("output count=%d but confirmed inputs=%d", output.LastSequence, confirmed)
	}
	for _, stream := range record.Streams {
		if stream.Stream.Terminal == "completed" {
			node, ok := record.Scopes[stream.Stream.ID.Scope].Nodes[stream.Stream.ID.NodeID]
			if !ok || record.Attempts[node.Attempt].State != durable.AttemptConfirmed {
				t.Fatal("source finish and complete node were not atomic")
			}
		}
	}
}

func TestStreamRecoveryProcessHelper(t *testing.T) {
	dir := os.Getenv("PULSE_STREAM_DIR")
	if dir == "" {
		return
	}
	scenario, phase := os.Getenv("PULSE_STREAM_CASE"), os.Getenv("PULSE_STREAM_PHASE")
	store, err := filestore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(scenario, "confirmed-values-") {
		confirmedManagedValuesProcess(t, store, dir, scenario, phase)
		return
	}
	if strings.HasPrefix(scenario, "shared-restore-") {
		sharedRestoreFailureProcess(t, store, dir, scenario, phase)
		return
	}
	backend := &streamInjectedStore{Store: store, scenario: scenario, first: phase == "run"}
	rt, err := durable.New(backend, durable.NewCodecs())
	if err != nil {
		t.Fatal(err)
	}
	g := streamRecoveryGraph(t, dir, scenario)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	opts := durable.RunOptions{RunID: "stream-run", Version: "stream-recovery/1"}
	var events, observations, starts atomic.Int32
	if strings.HasPrefix(scenario, "managed-") && phase == "resume" {
		opts.OnEvent = func(durable.Event) error { events.Add(1); return nil }
		opts.BeforeStart = func(*pulse.Execution) error { starts.Add(1); return nil }
		opts.ExecOptions = []pulse.ExecOption{pulse.WithExecObserver(pulse.ObserverFunc{
			GraphStarted: func(pulse.Site) { observations.Add(1) },
		})}
	}
	var e *pulse.Execution
	if phase == "run" {
		e, err = rt.Run(ctx, g, pulse.Inputs{}, opts)
	} else {
		e, err = rt.Resume(ctx, g, pulse.Inputs{}, opts)
	}
	if phase == "run" && strings.HasPrefix(scenario, "uncertain-") {
		var checkpoint *pulse.CheckpointError
		if err == nil || !errors.As(err, &checkpoint) || !errors.Is(err, pulse.ErrSkipped) || e.Snapshot().Graph.Status != pulse.GraphStatusFailed {
			t.Fatalf("uncertain reliable error swallowed: %v", err)
		}
		os.Exit(crashExit)
	}
	if (scenario == "managed-missing" || scenario == "managed-wrong-owner") && phase == "resume" {
		var checkpoint *pulse.CheckpointError
		if !errors.As(err, &checkpoint) || checkpoint.Operation != "validate restore" || events.Load() != 0 || observations.Load() != 0 || starts.Load() != 0 {
			t.Fatalf("incomplete history was not rejected before recovery events/Start: err=%v events=%d observations=%d starts=%d sum=%d", err, events.Load(), observations.Load(), starts.Load(), managedSum(t, loadStreamRecord(t, dir)))
		}
		return
	}
	if scenario == "managed-healthy" && phase == "resume" && (events.Load() == 0 || observations.Load() != 1 || starts.Load() != 1) {
		t.Fatalf("healthy control did not exercise restoration and Start: events=%d observations=%d starts=%d err=%v", events.Load(), observations.Load(), starts.Load(), err)
	}
	if scenario == "message-budget" && phase == "resume" {
		if err == nil {
			t.Fatal("exhausted message succeeded")
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(scenario, "ordinary-") {
		value, state, err := pulse.ReadOutput(e, pulse.NewKey[int]("ordinary"))
		if err != nil || state != pulse.SlotStateReady || value != ordinaryRecoveryValue(scenario) {
			t.Fatalf("message value lost across process exit: value=%d state=%s err=%v", value, state, err)
		}
	}
}

func ordinaryRecoveryValue(scenario string) int {
	if scenario == "ordinary-first-write" {
		return 11
	}
	return 42
}

func streamRecoveryGraph(t *testing.T, dir, scenario string) *pulse.Graph {
	t.Helper()
	g, err := pulse.New("stream-recovery", "真实进程退出增量恢复")
	if err != nil {
		t.Fatal(err)
	}
	input, output := pulse.NewKey[pulse.Stream[int]]("input"), pulse.NewKey[pulse.Stream[int]]("output")
	ordinary := pulse.NewKey[int]("ordinary")
	done := pulse.NewKey[int]("done")
	sinkProvides := pulse.Provides[int]()
	if scenario == "nested-stream" {
		sinkProvides = pulse.Provides(done)
		if err := g.DeclareOutput(pulse.NewPort(done)); err != nil {
			t.Fatal(err)
		}
	}
	processorProvides := pulse.Provides(output)
	if strings.HasPrefix(scenario, "ordinary-") {
		processorProvides = pulse.Deps(processorProvides, pulse.Provides(ordinary))
		if err := g.DeclareOutput(pulse.NewPort(ordinary)); err != nil {
			t.Fatal(err)
		}
	}
	add := func(n *pulse.Node) {
		if err := g.Add(n); err != nil {
			t.Fatal(err)
		}
	}
	sourceRequires := pulse.Requires[int]()
	if strings.HasPrefix(scenario, "managed-") {
		ready := pulse.NewKey[int]("source-ready")
		sourceRequires = pulse.Requires(ready)
		add(pulse.NewNode("source-ready", "恢复事件对照", nil, pulse.Provides(ready), func(rc *pulse.RunCtx) error {
			return pulse.Set(rc, ready, 1)
		}))
	}
	swallow := func(rc *pulse.RunCtx, next func(*pulse.RunCtx) error) error { _ = next(rc); return nil }
	add(pulse.NewNode("source", "可靠渐进来源", sourceRequires, pulse.Provides(input), func(rc *pulse.RunCtx) error {
		logCall(dir, "source")
		writer, err := pulse.OpenStream(rc, input, pulse.WithBuffer(4), pulse.WithStreamSource("source/integers", scenario != "completed-tail"))
		if err != nil {
			return err
		}
		position := writer.Position()
		if position.Sequence > 0 && position.Cursor != int(position.Sequence) {
			return errors.New("source lost its persisted cursor")
		}
		for value := int(position.Sequence) + 1; value <= 3; value++ {
			if err := writer.SendAt(value, value); err != nil {
				return err
			}
		}
		return nil
	}, swallow))
	add(pulse.NewNode("processor", "消息事务", pulse.Requires(input), processorProvides, func(rc *pulse.RunCtx) error {
		writer, err := pulse.OpenStream(rc, output, pulse.WithBuffer(4), pulse.WithStreamSource("processor/output", true))
		if err != nil {
			return err
		}
		stream, err := pulse.Get(rc, input)
		if err != nil {
			return err
		}
		if scenario == "ordinary-first-write" {
			if err := pulse.Set(rc, ordinary, 11); err != nil {
				return err
			}
		}
		for {
			delivery, err := stream.Receive(rc)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			if err := pulse.HandleMessage(rc, delivery, func(rc *pulse.RunCtx, value int) error {
				file, err := os.OpenFile(filepath.Join(dir, "processor.calls"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					return err
				}
				if _, err := fmt.Fprintf(file, "%d\n", value); err != nil {
					_ = file.Close()
					return err
				}
				if err := file.Sync(); err != nil {
					_ = file.Close()
					return err
				}
				if err := file.Close(); err != nil {
					return err
				}
				sum, _, err := pulse.GetMessageState[int](rc, "sum")
				if err != nil {
					return err
				}
				if err := pulse.SetMessageState(rc, "sum", sum+value); err != nil {
					return err
				}
				if err := writer.Send(sum + value); err != nil {
					return err
				}
				if strings.HasPrefix(scenario, "ordinary-") && value == 2 {
					ordinaryValue := 42
					if scenario == "ordinary-first-write" {
						ordinaryValue = 22
					}
					if err := pulse.Set(rc, ordinary, ordinaryValue); err != nil {
						return err
					}
				}
				trace, err := os.ReadFile(filepath.Join(dir, "processor.calls"))
				if err != nil {
					return err
				}
				if value == 2 && (scenario == "message-budget" || scenario == "message-failed" && strings.Count(string(trace), "2\n") == 1) {
					return errors.New("business failure of message 2")
				}
				return nil
			}, pulse.WithMessageRetry(2, 0)); err != nil {
				return err
			}
		}
	}, swallow))
	add(pulse.NewNode("sink", "可靠读取组", pulse.Requires(output), sinkProvides, func(rc *pulse.RunCtx) error {
		stream, err := pulse.Get(rc, output)
		if err != nil {
			return err
		}
		for {
			delivery, err := stream.Receive(rc)
			if err == io.EOF {
				if scenario == "nested-stream" {
					return pulse.Set(rc, done, 1)
				}
				return nil
			}
			if err != nil {
				return err
			}
			if err := delivery.Ack(); err != nil {
				return err
			}
		}
	}))
	if scenario == "nested-stream" {
		parent, err := pulse.New("stream-parent", "嵌套增量作用域")
		if err != nil {
			t.Fatal(err)
		}
		if err := pulse.Sub(parent, "mount", "", []pulse.SubBind{pulse.Out(done, pulse.NewKey[int]("result"))}, g); err != nil {
			t.Fatal(err)
		}
		return parent
	}
	return g
}

type failedStreamCase struct {
	Name                               string
	Outputs, At                        int
	Fault                              string
	Retries                            int
	Always, Nested                     bool
	Alias, HostCancel, Catch, Indirect bool
}

// The barrier is Session.Save's real return, never a sleep between Core calls.
// New multi-output failures cross every terminal threshold in one atomic Save.
func TestFailedStreamRecoveryAcrossProcessExit(t *testing.T) {
	cases := []failedStreamCase{
		{Name: "root", Outputs: 1, At: 1, Fault: "root", Retries: 2},
		{Name: "terminal", Outputs: 1, At: 1, Fault: "exit", Retries: 2},
		{Name: "uncertain", Outputs: 1, At: 1, Fault: "uncertain", Retries: 2},
		{Name: "multi-first", Outputs: 3, At: 1, Fault: "exit", Retries: 2},
		{Name: "multi-third", Outputs: 3, At: 3, Fault: "exit", Retries: 2},
		{Name: "multi-uncertain", Outputs: 3, At: 2, Fault: "uncertain", Retries: 2},
		{Name: "not-saved", Outputs: 3, At: 1, Fault: "before", Retries: 2},
		{Name: "legacy-first", Outputs: 3, At: 1, Fault: "legacy", Retries: 2},
		{Name: "legacy-second", Outputs: 3, At: 2, Fault: "legacy", Retries: 2},
		{Name: "constant", Outputs: 3, At: 1, Fault: "exit", Retries: 2, Always: true},
		{Name: "constant-control", Outputs: 1, Retries: 2, Always: true},
		{Name: "no-retry", Outputs: 1, At: 1, Fault: "exit", Always: true},
		{Name: "nested", Outputs: 2, At: 1, Fault: "uncertain", Retries: 2, Always: true, Nested: true},
		{Name: "confirmed-alias", Outputs: 1, At: 1, Fault: "exit", Retries: 2, Alias: true},
		{Name: "legacy-alias", Outputs: 1, At: 1, Fault: "legacy", Retries: 2, Alias: true},
		{Name: "parent-catch", Outputs: 1, At: 1, Fault: "exit", Retries: 2, Alias: true, Catch: true},
		{Name: "legacy-catch", Outputs: 1, At: 1, Fault: "legacy", Retries: 2, Alias: true, Catch: true},
		{Name: "indirect-alias", Outputs: 1, At: 1, Fault: "exit", Retries: 2, Alias: true, Indirect: true},
		{Name: "host-cancel", Outputs: 2, Retries: 2, HostCancel: true},
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			dir := t.TempDir()
			err := failedStreamProcess(t, dir, tc, "run")
			if tc.Fault == "exit" || tc.Fault == "root" || tc.Fault == "legacy" {
				var exit *exec.ExitError
				if !errors.As(err, &exit) || exit.ExitCode() != crashExit {
					t.Fatalf("did not reach Save return barrier: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			before := loadStreamRecord(t, dir)
			failed := 0
			for _, stream := range before.Streams {
				if stream.Stream.Terminal == "failed" {
					failed++
				}
				want := durable.AttemptFailed
				if tc.Fault == "before" || tc.Fault == "legacy" || tc.HostCancel {
					want = durable.AttemptActive
				}
				if got := before.Attempts[stream.Stream.Attempt].State; got != want {
					t.Fatalf("barrier root=%s, want %s", got, want)
				}
			}
			wantFailed := tc.Outputs
			if tc.Fault == "" && tc.Always {
				wantFailed *= tc.Retries
			}
			if tc.Fault == "legacy" {
				wantFailed = tc.At
			} else if tc.Fault == "before" || tc.HostCancel {
				wantFailed = 0
			}
			if failed != wantFailed {
				t.Fatalf("failed outputs=%d, want %d", failed, wantFailed)
			}
			t.Logf("exact barrier: rev=%d failed=%d/%d", before.Revision, failed, tc.Outputs)
			for _, phase := range []string{"resume", "again"} {
				if err := failedStreamProcess(t, dir, tc, phase); err != nil {
					t.Fatalf("%s: %v", phase, err)
				}
			}
			wantCalls := 2
			if tc.Retries == 0 || tc.Catch {
				wantCalls = 1
			} else if tc.Nested {
				wantCalls = 4
			}
			if got := callCount(dir, "failure-source"); got != wantCalls {
				t.Fatalf("source calls=%d want=%d; failed owner reran or budget changed", got, wantCalls)
			}
			after := loadStreamRecord(t, dir)
			maxIndex := 0
			for _, attempt := range after.Attempts {
				if attempt.Boundary != "" && attempt.Index > maxIndex {
					maxIndex = attempt.Index
				}
			}
			wantIndex := tc.Retries
			if tc.Fault == "before" || tc.HostCancel || tc.Catch {
				wantIndex = 1
			}
			if maxIndex != wantIndex {
				t.Fatalf("Retry index=%d, want %d", maxIndex, wantIndex)
			}
		})
	}
}

func failedStreamProcess(t *testing.T, dir string, tc failedStreamCase, phase string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	data, err := json.Marshal(tc)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestFailedStreamProcessHelper$", "-test.count=1", "-test.timeout=10s")
	cmd.Env = append(os.Environ(), "PULSE_FAILED_DIR="+dir, "PULSE_FAILED_CASE="+string(data), "PULSE_FAILED_PHASE="+phase)
	output, err := cmd.CombinedOutput()
	t.Logf("%s/%s: %s", tc.Name, phase, output)
	if ctx.Err() != nil {
		t.Fatal(ctx.Err())
	}
	return err
}

func TestFailedStreamProcessHelper(t *testing.T) {
	dir := os.Getenv("PULSE_FAILED_DIR")
	if dir == "" {
		return
	}
	var tc failedStreamCase
	if err := json.Unmarshal([]byte(os.Getenv("PULSE_FAILED_CASE")), &tc); err != nil {
		t.Fatal(err)
	}
	phase := os.Getenv("PULSE_FAILED_PHASE")
	store, err := filestore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	aliasSaved := make(chan struct{})
	rt, err := durable.New(&failedStreamStore{Store: store, test: tc, first: phase == "run", aliasSaved: aliasSaved}, durable.NewCodecs())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	g := failedStreamGraph(t, dir, tc, aliasSaved, cancel)
	opts := durable.RunOptions{RunID: "stream-run", Version: "failed-stream/1"}
	var execution *pulse.Execution
	if phase == "run" {
		execution, err = rt.Run(ctx, g, pulse.Inputs{}, opts)
	} else {
		execution, err = rt.Resume(ctx, g, pulse.Inputs{}, opts)
	}
	var checkpoint *pulse.CheckpointError
	if phase == "run" && (tc.Fault == "uncertain" || tc.Fault == "before") {
		if !errors.As(err, &checkpoint) || !errors.Is(err, failedStreamSaveError) {
			t.Fatalf("reliable Save failure did not stop execution: %v", err)
		}
		return
	}
	if phase == "run" && tc.HostCancel {
		if err == nil || errors.As(err, &checkpoint) {
			t.Fatalf("host cancellation became reliable failure or success: %v", err)
		}
		return
	}
	if tc.Always {
		if err == nil || errors.As(err, &checkpoint) {
			t.Fatalf("constant business failure became success/reliable failure: %v", err)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	if tc.Catch && phase != "run" {
		value, state, err := pulse.ReadOutput(execution, pulse.NewKey[int]("result"))
		if err != nil || state != pulse.SlotStateReady || value != 99 {
			t.Fatalf("parent catch output=%d/%s/%v", value, state, err)
		}
	}
}

func failedStreamGraph(t *testing.T, dir string, tc failedStreamCase, aliasSaved <-chan struct{}, cancel context.CancelFunc) *pulse.Graph {
	t.Helper()
	graph := func(id string) *pulse.Graph {
		g, err := pulse.New(id, "")
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	add := func(g *pulse.Graph, n *pulse.Node) {
		if err := g.Add(n); err != nil {
			t.Fatal(err)
		}
	}
	g := graph("failed-child")
	inputs := make([]pulse.Key[pulse.Stream[int]], tc.Outputs)
	for i := range inputs {
		inputs[i] = pulse.NewKey[pulse.Stream[int]](fmt.Sprintf("input%d", i))
	}
	result := pulse.NewKey[int]("result")
	if err := g.DeclareOutput(pulse.NewPort(result)); err != nil {
		t.Fatal(err)
	}
	producer := g
	if tc.Indirect {
		producer = graph("indirect-source")
		if err := producer.DeclareOutput(pulse.NewPort(inputs[0])); err != nil {
			t.Fatal(err)
		}
		if err := pulse.Sub(g, "producer", "", []pulse.SubBind{pulse.Out(inputs[0], inputs[0])}, producer); err != nil {
			t.Fatal(err)
		}
	}
	add(producer, pulse.NewNode("source", "", nil, pulse.Provides(inputs...), func(rc *pulse.RunCtx) error {
		calls := logCall(dir, "failure-source")
		for _, input := range inputs {
			if _, err := pulse.OpenStream(rc, input, pulse.WithBuffer(1), pulse.WithStreamSource(input.Name(), true)); err != nil {
				return err
			}
		}
		if tc.Alias {
			select {
			case <-aliasSaved:
			case <-rc.Context().Done():
				return rc.Context().Err()
			}
		}
		if tc.HostCancel && calls == 1 {
			cancel()
			return context.Canceled
		}
		if tc.Always || calls == 1 {
			return errors.New("same source business failure")
		}
		return nil
	}))
	reads := append([]pulse.Key[pulse.Stream[int]](nil), inputs...)
	if tc.Alias {
		pass, inlet, alias := graph("failed-pass"), pulse.NewKey[pulse.Stream[int]]("inlet"), pulse.NewKey[pulse.Stream[int]]("alias")
		if err := pass.DeclareInput(pulse.NewPort(inlet)); err != nil {
			t.Fatal(err)
		}
		if err := pass.DeclareOutput(pulse.NewPort(inlet)); err != nil {
			t.Fatal(err)
		}
		if err := pulse.Sub(g, "pass", "", []pulse.SubBind{pulse.In(inputs[0], inlet), pulse.Out(inlet, alias)}, pass); err != nil {
			t.Fatal(err)
		}
		reads[0] = alias
	}
	add(g, pulse.NewNode("sink", "", pulse.Requires(reads...), pulse.Provides(result), func(rc *pulse.RunCtx) error {
		for _, input := range reads {
			stream, err := pulse.Get(rc, input)
			if err != nil {
				return err
			}
			if _, err := stream.Receive(rc); err != io.EOF {
				return err
			}
		}
		return pulse.Set(rc, result, 42)
	}))
	if tc.Retries == 0 {
		return g
	}
	parent := graph("failed-parent")
	if err := parent.DeclareOutput(pulse.NewPort(result)); err != nil {
		t.Fatal(err)
	}
	aspects := []pulse.Aspect{pulse.Retry(tc.Retries, 0)}
	if tc.Nested {
		aspects = append(aspects, pulse.Retry(tc.Retries, 0))
	}
	if tc.Catch {
		aspects = append(aspects, func(rc *pulse.RunCtx, next func(*pulse.RunCtx) error) error {
			if err := next(rc); err != nil {
				var checkpoint *pulse.CheckpointError
				if errors.As(err, &checkpoint) {
					return err
				}
				return pulse.Set(rc, result, 99)
			}
			return nil
		})
	}
	if err := pulse.Sub(parent, "mount", "", []pulse.SubBind{pulse.Out(result, result)}, g, aspects...); err != nil {
		t.Fatal(err)
	}
	return parent
}

var failedStreamSaveError = errors.New("lost failed-terminal acknowledgement")

type failedStreamStore struct {
	durable.Store
	test       failedStreamCase
	first      bool
	aliasSaved chan struct{}
	aliasOnce  atomic.Bool
}

type failedStreamSession struct {
	durable.Session
	owner *failedStreamStore
}

func (s *failedStreamStore) Acquire(ctx context.Context, id string) (durable.Session, error) {
	session, err := s.Store.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	return &failedStreamSession{Session: session, owner: s}, nil
}

func (s *failedStreamSession) Save(ctx context.Context, record durable.RunRecord, expected uint64) error {
	tc := s.owner.test
	keys := make([]string, 0)
	for key, stream := range record.Streams {
		if stream.Stream.Terminal == "failed" {
			keys = append(keys, key)
		}
	}
	match := s.owner.first && tc.Fault != "" && len(keys) >= tc.At
	if match && tc.Fault == "root" {
		match = record.Attempts[record.Streams[keys[0]].Stream.Attempt].State == durable.AttemptFailed
	}
	if match {
		s.owner.first = false
		if tc.Fault == "before" {
			return failedStreamSaveError
		}
		if tc.Fault == "legacy" {
			// Explicit compatibility fixture: emulate the prior legal kth Finish
			// Save. This is not claimed to be a healthy filestore losing fields.
			for i, key := range sortedStreamKeys(record.Streams) {
				stream := record.Streams[key]
				if i >= tc.At {
					stream.Stream.Terminal = ""
					record.Streams[key] = stream
				}
				root := record.Attempts[stream.Stream.Attempt]
				root.State = durable.AttemptActive
				record.Attempts[stream.Stream.Attempt] = root
			}
		}
	}
	if err := s.Session.Save(ctx, record, expected); err != nil {
		return err
	}
	if tc.Alias {
		for _, scope := range record.Scopes {
			if _, exists := scope.Nodes["pass"]; exists && s.owner.aliasOnce.CompareAndSwap(false, true) {
				close(s.owner.aliasSaved)
			}
		}
	}
	if match {
		fmt.Printf("exact Save return: rev=%d failed=%d root=%s\n", record.Revision, len(keys), record.Attempts[record.Streams[keys[0]].Stream.Attempt].State)
		if tc.Fault == "uncertain" {
			return failedStreamSaveError
		}
		os.Exit(crashExit)
	}
	return nil
}

func sortedStreamKeys(streams map[string]durable.StreamRecord) []string {
	keys := make([]string, 0, len(streams))
	for key := range streams {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

type streamInjectedStore struct {
	durable.Store
	scenario string
	first    bool
	saves    *atomic.Int32
}

func (s *streamInjectedStore) Acquire(ctx context.Context, id string) (durable.Session, error) {
	session, err := s.Store.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	return &streamInjectedSession{Session: session, scenario: s.scenario, first: s.first, saves: s.saves}, nil
}

type streamInjectedSession struct {
	durable.Session
	scenario string
	first    bool
	saves    *atomic.Int32
}

func (s *streamInjectedSession) Save(ctx context.Context, record durable.RunRecord, expected uint64) error {
	if s.saves != nil {
		s.saves.Add(1)
	}
	match := false
	if s.first {
		switch s.scenario {
		case "append-before", "append-after", "uncertain-append":
			for _, stream := range record.Streams {
				if stream.Stream.ID.NodeID == "source" && stream.LastSequence == 2 {
					match = true
				}
			}
		case "commit-before", "commit-after", "uncertain-commit", "nested-stream", "ordinary-commit-after", "ordinary-first-write":
			for _, attempt := range record.Messages {
				if attempt.Sequence == 2 && attempt.State == durable.AttemptConfirmed {
					match = true
				}
			}
		case "managed-healthy", "managed-missing", "managed-wrong-owner":
			for _, attempt := range record.Messages {
				if attempt.Sequence == 1 && attempt.State == durable.AttemptConfirmed {
					match = true
				}
			}
		case "live-sub":
			_, match = record.Scopes[record.RootScope].Nodes["mount"]
		case "sugar-stop":
			// Ack 和显式 Stop 是两个可靠边界；只在 Stop 已持久化后证明它能跨进程保留。
			stopped, committed := false, false
			for _, stream := range record.Streams {
				if stream.Stream.ID.Key == "left" && stream.Stream.Stopped {
					stopped = true
				}
			}
			for _, attempt := range record.Messages {
				if attempt.Sequence == 2 && attempt.State == durable.AttemptConfirmed {
					committed = true
				}
			}
			match = stopped && committed
		case "message-failed", "message-budget":
			for _, attempt := range record.Messages {
				if attempt.Sequence == 2 && attempt.State == durable.AttemptFailed {
					match = true
				}
			}
		case "finish-before", "finish-after", "completed-tail":
			_, match = record.Scopes[record.RootScope].Nodes["source"]
		}
	}
	if match && strings.HasSuffix(s.scenario, "-before") {
		os.Exit(crashExit)
	}
	if err := s.Session.Save(ctx, record, expected); err != nil {
		return err
	}
	if match {
		s.first = false
		if strings.HasPrefix(s.scenario, "uncertain-") {
			return fmt.Errorf("storage acknowledgement lost: %w", pulse.ErrSkipped)
		}
		os.Exit(crashExit)
	}
	return nil
}
