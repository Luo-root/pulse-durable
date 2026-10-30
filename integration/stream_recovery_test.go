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
				t.Cleanup(func() {
					if err := session.Close(); err != nil {
						t.Error(err)
					}
				})
				// 有意持久化不完整记录；健康 filestore 并未丢失数据。
				for key, managed := range before.Managed {
					if scenario == "managed-missing" {
						delete(before.Managed, key)
					} else {
						managed.Parent = before.RootScope
						before.Managed[key] = managed
					}
				}
				installHistoryFixture(t, dir, before)
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
				t.Cleanup(func() {
					if err := session.Close(); err != nil {
						t.Error(err)
					}
				})
				record, err := session.Load(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				// 通过公开 CAS 有意执行逐项有效的记录修改；
				// 不声称健康 Runtime 会生成矛盾值。
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
					// 旧格式 2 形状：生产者在消费者确认后失败，
					// 但其根失败仍需一次协调 Save。
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
				installHistoryFixture(t, dir, record)
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

// 每个阶段均为使用真实 filestore 的新进程。Save 可在写入前
// 或真实 CAS 成功后失败；两者均不得丢失可靠分类。
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
			// 即使分类失败也继续恢复，避免将旧 Core 的反例
			// 误认为记录丢失或未等待 Save 结束。
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

// 取消已恢复子节点后，挂载必须仍可恢复。宿主仅在
// 健康恢复时消费并显式 Ack 原始尾部。
func TestNestedRestoredCancellationAcrossProcesses(t *testing.T) {
	for _, scenario := range []string{"healthy", "cancel-child", "cancel-root"} {
		t.Run(scenario, func(t *testing.T) {
			dir, name := t.TempDir(), "nested-restore-"+scenario
			if err := streamRecoveryProcess(t, dir, name, "seed"); err != nil {
				t.Fatalf("seed: %v", err)
			}
			before := loadStreamRecord(t, dir)
			assertNestedRestoredTail(t, before, 1, 1)
			if len(before.Attempts) != 3 || len(before.Scopes) != 2 || len(before.Scopes[before.RootScope].Nodes) != 0 {
				t.Fatal("seed did not retain one active mount and its two child attempts")
			}
			for id, attempt := range before.Attempts {
				want := durable.AttemptActive
				if attempt.NodeID == "source" {
					want = durable.AttemptConfirmed
				}
				if attempt.Index != 1 || attempt.Limit != 1 || attempt.State != want {
					t.Fatalf("seed attempt %s: %+v", id, attempt)
				}
			}
			for _, phase := range []string{"resume", "recover"} {
				encoded, err := json.Marshal(before)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("record phase=before-%s %s", phase, encoded)
				// 即使分类失败也保留第三个进程：其具体 Resume
				// 结果可区分恢复资格丢失与数据丢失。
				if err := streamRecoveryProcess(t, dir, name, phase); err != nil {
					t.Errorf("%s: %v", phase, err)
				}
				after := loadStreamRecord(t, dir)
				encoded, err = json.Marshal(after)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("record phase=%s %s", phase, encoded)
				canceled := phase == "resume" && scenario != "healthy"
				generation, pending := uint64(2), 0
				if phase == "recover" {
					generation = 3
				}
				if canceled {
					pending = 1
				}
				assertNestedRestoredTail(t, after, generation, pending)
				if after.RootScope != before.RootScope || after.RunID != before.RunID || after.Version != before.Version || after.Definition != before.Definition || after.Inputs != before.Inputs || after.Revision <= before.Revision || len(after.Attempts) != len(before.Attempts) || callCount(dir, "source") != 1 {
					t.Error("recovery changed identity/budget, failed to save or reran the confirmed source")
				}
				for id, attempt := range before.Attempts {
					next, ok := after.Attempts[id]
					identity := next
					identity.State = attempt.State
					if !ok || identity != attempt {
						t.Errorf("attempt identity/index/limit changed: %s before=%+v after=%+v", id, attempt, next)
					}
					if canceled {
						if next.State != attempt.State {
							t.Errorf("cancel-only changed attempt %s (%s): %s -> %s", id, attempt.NodeID, attempt.State, next.State)
						}
					} else if next.State != durable.AttemptConfirmed {
						t.Errorf("successful recovery left attempt %s (%s) %s", id, attempt.NodeID, next.State)
					}
				}
				if canceled {
					if len(after.Scopes[after.RootScope].Nodes) != 0 || !reflect.DeepEqual(before.Scopes, after.Scopes) {
						t.Error("cancel-only confirmed or changed a complete node")
					}
					for id, stream := range before.Streams {
						if !reflect.DeepEqual(stream.Pending, after.Streams[id].Pending) {
							t.Error("cancellation consumed the pending tail")
						}
					}
				} else if after.Scopes[after.RootScope].Nodes["mount"].Reason != "completed" {
					t.Error("healthy recovery did not confirm the mount")
				}
				before = after
			}
		})
	}
}

func assertNestedRestoredTail(t *testing.T, record durable.RunRecord, generation uint64, pending int) {
	t.Helper()
	if len(record.Streams) != 1 {
		t.Fatalf("streams=%d, want one confirmed source", len(record.Streams))
	}
	for _, stream := range record.Streams {
		node := record.Scopes[stream.Stream.ID.Scope].Nodes["source"]
		if stream.Stream.ID.Generation != generation || stream.Stream.Terminal != "completed" || stream.Stream.Stopped || stream.LastSequence != 1 || stream.AckedThrough != uint64(1-pending) || len(stream.Pending) != pending || node.Reason != "completed" || record.Attempts[node.Attempt].State != durable.AttemptConfirmed {
			t.Errorf("confirmed source/tail changed: generation=%d want=%d terminal=%s stopped=%t last=%d ack=%d pending=%d want=%d source=%+v", stream.Stream.ID.Generation, generation, stream.Stream.Terminal, stream.Stream.Stopped, stream.LastSequence, stream.AckedThrough, len(stream.Pending), pending, node)
		}
		if stream.Cursor == nil || string(stream.Cursor.Data) != "7" || (pending == 1 && (len(stream.Pending) != 1 || stream.Pending[0].Sequence != 1 || string(stream.Pending[0].Value.Data) != "7")) {
			t.Error("lost original source position or pending value")
		}
	}
}

func nestedRestoredCancellationProcess(t *testing.T, fs durable.Store, dir, scenario, phase string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	child, err := pulse.New("restored-child", "")
	if err != nil {
		t.Fatal(err)
	}
	inside, outside := pulse.NewKey[pulse.Stream[int]]("inside"), pulse.NewKey[pulse.Stream[int]]("outside")
	if err := child.DeclareOutput(pulse.NewPort(inside)); err != nil {
		t.Fatal(err)
	}
	holdStarted, sourceFinished := make(chan struct{}), make(chan struct{})
	if err := pulse.ProduceStream(child, "source", "confirmed tail", inside, func(_ *pulse.RunCtx, writer *pulse.StreamWriter[int]) error {
		select {
		case <-holdStarted:
		case <-ctx.Done():
			return ctx.Err()
		}
		logCall(dir, "source")
		return writer.SendAt(7, 7)
	}, pulse.WithBuffer(1), pulse.WithStreamSource("nested/7", true)); err != nil {
		t.Fatal(err)
	}
	canceled := phase == "seed" || (phase == "resume" && scenario != "nested-restore-healthy")
	if err := child.Add(pulse.NewNode("hold", "keep mount active", nil, nil, func(rc *pulse.RunCtx) error {
		close(holdStarted)
		if canceled {
			<-rc.Context().Done()
			// 已恢复源必须先锁存结果，此活跃节点才能
			// 返回单独分类的取消。
			select {
			case <-sourceFinished:
			case <-ctx.Done():
				return ctx.Err()
			}
			return rc.Context().Err()
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	root, err := pulse.New("restored-root", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := root.DeclareOutput(pulse.NewPort(outside)); err != nil {
		t.Fatal(err)
	}
	if err := pulse.Sub(root, "mount", "live child", []pulse.SubBind{pulse.Out(inside, outside)}, child); err != nil {
		t.Fatal(err)
	}
	store := &sharedRestoreStore{Store: fs, outer: ctx, entered: make(chan context.Context, 1), reply: make(chan struct{}), settled: make(chan struct{}), committed: scenario == "nested-restore-cancel-root", success: true}
	var release sync.Once
	defer release.Do(func() { close(store.reply) })
	var backend durable.Store = fs
	if phase == "resume" {
		backend = store
	}
	rt, err := durable.New(backend, durable.NewCodecs())
	if err != nil {
		t.Fatal(err)
	}
	type finish struct {
		reason pulse.NodeFinishReason
		err    error
	}
	sourceEvents, graphEvents := make(chan finish, 1), make(chan finish, 1)
	childStarted := make(chan *pulse.Execution, 1)
	reader := make(chan error, 1)
	var execution *pulse.Execution
	var sub *pulse.StreamSubscription[int]
	opts := durable.RunOptions{RunID: "stream-run", Version: "nested-restore/1", BeforeStart: func(e *pulse.Execution) error {
		execution = e
		var err error
		sub, err = pulse.SubscribeStream(e, outside)
		return err
	}}
	opts.ExecOptions = []pulse.ExecOption{pulse.WithExecObserver(pulse.ObserverFunc{
		GraphStarted: func(site pulse.Site) {
			if site.Path == "mount" {
				childStarted <- execution.Snapshot().Nodes[0].Child
			}
			if site.Path == "" && !canceled {
				go func() {
					delivery, err := sub.Receive(ctx)
					if err == nil && (delivery.Value != 7 || delivery.Sequence != 1) {
						err = fmt.Errorf("original tail=%d sequence=%d", delivery.Value, delivery.Sequence)
					}
					// 已完成的健康对照组已经消费其尾部。
					if phase == "recover" && scenario == "nested-restore-healthy" {
						if err == io.EOF {
							reader <- nil
							return
						}
						err = fmt.Errorf("already Acked tail replayed: %v", err)
					}
					if err == nil {
						err = delivery.Ack()
					}
					if err == nil {
						_, err = sub.Receive(ctx)
						if err == io.EOF {
							err = nil
						} else {
							err = fmt.Errorf("after original Ack: want EOF, got %v", err)
						}
					}
					reader <- err
				}()
			}
		},
		Finished: func(site pulse.Site, node string, reason pulse.NodeFinishReason, err error) {
			if site.Path == "mount" && node == "source" {
				close(sourceFinished)
				sourceEvents <- finish{reason, err}
				if phase == "seed" {
					execution.Cancel()
				}
			}
		},
		GraphFinished: func(site pulse.Site, reason pulse.NodeFinishReason, err error) {
			if site.Path == "" {
				graphEvents <- finish{reason, err}
			}
		},
	})}
	if phase == "seed" {
		if _, err := rt.Run(ctx, root, pulse.Inputs{}, opts); !errors.Is(err, context.Canceled) {
			t.Fatalf("seed must interrupt active mount: %v", err)
		}
		return
	}
	result := make(chan error, 1)
	go func() { _, err := rt.Resume(ctx, root, pulse.Inputs{}, opts); result <- err }()
	if phase == "recover" {
		if err := sharedRestoreAwait(t, ctx, result); err != nil {
			var scopeFailure *pulse.CheckpointScopeFailure
			t.Fatalf("third-process Resume: scope_failure=%t err=%v", errors.As(err, &scopeFailure), err)
		}
		if err := sharedRestoreAwait(t, ctx, reader); err != nil {
			t.Fatal(err)
		}
		if execution.Snapshot().Graph.Status != pulse.GraphStatusCompleted {
			t.Fatal("third process did not complete")
		}
		return
	}
	nested := sharedRestoreAwait(t, ctx, childStarted)
	saveCtx := sharedRestoreAwait(t, ctx, store.entered)
	// Save 持有 Durable 记录互斥锁；活跃的 hold 尝试可能仍在
	// 等待重新进入节点体。Save 被暂停时，绝不等待该节点体。
	if nested == nil || nested.Context().Err() != nil || execution.Context().Err() != nil || saveCtx.Err() != nil {
		t.Fatal("shared restore Save was not admitted with a healthy root and live child")
	}
	waited := make(chan error, 1)
	go func() { waited <- execution.Wait() }()
	if canceled {
		if scenario == "nested-restore-cancel-child" {
			nested.Cancel()
		} else {
			execution.Cancel()
		}
		source := sharedRestoreAwait(t, ctx, sourceEvents)
		if source.reason != pulse.NodeCanceled || !errors.Is(source.err, context.Canceled) {
			t.Errorf("restored source cancellation: reason=%s err=%v", source.reason, source.err)
		}
		for name, done := range map[string]<-chan error{"Runtime": result, "Wait": waited} {
			select {
			case err := <-done:
				t.Fatalf("%s returned before admitted Save reply: %v", name, err)
			default:
			}
		}
	}
	release.Do(func() { close(store.reply) })
	runErr := sharedRestoreAwait(t, ctx, result)
	sharedRestoreAwait(t, ctx, store.settled)
	final := sharedRestoreAwait(t, ctx, waited)
	graph := sharedRestoreAwait(t, ctx, graphEvents)
	snapshot := execution.Snapshot()
	var checkpoint *pulse.CheckpointError
	wantStatus, wantReason := pulse.GraphStatusCompleted, pulse.NodeCompleted
	if canceled {
		wantStatus, wantReason = pulse.GraphStatusCanceled, pulse.NodeCanceled
		if !errors.Is(runErr, context.Canceled) || errors.As(runErr, &checkpoint) {
			t.Errorf("cancel-only result=%v checkpoint=%t", runErr, checkpoint != nil)
		}
	} else {
		if runErr != nil {
			t.Error(runErr)
		}
		if err := sharedRestoreAwait(t, ctx, reader); err != nil {
			t.Error(err)
		}
		if source := sharedRestoreAwait(t, ctx, sourceEvents); source.reason != pulse.NodeCompleted || source.err != nil {
			t.Errorf("healthy restored source: reason=%s err=%v", source.reason, source.err)
		}
	}
	if snapshot.Graph.Status != wantStatus || graph.reason != wantReason || !errors.Is(runErr, final) || execution.Wait() != final || execution.Err() != final || snapshot.Graph.Err != final || graph.err != final || store.earlyClose.Load() || !store.closed.Load() || !store.succeeded.Load() {
		t.Errorf("Save/Wait/Runtime/Session disagree: status=%s want=%s GraphFinished=%s err=%v final=%v earlyClose=%t closed=%t Save_succeeded=%t", snapshot.Graph.Status, wantStatus, graph.reason, runErr, final, store.earlyClose.Load(), store.closed.Load(), store.succeeded.Load())
	}
	t.Logf("admitted Save succeeded=%t and joined; source_before_reply=%t root_status=%s source_calls=%d", store.succeeded.Load(), canceled, snapshot.Graph.Status, callCount(dir, "source"))
}

// 健康节点返回的本地 Fork 取消是最终业务
// 失败。真实 filestore 记录与全新进程必须保留该结论。
func TestForkWaitFailureAcrossProcesses(t *testing.T) {
	for _, entry := range []string{"open", "receive"} {
		t.Run(entry, func(t *testing.T) {
			dir, scenario := t.TempDir(), "fork-wait-"+entry
			var seed durable.RunRecord
			if entry == "receive" {
				if err := streamRecoveryProcess(t, dir, scenario, "seed"); err != nil {
					t.Fatalf("seed: %v", err)
				}
				seed = loadStreamRecord(t, dir)
				assertNestedRestoredTail(t, seed, 1, 1)
				if len(seed.Attempts) != 3 || callCount(dir, "source") != 1 || callCount(dir, "sink") != 1 {
					t.Fatal("seed did not retain one confirmed source and active mount/consumer")
				}
				for id, attempt := range seed.Attempts {
					want := durable.AttemptActive
					if attempt.NodeID == "source" {
						want = durable.AttemptConfirmed
					}
					if attempt.State != want || attempt.Index != 1 || attempt.Limit != 1 {
						t.Fatalf("seed attempt %s: %+v", id, attempt)
					}
				}
				encoded, err := json.Marshal(seed)
				if err != nil {
					t.Fatal(err)
				}
				t.Logf("record phase=seed %s", encoded)
			}
			phase, node, calls := "run", "source", 1
			if entry == "receive" {
				phase, node, calls = "resume", "sink", 2
			}
			if err := streamRecoveryProcess(t, dir, scenario, phase); err != nil {
				t.Errorf("local Fork classification: %v", err)
			}
			before := loadStreamRecord(t, dir)
			encoded, err := json.Marshal(before)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("record phase=failed %s", encoded)
			found := false
			for id, attempt := range before.Attempts {
				if attempt.Index != 1 || attempt.Limit != 1 {
					t.Errorf("attempt budget changed: %s %+v", id, attempt)
				}
				if entry == "receive" {
					previous, ok := seed.Attempts[id]
					identity := attempt
					identity.State = previous.State
					if !ok || identity != previous {
						t.Errorf("restoring changed attempt identity: %s seed=%+v after=%+v", id, previous, attempt)
					}
				}
				if attempt.NodeID == node {
					found = true
					if attempt.Parent != before.RootScope || attempt.Boundary != "" || attempt.State != durable.AttemptFailed {
						t.Errorf("local Fork failure not confirmed: %s %+v", id, attempt)
					}
				}
			}
			if !found || callCount(dir, node) != calls {
				t.Errorf("missing failed node or wrong body calls: node=%s calls=%d want=%d", node, callCount(dir, node), calls)
			}
			if entry == "receive" {
				assertNestedRestoredTail(t, before, 2, 1)
				if before.RootScope != seed.RootScope || len(before.Attempts) != len(seed.Attempts) || callCount(dir, "source") != 1 {
					t.Error("local cancellation changed identity/attempt count or reran the confirmed source")
				}
			} else if len(before.Attempts) != 1 {
				t.Errorf("attempts=%d, want one final root attempt", len(before.Attempts))
			}
			// 始终运行最后一个进程，包括旧 Core：实际获准的
			// Resume 加上额外节点体调用，比单独分类更能证明问题。
			if err := streamRecoveryProcess(t, dir, scenario, "recover"); err != nil {
				t.Errorf("failed-scope Resume: %v", err)
			}
			after := loadStreamRecord(t, dir)
			encoded, err = json.Marshal(after)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("record phase=recover %s", encoded)
			if !reflect.DeepEqual(before, after) || callCount(dir, node) != calls || callCount(dir, "source") != 1 {
				t.Error("rejected Resume changed the full record, attempt identity/index/limit or business calls")
			}
		})
	}
}

func forkWaitFailureProcess(t *testing.T, fs durable.Store, dir, scenario, phase string) {
	t.Helper()
	fmt.Printf("fork wait process: case=%s phase=%s pid=%d ppid=%d\n", scenario, phase, os.Getpid(), os.Getppid())
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	receive := scenario == "fork-wait-receive"
	injected := phase == "run" || phase == "resume"
	generation, node := uint64(1), "source"
	if receive {
		generation, node = 2, "sink"
	}
	store := &sharedRestoreStore{Store: fs, outer: ctx, entered: make(chan context.Context, 1), reply: make(chan struct{}), settled: make(chan struct{}), success: true, generation: generation}
	var release, startChild sync.Once
	childStart := make(chan struct{})
	defer release.Do(func() { close(store.reply) })
	defer startChild.Do(func() { close(childStart) })
	var saves, beforeStarts, restored, starts atomic.Int32
	var backend durable.Store = &streamInjectedStore{Store: fs, saves: &saves}
	if injected {
		backend = store
	}
	rt, err := durable.New(backend, durable.NewCodecs())
	if err != nil {
		t.Fatal(err)
	}
	g, err := pulse.New("fork-wait-"+node, "")
	if err != nil {
		t.Fatal(err)
	}
	key := pulse.NewKey[pulse.Stream[int]]("output")
	forks := make(chan *pulse.RunCtx, 1)
	type bodyResult struct {
		ownerHealthy, executionHealthy bool
		err                            error
	}
	type finish struct {
		reason pulse.NodeFinishReason
		err    error
	}
	bodies, finished, graphFinished := make(chan bodyResult, 1), make(chan finish, 1), make(chan finish, 1)
	ownerEntered := make(chan struct{})
	var execution *pulse.Execution
	body := func(rc *pulse.RunCtx) error {
		logCall(dir, node)
		var input pulse.Stream[int]
		if receive {
			var err error
			input, err = pulse.Get(rc, key)
			if err != nil {
				return err
			}
			close(ownerEntered)
			if phase == "seed" {
				<-rc.Context().Done()
				return rc.Context().Err()
			}
		}
		fork := rc.Fork()
		defer fork.Cancel()
		forks <- fork
		var err error
		if receive {
			var delivery pulse.Delivery[int]
			delivery, err = input.Receive(fork)
			if err == nil {
				if delivery.Value != 7 || delivery.Sequence != 1 {
					return fmt.Errorf("original tail changed: %+v", delivery)
				}
				if err = delivery.Ack(); err == nil {
					_, err = input.Receive(fork)
					if err == io.EOF {
						err = nil
					}
				}
			}
		} else {
			_, err = pulse.OpenStream(fork, key, pulse.WithBuffer(1), pulse.WithStreamSource("fork-wait/source", true))
		}
		bodies <- bodyResult{rc.Context().Err() == nil, execution.Context().Err() == nil, err}
		return err
	}
	if receive {
		child, err := pulse.New("fork-wait-source", "")
		if err != nil {
			t.Fatal(err)
		}
		inside := pulse.NewKey[pulse.Stream[int]]("inside")
		if err := child.DeclareOutput(pulse.NewPort(inside)); err != nil {
			t.Fatal(err)
		}
		if err := pulse.ProduceStream(child, "source", "confirmed tail", inside, func(rc *pulse.RunCtx, writer *pulse.StreamWriter[int]) error {
			logCall(dir, "source")
			select {
			case <-ownerEntered:
			case <-rc.Context().Done():
				return rc.Context().Err()
			}
			return writer.SendAt(7, 7)
		}, pulse.WithBuffer(1), pulse.WithStreamSource("fork-wait/source", true)); err != nil {
			t.Fatal(err)
		}
		if err := pulse.Sub(g, "mount", "", []pulse.SubBind{pulse.Out(inside, key)}, child); err != nil {
			t.Fatal(err)
		}
		err = g.Add(pulse.NewNode(node, "return only the local Fork error", pulse.Requires(key), nil, body))
	} else {
		if err := g.DeclareOutput(pulse.NewPort(key)); err != nil {
			t.Fatal(err)
		}
		err = g.Add(pulse.NewNode(node, "return only the local Fork error", nil, pulse.Provides(key), body))
	}
	if err != nil {
		t.Fatal(err)
	}
	var sub *pulse.StreamSubscription[int]
	reader := make(chan error, 1)
	started := make(chan *pulse.Execution, 1)
	opts := durable.RunOptions{RunID: "stream-run", Version: "fork-wait/1", OnEvent: func(durable.Event) error { restored.Add(1); return nil }, BeforeStart: func(e *pulse.Execution) error {
		beforeStarts.Add(1)
		execution = e
		if !receive {
			var err error
			sub, err = pulse.SubscribeStream(e, key)
			if err != nil {
				return err
			}
		}
		started <- e
		return nil
	}, ExecOptions: []pulse.ExecOption{pulse.WithExecObserver(pulse.ObserverFunc{
		GraphStarted: func(site pulse.Site) {
			starts.Add(1)
			if site.Path == "mount" && injected {
				// Out 已绑定，因此父消费者可以进入 Receive。
				// 其 Open 必须在提交已恢复源前启动；否则
				// 该 Open 的记录互斥锁可能阻塞消费者的 OpenAttempt。
				sharedRestoreAwait(t, ctx, childStart)
			}
			if site.Path == "" && !receive {
				go func() { _, err := sub.Receive(ctx); reader <- err }()
			}
		},
		Finished: func(site pulse.Site, id string, reason pulse.NodeFinishReason, err error) {
			if phase == "seed" && site.Path == "mount" && id == "source" {
				execution.Cancel()
			}
			if site.Path == "" && id == node {
				finished <- finish{reason, err}
			}
		},
		GraphFinished: func(site pulse.Site, reason pulse.NodeFinishReason, err error) {
			if site.Path == "" {
				graphFinished <- finish{reason, err}
			}
		},
	})}}
	if phase == "seed" {
		if _, err := rt.Run(ctx, g, pulse.Inputs{}, opts); !errors.Is(err, context.Canceled) {
			t.Fatalf("seed must interrupt the mount/consumer after source confirmation: %v", err)
		}
		return
	}
	result := make(chan error, 1)
	go func() {
		var err error
		if phase == "run" {
			_, err = rt.Run(ctx, g, pulse.Inputs{}, opts)
		} else {
			_, err = rt.Resume(ctx, g, pulse.Inputs{}, opts)
		}
		result <- err
	}()
	if phase == "recover" {
		err := sharedRestoreAwait(t, ctx, result)
		var scopeFailure *pulse.CheckpointScopeFailure
		var checkpoint *pulse.CheckpointError
		rejected := errors.As(err, &scopeFailure)
		fmt.Printf("fork wait recover: scope_failure=%t checkpoint=%t Save=%d BeforeStart=%d restored=%d Start=%d source_calls=%d node_calls=%d err=%v\n", rejected, errors.As(err, &checkpoint), saves.Load(), beforeStarts.Load(), restored.Load(), starts.Load(), callCount(dir, "source"), callCount(dir, node), err)
		if !rejected || errors.As(err, &checkpoint) || scopeFailure.Scope != loadStreamRecord(t, dir).RootScope || saves.Load()+beforeStarts.Load()+restored.Load()+starts.Load() != 0 {
			t.Error("failed local Fork attempt remained resumable or reached Save/events/BeforeStart/Start")
		}
		return
	}
	e := sharedRestoreAwait(t, ctx, started)
	fork := sharedRestoreAwait(t, ctx, forks)
	saveCtx := sharedRestoreAwait(t, ctx, store.entered)
	if e.Context().Err() != nil || fork.Context().Err() != nil || saveCtx.Err() != nil {
		t.Fatal("Fork/Execution/shared Save not healthy at admission")
	}
	waited := make(chan error, 1)
	go func() { waited <- e.Wait() }()
	fork.Cancel()
	returned := sharedRestoreAwait(t, ctx, bodies)
	if !returned.ownerHealthy || !returned.executionHealthy || !errors.Is(returned.err, context.Canceled) || saveCtx.Err() != nil {
		t.Fatalf("local cancellation escaped its owner or canceled shared Save: %+v Save=%v", returned, saveCtx.Err())
	}
	// Save 持有记录互斥锁，而 FailAttempt 需要该锁。在放行真实 CAS 前，
	// 仅等待业务返回屏障，绝不等待 Finished。
	for name, done := range map[string]<-chan error{"Runtime": result, "Wait": waited} {
		select {
		case err := <-done:
			t.Fatalf("%s returned before admitted Save reply: %v", name, err)
		default:
		}
	}
	release.Do(func() { close(store.reply) })
	sharedRestoreAwait(t, ctx, store.settled)
	startChild.Do(func() { close(childStart) })
	runErr := sharedRestoreAwait(t, ctx, result)
	final := sharedRestoreAwait(t, ctx, waited)
	event, graph := sharedRestoreAwait(t, ctx, finished), sharedRestoreAwait(t, ctx, graphFinished)
	var checkpoint *pulse.CheckpointError
	snapshot := e.Snapshot()
	fmt.Printf("fork wait result: owner_healthy=%t execution_healthy=%t node=%s graph=%s Save_succeeded=%t earlyClose=%t closed=%t source_calls=%d node_calls=%d\n", returned.ownerHealthy, returned.executionHealthy, event.reason, snapshot.Graph.Status, store.succeeded.Load(), store.earlyClose.Load(), store.closed.Load(), callCount(dir, "source"), callCount(dir, node))
	if !errors.Is(runErr, context.Canceled) || errors.As(runErr, &checkpoint) || !errors.Is(runErr, final) || e.Wait() != final || e.Err() != final || snapshot.Graph.Err != final || graph.err != final || !store.succeeded.Load() || store.earlyClose.Load() || !store.closed.Load() {
		t.Errorf("local error or successful shared Save lost: Runtime=%v Wait=%v saved=%t earlyClose=%t closed=%t", runErr, final, store.succeeded.Load(), store.earlyClose.Load(), store.closed.Load())
	}
	if event.reason != pulse.NodeFailed || !errors.Is(event.err, context.Canceled) || snapshot.Graph.Status != pulse.GraphStatusFailed || graph.reason != pulse.NodeFailed {
		t.Errorf("healthy node returned local Fork error: node=%s graph=%s GraphFinished=%s, want failed", event.reason, snapshot.Graph.Status, graph.reason)
	}
	if !receive {
		sharedRestoreAwait(t, ctx, reader)
	}
}

type sharedRestoreStore struct {
	durable.Store
	outer                        context.Context
	entered                      chan context.Context
	reply, settled               chan struct{}
	once                         sync.Once
	inflight, earlyClose, closed atomic.Bool
	succeeded                    atomic.Bool
	committed, success           bool
	generation                   uint64
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

func (s *sharedRestoreSession) Apply(ctx context.Context, batch durable.ChangeBatch) (durable.CommitAck, error) {
	return observeApply(ctx, s.Session, batch, func(record durable.RunRecord, commit func() error) error { return s.observe(ctx, record, commit) })
}
func (s *sharedRestoreSession) observe(ctx context.Context, record durable.RunRecord, commit func() error) error {
	o := s.owner
	generation := o.generation
	if generation == 0 {
		generation = 2
	}
	for _, stream := range record.Streams {
		if stream.Stream.ID.Generation != generation {
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
			if err := commit(); err != nil {
				return err
			}
		}
		o.entered <- ctx
		select {
		case <-o.reply:
		case <-o.outer.Done():
			return o.outer.Err()
		}
		if o.success {
			var err error
			if !o.committed {
				err = commit()
			}
			o.succeeded.Store(err == nil)
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return errors.New("accepted restore Save failed")
	}
	return commit()
}

func (s *sharedRestoreSession) Close() error {
	o := s.owner
	defer o.closed.Store(true)
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
	// Runtime 合并会话关闭结果；在该错误链中比较可靠错误身份，
	// 并保持 Core 自身终态值完全相同。
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
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStreamRecoveryProcessHelper$", "-test.count=1", "-test.timeout=12s")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "PULSE_STREAM_DIR="+dir, "PULSE_STREAM_CASE="+scenario, "PULSE_STREAM_PHASE="+phase, "GORACE=atexit_sleep_ms=0")
	output, err := boundedProcessOutput(cmd)
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
	if strings.HasPrefix(scenario, "nested-restore-") {
		nestedRestoredCancellationProcess(t, store, dir, scenario, phase)
		return
	}
	if strings.HasPrefix(scenario, "fork-wait-") {
		forkWaitFailureProcess(t, store, dir, scenario, phase)
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

// 屏障是 Session.Save 的真实返回，绝不是 Core 调用间的休眠。
// 新的多输出失败在一次原子 Save 中跨过所有终态阈值。
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
	output, err := boundedProcessOutput(cmd)
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
	rt, err := durable.New(&failedStreamStore{Store: store, test: tc, first: phase == "run", aliasSaved: aliasSaved, t: t, dir: dir}, durable.NewCodecs())
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
	t          *testing.T
	dir        string
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
	s.t.Cleanup(func() {
		if err := session.Close(); err != nil {
			s.t.Error(err)
		}
	})
	return &failedStreamSession{Session: session, owner: s}, nil
}

func (s *failedStreamSession) Apply(ctx context.Context, batch durable.ChangeBatch) (durable.CommitAck, error) {
	return observeApply(ctx, s.Session, batch, s.observe)
}
func (s *failedStreamSession) observe(record durable.RunRecord, commit func() error) error {
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
			// 合成的新格式类型化历史：具有活跃根的部分终态集合
			// 必须完成协调，且不恢复已耗尽预算。
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
			installHistoryFixture(s.owner.t, s.owner.dir, record)
			os.Exit(crashExit)
		}
	}
	if err := commit(); err != nil {
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

func (s *streamInjectedSession) Apply(ctx context.Context, batch durable.ChangeBatch) (durable.CommitAck, error) {
	return observeApply(ctx, s.Session, batch, s.observe)
}
func (s *streamInjectedSession) observe(record durable.RunRecord, commit func() error) error {
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
	if err := commit(); err != nil {
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
