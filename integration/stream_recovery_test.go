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
	"strings"
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

type streamInjectedStore struct {
	durable.Store
	scenario string
	first    bool
}

func (s *streamInjectedStore) Acquire(ctx context.Context, id string) (durable.Session, error) {
	session, err := s.Store.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	return &streamInjectedSession{Session: session, scenario: s.scenario, first: s.first}, nil
}

type streamInjectedSession struct {
	durable.Session
	scenario string
	first    bool
}

func (s *streamInjectedSession) Save(ctx context.Context, record durable.RunRecord, expected uint64) error {
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
