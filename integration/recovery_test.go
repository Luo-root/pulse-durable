package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/filestore"
)

const crashExit = 73

// 真进程验收：子进程在指定持久边界直接退出，新的进程重建 Graph 并 Resume。
// 计数文件仅用于观察业务体是否进入，不作为引擎的恢复来源。
func TestCheckpointRecoveryAcrossProcessExit(t *testing.T) {
	cases := []struct {
		name     string
		counts   []int
		expected int
	}{
		{"mid-child", []int{1, 1, 2}, 12},
		{"child-finished", []int{1, 1, 1}, 12},
		{"retry-failed", []int{2, 2, 2}, 22},
		{"retry-active", []int{2, 2, 3}, 22},
		{"nested-retry", []int{3, 3, 3}, 32},
		{"nested-sub", []int{1, 1, 2}, 12},
		{"uncertain-commit", []int{1, 1, 1}, 12},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			first := recoveryProcess(t, dir, tc.name, "run")
			var exit *exec.ExitError
			if !errors.As(first, &exit) || exit.ExitCode() != crashExit {
				t.Fatalf("first process must exit at the injected boundary: %v", first)
			}
			before := loadRecord(t, dir)
			root := before.RootScope
			// 子图完成前没有父成果，持久根尝试仍活跃，不能按 Path 重新分配。
			if _, confirmed := before.Scopes[root].Nodes["process"]; confirmed {
				t.Fatal("parent checkpoint must not exist before the crash")
			}
			if err := recoveryProcess(t, dir, tc.name, "resume"); err != nil {
				t.Fatalf("second process failed: %v", err)
			}
			for i, want := range tc.counts {
				name := fmt.Sprintf("C%d", i+1)
				if got := callCount(dir, name); got != want {
					t.Errorf("%s body calls = %d, want %d", name, got, want)
				}
			}
			data, err := os.ReadFile(filepath.Join(dir, "output"))
			if err != nil || string(data) != fmt.Sprint(tc.expected) {
				t.Fatalf("output=%q error=%v, want %d", data, err, tc.expected)
			}
			after := loadRecord(t, dir)
			if after.RootScope != root || after.Revision <= before.Revision {
				t.Fatal("Resume changed the run scope or did not advance the revision")
			}
			parent := after.Scopes[root].Nodes["process"]
			if parent.Reason != "completed" || after.Attempts[parent.Attempt].State != durable.AttemptConfirmed {
				t.Fatal("parent result and root attempt were not confirmed together")
			}
			if tc.name == "retry-active" || tc.name == "retry-failed" {
				var latest int
				for _, a := range after.Attempts {
					if a.NodeID == "process" && a.Boundary != "" && a.Index > latest {
						latest = a.Index
					}
				}
				if latest != 2 {
					t.Fatalf("persistent retry index=%d, want 2", latest)
				}
			}
			t.Logf("real exit/restart: body calls=%v, output=%d, revision=%d→%d", tc.counts, tc.expected, before.Revision, after.Revision)
		})
	}
}

// Run returns and closes the real filestore session before the process exits.
// Each Resume uses a new process; the call file only observes business entries.
func TestExecutionFailureBudgetAcrossProcessExit(t *testing.T) {
	for _, kind := range []string{"mixed-skip", "cancel-nil", "cancel-skip"} {
		t.Run(kind, func(t *testing.T) {
			dir := t.TempDir()
			if kind != "mixed-skip" {
				if err := os.WriteFile(filepath.Join(dir, "interrupt.request"), []byte("pending"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			dependencyRecoveryProcess(t, dir, kind, "run")
			before := loadRecord(t, dir)
			calls := callCount(dir, "work")
			failed, active := 0, 0
			for _, attempt := range before.Attempts {
				if attempt.State == durable.AttemptFailed {
					failed++
				}
				if attempt.State == durable.AttemptActive {
					active++
				}
			}
			if kind == "mixed-skip" {
				if failed != 3 || active != 0 || calls != 2 || len(before.Scopes[before.RootScope].Nodes) != 0 {
					t.Errorf("real failure became skip or lost Retry budget: active=%d failed=%d calls=%d", active, failed, calls)
				}
			} else if active != 1 || failed != 0 || calls != 1 || len(before.Scopes[before.RootScope].Nodes) != 0 {
				t.Errorf("local Cancel consumed persistent root budget: active=%d failed=%d calls=%d", active, failed, calls)
			}
			dependencyRecoveryProcess(t, dir, kind, "resume")
			after := loadRecord(t, dir)
			if before.RootScope != after.RootScope || len(before.Attempts) != len(after.Attempts) {
				t.Fatal("Resume allocated a new root or refreshed Retry budget")
			}
			for id, old := range before.Attempts {
				next, ok := after.Attempts[id]
				if !ok || old.Parent != next.Parent || old.Index != next.Index || old.Limit != next.Limit {
					t.Fatal("Resume replaced a persisted attempt identity")
				}
			}
			if kind == "mixed-skip" {
				if !reflect.DeepEqual(before, after) || callCount(dir, "work") != 2 {
					t.Error("Resume reran exhausted failure or changed its history")
				}
			} else {
				node, ok := after.Scopes[after.RootScope].Nodes["work"]
				if !ok || node.Reason != "completed" || after.Attempts[node.Attempt].State != durable.AttemptConfirmed || after.Revision <= before.Revision || callCount(dir, "work") != 2 {
					t.Error("active interrupted root did not resume and confirm once")
				}
			}
			dependencyRecoveryProcess(t, dir, kind, "resume")
			if final := loadRecord(t, dir); !reflect.DeepEqual(after, final) || callCount(dir, "work") != 2 {
				t.Error("repeated Resume changed completed/exhausted history")
			}
		})
	}
}

func dependencyRecoveryProcess(t *testing.T, dir, kind, phase string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDependencyRecoveryProcessHelper$", "-test.count=1", "-test.timeout=12s")
	cmd.Env = append(os.Environ(), "PULSE_DEPENDENCY_DIR="+dir, "PULSE_DEPENDENCY_CASE="+kind, "PULSE_DEPENDENCY_PHASE="+phase, "GORACE=atexit_sleep_ms=0")
	output, err := boundedProcessOutput(cmd)
	if ctx.Err() != nil {
		t.Fatalf("dependency helper deadline: %v\n%s", ctx.Err(), output)
	}
	if phase == "run" {
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != crashExit {
			t.Fatalf("Run did not exit after closing the real session: %v\n%s", err, output)
		}
	} else if err != nil {
		t.Fatalf("new-process Resume: %v\n%s", err, output)
	}
}

func TestDependencyRecoveryProcessHelper(t *testing.T) {
	dir := os.Getenv("PULSE_DEPENDENCY_DIR")
	if dir == "" {
		return
	}
	kind, phase := os.Getenv("PULSE_DEPENDENCY_CASE"), os.Getenv("PULSE_DEPENDENCY_PHASE")
	store, err := filestore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	rt, err := durable.New(store, durable.NewCodecs())
	if err != nil {
		t.Fatal(err)
	}
	g, err := pulse.New("dependency-"+kind, "")
	if err != nil {
		t.Fatal(err)
	}
	business := errors.New("mixed business failure")
	if kind == "cursor" {
		g = dependencyCursorGraph(t, dir)
	} else {
		var aspects []pulse.Aspect
		if kind == "mixed-skip" {
			aspects = append(aspects, pulse.Retry(2, 0))
		}
		err = g.Add(pulse.NewNode("work", "", nil, nil, func(rc *pulse.RunCtx) error {
			logCall(dir, "work")
			if kind == "mixed-skip" {
				return errors.Join(pulse.NoValue(), business)
			}
			request := filepath.Join(dir, "interrupt.request")
			data, err := os.ReadFile(request)
			if err != nil {
				return err
			}
			switch string(data) {
			case "pending":
				if err := os.WriteFile(request, []byte("consumed"), 0600); err != nil {
					return err
				}
				rc.Cancel()
				if kind == "cancel-skip" {
					return pulse.NoValue()
				}
				return nil
			case "consumed":
				return nil
			default:
				return errors.New("unknown interrupt request")
			}
		}, aspects...))
		if err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	opts := durable.RunOptions{RunID: "process-run", Version: "dependency/1"}
	if phase == "run" {
		_, err = rt.Run(ctx, g, pulse.Inputs{}, opts)
	} else {
		_, err = rt.Resume(ctx, g, pulse.Inputs{}, opts)
	}
	var checkpoint *pulse.CheckpointError
	if errors.As(err, &checkpoint) || ctx.Err() != nil {
		t.Fatalf("unexpected reliable/host failure: %v", err)
	}
	if kind == "mixed-skip" {
		if err == nil || phase == "run" && !errors.Is(err, business) {
			t.Fatalf("mixed real failure became success: %v", err)
		}
	} else if phase == "run" && kind != "cursor" {
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("local interruption was not cancellation: %v", err)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	if phase == "run" {
		os.Exit(crashExit)
	}
}

func recoveryProcess(t *testing.T, dir, scenario, phase string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRecoveryProcessHelper$", "-test.count=1")
	cmd.Env = append(os.Environ(), "PULSE_RECOVERY_DIR="+dir, "PULSE_RECOVERY_CASE="+scenario, "PULSE_RECOVERY_PHASE="+phase)
	output, err := boundedProcessOutput(cmd)
	if len(output) > 0 {
		t.Logf("%s/%s: %s", scenario, phase, output)
	}
	if ctx.Err() != nil {
		t.Fatalf("helper exceeded deadline: %v", ctx.Err())
	}
	return err
}

func loadRecord(t *testing.T, dir string) durable.RunRecord {
	t.Helper()
	store, err := filestore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	session, err := store.Acquire(context.Background(), "process-run")
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

func callCount(dir, id string) int {
	data, _ := os.ReadFile(filepath.Join(dir, id+".calls"))
	return strings.Count(string(data), "call\n")
}

func logCall(dir, id string) int {
	file, err := os.OpenFile(filepath.Join(dir, id+".calls"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	if _, err := file.WriteString("call\n"); err != nil {
		panic(err)
	}
	if err := file.Sync(); err != nil {
		panic(err)
	}
	if err := file.Close(); err != nil {
		panic(err)
	}
	return callCount(dir, id)
}

func TestRecoveryProcessHelper(t *testing.T) {
	dir := os.Getenv("PULSE_RECOVERY_DIR")
	if dir == "" {
		return
	}
	scenario, phase := os.Getenv("PULSE_RECOVERY_CASE"), os.Getenv("PULSE_RECOVERY_PHASE")
	store, err := filestore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	var backend durable.Store = store
	if scenario == "retry-failed" || scenario == "nested-retry" || scenario == "uncertain-commit" {
		backend = &injectedStore{Store: store, scenario: scenario, first: phase == "run"}
	}
	runtime, err := durable.New(backend, durable.NewCodecs())
	if err != nil {
		t.Fatal(err)
	}
	g, key := recoveryGraph(t, dir, scenario, phase)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	opts := durable.RunOptions{RunID: "process-run", Version: "recovery-v1"}
	var e *pulse.Execution
	if phase == "run" {
		e, err = runtime.Run(ctx, g, pulse.Inputs{}, opts)
	} else {
		e, err = runtime.Resume(ctx, g, pulse.Inputs{}, opts)
	}
	if scenario == "uncertain-commit" && phase == "run" {
		if err == nil || !errors.Is(err, pulse.ErrSkipped) || e.Snapshot().Graph.Status != pulse.GraphStatusFailed {
			t.Fatalf("uncertain commit was swallowed: %v", err)
		}
		os.Exit(crashExit)
	}
	if err != nil {
		t.Fatal(err)
	}
	value, state, err := pulse.ReadOutput(e, key)
	if err != nil || state != pulse.SlotStateReady {
		t.Fatalf("output state=%s error=%v", state, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "output"), []byte(fmt.Sprint(value)), 0600); err != nil {
		t.Fatal(err)
	}
}

func recoveryGraph(t *testing.T, dir, scenario, phase string) (*pulse.Graph, pulse.Key[int]) {
	t.Helper()
	newGraph := func(id, description string) *pulse.Graph {
		g, err := pulse.New(id, description)
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	c1, c2, c3 := pulse.NewKey[int]("c1"), pulse.NewKey[int]("c2"), pulse.NewKey[int]("c3")
	child := newGraph("child", "文档处理")
	add := func(g *pulse.Graph, n *pulse.Node) {
		if err := g.Add(n); err != nil {
			t.Fatal(err)
		}
	}
	add(child, pulse.NewNode("C1", "下载", nil, pulse.Provides(c1), func(rc *pulse.RunCtx) error {
		return pulse.Set(rc, c1, logCall(dir, "C1")*10)
	}))
	add(child, pulse.NewNode("C2", "提取", pulse.Requires(c1), pulse.Provides(c2), func(rc *pulse.RunCtx) error {
		logCall(dir, "C2")
		v, err := pulse.Get(rc, c1)
		if err != nil {
			return err
		}
		return pulse.Set(rc, c2, v+1)
	}))
	add(child, pulse.NewNode("C3", "摘要", pulse.Requires(c2), pulse.Provides(c3), func(rc *pulse.RunCtx) error {
		count := logCall(dir, "C3")
		if phase == "run" {
			switch scenario {
			case "mid-child", "nested-sub":
				os.Exit(crashExit)
			case "retry-failed", "nested-retry":
				return errors.New("retryable business failure")
			case "retry-active":
				if count == 1 {
					return errors.New("first attempt failed")
				}
				os.Exit(crashExit)
			}
		}
		v, err := pulse.Get(rc, c2)
		if err != nil {
			return err
		}
		return pulse.Set(rc, c3, v+1)
	}))
	if err := child.DeclareOutput(pulse.NewPort(c3)); err != nil {
		t.Fatal(err)
	}
	mounted, mountedKey := child, c3
	if scenario == "nested-sub" {
		middle := newGraph("middle", "中层组合")
		mk := pulse.NewKey[int]("middle-result")
		if err := pulse.Sub(middle, "deep", "深层子图", []pulse.SubBind{pulse.Out(c3, mk)}, child); err != nil {
			t.Fatal(err)
		}
		if err := middle.DeclareOutput(pulse.NewPort(mk)); err != nil {
			t.Fatal(err)
		}
		mounted, mountedKey = middle, mk
	}
	var aspects []pulse.Aspect
	if scenario == "child-finished" {
		aspects = append(aspects, func(rc *pulse.RunCtx, next func(*pulse.RunCtx) error) error {
			err := next(rc)
			if err == nil && phase == "run" {
				os.Exit(crashExit)
			}
			return err
		})
	}
	if scenario == "uncertain-commit" {
		aspects = append(aspects, func(rc *pulse.RunCtx, next func(*pulse.RunCtx) error) error {
			_ = next(rc) // 故意吞错，Core 的整树可靠失败锁存仍必须阻止成功。
			return nil
		})
	}
	if strings.HasPrefix(scenario, "retry-") || scenario == "nested-retry" || scenario == "uncertain-commit" {
		aspects = append(aspects, pulse.Retry(2, 0))
	}
	if scenario == "nested-retry" {
		aspects = append(aspects, pulse.Retry(2, 0))
	}
	parent := newGraph("parent", "父工作流")
	result := pulse.NewKey[int]("result")
	if err := pulse.Sub(parent, "process", "处理文档", []pulse.SubBind{pulse.Out(mountedKey, result)}, mounted, aspects...); err != nil {
		t.Fatal(err)
	}
	if err := parent.DeclareOutput(pulse.NewPort(result)); err != nil {
		t.Fatal(err)
	}
	return parent, result
}

// 故障在真实 Save 成功后注入，确保崩溃点位于持久确认之后，避免测成调度竞速。
type injectedStore struct {
	durable.Store
	scenario string
	first    bool
}

func (s *injectedStore) Acquire(ctx context.Context, id string) (durable.Session, error) {
	session, err := s.Store.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	return &injectedSession{Session: session, scenario: s.scenario, first: s.first}, nil
}

type injectedSession struct {
	durable.Session
	scenario string
	first    bool
}

func (s *injectedSession) Apply(ctx context.Context, batch durable.ChangeBatch) (durable.CommitAck, error) {
	return observeApply(ctx, s.Session, batch, s.observe)
}
func (s *injectedSession) observe(record durable.RunRecord, commit func() error) error {
	if err := commit(); err != nil {
		return err
	}
	if !s.first {
		return nil
	}
	if s.scenario == "uncertain-commit" {
		for _, scope := range record.Scopes {
			if _, ok := scope.Nodes["C2"]; ok {
				s.first = false
				return fmt.Errorf("injected uncertain write: %w", pulse.ErrSkipped)
			}
		}
	}
	for _, attempt := range record.Attempts {
		if attempt.NodeID == "process" && attempt.State == durable.AttemptFailed && attempt.Boundary == "aspect/0" {
			os.Exit(crashExit)
		}
	}
	return nil
}
