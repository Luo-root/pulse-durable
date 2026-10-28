package integration_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/filestore"
)

// 应用保存等待事实并取消本轮；另一个进程审批后用同一 RunID 恢复。
// 这里证明现有机制可组合，不声明内置挂起服务或等待记录与检查点的原子性。
func TestApprovalAspectResumeAcrossProcesses(t *testing.T) {
	if role := os.Getenv("PULSE_APPROVAL_ROLE"); role != "" {
		approvalProcess(t, role, os.Getenv("PULSE_APPROVAL_DIR"))
		return
	}
	dir := t.TempDir()
	run := func(role string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestApprovalAspectResumeAcrossProcesses$", "-test.timeout=10s")
		cmd.Env = append(os.Environ(), "PULSE_APPROVAL_ROLE="+role, "PULSE_APPROVAL_DIR="+dir, "GORACE=atexit_sleep_ms=0")
		if output, err := boundedProcessOutput(cmd); err != nil {
			t.Fatalf("%s process: %v\n%s", role, err, output)
		}
	}
	run("pause")
	registration, err := os.ReadFile(filepath.Join(dir, "waiting"))
	approvalOK(t, err)
	if string(registration) != "approval-run/deploy/approval-v1" {
		t.Fatalf("wait registration = %q", registration)
	}
	before := approvalRecord(t, dir) // 新 Session 可读取，证明上一轮已经释放运行所有权。
	active := false
	for _, attempt := range before.Attempts {
		active = active || (attempt.NodeID == "deploy" && attempt.State == durable.AttemptActive)
	}
	if !active {
		t.Fatal("intentional host cancellation lost the active attempt")
	}
	approvalOK(t, os.WriteFile(filepath.Join(dir, "approved"), []byte("yes"), 0600))
	run("resume")
	run("resume") // 全部完成后再恢复：不再执行审批切面或业务体。
	for name, want := range map[string]int{"build": 1, "gate": 2, "deploy": 1} {
		data, err := os.ReadFile(filepath.Join(dir, name+".log"))
		approvalOK(t, err)
		if got := len(strings.Fields(string(data))); got != want {
			t.Errorf("%s calls=%d, want %d", name, got, want)
		}
	}
	if after := approvalRecord(t, dir); after.RootScope != before.RootScope {
		t.Fatal("approval resume replaced the stable root scope")
	}
}

func approvalOK(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func approvalRecord(t *testing.T, dir string) durable.RunRecord {
	t.Helper()
	store, err := filestore.New(filepath.Join(dir, "checkpoints"))
	approvalOK(t, err)
	session, err := store.Acquire(t.Context(), "approval-run")
	approvalOK(t, err)
	defer func() { approvalOK(t, session.Close()) }()
	record, err := session.Load(t.Context())
	approvalOK(t, err)
	return record
}

func approvalProcess(t *testing.T, role, dir string) {
	t.Helper()
	deadline, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	ctx, cancel := context.WithCancelCause(deadline)
	defer cancel(nil)
	waitCause := errors.New("application is waiting for approval")
	count := func(name string) error {
		file, err := os.OpenFile(filepath.Join(dir, name+".log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		_, writeErr := file.WriteString("call\n")
		return errors.Join(writeErr, file.Close())
	}
	artifact, result := pulse.NewKey[string]("artifact"), pulse.NewKey[string]("result")
	g, err := pulse.New("approval-composition", "宿主等待协议与切面重入", pulse.WithMaxRunning(1))
	approvalOK(t, err)
	approvalOK(t, g.DeclareOutput(pulse.NewPort(result)))
	approvalOK(t, g.Add(pulse.NewNode("build", "已确认构建只运行一次", nil, pulse.Provides(artifact), func(rc *pulse.RunCtx) error {
		if err := count("build"); err != nil {
			return err
		}
		return pulse.Set(rc, artifact, "artifact:abc")
	})))
	gate := func(rc *pulse.RunCtx, next func(*pulse.RunCtx) error) error {
		// 切面先等待真实前置成果，避免在构建确认前取消整轮。
		if _, err := pulse.Get(rc, artifact); err != nil {
			return err
		}
		if err := count("gate"); err != nil {
			return err
		}
		decision, err := os.ReadFile(filepath.Join(dir, "approved"))
		if errors.Is(err, os.ErrNotExist) {
			if err := os.WriteFile(filepath.Join(dir, "waiting"), []byte("approval-run/deploy/approval-v1"), 0600); err != nil {
				return err
			}
			cancel(waitCause)
			return rc.Context().Err()
		}
		if err != nil {
			return err
		}
		if string(decision) != "yes" {
			return fmt.Errorf("invalid approval decision")
		}
		return next(rc)
	}
	approvalOK(t, g.Add(pulse.NewNode("deploy", "批准后执行", pulse.Requires(artifact), pulse.Provides(result), func(rc *pulse.RunCtx) error {
		value, err := pulse.Get(rc, artifact)
		if err != nil {
			return err
		}
		if err := count("deploy"); err != nil {
			return err
		}
		return pulse.Set(rc, result, value+"@production")
	}, gate)))
	store, err := filestore.New(filepath.Join(dir, "checkpoints"))
	approvalOK(t, err)
	runtime, err := durable.New(store, durable.NewCodecs())
	approvalOK(t, err)
	opts := durable.RunOptions{RunID: "approval-run", Version: "approval/1"}
	if role == "pause" {
		e, err := runtime.Run(ctx, g, pulse.Inputs{}, opts)
		if !errors.Is(err, context.Canceled) || context.Cause(ctx) != waitCause || e == nil || e.Snapshot().Graph.Status != pulse.GraphStatusCanceled {
			t.Fatalf("intentional cancellation: execution=%v err=%v cause=%v", e != nil, err, context.Cause(ctx))
		}
		return
	}
	e, err := runtime.Resume(ctx, g, pulse.Inputs{}, opts)
	approvalOK(t, err)
	value, state, err := pulse.ReadOutput(e, result)
	approvalOK(t, err)
	if value != "artifact:abc@production" || state != pulse.SlotStateReady {
		t.Fatalf("resumed result=%q state=%s", value, state)
	}
}

func TestBusinessWaitingErrorDoesNotSuspendRun(t *testing.T) {
	g, err := pulse.New("waiting-error", "普通错误不能代替宿主取消协议")
	approvalOK(t, err)
	waiting := errors.New("still waiting")
	gateCalls, bodyCalls := 0, 0
	approvalOK(t, g.Add(pulse.NewNode("work", "业务错误消耗根尝试", nil, nil, func(*pulse.RunCtx) error {
		bodyCalls++
		return nil
	}, func(*pulse.RunCtx, func(*pulse.RunCtx) error) error {
		gateCalls++
		return waiting
	})))
	runtime, err := durable.New(durable.NewMemoryStore(), durable.NewCodecs())
	approvalOK(t, err)
	opts := durable.RunOptions{RunID: "waiting-error", Version: "1"}
	if _, err := runtime.Run(t.Context(), g, pulse.Inputs{}, opts); !errors.Is(err, waiting) {
		t.Fatalf("business error changed: %v", err)
	}
	if _, err := runtime.Resume(t.Context(), g, pulse.Inputs{}, opts); err == nil || gateCalls != 1 || bodyCalls != 0 {
		t.Fatalf("failure was silently resumed: err=%v gates=%d bodies=%d", err, gateCalls, bodyCalls)
	}
}
