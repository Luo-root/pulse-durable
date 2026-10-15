package integration_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/filestore"
)

func TestLiveSubRecoveryAcrossProcessExit(t *testing.T) {
	dir := t.TempDir()
	for _, phase := range []string{"run", "resume"} {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLiveSubRecoveryProcessHelper$", "-test.count=1")
		cmd.Env = append(os.Environ(), "PULSE_LIVE_SUB_DIR="+dir, "PULSE_LIVE_SUB_PHASE="+phase)
		output, err := cmd.CombinedOutput()
		cancel()
		if len(output) != 0 {
			t.Logf("%s: %s", phase, output)
		}
		if phase == "run" {
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != crashExit {
				t.Fatalf("mount did not reach its durable exit boundary: %v", err)
			}
			before := loadStreamRecord(t, dir)
			nodes := before.Scopes[before.RootScope].Nodes
			if _, ok := nodes["mount"]; !ok {
				t.Fatal("mount was not confirmed before process exit")
			}
			if _, ok := nodes["feedback"]; ok {
				t.Fatal("exit occurred after the unused inlet producer committed")
			}
		} else if err != nil {
			t.Fatalf("restore confirmed mount with a still-pending unused inlet: %v", err)
		}
	}
	if calls := callCount(dir, "independent-child"); calls != 1 {
		t.Fatalf("confirmed child body ran %d times, want once", calls)
	}
	after := loadStreamRecord(t, dir)
	if _, ok := after.Scopes[after.RootScope].Nodes["feedback"]; !ok {
		t.Fatal("remaining feedback producer was not confirmed after recovery")
	}
	t.Logf("real exit/restart: mount committed before feedback, child body ran once, revision=%d", after.Revision)
}

func TestLiveSubRecoveryProcessHelper(t *testing.T) {
	dir := os.Getenv("PULSE_LIVE_SUB_DIR")
	if dir == "" {
		return
	}
	phase := os.Getenv("PULSE_LIVE_SUB_PHASE")
	store, err := filestore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	backend := &streamInjectedStore{Store: store, scenario: "live-sub", first: phase == "run"}
	runtime, err := durable.New(backend, durable.NewCodecs())
	if err != nil {
		t.Fatal(err)
	}
	g, err := pulse.New("live-feedback", "入口未到达时独立子图成果仍可恢复")
	if err != nil {
		t.Fatal(err)
	}
	child, err := pulse.New("independent-child", "不使用绑定入口")
	if err != nil {
		t.Fatal(err)
	}
	feedback, result := pulse.NewKey[int]("feedback"), pulse.NewKey[int]("result")
	inlet, answer := pulse.NewKey[int]("unused-inlet"), pulse.NewKey[int]("answer")
	if err := child.DeclareInput(pulse.NewPort(inlet)); err != nil {
		t.Fatal(err)
	}
	if err := child.DeclareOutput(pulse.NewPort(answer)); err != nil {
		t.Fatal(err)
	}
	if err := child.Add(pulse.NewNode("independent", "独立成果", nil, pulse.Provides(answer), func(rc *pulse.RunCtx) error {
		logCall(dir, "independent-child")
		return pulse.Set(rc, answer, 42)
	})); err != nil {
		t.Fatal(err)
	}
	if err := pulse.Sub(g, "mount", "不等待无人使用的入口", []pulse.SubBind{pulse.In(feedback, inlet), pulse.Out(answer, result)}, child); err != nil {
		t.Fatal(err)
	}
	if err := g.Add(pulse.NewNode("feedback", "子图完成后才提供入口", pulse.Requires(result), pulse.Provides(feedback), func(rc *pulse.RunCtx) error {
		value, err := pulse.Get(rc, result)
		if err != nil {
			return err
		}
		return pulse.Set(rc, feedback, value+1)
	})); err != nil {
		t.Fatal(err)
	}
	if err := g.DeclareOutput(pulse.NewPort(result), pulse.NewPort(feedback)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	opts := durable.RunOptions{RunID: "stream-run", Version: "live-feedback/1"}
	var e *pulse.Execution
	if phase == "run" {
		e, err = runtime.Run(ctx, g, pulse.Inputs{}, opts)
	} else {
		e, err = runtime.Resume(ctx, g, pulse.Inputs{}, opts)
	}
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]int{"result": 42, "feedback": 43} {
		value, state, err := pulse.ReadOutput(e, pulse.NewKey[int](name))
		if err != nil || state != pulse.SlotStateReady || value != want {
			t.Fatalf("%s=%d state=%s err=%v, want %d", name, value, state, err, want)
		}
	}
}
