package crossmachine

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Luo-root/pulse-durable/distributed/systemtest"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"
)

// A real wrapper exits while a living descendant keeps its stdout handle open.
func TestInheritedStdoutEntry(t *testing.T) {
	mode := os.Getenv("CM_STDOUT_PROBE")
	if mode == "" {
		return
	}
	defer os.Exit(0)
	if mode == "descendant" {
		time.Sleep(15 * time.Second)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestInheritedStdoutEntry$")
	// Replace instead of duplicate environment keys.
	cmd.Env = nil
	for _, kv := range os.Environ() {
		if len(kv) < 16 || kv[:16] != "CM_STDOUT_PROBE=" {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, "CM_STDOUT_PROBE=descendant")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if cmd.Start() != nil {
		return
	}
	_ = json.NewEncoder(os.Stdout).Encode(systemtest.Event{Kind: "ready", PID: cmd.Process.Pid})
}
func TestCloseBoundedWithInheritedStdout(t *testing.T) {
	env := append(os.Environ(), "CM_STDOUT_PROBE=wrapper")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	p, err := Launch(ctx, Command{Path: os.Args[0], Args: []string{"-test.run=^TestInheritedStdoutEntry$"}}, nil, env)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	var descendant int
	until := time.After(3 * time.Second)
	for descendant == 0 {
		for _, e := range p.Events() {
			if e.Kind == "ready" {
				descendant = e.PID
			}
		}
		select {
		case <-until:
			t.Fatal("probe ready timeout")
		default:
		}
		time.Sleep(time.Millisecond)
	}
	defer func() {
		if proc, e := os.FindProcess(descendant); e == nil {
			_ = proc.Kill()
		}
	}()
	start := time.Now()
	if p.Close() != nil {
		t.Fatal("close timeout")
	}
	if time.Since(start) > 2500*time.Millisecond {
		t.Fatal("unbounded close")
	}
	done, err := p.Exited()
	if !done || err == nil {
		t.Fatal("incomplete stdout reported success")
	}
}

type lateProcess struct {
	Process
	mu   sync.Mutex
	late []systemtest.Event
	done chan struct{}
	once sync.Once
	run  string
}

func (p *lateProcess) Send(s string) error {
	if s != "drain" {
		return p.Process.Send(s)
	}
	err := p.Process.Send(s)
	p.once.Do(func() {
		go func() {
			time.Sleep(20 * time.Millisecond)
			p.mu.Lock()
			p.late = append(p.late, systemtest.Event{Kind: "prefix", RunID: p.run, Epoch: 2})
			p.mu.Unlock()
			close(p.done)
		}()
	})
	return err
}
func (p *lateProcess) Events() []systemtest.Event {
	v := p.Process.Events()
	p.mu.Lock()
	defer p.mu.Unlock()
	return append(v, p.late...)
}
func (p *lateProcess) Exited() (bool, error) {
	select {
	case <-p.done:
		return p.Process.Exited()
	default:
		return false, nil
	}
}
func (p *lateProcess) Close() error { return p.Process.Close() }
func TestLatePrefixAfterDBSuccess(t *testing.T) {
	db := newFake()
	ctl := fakeController(db)
	base := ctl.Launch
	ctl.Launch = func(ctx context.Context, c Command, args, env []string) (Process, error) {
		p, e := base(ctx, c, args, env)
		if len(db.ps) == 1 {
			return &lateProcess{Process: p, done: make(chan struct{}), run: "run-0002"}, e
		}
		return p, e
	}
	c := testConfig(1)
	c.Scenario = "checkpoint-kill"
	c.KillHost = "vps"
	r := ctl.Run(context.Background(), c, db)
	if r.Result != "FAIL" || r.Reason != "prefix_replayed" {
		t.Fatalf("late replay escaped: %+v", r)
	}
}
func TestKilledIncompleteCannotPass(t *testing.T) {
	db := newFake()
	ctl := fakeController(db)
	kill := ctl.Kill
	ctl.Kill = func(ctx context.Context, w WorkerEvidence) error {
		e := kill(ctx, w)
		for _, p := range db.ps {
			if p.id == w.ID {
				p.exitErr = ErrIncomplete
			}
		}
		return e
	}
	c := testConfig(1)
	c.Scenario = "checkpoint-kill"
	c.KillHost = "vps"
	r := ctl.Run(context.Background(), c, db)
	if r.Result != "FAIL" || r.Reason != "process_exit_or_incomplete_events" {
		t.Fatalf("%+v", r)
	}
}
func TestTimingArguments(t *testing.T) {
	db := newFake()
	ctl := fakeController(db)
	base := ctl.Launch
	c := testConfig(1)
	c.TTL = 20 * time.Second
	c.RenewInterval = time.Second
	c.RenewTimeout = 3 * time.Second
	c.OperationTimeout = 4 * time.Second
	c.PollMin = 100 * time.Millisecond
	c.PollMax = 200 * time.Millisecond
	ctl.Launch = func(ctx context.Context, command Command, args, env []string) (Process, error) {
		flags := map[string]string{}
		for i := 0; i+1 < len(args); i += 2 {
			flags[args[i]] = args[i+1]
		}
		for k, v := range map[string]string{"-ttl": "20s", "-renew-interval": "1s", "-renew-timeout": "3s", "-operation-timeout": "4s", "-poll-min": "100ms", "-poll-max": "200ms"} {
			if flags[k] != v {
				t.Errorf("%s = %s", k, flags[k])
			}
		}
		return base(ctx, command, args, env)
	}
	if r := ctl.Run(context.Background(), c, db); r.Result != "PASS" {
		t.Fatal(fmt.Sprint(r))
	}
}
