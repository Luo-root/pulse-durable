package soaktest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/Luo-root/pulse-durable/distributed/postgres"
	"github.com/Luo-root/pulse-durable/distributed/worker"
)

type Event struct {
	At        time.Time `json:"at"`
	PID       int       `json:"pid"`
	Worker    d.Worker  `json:"worker"`
	Kind      string    `json:"kind"`
	Operation string    `json:"operation,omitempty"`
	Stage     string    `json:"stage,omitempty"`
	Reason    string    `json:"reason,omitempty"`
	RunID     string    `json:"run_id,omitempty"`
	Epoch     int64     `json:"epoch,omitempty"`
}

// Child reads only the worker DSN; EOF seals admission and drains before cancellation.
// No DSN is an argument.
func Child(ctx context.Context, c Config, id string, in io.Reader, out io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	shutdown := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, in); close(shutdown) }()
	backend, e := postgres.New(ctx, os.Getenv(c.WorkerEnv))
	if e != nil {
		return errors.New("worker backend unavailable")
	}
	defer backend.Close()
	renew, e := postgres.New(ctx, os.Getenv(c.WorkerEnv))
	if e != nil {
		return errors.New("worker renew pool unavailable")
	}
	defer renew.Close()
	reg := &d.Registry{}
	_, e = reg.Register("soak", c.version(), func() (d.Assembly, error) { return assembly(c.CPURounds, c.Wait) })
	if e != nil {
		return errors.New("worker graph registration failed")
	}
	var mu sync.Mutex
	var stageMu sync.RWMutex
	stage := "runtime"
	currentStage := func() string {
		stageMu.RLock()
		defer stageMu.RUnlock()
		return stage
	}
	emit := func(v Event) {
		mu.Lock()
		defer mu.Unlock()
		v.At = time.Now().UTC()
		v.PID = os.Getpid()
		_ = json.NewEncoder(out).Encode(v)
	}
	w, e := worker.New(worker.Config{Coordinator: backend, Renewer: renew, Registry: reg, Namespace: c.Namespace, WorkerID: id, Capacity: c.Capacity, BatchMax: c.Capacity, TTL: 10 * time.Second, RenewInterval: 2 * time.Second, RenewTimeout: time.Second, OperationTimeout: 3 * time.Second, PollMin: 10 * time.Millisecond, PollMax: 30 * time.Millisecond, Observe: func(v worker.Event) {
		emit(Event{Worker: v.Worker, Kind: v.Kind, Operation: v.Kind, Stage: currentStage(), Reason: v.Reason, RunID: v.Token.Identity.RunID, Epoch: v.Token.Epoch})
	}})
	if e != nil {
		return errors.New("worker configuration failed")
	}
	emit(Event{Kind: "ready", Operation: "ready", Stage: "runtime", Worker: w.Identity()})
	return runLifecycle(ctx, shutdown, w, func(kind string) {
		if kind == "drain_started" {
			stageMu.Lock()
			stage = "shutdown"
			stageMu.Unlock()
		}
		emit(Event{Kind: kind, Operation: kind, Stage: currentStage(), Worker: w.Identity()})
	})
}

type child struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
	done  chan error
	scan  chan struct{}
}

func launch(c Config, id string, observe func(Event)) (*child, error) {
	exe, e := os.Executable()
	if e != nil {
		return nil, e
	}
	b, _ := json.Marshal(c)
	cmd := exec.Command(exe, "worker", "-config", string(b), "-id", id)
	// Do not inherit administrator credentials or unrelated secret environment variables.
	for _, key := range []string{"PATH", "SystemRoot", "WINDIR", "HOME", "USERPROFILE", "TMP", "TEMP", "SSL_CERT_FILE", "SSL_CERT_DIR"} {
		if v, ok := os.LookupEnv(key); ok {
			cmd.Env = append(cmd.Env, key+"="+v)
		}
	}
	cmd.Env = append(cmd.Env, c.WorkerEnv+"="+os.Getenv(c.WorkerEnv), "PULSE_DISTRIBUTED_TEST_ALLOW=yes")
	in, e := cmd.StdinPipe()
	if e != nil {
		return nil, e
	}
	out, e := cmd.StdoutPipe()
	if e != nil {
		in.Close()
		return nil, e
	}
	cmd.Stderr = io.Discard
	if e = cmd.Start(); e != nil {
		in.Close()
		return nil, errors.New("worker start failed")
	}
	p := &child{cmd: cmd, stdin: in, done: make(chan error, 1), scan: make(chan struct{})}
	go func() {
		defer close(p.scan)
		dec := json.NewDecoder(out)
		for {
			var v Event
			if dec.Decode(&v) != nil {
				return
			}
			observe(v)
		}
	}()
	go func() { <-p.scan; p.done <- cmd.Wait() }()
	return p, nil
}
func (p *child) stop() error {
	_ = p.stdin.Close()
	select {
	case e := <-p.done:
		return e
	case <-time.After(10 * time.Second):
		_ = p.cmd.Process.Kill()
		<-p.done
		return errors.New("worker forced termination")
	}
}
func workerID(i int) string { return fmt.Sprintf("worker-%02d", i) }
func badEvent(e Event) bool {
	if e.Kind == "block" || e.Reason == "lease_lost" || e.Kind == "execution" && e.Reason != "completed" {
		return true
	}
	return e.Reason != "" && e.Reason != "ok" && e.Reason != "completed" && e.Reason != "closed" && e.Reason != "claimed" && !strings.HasPrefix(e.Kind, "ready")
}
