// Package systemtest provides an opt-in process harness for #6, #88 and #90.
// It is test infrastructure, not a production worker launcher. It never migrates
// or grants database permissions; provisioning belongs to the test coordinator.
package systemtest

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/Luo-root/pulse-durable/distributed/postgres"
	"github.com/Luo-root/pulse-durable/distributed/worker"
)

// Config contains no credentials. DSN is supplied separately through environment.
// Separate hosts may use the CLI with the same namespace and version, each with
// a unique ID. Duration is a hard ceiling, not evidence of a soak run.
type Config struct {
	Namespace  string        `json:"namespace"`
	ID         string        `json:"worker_id"`
	Version    string        `json:"version"`
	Capacity   int           `json:"capacity"`
	Duration   time.Duration `json:"duration"`
	HoldFinish bool          `json:"hold_finish"`
}

// Event is an allowlisted JSON-line result. Raw backend errors/payloads/DSNs are
// intentionally omitted. Business counts describe attempts, not exactly-once I/O.
type Event struct {
	Kind   string   `json:"kind"`
	PID    int      `json:"pid"`
	Worker d.Worker `json:"worker"`
	RunID  string   `json:"run_id,omitempty"`
	Epoch  int64    `json:"epoch,omitempty"`
	Reason string   `json:"reason,omitempty"`
	Active int      `json:"active,omitempty"`
}

type emitter struct {
	mu       sync.Mutex
	out      io.Writer
	identity d.Worker
}

func (e *emitter) emit(v Event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	v.PID = os.Getpid()
	v.Worker = e.identity
	_ = json.NewEncoder(e.out).Encode(v)
}

// Assembly is shared by submitters and workers; callbacks are never invoked by
// graph construction. The second node depends on a persisted first-node output.
func Assembly(body func(*pulse.RunCtx, string, string) error) (d.Assembly, error) {
	g, err := pulse.New("distributed-systemtest", "checkpoint process recovery")
	if err != nil {
		return d.Assembly{}, err
	}
	id := pulse.NewKey[string]("run_id")
	prefix := pulse.NewKey[int]("prefix")
	result := pulse.NewKey[int]("result")
	if err = g.DeclareInput(pulse.NewPort(id)); err != nil {
		return d.Assembly{}, err
	}
	if err = g.Add(pulse.NewNode("prefix", "", pulse.Requires(id), pulse.Provides(prefix), func(rc *pulse.RunCtx) error {
		run, err := pulse.Get(rc, id)
		if err != nil {
			return err
		}
		if body != nil {
			if err = body(rc, run, "prefix"); err != nil {
				return err
			}
		}
		return pulse.Set(rc, prefix, 41)
	})); err != nil {
		return d.Assembly{}, err
	}
	if err = g.Add(pulse.NewNode("suffix", "", append(pulse.Requires(id), pulse.Requires(prefix)...), pulse.Provides(result), func(rc *pulse.RunCtx) error {
		run, err := pulse.Get(rc, id)
		if err != nil {
			return err
		}
		if body != nil {
			if err = body(rc, run, "suffix"); err != nil {
				return err
			}
		}
		v, err := pulse.Get(rc, prefix)
		if err != nil {
			return err
		}
		return pulse.Set(rc, result, v+1)
	})); err != nil {
		return d.Assembly{}, err
	}
	if err = g.DeclareOutput(pulse.NewPort(result)); err != nil {
		return d.Assembly{}, err
	}
	keys := pulse.NewRegistry()
	if err = pulse.RegisterKey(keys, id); err != nil {
		return d.Assembly{}, err
	}
	return d.Assembly{Graph: g, Codecs: durable.NewCodecs(), Keys: keys}, nil
}
func Task(ctx context.Context, ns, id, version string) (d.Task, error) {
	a, err := Assembly(nil)
	if err != nil {
		return d.Task{}, err
	}
	var in pulse.Inputs
	if err = pulse.Seed(&in, pulse.NewKey[string]("run_id"), id); err != nil {
		return d.Task{}, err
	}
	return d.NewTask(ctx, d.Identity{Namespace: ns, RunID: id}, "systemtest", version, a.Graph, in, a.Codecs)
}

type observed struct {
	d.Coordinator
	emit       *emitter
	gate       <-chan struct{}
	hold       bool
	mu         sync.Mutex
	tokens     map[string]d.Token
	finishHeld *d.Token
}

func (o *observed) Claim(ctx context.Context, r d.ClaimRequest) ([]d.Claim, error) {
	rows, err := o.Coordinator.Claim(ctx, r)
	if err == nil {
		o.mu.Lock()
		for _, c := range rows {
			o.tokens[c.Token.Identity.RunID] = c.Token
			o.emit.emit(Event{Kind: "owned", RunID: c.Token.Identity.RunID, Epoch: c.Token.Epoch})
		}
		o.mu.Unlock()
	}
	return rows, err
}
func (o *observed) token(id string) d.Token { o.mu.Lock(); defer o.mu.Unlock(); return o.tokens[id] }
func (o *observed) Finish(ctx context.Context, r d.FinishRequest) (d.Snapshot, error) {
	if o.hold {
		o.mu.Lock()
		token := r.Token
		o.finishHeld = &token
		o.mu.Unlock()
		o.emit.emit(Event{Kind: "before_finish", RunID: token.Identity.RunID, Epoch: token.Epoch})
		// Deliberately non-cooperative fault injection: an operation deadline must
		// not remove the kill window. Only process kill (or the unit-test gate)
		// releases it. Parent cleanup always kills and waits for this process.
		<-o.gate
		o.mu.Lock()
		o.finishHeld = nil
		o.mu.Unlock()
		if err := ctx.Err(); err != nil {
			return d.Snapshot{}, err
		}
	}
	return o.Coordinator.Finish(ctx, r)
}

type drainer interface {
	Drain(context.Context) error
	Active() int
}

func sealDrain(ctx context.Context, w drainer, emit *emitter) {
	seal, cancel := context.WithCancel(ctx)
	cancel()
	_ = w.Drain(seal) // sets draining before checking the canceled wait context
	emit.emit(Event{Kind: "draining", Active: w.Active()})
}

// Run uses stdin JSON strings: "start", "release", "drain", "probe_finish". EOF cancels the
// process. Drain stops admission while retaining renewal until active work exits.
// Both PULSE_DISTRIBUTED_TEST_DSN and PULSE_DISTRIBUTED_TEST_ALLOW=yes are required.
func Run(parent context.Context, c Config, in io.Reader, out io.Writer) error {
	if os.Getenv("PULSE_DISTRIBUTED_TEST_ALLOW") != "yes" || os.Getenv("PULSE_DISTRIBUTED_TEST_DSN") == "" {
		return errors.New("systemtest: explicit opt-in required")
	}
	if c.Namespace == "" || c.ID == "" || c.Version == "" || c.Capacity < 1 || c.Duration <= 0 {
		return errors.New("systemtest: invalid configuration")
	}
	ctx, cancel := context.WithTimeout(parent, c.Duration)
	defer cancel()
	backend, err := postgres.New(ctx, os.Getenv("PULSE_DISTRIBUTED_TEST_DSN"))
	if err != nil {
		return errors.New("systemtest: coordinator unavailable")
	}
	defer backend.Close()
	renew, err := postgres.New(ctx, os.Getenv("PULSE_DISTRIBUTED_TEST_DSN"))
	if err != nil {
		return errors.New("systemtest: renewer unavailable")
	}
	defer renew.Close()
	emit := &emitter{out: out}
	gate := make(chan struct{})
	start := make(chan struct{})
	var releaseOnce, startOnce sync.Once
	finishGate := make(chan struct{}) // intentionally never opened: controller kills in this window
	o := &observed{Coordinator: backend, emit: emit, gate: finishGate, hold: c.HoldFinish, tokens: map[string]d.Token{}}
	reg := &d.Registry{}
	_, err = reg.Register("systemtest", c.Version, func() (d.Assembly, error) {
		return Assembly(func(rc *pulse.RunCtx, id, step string) error {
			tok := o.token(id)
			emit.emit(Event{Kind: step, RunID: id, Epoch: tok.Epoch})
			if step == "suffix" {
				select {
				case <-gate:
				case <-rc.Context().Done():
					return rc.Context().Err()
				}
			}
			return nil
		})
	})
	if err != nil {
		return err
	}
	w, err := worker.New(worker.Config{Coordinator: o, Renewer: renew, Registry: reg, Namespace: c.Namespace, WorkerID: c.ID, Capacity: c.Capacity, BatchMax: c.Capacity, TTL: 2 * time.Second, RenewInterval: 300 * time.Millisecond, RenewTimeout: 400 * time.Millisecond, OperationTimeout: time.Second, PollMin: 20 * time.Millisecond, PollMax: 50 * time.Millisecond, Observe: func(e worker.Event) {
		emit.emit(Event{Kind: e.Kind, RunID: e.Token.Identity.RunID, Epoch: e.Token.Epoch, Reason: e.Reason})
	}})
	if err != nil {
		return err
	}
	emit.identity = w.Identity()
	emit.emit(Event{Kind: "ready"})
	drainDone := make(chan struct{})
	var drainOnce sync.Once
	go func() {
		s := bufio.NewScanner(in)
		for s.Scan() {
			var cmd string
			if json.Unmarshal(s.Bytes(), &cmd) != nil {
				continue
			}
			switch cmd {
			case "start":
				startOnce.Do(func() { close(start) })
			case "release":
				releaseOnce.Do(func() { close(gate) })
			case "probe_finish":
				o.mu.Lock()
				if token := o.finishHeld; token != nil {
					emit.emit(Event{Kind: "finish_held", RunID: token.Identity.RunID, Epoch: token.Epoch})
				}
				o.mu.Unlock()
			case "drain":
				drainOnce.Do(func() {
					// Drain seals admission synchronously even with an expired wait
					// context. ACK only after that call returns, never on receipt.
					sealDrain(ctx, w, emit)
					go func() {
						err := w.Drain(ctx)
						reason := "ok"
						if err != nil {
							reason = "timeout"
						}
						emit.emit(Event{Kind: "drained", Reason: reason})
						close(drainDone)
					}()
				})
			}
		}
		cancel()
	}()
	select {
	case <-start:
	case <-ctx.Done():
		return ctx.Err()
	}
	err = w.Run(ctx)
	if err == nil { // Run may return before the Drain goroutine emits its result.
		select {
		case <-drainDone:
		case <-ctx.Done():
		}
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("systemtest: worker stopped")
	}
	return nil
}
