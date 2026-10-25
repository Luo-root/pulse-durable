// Package crossmachine controls explicitly configured systemtest harness processes.
// It never discovers hosts, installs binaries, or forwards credentials over argv.
package crossmachine

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/Luo-root/pulse-durable/distributed/systemtest"
)

// Command is an executable and fixed argument vector, not a shell fragment.
// Env maps child variable names to parent variable names. For SSH this configures
// the local ssh/wrapper process only: a trusted parent wrapper must arrange remote
// injection (e.g. an existing protected remote environment); stdin is JSON control.
// SSH itself interprets its remote command through the remote login shell. Supply
// only trusted fixed command arguments; no credentials or user shell fragments.
type Command struct {
	Path string            `json:"path"`
	Args []string          `json:"args"`
	Env  map[string]string `json:"command_env,omitempty"`
}

type Process interface {
	Send(string) error
	Events() []systemtest.Event
	Exited() (bool, error)
	Close() error
}
type Launcher func(context.Context, Command, []string, []string) (Process, error)

// ErrIncomplete means event collection was interrupted; even an expected kill
// cannot turn an incomplete event stream into passing recovery evidence.
var ErrIncomplete = errors.New("event stream incomplete")
var ErrProcessFailed = errors.New("process failed")

type execProcess struct {
	cmd    *exec.Cmd
	input  io.WriteCloser
	output io.ReadCloser
	mu     sync.Mutex
	sendMu sync.Mutex
	events []systemtest.Event
	exited bool
	err    error
	done   chan struct{}
}

// Launch uses exec directly; stderr is intentionally discarded rather than risking
// credential-bearing ssh/backend diagnostics in a report or terminal log.
func Launch(ctx context.Context, c Command, args, env []string) (Process, error) {
	if c.Path == "" {
		return nil, errors.New("empty executable")
	}
	cmd := exec.CommandContext(ctx, c.Path, append(append([]string(nil), c.Args...), args...)...)
	cmd.Env = env
	cmd.Stderr = io.Discard
	input, err := cmd.StdinPipe()
	if err != nil {
		return nil, errors.New("stdin pipe failed")
	}
	// Own the read end: Wait must reap the wrapper independently of EOF from
	// descendants, and must not close stdout before the scanner has consumed it.
	output, writer, err := os.Pipe()
	if err != nil {
		_ = input.Close()
		return nil, errors.New("stdout pipe failed")
	}
	cmd.Stdout = writer
	cmd.WaitDelay = time.Second
	if err = cmd.Start(); err != nil {
		_ = input.Close()
		_ = output.Close()
		_ = writer.Close()
		return nil, errors.New("process start failed")
	}
	_ = writer.Close()
	p := &execProcess{cmd: cmd, input: input, output: output, done: make(chan struct{})}
	scanDone := make(chan struct{})
	go func() {
		defer close(scanDone)
		defer output.Close()
		s := bufio.NewScanner(output)
		s.Buffer(make([]byte, 4096), 64*1024)
		malformed := false
		for s.Scan() {
			var e systemtest.Event
			if json.Unmarshal(s.Bytes(), &e) != nil {
				malformed = true
				continue
			}
			if !eventKind(e.Kind) {
				continue
			}
			p.mu.Lock()
			// Bound memory even for a misconfigured/noisy executable.
			if len(p.events) < 100000 {
				p.events = append(p.events, e)
			} else {
				malformed = true
			}
			p.mu.Unlock()
		}
		p.mu.Lock()
		if (s.Err() != nil || malformed) && p.err == nil {
			p.err = errors.New("process/protocol failed")
		}
		p.mu.Unlock()
	}()
	go func() {
		err := cmd.Wait()
		// A wrapper may exit while its descendant retains stdout. Give buffered
		// output a bounded grace period, then fail rather than invent complete evidence.
		select {
		case <-scanDone:
		case <-ctx.Done():
			_ = output.Close()
			<-scanDone
			p.mu.Lock()
			p.err = ErrIncomplete
			p.mu.Unlock()
		case <-time.After(time.Second):
			_ = output.Close()
			<-scanDone
			p.mu.Lock()
			p.err = ErrIncomplete
			p.mu.Unlock()
		}
		p.mu.Lock()
		if err != nil && p.err == nil {
			p.err = ErrProcessFailed
		}
		p.exited = true
		p.mu.Unlock()
		close(p.done)
	}()
	return p, nil
}
func eventKind(s string) bool {
	switch s {
	case "ready", "owned", "prefix", "suffix", "before_finish", "finish_held", "draining", "drained", "claim", "renew", "finish", "release", "blocked", "lost", "resolve", "save", "error":
		return true
	}
	// Worker observer event names are not business payloads. Unknown events are
	// ignored at source by a small allowlist, never reflected into reports.
	return false
}
func (p *execProcess) Send(s string) error {
	switch s {
	case "start", "release", "drain", "probe_finish":
	default:
		return errors.New("invalid control command")
	}
	p.sendMu.Lock()
	defer p.sendMu.Unlock()
	if json.NewEncoder(p.input).Encode(s) != nil {
		return errors.New("control write failed")
	}
	return nil
}
func (p *execProcess) Events() []systemtest.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]systemtest.Event(nil), p.events...)
}
func (p *execProcess) Exited() (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.exited, p.err
}

// Close closes stdin first (harness cancellation). Killing an SSH client is NOT
// evidence that its remote worker died. Successful runs require drained + exit;
// failed remote runs may require parent cleanup using the recorded PID and boot.
func (p *execProcess) Close() error {
	_ = p.input.Close()
	select {
	case <-p.done:
		return nil
	default:
	}
	// Unblock a scanner even when a descendant inherited the write handle.
	p.mu.Lock()
	p.err = ErrIncomplete
	p.mu.Unlock()
	_ = p.output.Close()
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
	select {
	case <-p.done:
		return nil
	case <-time.After(2 * time.Second):
		return errors.New("local process reap timed out; remote state unknown")
	}
}

func commandEnvironment(c Command, forbidden []string) ([]string, error) {
	blocked := map[string]bool{"PULSE_DISTRIBUTED_TEST_DSN": true, "PULSE_DISTRIBUTED_TEST_ALLOW": true}
	for _, k := range forbidden {
		blocked[strings.ToUpper(k)] = true
	}
	for k := range c.Env {
		blocked[strings.ToUpper(k)] = true
	}
	var env []string
	for _, kv := range os.Environ() {
		key, _, _ := strings.Cut(kv, "=")
		if !blocked[strings.ToUpper(key)] {
			env = append(env, kv)
		}
	}
	for child, parent := range c.Env {
		if child == "" || strings.ContainsAny(child, "=\x00") || parent == "" {
			return nil, errors.New("invalid command environment mapping")
		}
		value, ok := os.LookupEnv(parent)
		if !ok {
			return nil, errors.New("command environment source missing")
		}
		env = append(env, child+"="+value)
	}
	return env, nil
}
