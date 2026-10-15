package durable_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)

type failedAliasSignals struct {
	failSource                         bool
	sourceCalls, sinkCalls, mountCalls atomic.Int32
	read, saved                        [2]chan struct{}
	readOnce, saveOnce                 [2]sync.Once
	mu                                 sync.Mutex
	announced                          map[string]bool
}

func newFailedAliasSignals(failSource bool) *failedAliasSignals {
	s := &failedAliasSignals{failSource: failSource, announced: make(map[string]bool)}
	for i := range s.read {
		s.read[i], s.saved[i] = make(chan struct{}), make(chan struct{})
	}
	return s
}

func waitFailedAlias(rc *pulse.RunCtx, signal <-chan struct{}) error {
	select {
	case <-signal:
		return nil
	case <-rc.Context().Done():
		return rc.Context().Err()
	}
}

func failedAliasGraph(t *testing.T, s *failedAliasSignals, resumable bool) *pulse.Graph {
	t.Helper()
	graph := func(id string) *pulse.Graph {
		g, err := pulse.New(id, "")
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	root, child, pass := graph("retry-alias-root"), graph("retry-alias-child"), graph("retry-alias-pass")
	input, inlet, alias := pulse.NewKey[pulse.Stream[int]]("input"), pulse.NewKey[pulse.Stream[int]]("inlet"), pulse.NewKey[pulse.Stream[int]]("alias")
	result, output := pulse.NewKey[int]("result"), pulse.NewKey[int]("output")
	add := func(g *pulse.Graph, n *pulse.Node) {
		if err := g.Add(n); err != nil {
			t.Fatal(err)
		}
	}
	add(child, pulse.NewNode("source", "", nil, pulse.Provides(input), func(rc *pulse.RunCtx) error {
		attempt := int(s.sourceCalls.Add(1))
		if attempt > 2 {
			return fmt.Errorf("source reran attempt %d", attempt)
		}
		writer, err := pulse.OpenStream(rc, input, pulse.WithBuffer(1), pulse.WithStreamSource("finite-source", resumable))
		if err != nil {
			return err
		}
		if err := writer.SendAt(7, 7); err != nil {
			return err
		}
		if err := waitFailedAlias(rc, s.saved[attempt-1]); err != nil {
			return err
		}
		if attempt == 1 {
			if s.failSource {
				return errors.New("first source fails after its alias committed")
			}
			<-rc.Context().Done()
			return rc.Context().Err()
		}
		return nil
	}))
	if err := pass.DeclareInput(pulse.NewPort(inlet)); err != nil {
		t.Fatal(err)
	}
	if err := pass.DeclareOutput(pulse.NewPort(inlet)); err != nil {
		t.Fatal(err)
	}
	observe := func(rc *pulse.RunCtx, next func(*pulse.RunCtx) error) error {
		s.mountCalls.Add(1)
		if err := next(rc); err != nil {
			return err
		}
		attempt := int(s.sourceCalls.Load())
		if attempt < 1 || attempt > 2 {
			return fmt.Errorf("unexpected attempt %d", attempt)
		}
		if err := waitFailedAlias(rc, s.read[attempt-1]); err != nil {
			return err
		}
		stream, err := pulse.Get(rc, alias)
		if err != nil {
			return err
		}
		if stream.Snapshot().Terminal != "" {
			return errors.New("alias did not precede source completion")
		}
		return nil
	}
	if err := pulse.Sub(child, "pass", "", []pulse.SubBind{pulse.In(input, inlet), pulse.Out(inlet, alias)}, pass, observe); err != nil {
		t.Fatal(err)
	}
	add(child, pulse.NewNode("sink", "", pulse.Requires(alias), pulse.Provides(result), func(rc *pulse.RunCtx) error {
		s.sinkCalls.Add(1)
		stream, err := pulse.Get(rc, alias)
		if err != nil {
			return err
		}
		message, err := stream.Receive(rc)
		if err != nil || message.Value != 7 {
			return fmt.Errorf("delivery=%d err=%v", message.Value, err)
		}
		attempt := int(s.sourceCalls.Load())
		s.readOnce[attempt-1].Do(func() { close(s.read[attempt-1]) })
		if err := waitFailedAlias(rc, s.saved[attempt-1]); err != nil {
			return err
		}
		if attempt == 1 {
			if s.failSource {
				<-rc.Context().Done()
				return rc.Context().Err()
			}
			return errors.New("first sink fails after its alias committed")
		}
		if err := message.Ack(); err != nil {
			return err
		}
		if _, err := stream.Receive(rc); err != io.EOF {
			return fmt.Errorf("tail=%v", err)
		}
		return pulse.Set(rc, result, 7)
	}))
	if err := child.DeclareOutput(pulse.NewPort(result)); err != nil {
		t.Fatal(err)
	}
	if err := pulse.Sub(root, "outer", "", []pulse.SubBind{pulse.Out(result, output)}, child, pulse.Retry(2, 0)); err != nil {
		t.Fatal(err)
	}
	if err := root.DeclareOutput(pulse.NewPort(output)); err != nil {
		t.Fatal(err)
	}
	return root
}

type failedAliasStore struct {
	durable.Store
	signals *failedAliasSignals
}
type failedAliasSession struct {
	durable.Session
	signals *failedAliasSignals
}

func (s *failedAliasStore) Acquire(ctx context.Context, id string) (durable.Session, error) {
	session, err := s.Store.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	return &failedAliasSession{Session: session, signals: s.signals}, nil
}

func (s *failedAliasSession) Save(ctx context.Context, record durable.RunRecord, expected uint64) error {
	if err := s.Session.Save(ctx, record, expected); err != nil {
		return err
	}
	s.signals.mu.Lock()
	defer s.signals.mu.Unlock()
	for _, scope := range record.Scopes {
		if scope.Graph != "retry-alias-child" {
			continue
		}
		node, saved := scope.Nodes["pass"]
		if !saved || s.signals.announced[node.Attempt] {
			continue
		}
		s.signals.announced[node.Attempt] = true
		attempt := int(s.signals.sourceCalls.Load())
		if attempt >= 1 && attempt <= 2 {
			s.signals.saveOnce[attempt-1].Do(func() { close(s.signals.saved[attempt-1]) })
		}
	}
	return nil
}

func TestFailedAliasHistoryDoesNotBlockSuccessfulRetryResume(t *testing.T) {
	for _, scenario := range []struct {
		name                  string
		resumable, failSource bool
	}{
		{"active-non-resumable", false, false},
		{"active-resumable", true, false},
		{"failed-source", true, true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			base, signals := durable.NewMemoryStore(), newFailedAliasSignals(scenario.failSource)
			runtime, err := durable.New(&failedAliasStore{Store: base, signals: signals}, durable.NewCodecs())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			opts := durable.RunOptions{RunID: "failed-alias-history", Version: "1"}
			execution, err := runtime.Run(ctx, failedAliasGraph(t, signals, scenario.resumable), pulse.Inputs{}, opts)
			if err != nil {
				t.Fatalf("two-attempt Run: %v", err)
			}
			assertOutput := func(e *pulse.Execution) {
				value, state, err := pulse.ReadOutput(e, pulse.NewKey[int]("output"))
				if err != nil || state != pulse.SlotStateReady || value != 7 {
					t.Fatalf("output=%d %s %v", value, state, err)
				}
			}
			assertOutput(execution)
			session, err := base.Acquire(ctx, opts.RunID)
			if err != nil {
				t.Fatal(err)
			}
			record, err := session.Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := session.Close(); err != nil {
				t.Fatal(err)
			}
			old, completed := 0, 0
			for _, stream := range record.Streams {
				if stream.Stream.Terminal == string(pulse.StreamCompleted) {
					completed++
					continue
				}
				old++
				if record.Attempts[stream.Stream.ID.Scope].State != durable.AttemptFailed {
					t.Fatal("old source is not under a failed ancestor")
				}
				want := ""
				if scenario.failSource {
					want = string(pulse.StreamFailed)
				}
				if stream.Stream.Terminal != want {
					t.Fatalf("old source terminal=%q want %q", stream.Stream.Terminal, want)
				}
				alias := record.Scopes[stream.Stream.ID.Scope].Nodes["pass"]
				if alias.NodeID != "pass" || alias.Outputs[0].Stream.Terminal != "" {
					t.Fatal("missing confirmed active alias in old history")
				}
			}
			if old != 1 || completed != 1 {
				t.Fatalf("old=%d completed=%d", old, completed)
			}
			execution, err = runtime.Resume(ctx, failedAliasGraph(t, signals, scenario.resumable), pulse.Inputs{}, opts)
			if err != nil {
				t.Fatalf("failed historical scope was treated as an active recovery plan: %v", err)
			}
			assertOutput(execution)
			if signals.sourceCalls.Load() != 2 || signals.sinkCalls.Load() != 2 || signals.mountCalls.Load() != 2 {
				t.Fatalf("old or completed work reran: source=%d sink=%d mount=%d", signals.sourceCalls.Load(), signals.sinkCalls.Load(), signals.mountCalls.Load())
			}
		})
	}
}
