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
	"github.com/Luo-root/pulse-durable/filestore"
)

type activeAliasSignals struct {
	appended, received, saved      chan struct{}
	appendOnce, readOnce, saveOnce sync.Once
	sourceCalls, mountCalls, reads atomic.Int32
	terminal                       atomic.Value
	storeMu                        sync.Mutex
	storeErr                       error
}

func (s *activeAliasSignals) recordStoreError(op string, ctx context.Context, err error) {
	s.storeMu.Lock()
	defer s.storeMu.Unlock()
	if s.storeErr == nil {
		// Keep the original cause and context state, never the batch or record payload.
		s.storeErr = fmt.Errorf("%s: %w (context: %v)", op, err, ctx.Err())
	}
}

func (s *activeAliasSignals) storeError() error {
	s.storeMu.Lock()
	defer s.storeMu.Unlock()
	return s.storeErr
}

func newActiveAliasSignals() *activeAliasSignals {
	s := &activeAliasSignals{appended: make(chan struct{}), received: make(chan struct{}), saved: make(chan struct{})}
	s.terminal.Store(pulse.StreamTerminal(""))
	return s
}

func activeAliasGraph(t *testing.T, s *activeAliasSignals, resumable bool) *pulse.Graph {
	t.Helper()
	root, err := pulse.New("active-alias", "")
	if err != nil {
		t.Fatal(err)
	}
	child, err := pulse.New("empty-pass", "")
	if err != nil {
		t.Fatal(err)
	}
	original, inlet, alias := pulse.NewKey[pulse.Stream[int]]("original"), pulse.NewKey[pulse.Stream[int]]("inlet"), pulse.NewKey[pulse.Stream[int]]("alias")
	if err := root.Add(pulse.NewNode("source", "", nil, pulse.Provides(original), func(rc *pulse.RunCtx) error {
		s.sourceCalls.Add(1)
		writer, err := pulse.OpenStream(rc, original, pulse.WithBuffer(1), pulse.WithStreamSource("one-value", resumable))
		if err != nil {
			return err
		}
		if writer.Position().Sequence == 0 {
			if err := writer.SendAt(7, 7); err != nil {
				return err
			}
		}
		s.appendOnce.Do(func() { close(s.appended) })
		select {
		case <-s.saved:
			return nil
		case <-rc.Context().Done():
			return rc.Context().Err()
		}
	})); err != nil {
		t.Fatal(err)
	}
	if err := child.DeclareInput(pulse.NewPort(inlet)); err != nil {
		t.Fatal(err)
	}
	if err := child.DeclareOutput(pulse.NewPort(inlet)); err != nil {
		t.Fatal(err)
	}
	aspect := func(rc *pulse.RunCtx, next func(*pulse.RunCtx) error) error {
		s.mountCalls.Add(1)
		select {
		case <-s.appended:
		case <-rc.Context().Done():
			return rc.Context().Err()
		}
		if err := next(rc); err != nil {
			return err
		}
		select {
		case <-s.received:
		case <-rc.Context().Done():
			return rc.Context().Err()
		}
		stream, err := pulse.Get(rc, alias)
		if err != nil {
			return err
		}
		s.terminal.Store(stream.Snapshot().Terminal)
		return nil
	}
	if err := pulse.Sub(root, "mount", "", []pulse.SubBind{pulse.In(original, inlet), pulse.Out(inlet, alias)}, child, aspect); err != nil {
		t.Fatal(err)
	}
	if err := root.Add(pulse.NewNode("sink", "", pulse.Requires(alias), nil, func(rc *pulse.RunCtx) error {
		stream, err := pulse.Get(rc, alias)
		if err != nil {
			return err
		}
		message, err := stream.Receive(rc)
		if err != nil || message.Value != 7 {
			return fmt.Errorf("delivery=%d err=%v", message.Value, err)
		}
		s.reads.Add(1)
		s.readOnce.Do(func() { close(s.received) })
		select {
		case <-s.saved:
		case <-rc.Context().Done():
			return rc.Context().Err()
		}
		if err := message.Ack(); err != nil {
			return err
		}
		if _, err := stream.Receive(rc); err != io.EOF {
			return fmt.Errorf("tail=%v", err)
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	return root
}

type activeAliasStore struct {
	durable.Store
	signals *activeAliasSignals
}
type activeAliasSession struct {
	durable.Session
	signals *activeAliasSignals
}

func (s *activeAliasStore) Acquire(ctx context.Context, id string) (durable.Session, error) {
	session, err := s.Store.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	return &activeAliasSession{Session: session, signals: s.signals}, nil
}

func (s *activeAliasSession) Apply(ctx context.Context, batch durable.ChangeBatch) (durable.CommitAck, error) {
	ack, err := s.Session.Apply(ctx, batch)
	if err != nil {
		s.signals.recordStoreError("Apply", ctx, err)
		return durable.CommitAck{}, err
	}
	record, err := s.Session.Load(ctx)
	if err != nil {
		s.signals.recordStoreError("Load after Apply", ctx, err)
		return durable.CommitAck{}, err
	}
	if _, ok := record.Scopes[record.RootScope].Nodes["mount"]; ok {
		s.signals.saveOnce.Do(func() { close(s.signals.saved) })
	}
	return ack, nil
}

func TestActiveAliasCanCommitBeforeSourceFinishes(t *testing.T) {
	for _, backend := range []string{"memory", "filestore"} {
		for _, resumable := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/resumable-%t", backend, resumable), func(t *testing.T) {
				var store durable.Store = durable.NewMemoryStore()
				if backend == "filestore" {
					file, err := filestore.New(t.TempDir())
					if err != nil {
						t.Fatal(err)
					}
					store = file
				}
				signals := newActiveAliasSignals()
				runtime, err := durable.New(&activeAliasStore{Store: store, signals: signals}, durable.NewCodecs())
				if err != nil {
					t.Fatal(err)
				}
				timeout := 4 * time.Second
				if backend == "filestore" {
					// Channels enforce ordering; this bounds deadlocks, not storage speed.
					// Allow race-instrumented file syncs and the wrapper's full Load after each Apply.
					timeout = 10 * time.Second
				}
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				defer cancel()
				opts := durable.RunOptions{RunID: "active-alias", Version: "1"}
				if _, err := runtime.Run(ctx, activeAliasGraph(t, signals, resumable), pulse.Inputs{}, opts); err != nil {
					t.Fatalf("legal alias while source is active: %v; context: %v; store: %v", err, ctx.Err(), signals.storeError())
				}
				if signals.terminal.Load() != pulse.StreamTerminal("") || signals.sourceCalls.Load() != 1 || signals.mountCalls.Load() != 1 || signals.reads.Load() != 1 {
					t.Fatalf("source was not active at the alias commit: terminal=%v source=%d mount=%d reads=%d", signals.terminal.Load(), signals.sourceCalls.Load(), signals.mountCalls.Load(), signals.reads.Load())
				}
				if _, err := runtime.Resume(ctx, activeAliasGraph(t, signals, resumable), pulse.Inputs{}, opts); err != nil {
					t.Fatalf("Resume: %v; context: %v; store: %v", err, ctx.Err(), signals.storeError())
				}
				if signals.sourceCalls.Load() != 1 || signals.mountCalls.Load() != 1 || signals.reads.Load() != 1 {
					t.Fatal("completed source or saved alias reran on Resume")
				}
				session, err := store.Acquire(ctx, opts.RunID)
				if err != nil {
					t.Fatal(err)
				}
				defer session.Close()
				record, err := session.Load(ctx)
				if err != nil {
					t.Fatal(err)
				}
				nodes := record.Scopes[record.RootScope].Nodes
				if nodes["mount"].Outputs[0].Stream.Terminal != "" {
					t.Fatal("historical active alias was rewritten")
				}
				for _, stream := range record.Streams {
					if stream.Stream.Terminal != string(pulse.StreamCompleted) || stream.AckedThrough != 1 || len(stream.Pending) != 0 {
						t.Fatalf("source terminal or ledger lost: %+v", stream)
					}
				}
			})
		}
	}
}

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

func (s *failedAliasSession) Apply(ctx context.Context, batch durable.ChangeBatch) (durable.CommitAck, error) {
	ack, err := s.Session.Apply(ctx, batch)
	if err != nil {
		return durable.CommitAck{}, err
	}
	record, err := s.Session.Load(ctx)
	if err != nil {
		return durable.CommitAck{}, err
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
	return ack, nil
}

func TestFailedAliasPartialHistoryCanAdvanceParentRetry(t *testing.T) {
	for _, confirmRoot := range []bool{false, true} {
		t.Run(fmt.Sprintf("root-failed=%t", confirmRoot), func(t *testing.T) {
			base, signals := durable.NewMemoryStore(), newFailedAliasSignals(true)
			fault := &failedAliasBoundaryStore{Store: &failedAliasStore{Store: base, signals: signals}, confirmRoot: confirmRoot}
			runtime, err := durable.New(fault, durable.NewCodecs())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			opts := durable.RunOptions{RunID: "failed-alias-partial", Version: "1"}
			if _, err := runtime.Run(ctx, failedAliasGraph(t, signals, true), pulse.Inputs{}, opts); err == nil {
				t.Fatal("failed-terminal fault did not fire")
			}
			if _, err := runtime.Resume(ctx, failedAliasGraph(t, signals, true), pulse.Inputs{}, opts); err != nil {
				t.Fatalf("confirmed active alias must not mask the source's confirmed business failure: %v", err)
			}
		})
	}
}

type failedAliasBoundaryStore struct {
	durable.Store
	confirmRoot bool
	fired       atomic.Bool
}

type failedAliasBoundarySession struct {
	durable.Session
	owner *failedAliasBoundaryStore
}

func (s *failedAliasBoundaryStore) Acquire(ctx context.Context, id string) (durable.Session, error) {
	session, err := s.Store.Acquire(ctx, id)
	return &failedAliasBoundarySession{Session: session, owner: s}, err
}

func (s *failedAliasBoundarySession) Apply(ctx context.Context, batch durable.ChangeBatch) (durable.CommitAck, error) {
	record, err := s.Session.Load(ctx)
	if err != nil {
		return durable.CommitAck{}, err
	}
	if _, err := batch.ApplyTo(&record); err != nil {
		return durable.CommitAck{}, err
	}
	match := false
	for _, stream := range record.Streams {
		if stream.Stream.Terminal == "failed" && s.owner.fired.CompareAndSwap(false, true) {
			match = true
			if !s.owner.confirmRoot {
				// Recreate the legacy partial format at a real Save boundary.
				root := record.Attempts[stream.Stream.Attempt]
				root.State = durable.AttemptActive
				record.Attempts[stream.Stream.Attempt] = root
				batch = durable.OverrideAttemptForTest(batch, stream.Stream.Attempt, root)
			}
		}
	}
	ack, err := s.Session.Apply(ctx, batch)
	if err != nil {
		return durable.CommitAck{}, err
	}
	if match {
		return durable.CommitAck{}, errors.New("after failed-terminal Apply")
	}
	return ack, nil
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
