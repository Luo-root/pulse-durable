package durable_test

import (
	"context"
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

func (s *activeAliasSession) Save(ctx context.Context, record durable.RunRecord, revision uint64) error {
	if err := s.Session.Save(ctx, record, revision); err != nil {
		return err
	}
	if _, ok := record.Scopes[record.RootScope].Nodes["mount"]; ok {
		s.signals.saveOnce.Do(func() { close(s.signals.saved) })
	}
	return nil
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
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				opts := durable.RunOptions{RunID: "active-alias", Version: "1"}
				if _, err := runtime.Run(ctx, activeAliasGraph(t, signals, resumable), pulse.Inputs{}, opts); err != nil {
					t.Fatalf("legal alias while source is active: %v", err)
				}
				if signals.terminal.Load() != pulse.StreamTerminal("") || signals.sourceCalls.Load() != 1 || signals.mountCalls.Load() != 1 || signals.reads.Load() != 1 {
					t.Fatalf("source was not active at the alias commit: terminal=%v source=%d mount=%d reads=%d", signals.terminal.Load(), signals.sourceCalls.Load(), signals.mountCalls.Load(), signals.reads.Load())
				}
				if _, err := runtime.Resume(ctx, activeAliasGraph(t, signals, resumable), pulse.Inputs{}, opts); err != nil {
					t.Fatal(err)
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
