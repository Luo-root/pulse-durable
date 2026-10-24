package durable

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"

	"github.com/Luo-root/pulse"
)

type countedStore struct {
	Store
	acquires, closes atomic.Int32
}

func (s *countedStore) Acquire(ctx context.Context, id string) (Session, error) {
	s.acquires.Add(1)
	session, err := s.Store.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	return &claimedTestSession{Session: session, id: id, owner: s}, nil
}

type claimedTestSession struct {
	Session
	id          string
	owner       *countedStore
	uncertain   bool
	uncertainAt uint64
}

func (s *claimedTestSession) RunID() string { return s.id }
func (s *claimedTestSession) Close() error  { s.owner.closes.Add(1); return s.Session.Close() }
func (s *claimedTestSession) Save(ctx context.Context, r RunRecord, rev uint64) error {
	err := s.Session.Save(ctx, r, rev)
	if err == nil && (s.uncertain || s.uncertainAt == r.Revision) {
		return context.Canceled
	}
	return err
}
func claimForTest(t *testing.T, s *countedStore, id string) *claimedTestSession {
	t.Helper()
	session, err := s.Acquire(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return session.(*claimedTestSession)
}

func TestClaimedOwnershipAndCompletedRestore(t *testing.T) {
	s := &countedStore{Store: NewMemoryStore()}
	rt := testRuntime(t, s, NewCodecs())
	session := claimForTest(t, s, "run")
	defer session.Close()
	g := testGraph(t, "graph")
	var calls atomic.Int32
	testAdd(t, g, pulse.NewNode("work", "", nil, nil, func(*pulse.RunCtx) error { calls.Add(1); return nil }))
	for i := 0; i < 2; i++ {
		var e *pulse.Execution
		var outcome Outcome
		var err error
		if i == 0 {
			e, outcome, err = rt.RunClaimed(context.Background(), session, g, pulse.Inputs{}, testOptions("run"))
		} else {
			e, outcome, err = rt.ResumeClaimed(context.Background(), session, g, pulse.Inputs{}, testOptions("run"))
		}
		if err != nil || outcome != OutcomeCompleted || e == nil {
			t.Fatalf("%v %v %v", e, outcome, err)
		}
		if s.acquires.Load() != 1 || s.closes.Load() != 0 {
			t.Fatal("runtime acquired/closed caller session")
		}
		// Simulate the coordinator's Finish boundary: authority is still accessible
		// and a competing owner cannot enter until the caller explicitly closes.
		if _, err := session.Load(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Store.Acquire(context.Background(), "run"); !errors.Is(err, ErrBusy) {
			t.Fatalf("ownership lost before Finish: %v", err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("business reran %d", calls.Load())
	}
}

func TestClaimedBindingRejected(t *testing.T) {
	s := &countedStore{Store: NewMemoryStore()}
	rt := testRuntime(t, s, NewCodecs())
	g := testGraph(t, "graph")
	session := claimForTest(t, s, "run")
	defer session.Close()
	var typedNil *claimedTestSession
	for _, c := range []ClaimedSession{nil, typedNil, &claimedTestSession{id: ""}, &claimedTestSession{id: "other"}, &claimedTestSession{id: string([]byte{255})}} {
		e, outcome, err := rt.RunClaimed(context.Background(), c, g, pulse.Inputs{}, testOptions("run"))
		if e != nil || outcome != OutcomeRejected || err == nil {
			t.Fatalf("%v %v %v", e, outcome, err)
		}
	}
	if s.acquires.Load() != 1 || s.closes.Load() != 0 {
		t.Fatal("unexpected lifecycle calls")
	}
}

func TestClaimedOutcomes(t *testing.T) {
	for _, mode := range []string{"failure", "canceled-business-error", "cancel", "uncertain", "before-start"} {
		t.Run(mode, func(t *testing.T) {
			s := &countedStore{Store: NewMemoryStore()}
			rt := testRuntime(t, s, NewCodecs())
			session := claimForTest(t, s, "run")
			defer session.Close()
			g := testGraph(t, "graph")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var calls atomic.Int32
			testAdd(t, g, pulse.NewNode("work", "", nil, nil, func(*pulse.RunCtx) error {
				calls.Add(1)
				switch mode {
				case "failure":
					return errors.New("business")
				case "canceled-business-error":
					return context.Canceled
				case "cancel":
					cancel()
					return ctx.Err()
				}
				return nil
			}))
			opts := testOptions("run")
			if mode == "uncertain" {
				session.uncertain = true
			}
			if mode == "before-start" {
				opts.BeforeStart = func(*pulse.Execution) error { return errors.New("host") }
			}
			e, outcome, err := rt.RunClaimed(ctx, session, g, pulse.Inputs{}, opts)
			want := OutcomeFailed
			switch mode {
			case "cancel", "before-start":
				want = OutcomeInterrupted
			case "uncertain":
				want = OutcomeInfrastructure
			}
			if err == nil || outcome != want {
				t.Fatalf("got %v %v want %v", outcome, err, want)
			}
			if mode == "failure" || mode == "canceled-business-error" {
				before := calls.Load()
				opts.BeforeStart = func(*pulse.Execution) error { t.Error("BeforeStart on final failure"); return nil }
				opts.OnEvent = func(Event) error { t.Error("recovery event on final failure"); return nil }
				e, outcome, err = rt.ResumeClaimed(context.Background(), session, g, pulse.Inputs{}, opts)
				var failure *pulse.CheckpointScopeFailure
				if !errors.As(err, &failure) || outcome != OutcomeFailed || e == nil || e.Snapshot().Graph.Status != pulse.GraphStatusPending || calls.Load() != before {
					t.Fatalf("restored final failure: %v %v", outcome, err)
				}
			}
			if mode == "uncertain" {
				session.uncertain = false
				_, outcome, err = rt.ResumeClaimed(context.Background(), session, g, pulse.Inputs{}, testOptions("run"))
				if err != nil || outcome != OutcomeCompleted {
					t.Fatalf("uncertain restore: %v %v", outcome, err)
				}
			}
			if s.acquires.Load() != 1 || s.closes.Load() != 0 {
				t.Fatal("unexpected lifecycle calls")
			}
		})
	}
}

func TestClaimedUncertainFinalSave(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			s := &countedStore{Store: NewMemoryStore()}
			rt := testRuntime(t, s, NewCodecs())
			session := claimForTest(t, s, "run")
			defer session.Close()
			session.uncertainAt = 3 // initialize, open root attempt, then commit/fail
			g := testGraph(t, "graph")
			var calls atomic.Int32
			testAdd(t, g, pulse.NewNode("work", "", nil, nil, func(*pulse.RunCtx) error {
				calls.Add(1)
				if fail {
					return errors.New("business")
				}
				return nil
			}))
			_, outcome, err := rt.RunClaimed(context.Background(), session, g, pulse.Inputs{}, testOptions("run"))
			if err == nil || outcome != OutcomeInfrastructure {
				t.Fatalf("uncertain final write: %v %v", outcome, err)
			}
			session.uncertainAt = 0
			_, outcome, err = rt.ResumeClaimed(context.Background(), session, g, pulse.Inputs{}, testOptions("run"))
			want := OutcomeCompleted
			if fail {
				want = OutcomeFailed
			}
			if outcome != want || (!fail && err != nil) || calls.Load() != 1 {
				t.Fatalf("authority reload: %v %v calls=%d", outcome, err, calls.Load())
			}
		})
	}
}

func TestClaimedSubgraphRetryStream(t *testing.T) {
	s := &countedStore{Store: NewMemoryStore()}
	rt := testRuntime(t, s, NewCodecs())
	session := claimForTest(t, s, "run")
	defer session.Close()
	child := testGraph(t, "child")
	key := pulse.NewKey[pulse.Stream[int]]("items")
	result, output := pulse.NewKey[int]("result"), pulse.NewKey[int]("output")
	var calls atomic.Int32
	testAdd(t, child, pulse.NewNode("retry", "", nil, nil, func(*pulse.RunCtx) error {
		if calls.Add(1) == 1 {
			return errors.New("retry")
		}
		return nil
	}, pulse.Retry(2, 0)))
	testAdd(t, child, pulse.NewNode("source", "", nil, pulse.Provides(key), func(rc *pulse.RunCtx) error {
		w, err := pulse.OpenStream(rc, key, pulse.WithBuffer(1), pulse.WithStreamSource("items", true))
		if err != nil {
			return err
		}
		return w.SendAt(42, 1)
	}))
	testAdd(t, child, pulse.NewNode("sink", "", pulse.Requires(key), pulse.Provides(result), func(rc *pulse.RunCtx) error {
		stream, err := pulse.Get(rc, key)
		if err != nil {
			return err
		}
		for {
			delivery, err := stream.Receive(rc)
			if err == io.EOF {
				return pulse.Set(rc, result, 42)
			}
			if err != nil {
				return err
			}
			if delivery.Value != 42 {
				return errors.New("value")
			}
			if err := delivery.Ack(); err != nil {
				return err
			}
		}
	}))
	if err := child.DeclareOutput(pulse.NewPort(result)); err != nil {
		t.Fatal(err)
	}
	root := testGraph(t, "root")
	if err := pulse.Sub(root, "child", "", []pulse.SubBind{pulse.Out(result, output)}, child); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		var outcome Outcome
		var err error
		if i == 0 {
			_, outcome, err = rt.RunClaimed(context.Background(), session, root, pulse.Inputs{}, testOptions("run"))
		} else {
			_, outcome, err = rt.ResumeClaimed(context.Background(), session, root, pulse.Inputs{}, testOptions("run"))
		}
		if err != nil || outcome != OutcomeCompleted {
			t.Fatalf("%v %v", outcome, err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("retry budget or restoration changed: %d", calls.Load())
	}
}

func TestClaimedRestoreValidation(t *testing.T) {
	s := &countedStore{Store: NewMemoryStore()}
	rt := testRuntime(t, s, NewCodecs())
	session := claimForTest(t, s, "run")
	defer session.Close()
	g := testGraph(t, "graph")
	key := pulse.NewKey[int]("input")
	if err := g.DeclareInput(pulse.NewPort(key)); err != nil {
		t.Fatal(err)
	}
	var in pulse.Inputs
	if err := pulse.Seed(&in, key, 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := rt.RunClaimed(context.Background(), session, g, in, testOptions("run")); err != nil {
		t.Fatal(err)
	}
	opts := testOptions("run")
	opts.BeforeStart = func(*pulse.Execution) error { t.Error("callback before validation"); return nil }
	opts.OnEvent = func(Event) error { t.Error("event before validation"); return nil }
	for _, mode := range []string{"version", "input", "format"} {
		t.Run(mode, func(t *testing.T) {
			options := opts
			inputs := in
			switch mode {
			case "version":
				options.Version = "other"
			case "input":
				inputs = pulse.Inputs{}
				if err := pulse.Seed(&inputs, key, 2); err != nil {
					t.Fatal(err)
				}
			case "format":
				record, err := session.Load(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				rev := record.Revision
				record.Format = 999
				record.Revision++
				if err := session.Save(context.Background(), record, rev); err != nil {
					t.Fatal(err)
				}
			}
			_, outcome, err := rt.ResumeClaimed(context.Background(), session, g, inputs, options)
			if err == nil || outcome != OutcomeRejected {
				t.Fatalf("validation: %v %v", outcome, err)
			}
		})
	}
}

func TestLegacySessionLifecycle(t *testing.T) {
	s := &countedStore{Store: NewMemoryStore()}
	rt := testRuntime(t, s, NewCodecs())
	g := testGraph(t, "graph")
	for i := 0; i < 2; i++ {
		var err error
		if i == 0 {
			_, err = rt.Run(context.Background(), g, pulse.Inputs{}, testOptions("run"))
		} else {
			_, err = rt.Resume(context.Background(), g, pulse.Inputs{}, testOptions("run"))
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if s.acquires.Load() != 2 || s.closes.Load() != 2 {
		t.Fatalf("Acquire=%d Close=%d", s.acquires.Load(), s.closes.Load())
	}
}
