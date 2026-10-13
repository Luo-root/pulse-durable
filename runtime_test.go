package durable

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
)

func testGraph(t *testing.T, id string, options ...pulse.Option) *pulse.Graph {
	t.Helper()
	g, err := pulse.New(id, "", options...)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func testAdd(t *testing.T, g *pulse.Graph, n *pulse.Node) {
	t.Helper()
	if err := g.Add(n); err != nil {
		t.Fatal(err)
	}
}

func testRuntime(t *testing.T, store Store, codecs *Codecs) *Runtime {
	t.Helper()
	r, err := New(store, codecs)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func testOptions(id string) RunOptions { return RunOptions{RunID: id, Version: "business/1"} }

func readRecord(t *testing.T, store Store, id string) RunRecord {
	t.Helper()
	s, err := store.Acquire(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r, err := s.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func rewriteRecord(t *testing.T, store Store, id string, edit func(*RunRecord)) {
	t.Helper()
	s, err := store.Acquire(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r, err := s.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	expected := r.Revision
	edit(&r)
	r.Revision = expected + 1
	if err := s.Save(context.Background(), r, expected); err != nil {
		t.Fatal(err)
	}
}

func TestRunResumeCompleteAndSkippedRecords(t *testing.T) {
	for _, mode := range []string{"normal", "implicit-skip", "no-value", "skip-input", "zero-output"} {
		t.Run(mode, func(t *testing.T) {
			store := NewMemoryStore()
			rt := testRuntime(t, store, NewCodecs())
			g := testGraph(t, "graph")
			input, output := pulse.NewKey[int]("input"), pulse.NewKey[int]("output")
			if err := g.DeclareInput(pulse.NewPort(input)); err != nil {
				t.Fatal(err)
			}
			var body, aspects atomic.Int32
			aspect := func(rc *pulse.RunCtx, next func(*pulse.RunCtx) error) error { aspects.Add(1); return next(rc) }
			provides := pulse.Provides(output)
			if mode == "zero-output" {
				provides = nil
			}
			testAdd(t, g, pulse.NewNode("work", "", pulse.Requires(input), provides, func(rc *pulse.RunCtx) error {
				body.Add(1)
				switch mode {
				case "no-value":
					return pulse.NoValue()
				case "implicit-skip", "zero-output":
					return nil
				default:
					v, err := pulse.Get(rc, input)
					if err != nil {
						return err
					}
					return pulse.Set(rc, output, v+1)
				}
			}, aspect))
			if mode != "zero-output" {
				if err := g.DeclareOutput(pulse.NewPort(output)); err != nil {
					t.Fatal(err)
				}
			}
			var in pulse.Inputs
			if mode == "skip-input" {
				if err := pulse.SkipSeed(&in, input); err != nil {
					t.Fatal(err)
				}
			} else if err := pulse.Seed(&in, input, 41); err != nil {
				t.Fatal(err)
			}
			e, err := rt.Run(context.Background(), g, in, testOptions("run"))
			if err != nil {
				t.Fatal(err)
			}
			firstBody, firstAspects := body.Load(), aspects.Load()
			var restored atomic.Int32
			opts := testOptions("run")
			opts.OnEvent = func(e Event) error {
				if e.Kind != EventRestored || e.NodeID != "work" || e.Path != "" {
					t.Errorf("event: %+v", e)
				}
				restored.Add(1)
				return nil
			}
			resumed, err := rt.Resume(context.Background(), g, in, opts)
			if err != nil {
				t.Fatal(err)
			}
			if resumed.ID() == e.ID() || body.Load() != firstBody || aspects.Load() != firstAspects || restored.Load() != 1 {
				t.Fatalf("restoration reentered business: body=%d aspects=%d events=%d", body.Load(), aspects.Load(), restored.Load())
			}
			if mode != "zero-output" {
				value, state, err := pulse.ReadOutput(resumed, output)
				wantState := pulse.SlotStateSkipped
				if mode == "normal" {
					wantState = pulse.SlotStateReady
				}
				if err != nil || state != wantState || (mode == "normal" && value != 42) {
					t.Fatalf("output: %v %v %v", value, state, err)
				}
			}
		})
	}
}

func TestNilAndZeroValuesRoundTripReady(t *testing.T) {
	type payload struct{ Number int }
	codecs := NewCodecs()
	for _, err := range []error{
		RegisterJSON[[]int](codecs, "slice", "1"), RegisterJSON[map[string]int](codecs, "map", "1"),
		RegisterJSON[*payload](codecs, "pointer", "1"),
		codecs.Register(codecFunc{typeOf: reflect.TypeFor[any](), id: "nil-interface", ver: "1",
			encodeFn: func(v any) ([]byte, error) {
				if v != nil {
					return nil, errors.New("non-nil")
				}
				return []byte("null"), nil
			},
			decodeFn: func(b []byte) (any, error) {
				if string(b) != "null" {
					return nil, errors.New("invalid")
				}
				return nil, nil
			}}),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	g := testGraph(t, "nil-values")
	slice := pulse.NewKey[[]int]("slice")
	mapped := pulse.NewKey[map[string]int]("map")
	pointer := pulse.NewKey[*payload]("pointer")
	iface := pulse.NewKey[any]("interface")
	zero := pulse.NewKey[int]("zero")
	var calls atomic.Int32
	testAdd(t, g, pulse.NewNode("values", "", nil,
		pulse.Deps(pulse.Provides(slice), pulse.Provides(mapped), pulse.Provides(pointer), pulse.Provides(iface), pulse.Provides(zero)),
		func(rc *pulse.RunCtx) error {
			calls.Add(1)
			return errors.Join(pulse.Set(rc, slice, nil), pulse.Set(rc, mapped, nil), pulse.Set(rc, pointer, nil), pulse.Set(rc, iface, nil), pulse.Set(rc, zero, 0))
		}))
	for _, port := range []pulse.Port{pulse.NewPort(slice), pulse.NewPort(mapped), pulse.NewPort(pointer), pulse.NewPort(iface), pulse.NewPort(zero)} {
		if err := g.DeclareOutput(port); err != nil {
			t.Fatal(err)
		}
	}
	rt := testRuntime(t, NewMemoryStore(), codecs)
	for _, resume := range []bool{false, true} {
		var e *pulse.Execution
		var err error
		if resume {
			e, err = rt.Resume(context.Background(), g, pulse.Inputs{}, testOptions("nil"))
		} else {
			e, err = rt.Run(context.Background(), g, pulse.Inputs{}, testOptions("nil"))
		}
		if err != nil {
			t.Fatal(err)
		}
		if v, s, err := pulse.ReadOutput(e, slice); v != nil || s != pulse.SlotStateReady || err != nil {
			t.Fatalf("nil slice: %v %v %v", v, s, err)
		}
		if v, s, err := pulse.ReadOutput(e, mapped); v != nil || s != pulse.SlotStateReady || err != nil {
			t.Fatalf("nil map: %v %v %v", v, s, err)
		}
		if v, s, err := pulse.ReadOutput(e, pointer); v != nil || s != pulse.SlotStateReady || err != nil {
			t.Fatalf("nil pointer: %v %v %v", v, s, err)
		}
		if v, s, err := pulse.ReadOutput(e, iface); v != nil || s != pulse.SlotStateReady || err != nil {
			t.Fatalf("nil interface: %v %v %v", v, s, err)
		}
		if v, s, err := pulse.ReadOutput(e, zero); v != 0 || s != pulse.SlotStateReady || err != nil {
			t.Fatalf("zero: %v %v %v", v, s, err)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("nil restoration reran business")
	}
}

type faultStore struct {
	Store
	match   func(RunRecord) bool
	after   bool
	failure error
	fired   atomic.Bool
	saves   atomic.Int32
}

type faultSession struct {
	Session
	owner *faultStore
}

func (s *faultStore) Acquire(ctx context.Context, id string) (Session, error) {
	session, err := s.Store.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	return &faultSession{Session: session, owner: s}, nil
}

func (s *faultSession) Save(ctx context.Context, record RunRecord, expected uint64) error {
	s.owner.saves.Add(1)
	if s.owner.match(record) && s.owner.fired.CompareAndSwap(false, true) {
		if s.owner.after {
			if err := s.Session.Save(ctx, record, expected); err != nil {
				return err
			}
		}
		return s.owner.failure
	}
	return s.Session.Save(ctx, record, expected)
}

func recordHasNode(record RunRecord, graph, node string) bool {
	for _, scope := range record.Scopes {
		if scope.Graph == graph {
			if _, ok := scope.Nodes[node]; ok {
				return true
			}
		}
	}
	return false
}

func TestSaveUncertaintyStopsAndResumeLoadsAuthority(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			store := &faultStore{Store: NewMemoryStore(), after: after, failure: fmt.Errorf("private bytes: %w", pulse.ErrSkipped),
				match: func(r RunRecord) bool { return recordHasNode(r, "graph", "first") }}
			rt := testRuntime(t, store, NewCodecs())
			g := testGraph(t, "graph")
			key := pulse.NewKey[int]("key")
			var first, second atomic.Int32
			testAdd(t, g, pulse.NewNode("first", "", nil, pulse.Provides(key), func(rc *pulse.RunCtx) error { first.Add(1); return pulse.Set(rc, key, 1) }))
			testAdd(t, g, pulse.NewNode("second", "", pulse.Requires(key), nil, func(*pulse.RunCtx) error { second.Add(1); return nil }))
			if _, err := rt.Run(context.Background(), g, pulse.Inputs{}, testOptions("run")); err == nil || strings.Contains(err.Error(), "private bytes") {
				t.Fatalf("uncertain write: %v", err)
			}
			if second.Load() != 0 {
				t.Fatal("downstream ran before storage confirmation")
			}
			if _, err := rt.Resume(context.Background(), g, pulse.Inputs{}, testOptions("run")); err != nil {
				t.Fatal(err)
			}
			wantFirst := int32(2)
			if after {
				wantFirst = 1
			}
			if first.Load() != wantFirst || second.Load() != 1 {
				t.Fatalf("authority recovery first=%d second=%d", first.Load(), second.Load())
			}
		})
	}
}

func TestRootFailureCannotRefillRetryBudgetOnResume(t *testing.T) {
	store := NewMemoryStore()
	rt := testRuntime(t, store, NewCodecs())
	g := testGraph(t, "graph")
	var calls atomic.Int32
	testAdd(t, g, pulse.NewNode("fail", "", nil, nil, func(*pulse.RunCtx) error { calls.Add(1); return errors.New("business failure") }, pulse.Retry(1, 0)))
	if _, err := rt.Run(context.Background(), g, pulse.Inputs{}, testOptions("run")); err == nil {
		t.Fatal("business failure lost")
	}
	for range 2 {
		if _, err := rt.Resume(context.Background(), g, pulse.Inputs{}, testOptions("run")); err == nil {
			t.Fatal("failed root restarted")
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("Resume refilled budget: %d", calls.Load())
	}
	r := readRecord(t, store, "run")
	for _, a := range r.Attempts {
		if a.State != AttemptFailed || a.Index != 1 {
			t.Fatalf("unexpected failed cursor: %+v", a)
		}
	}
	if _, err := rt.Run(context.Background(), g, pulse.Inputs{}, testOptions("new-run")); err == nil {
		t.Fatal("expected second business failure")
	}
	if calls.Load() != 2 {
		t.Fatal("new RunID did not get its own budget")
	}
}

func TestNestedRetryAndFailureCursorSurviveSaveError(t *testing.T) {
	t.Run("nested", func(t *testing.T) {
		store := NewMemoryStore()
		rt := testRuntime(t, store, NewCodecs())
		g := testGraph(t, "graph")
		key := pulse.NewKey[int]("out")
		var calls atomic.Int32
		testAdd(t, g, pulse.NewNode("work", "", nil, pulse.Provides(key), func(rc *pulse.RunCtx) error {
			n := calls.Add(1)
			if err := pulse.Set(rc, key, int(n)); err != nil {
				return err
			}
			if n < 4 {
				return errors.New("retry me")
			}
			return nil
		}, pulse.Retry(2, 0), pulse.Retry(2, 0)))
		if err := g.DeclareOutput(pulse.NewPort(key)); err != nil {
			t.Fatal(err)
		}
		e, err := rt.Run(context.Background(), g, pulse.Inputs{}, testOptions("run"))
		if err != nil {
			t.Fatal(err)
		}
		v, state, err := pulse.ReadOutput(e, key)
		if err != nil || state != pulse.SlotStateReady || v != 4 || calls.Load() != 4 {
			t.Fatalf("nested result: %v %v %v calls=%d", v, state, err, calls.Load())
		}
		if len(readRecord(t, store, "run").Attempts) != 7 {
			t.Fatal("nested boundaries were conflated")
		}
	})
	t.Run("resume-after-fail-confirmed", func(t *testing.T) {
		store := &faultStore{Store: NewMemoryStore(), after: true, failure: errors.New("save acknowledgment lost"), match: func(r RunRecord) bool {
			for _, a := range r.Attempts {
				if a.Boundary != "" && a.State == AttemptFailed && a.Index == 1 {
					return true
				}
			}
			return false
		}}
		rt := testRuntime(t, store, NewCodecs())
		g := testGraph(t, "graph")
		var calls atomic.Int32
		testAdd(t, g, pulse.NewNode("work", "", nil, nil, func(*pulse.RunCtx) error {
			if calls.Add(1) == 1 {
				return errors.New("first failed")
			}
			return nil
		}, pulse.Retry(2, 0)))
		if _, err := rt.Run(context.Background(), g, pulse.Inputs{}, testOptions("run")); err == nil {
			t.Fatal("uncertain failure confirmation ignored")
		}
		if _, err := rt.Resume(context.Background(), g, pulse.Inputs{}, testOptions("run")); err != nil {
			t.Fatal(err)
		}
		if calls.Load() != 2 {
			t.Fatal("Retry cursor restarted")
		}
		var found bool
		for _, a := range readRecord(t, store, "run").Attempts {
			if a.Boundary != "" && a.Index == 2 {
				found = true
			}
		}
		if !found {
			t.Fatal("did not persist next Retry index")
		}
	})
}

func TestRunIDBusyAndCancellationLeavesActiveRoot(t *testing.T) {
	store := NewMemoryStore()
	rt := testRuntime(t, store, NewCodecs())
	g := testGraph(t, "graph")
	started := make(chan struct{})
	var calls atomic.Int32
	testAdd(t, g, pulse.NewNode("work", "", nil, nil, func(rc *pulse.RunCtx) error {
		if calls.Add(1) == 1 {
			close(started)
			<-rc.Context().Done()
			return rc.Context().Err()
		}
		return nil
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := rt.Run(ctx, g, pulse.Inputs{}, testOptions("run")); result <- err }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("business did not start")
	}
	if _, err := rt.Resume(context.Background(), g, pulse.Inputs{}, testOptions("run")); !errors.Is(err, ErrBusy) {
		t.Fatalf("busy Resume: %v", err)
	}
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled run: %v", err)
	}
	for _, a := range readRecord(t, store, "run").Attempts {
		if a.State != AttemptActive {
			t.Fatal("host cancellation marked business failure")
		}
	}
	if _, err := rt.Resume(context.Background(), g, pulse.Inputs{}, testOptions("run")); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatal("active root was not recognized")
	}
}
