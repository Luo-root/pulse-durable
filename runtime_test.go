package durable

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
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
	replaceMemoryFixture(s, r)
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

func (s *faultSession) Initialize(ctx context.Context, record RunRecord) error {
	s.owner.saves.Add(1)
	if s.owner.match(record) && s.owner.fired.CompareAndSwap(false, true) {
		if s.owner.after {
			if err := s.Session.Initialize(ctx, record); err != nil {
				return err
			}
		}
		return s.owner.failure
	}
	return s.Session.Initialize(ctx, record)
}

func (s *faultSession) Apply(ctx context.Context, batch ChangeBatch) (CommitAck, error) {
	s.owner.saves.Add(1)
	record, err := s.Session.Load(ctx)
	if err != nil {
		return CommitAck{}, err
	}
	if _, err := batch.ApplyTo(&record); err != nil {
		return CommitAck{}, err
	}
	if s.owner.match(record) && s.owner.fired.CompareAndSwap(false, true) {
		if s.owner.after {
			if _, err := s.Session.Apply(ctx, batch); err != nil {
				return CommitAck{}, err
			}
		}
		return CommitAck{}, s.owner.failure
	}
	return s.Session.Apply(ctx, batch)
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

// 控制 Core 确认边界时使用真实 Durable 钩子。
// 这些测试约束依赖升级，不替换为另一套引擎。
func testCheckpointState(t *testing.T, ctx context.Context, g *pulse.Graph) (*runState, Store) {
	t.Helper()
	store := NewMemoryStore()
	session, err := store.Acquire(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = session.Close() })
	def, err := inspectDefinition(g, NewCodecs().snapshot())
	if err != nil {
		t.Fatal(err)
	}
	root, err := randomID()
	if err != nil {
		t.Fatal(err)
	}
	inputs, err := def.inputsFingerprint(def.root, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := &runState{session: session, def: def, record: RunRecord{Format: RecordFormat, RunID: "run", Version: "business/1",
		Definition: def.fingerprint, Inputs: inputs, RootScope: root, Attempts: make(map[string]AttemptRecord),
		Scopes: map[string]ScopeRecord{root: {Graph: def.root.graph, Inputs: inputs, Nodes: make(map[string]NodeRecord)}}}}
	if err := s.initialize(ctx, cloneRecord(s.record)); err != nil {
		t.Fatal(err)
	}
	return s, store
}

func testAwait[T any](t *testing.T, ctx context.Context, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		var zero T
		return zero
	}
}

func TestTimeoutCleanupDoesNotConsumeRootBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	store := NewMemoryStore()
	g := testGraph(t, "timeout-cleanup")
	testAdd(t, g, pulse.NewNode("work", "", nil, nil, func(rc *pulse.RunCtx) error {
		defer rc.Cancel() // 本地 Fork 清理，而非一小时截止时间。
		return nil
	}, pulse.Timeout(time.Hour)))
	e, err := testRuntime(t, store, NewCodecs()).Run(ctx, g, pulse.Inputs{}, testOptions("run"))
	if err != nil || e.Snapshot().Graph.Status != pulse.GraphStatusCompleted {
		t.Fatalf("cleanup replaced completed result: %v", err)
	}
	r := readRecord(t, store, "run")
	node, ok := r.Scopes[r.RootScope].Nodes["work"]
	if !ok || node.Reason != "completed" || len(r.Attempts) != 1 || r.Attempts[node.Attempt].State != AttemptConfirmed {
		t.Fatalf("cleanup consumed persistent budget: %+v", r.Attempts)
	}
}

type confirmedSkipError struct {
	confirmed atomic.Bool
	late      atomic.Int32
}

func (*confirmedSkipError) Error() string { return "normal absence" }
func (e *confirmedSkipError) Is(target error) bool {
	if e.confirmed.Load() {
		e.late.Add(1)
		return false
	}
	return target == pulse.ErrSkipped
}
func (e *confirmedSkipError) Unwrap() error {
	if e.confirmed.Load() {
		e.late.Add(1)
		return errors.New("changed after confirmation")
	}
	return nil
}

func TestCheckpointConfirmationFreezesSkippedConclusion(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	g := testGraph(t, "confirmed-conclusion")
	cause := &confirmedSkipError{}
	var calls atomic.Int32
	testAdd(t, g, pulse.NewNode("work", "", nil, nil, func(*pulse.RunCtx) error { calls.Add(1); return cause }))
	s, store := testCheckpointState(t, ctx, g)
	h := s.hooks()
	commit := h.Commit
	h.Commit = func(ctx context.Context, req pulse.CheckpointCommit) error {
		if err := commit(ctx, req); err != nil {
			return err
		}
		cause.confirmed.Store(true) // 原始错误改变前，Save 已成功。
		return nil
	}
	e, err := g.NewExecution(ctx, pulse.Inputs{}, pulse.WithExecCheckpoint(s.record.RootScope, h))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Cancel()
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	err = e.Wait()
	node := s.record.Scopes[s.record.RootScope].Nodes["work"]
	if err != nil || e.Snapshot().Nodes[0].Status != pulse.NodeStatusSkipped || node.Reason != "skipped" || cause.late.Load() != 0 {
		t.Errorf("confirmed skip was reinspected: err=%v saved=%s late=%d", err, node.Reason, cause.late.Load())
	}
	if err := s.session.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := testRuntime(t, store, NewCodecs()).Resume(ctx, g, pulse.Inputs{}, testOptions("run")); err != nil || calls.Load() != 1 {
		t.Fatalf("confirmed skip did not restore: err=%v calls=%d", err, calls.Load())
	}
}

func TestCheckpointAcceptedHostStopJoinsWait(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			g := testGraph(t, "accepted-stop")
			key := pulse.NewKey[pulse.Stream[int]]("out")
			if err := g.DeclareOutput(pulse.NewPort(key)); err != nil {
				t.Fatal(err)
			}
			if err := pulse.ProduceStream(g, "source", "", key, func(*pulse.RunCtx, *pulse.StreamWriter[int]) error { return nil }, pulse.WithStreamSource("empty/1", true)); err != nil {
				t.Fatal(err)
			}
			s, _ := testCheckpointState(t, ctx, g)
			h := s.hooks()
			commit := h.StreamHooks.Commit
			entered, release, sourceDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			cause := errors.New("accepted Stop save failed")
			h.StreamHooks.Commit = func(ctx context.Context, req pulse.CheckpointMessageCommit) error {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
				if fail {
					return cause
				}
				return commit(ctx, req)
			}
			var finished atomic.Int32
			e, err := g.NewExecution(ctx, pulse.Inputs{}, pulse.WithExecCheckpoint(s.record.RootScope, h), pulse.WithExecObserver(pulse.ObserverFunc{
				Finished:      func(_ pulse.Site, _ string, _ pulse.NodeFinishReason, _ error) { close(sourceDone) },
				GraphFinished: func(pulse.Site, pulse.NodeFinishReason, error) { finished.Add(1) },
			}))
			if err != nil {
				t.Fatal(err)
			}
			defer e.Cancel()
			sub, err := pulse.SubscribeStream(e, key)
			if err != nil {
				t.Fatal(err)
			}
			if err := e.Start(); err != nil {
				t.Fatal(err)
			}
			testAwait(t, ctx, sourceDone)
			if _, err := sub.Receive(ctx); err != io.EOF {
				t.Fatalf("empty source EOF=%v", err)
			}
			stopDone, waitDone := make(chan error, 1), make(chan error, 1)
			go func() { stopDone <- sub.Stop(ctx) }()
			testAwait(t, ctx, entered) // Wait 封闭新工作前，Stop 已被接受。
			go func() { waitDone <- e.Wait() }()
			var first error
			premature := false
			select {
			case first = <-waitDone:
				premature = true
				t.Errorf("Wait overtook accepted Stop: %v", first)
			case <-time.After(150 * time.Millisecond):
			}
			if finished.Load() != 0 {
				t.Error("GraphFinished overtook Stop confirmation")
			}
			once.Do(func() { close(release) })
			stopErr := testAwait(t, ctx, stopDone)
			if !premature {
				first = testAwait(t, ctx, waitDone)
			}
			if fail && (!errors.Is(first, cause) || !errors.Is(stopErr, cause)) || !fail && (first != nil || stopErr != nil) {
				t.Errorf("first Wait lost Stop result: Wait=%v Stop=%v", first, stopErr)
			}
			if e.Wait() != first || e.Err() != first {
				t.Error("terminal result changed after first Wait")
			}
		})
	}
}

func TestCheckpointWriterTerminalSerializesWithStop(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	g := testGraph(t, "terminal-stop")
	key := pulse.NewKey[pulse.Stream[int]]("out")
	if err := g.DeclareOutput(pulse.NewPort(key)); err != nil {
		t.Fatal(err)
	}
	finishBody, bodyReturned := make(chan struct{}), make(chan struct{})
	endpoint := make(chan pulse.Stream[int], 1)
	if err := pulse.ProduceStream(g, "source", "", key, func(rc *pulse.RunCtx, _ *pulse.StreamWriter[int]) error {
		stream, err := pulse.Get(rc, key)
		if err != nil {
			return err
		}
		endpoint <- stream
		select {
		case <-finishBody:
		case <-rc.Context().Done():
			return rc.Context().Err()
		}
		close(bodyReturned)
		return nil
	}, pulse.WithStreamSource("empty/1", true)); err != nil {
		t.Fatal(err)
	}
	s, _ := testCheckpointState(t, ctx, g)
	h := s.hooks()
	stop, commit := h.StreamHooks.Commit, h.Commit
	entered, releaseStop, releaseWriter := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var stopOnce, writerOnce sync.Once
	defer stopOnce.Do(func() { close(releaseStop) })
	defer writerOnce.Do(func() { close(releaseWriter) })
	sampled := make(chan pulse.CheckpointCommit, 1)
	h.StreamHooks.Commit = func(ctx context.Context, req pulse.CheckpointMessageCommit) error {
		close(entered)
		select {
		case <-releaseStop:
			return stop(ctx, req)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	h.Commit = func(ctx context.Context, req pulse.CheckpointCommit) error {
		sampled <- req
		select {
		case <-releaseWriter:
			return commit(ctx, req)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	e, err := g.NewExecution(ctx, pulse.Inputs{}, pulse.WithExecCheckpoint(s.record.RootScope, h))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Cancel()
	sub, err := pulse.SubscribeStream(e, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	stream := testAwait(t, ctx, endpoint)
	stopDone := make(chan error, 1)
	go func() { stopDone <- sub.Stop(ctx) }()
	testAwait(t, ctx, entered)
	close(finishBody)
	testAwait(t, ctx, bodyReturned)
	var saved pulse.CheckpointCommit
	early := false
	select {
	case saved = <-sampled:
		early = true
	case <-time.After(150 * time.Millisecond):
	}
	stopOnce.Do(func() { close(releaseStop) })
	if err := testAwait(t, ctx, stopDone); err != nil {
		t.Fatal(err)
	}
	if !early {
		saved = testAwait(t, ctx, sampled)
	}
	writerOnce.Do(func() { close(releaseWriter) })
	if err := e.Wait(); err != nil {
		t.Fatalf("Core supplied an inconsistent terminal to Durable: %v", err)
	}
	local := stream.Snapshot()
	if early || len(saved.Streams) != 1 || len(saved.Node.Outputs) != 1 || saved.Node.Outputs[0].Stream == nil {
		t.Fatalf("terminal sampled before Stop installation: early=%t payload=%+v", early, saved)
	}
	if saved.Streams[0].Terminal != local.Terminal || saved.Node.Outputs[0].Stream.Stopped != local.Stopped || local.Terminal != pulse.StreamStopped || !local.Stopped {
		t.Fatalf("confirmed terminal disagrees with local stream: saved=%+v local=%+v", saved.Streams, local)
	}
}

func TestCheckpointHostReceiveCannotCancelSharedRestore(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	type contextKey struct{}
	ctx = context.WithValue(ctx, contextKey{}, "shared-restore")
	g := testGraph(t, "host-first-restore")
	key := pulse.NewKey[pulse.Stream[int]]("out")
	if err := g.DeclareOutput(pulse.NewPort(key)); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	if err := pulse.ProduceStream(g, "source", "", key, func(*pulse.RunCtx, *pulse.StreamWriter[int]) error { calls.Add(1); return nil }, pulse.WithStreamSource("empty/1", true)); err != nil {
		t.Fatal(err)
	}
	s, _ := testCheckpointState(t, ctx, g)
	first, err := g.NewExecution(ctx, pulse.Inputs{}, pulse.WithExecCheckpoint(s.record.RootScope, s.hooks()))
	if err != nil {
		t.Fatal(err)
	}
	defer first.Cancel()
	sub, err := pulse.SubscribeStream(first, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := sub.Receive(ctx); err != io.EOF {
		t.Fatal(err)
	}
	if err := first.Wait(); err != nil {
		t.Fatal(err)
	}
	restored := &runState{session: s.session, def: s.def, record: cloneRecord(s.record)}
	records, err := restored.decodeScope(s.record.RootScope)
	if err != nil {
		t.Fatal(err)
	}
	h := restored.hooks()
	open := h.StreamHooks.Open
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	h.StreamHooks.Open = func(ctx context.Context, req pulse.CheckpointStreamOpen) (pulse.CheckpointStreamRecord, error) {
		entered <- ctx
		select {
		case <-release:
			return open(ctx, req)
		case <-ctx.Done():
			return pulse.CheckpointStreamRecord{}, ctx.Err()
		}
	}
	var e *pulse.Execution
	var sharedOpenCtx context.Context
	callCtx, cancelCall := context.WithCancel(ctx)
	defer cancelCall()
	result := make(chan error, 1)
	observer := pulse.ObserverFunc{GraphStarted: func(pulse.Site) {
		go func() { _, err := sub.Receive(callCtx); result <- err }()
		openCtx := testAwait(t, ctx, entered) // 观察器先于所有内部工作协程。
		sharedOpenCtx = openCtx
		deadline, hasDeadline := openCtx.Deadline()
		wantDeadline, wantHasDeadline := ctx.Deadline()
		if openCtx.Value(contextKey{}) != "shared-restore" || hasDeadline != wantHasDeadline || !deadline.Equal(wantDeadline) {
			t.Error("shared Open lost root values or deadline")
		}
		cancelCall()
		if err := testAwait(t, ctx, result); !errors.Is(err, context.Canceled) {
			t.Errorf("single Receive cancellation=%v", err)
		}
		if openCtx.Err() != nil || e.Context().Err() != nil {
			t.Error("single Receive canceled the shared Open or execution")
		}
		once.Do(func() { close(release) })
	}}
	e, err = g.NewExecution(ctx, pulse.Inputs{}, pulse.WithExecCheckpoint(s.record.RootScope, h), pulse.WithExecObserver(observer))
	if err != nil {
		t.Fatal(err)
	}
	defer e.Cancel()
	sub, err = pulse.SubscribeStream(e, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.RestoreNodes(records); err != nil {
		t.Fatal(err)
	}
	if err := e.Start(); err != nil {
		t.Fatal(err)
	}
	if err := e.Wait(); err != nil || calls.Load() != 1 {
		t.Fatalf("healthy restored run failed or reentered source: err=%v calls=%d", err, calls.Load())
	}
	if sharedOpenCtx == nil || !errors.Is(sharedOpenCtx.Err(), context.Canceled) {
		t.Error("shared Open did not retain root execution cancellation")
	}
}
