package durable_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"unicode/utf8"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/filestore"
)

func recoveryGraph(t *testing.T, id string) *pulse.Graph {
	t.Helper()
	g, err := pulse.New(id, "")
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func recoveryAdd(t *testing.T, g *pulse.Graph, n *pulse.Node) {
	t.Helper()
	if err := g.Add(n); err != nil {
		t.Fatal(err)
	}
}

func recoveryRuntime(t *testing.T, store durable.Store, codecs *durable.Codecs) *durable.Runtime {
	t.Helper()
	rt, err := durable.New(store, codecs)
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

func recoveryOptions(id string) durable.RunOptions {
	return durable.RunOptions{RunID: id, Version: "business/1"}
}

func recoveryRecord(t *testing.T, store durable.Store, id string) durable.RunRecord {
	t.Helper()
	session, err := store.Acquire(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := session.Close(); err != nil {
			t.Error(err)
		}
	}()
	record, err := session.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return record
}

func recoveryHasNode(record durable.RunRecord, graph, node string) bool {
	for _, scope := range record.Scopes {
		if scope.Graph == graph {
			if _, ok := scope.Nodes[node]; ok {
				return true
			}
		}
	}
	return false
}

func recoveryBackend(t *testing.T, name string) durable.Store {
	t.Helper()
	if name == "memory" {
		return durable.NewMemoryStore()
	}
	store, err := filestore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return store
}

type recoveryCauseError struct{ cause error }

func (*recoveryCauseError) Error() string   { return "private recovery data" }
func (e *recoveryCauseError) Unwrap() error { return e.cause }

func checkRecoveryFailure(t *testing.T, err error, cause *recoveryCauseError) {
	t.Helper()
	var checkpoint *pulse.CheckpointError
	if !errors.As(err, &checkpoint) || checkpoint.Operation == "" || checkpoint.Err == nil {
		t.Fatalf("operation was not a checkpoint failure: %v", err)
	}
	if strings.Contains(err.Error(), "private") {
		t.Fatalf("reliable failure leaked business text: %v", err)
	}
	if cause != nil {
		var typed *recoveryCauseError
		if !errors.Is(err, cause.cause) || !errors.As(err, &typed) || typed != cause {
			t.Fatalf("reliable failure lost its cause: %v", err)
		}
	}
}

func TestRecoveryEventsClassifyRootAndChildFailures(t *testing.T) {
	for _, site := range []string{"root", "child"} {
		for _, failure := range []string{"error", "panic-error", "panic-text", "panic-nil"} {
			t.Run(site+"/"+failure, func(t *testing.T) {
				cause := &recoveryCauseError{cause: errors.Join(errors.New("event sentinel"), pulse.ErrSkipped, context.Canceled)}
				leaf := recoveryGraph(t, "leaf-graph")
				leafOutput := pulse.NewKey[int]("output")
				output := leafOutput
				var body atomic.Int32
				recoveryAdd(t, leaf, pulse.NewNode("leaf", "", nil, pulse.Provides(leafOutput), func(rc *pulse.RunCtx) error {
					body.Add(1)
					return pulse.Set(rc, leafOutput, 7)
				}))
				if err := leaf.DeclareOutput(pulse.NewPort(output)); err != nil {
					t.Fatal(err)
				}
				g := leaf
				var store durable.Store = durable.NewMemoryStore()
				if site == "child" {
					g = recoveryGraph(t, "parent")
					bridged := pulse.NewKey[int]("bridged")
					if err := pulse.Sub(g, "mount", "", []pulse.SubBind{pulse.Out(output, bridged)}, leaf,
						pulse.Retry(2, 0), func(rc *pulse.RunCtx, next func(*pulse.RunCtx) error) error { _ = next(rc); return nil }); err != nil {
						t.Fatal(err)
					}
					if err := g.DeclareOutput(pulse.NewPort(bridged)); err != nil {
						t.Fatal(err)
					}
					output = bridged
					store = &startupFaultStore{Store: store, cause: errors.New("stop before parent confirmation"),
						saveMatch: func(r durable.RunRecord) bool { return recoveryHasNode(r, "parent", "mount") }}
				}
				rt := recoveryRuntime(t, store, durable.NewCodecs())
				_, firstErr := rt.Run(context.Background(), g, pulse.Inputs{}, recoveryOptions("run"))
				if (firstErr != nil) != (site == "child") || body.Load() != 1 {
					t.Fatalf("invalid recovery fixture: body=%d err=%v", body.Load(), firstErr)
				}
				before := recoveryRecord(t, store, "run")
				var leafScope, leafPath string
				for id, scope := range before.Scopes {
					if scope.Graph == "leaf-graph" {
						leafScope, leafPath = id, scope.Path
					}
				}
				var events, leafObservations, parentFinished atomic.Int32
				observer := pulse.ObserverFunc{
					GraphStarted: func(s pulse.Site) {
						if s.Graph == "leaf-graph" {
							leafObservations.Add(1)
						}
					},
					GraphFinished: func(s pulse.Site, reason pulse.NodeFinishReason, err error) {
						if s.Graph == "leaf-graph" {
							leafObservations.Add(1)
						} else {
							parentFinished.Add(1)
							var checkpoint *pulse.CheckpointError
							if reason != pulse.NodeFailed || !errors.As(err, &checkpoint) {
								t.Errorf("callback failure became success or cancellation: %s %v", reason, err)
							}
						}
					},
					Waiting: func(s pulse.Site, _ string) {
						if s.Graph == "leaf-graph" {
							leafObservations.Add(1)
						}
					},
					Running: func(s pulse.Site, _ string) {
						if s.Graph == "leaf-graph" {
							leafObservations.Add(1)
						}
					},
					Finished: func(s pulse.Site, _ string, _ pulse.NodeFinishReason, _ error) {
						if s.Graph == "leaf-graph" {
							leafObservations.Add(1)
						}
					},
				}
				opts := recoveryOptions("run")
				opts.ExecOptions = []pulse.ExecOption{pulse.WithExecObserver(observer)}
				opts.OnEvent = func(e durable.Event) error {
					events.Add(1)
					if e.Kind != durable.EventRestored || e.RunID != "run" || e.Graph != "leaf-graph" || e.NodeID != "leaf" || e.Scope != leafScope || e.Path != leafPath {
						t.Errorf("wrong recovery fact: %+v", e)
					}
					switch failure {
					case "panic-error":
						panic(cause)
					case "panic-text":
						panic("private recovery data")
					case "panic-nil":
						panic(nil)
					}
					return cause
				}
				e, err := rt.Resume(context.Background(), g, pulse.Inputs{}, opts)
				wantCause := cause
				if failure == "panic-text" || failure == "panic-nil" {
					wantCause = nil
				}
				checkRecoveryFailure(t, err, wantCause)
				if e == nil || events.Load() != 1 || body.Load() != 1 || leafObservations.Load() != 0 {
					t.Fatalf("recovery failure replayed business or invented lifecycle: execution=%v events=%d body=%d observations=%d", e != nil, events.Load(), body.Load(), leafObservations.Load())
				}
				if site == "root" && e.Snapshot().Graph.Status != pulse.GraphStatusPending {
					t.Fatal("root recovery failure invented a started execution")
				}
				if site == "child" && (e.Snapshot().Graph.Status != pulse.GraphStatusFailed || parentFinished.Load() != 1) {
					t.Fatal("parent swallowed recovery failure or classified it as cancellation")
				}
				if after := recoveryRecord(t, store, "run"); !reflect.DeepEqual(before, after) {
					t.Fatal("failed callback altered confirmed records or attempt cursors")
				}
				resumed, err := rt.Resume(context.Background(), g, pulse.Inputs{}, recoveryOptions("run"))
				if err != nil {
					t.Fatal(err)
				}
				value, state, err := pulse.ReadOutput(resumed, output)
				if err != nil || value != 7 || state != pulse.SlotStateReady || body.Load() != 1 {
					t.Fatalf("recovery did not retain confirmed work: %v %v %v body=%d", value, state, err, body.Load())
				}
			})
		}
	}
}

type startupFaultStore struct {
	durable.Store
	op         string
	cause      error
	panicValue any
	panicNil   bool
	saveMatch  func(durable.RunRecord) bool
	fired      atomic.Bool
	acquires   atomic.Int32
}

type startupFaultSession struct {
	durable.Session
	owner *startupFaultStore
}

func (s *startupFaultStore) fail() error {
	if s.panicNil || s.panicValue != nil {
		panic(s.panicValue)
	}
	return s.cause
}

func (s *startupFaultStore) Acquire(ctx context.Context, id string) (durable.Session, error) {
	s.acquires.Add(1)
	if s.op == "acquire" {
		return nil, s.fail()
	}
	session, err := s.Store.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	return &startupFaultSession{Session: session, owner: s}, nil
}

func (s *startupFaultSession) Load(ctx context.Context) (durable.RunRecord, error) {
	if s.owner.op == "load" {
		return durable.RunRecord{}, s.owner.fail()
	}
	return s.Session.Load(ctx)
}

func (s *startupFaultSession) Save(ctx context.Context, r durable.RunRecord, revision uint64) error {
	if s.owner.op == "save" || (s.owner.saveMatch != nil && s.owner.saveMatch(r) && s.owner.fired.CompareAndSwap(false, true)) {
		return s.owner.fail()
	}
	return s.Session.Save(ctx, r, revision)
}

func (s *startupFaultSession) Close() error {
	if err := s.Session.Close(); err != nil {
		return err
	}
	if s.owner.op == "close" {
		return s.owner.fail()
	}
	return nil
}

func TestRuntimeStoreOperationsAreCheckpointFailures(t *testing.T) {
	for _, op := range []string{"acquire", "load", "save", "close"} {
		for _, failure := range []string{"error", "panic-text", "panic-nil", "panic-error"} {
			t.Run(op+"/"+failure, func(t *testing.T) {
				cause := &recoveryCauseError{cause: errors.Join(errors.New("store sentinel"), pulse.ErrSkipped)}
				base := durable.NewMemoryStore()
				store := &startupFaultStore{Store: base, op: op, cause: cause}
				wantCause := cause
				switch failure {
				case "panic-text":
					store.panicValue, wantCause = "private record bytes", nil
				case "panic-nil":
					store.panicNil, wantCause = true, nil
				case "panic-error":
					store.panicValue, wantCause = cause, nil // Store panics retain no business cause.
				}
				g := recoveryGraph(t, "store-failure")
				var body atomic.Int32
				recoveryAdd(t, g, pulse.NewNode("work", "", nil, nil, func(*pulse.RunCtx) error { body.Add(1); return nil }))
				e, err := recoveryRuntime(t, store, durable.NewCodecs()).Run(context.Background(), g, pulse.Inputs{}, recoveryOptions("run"))
				checkRecoveryFailure(t, err, wantCause)
				if strings.HasPrefix(failure, "panic-") && !strings.Contains(err.Error(), op+" panicked") {
					t.Fatalf("Store panic was attributed to a different operation: %v", err)
				}
				wantBody := int32(0)
				if op == "close" {
					wantBody = 1
					if e == nil || e.Snapshot().Graph.Status != pulse.GraphStatusCompleted {
						t.Fatal("session cleanup changed the actual execution result")
					}
				}
				if body.Load() != wantBody {
					t.Fatalf("startup failure entered business: %d", body.Load())
				}
				session, err := base.Acquire(context.Background(), "run")
				if err != nil {
					t.Fatal("reliable failure leaked run ownership", err)
				}
				if err := session.Close(); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestRuntimeArgumentsAndConflictsAreNotCheckpointFailures(t *testing.T) {
	store := durable.NewMemoryStore()
	rt := recoveryRuntime(t, store, durable.NewCodecs())
	g := recoveryGraph(t, "arguments")
	input := pulse.NewKey[int]("input")
	if err := g.DeclareInput(pulse.NewPort(input)); err != nil {
		t.Fatal(err)
	}
	var body atomic.Int32
	recoveryAdd(t, g, pulse.NewNode("work", "", pulse.Requires(input), nil, func(*pulse.RunCtx) error { body.Add(1); return nil }))
	var in, changed pulse.Inputs
	if err := pulse.Seed(&in, input, 7); err != nil {
		t.Fatal(err)
	}
	if err := pulse.Seed(&changed, input, 8); err != nil {
		t.Fatal(err)
	}
	opts := recoveryOptions("run")
	if _, err := rt.Run(context.Background(), g, in, opts); err != nil {
		t.Fatal(err)
	}
	held, err := store.Acquire(context.Background(), "busy")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	var absent *durable.Runtime
	cases := []struct {
		name    string
		runtime *durable.Runtime
		ctx     context.Context
		graph   *pulse.Graph
		inputs  pulse.Inputs
		opts    durable.RunOptions
		resume  bool
		want    error
	}{
		{name: "runtime", ctx: context.Background(), graph: g, inputs: in, opts: opts, runtime: absent},
		{name: "run-id", ctx: context.Background(), graph: g, inputs: in, opts: durable.RunOptions{Version: opts.Version}},
		{name: "version", ctx: context.Background(), graph: g, inputs: in, opts: durable.RunOptions{RunID: opts.RunID}},
		{name: "nil-context", graph: g, inputs: in, opts: opts},
		{name: "canceled-context", ctx: canceled, graph: g, inputs: in, opts: opts, want: context.Canceled},
		{name: "graph", ctx: context.Background(), opts: opts},
		{name: "missing-input", ctx: context.Background(), graph: g, opts: recoveryOptions("missing-input")},
		{name: "existing-run", ctx: context.Background(), graph: g, inputs: in, opts: opts, want: durable.ErrConflict},
		{name: "version-drift", ctx: context.Background(), graph: g, inputs: in, opts: durable.RunOptions{RunID: opts.RunID, Version: "other"}, resume: true, want: durable.ErrConflict},
		{name: "input-drift", ctx: context.Background(), graph: g, inputs: changed, opts: opts, resume: true, want: durable.ErrConflict},
		{name: "missing-run", ctx: context.Background(), graph: g, inputs: in, opts: recoveryOptions("absent"), resume: true, want: durable.ErrNotFound},
		{name: "busy-run", ctx: context.Background(), graph: g, inputs: in, opts: recoveryOptions("busy"), want: durable.ErrBusy},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runtime := rt
			if tc.name == "runtime" {
				runtime = tc.runtime
			}
			var err error
			if tc.resume {
				_, err = runtime.Resume(tc.ctx, tc.graph, tc.inputs, tc.opts)
			} else {
				_, err = runtime.Run(tc.ctx, tc.graph, tc.inputs, tc.opts)
			}
			var checkpoint *pulse.CheckpointError
			if err == nil || errors.As(err, &checkpoint) || (tc.want != nil && !errors.Is(err, tc.want)) {
				t.Fatalf("ordinary argument/ownership/conflict error misclassified: %v", err)
			}
		})
	}
	if body.Load() != 1 {
		t.Fatal("invalid invocation entered business")
	}
}

func TestRuntimeRunIDValidationBeforeStore(t *testing.T) {
	cases := []struct {
		name, id string
		valid    bool
	}{
		{name: "empty"},
		{name: "invalid-byte", id: "private-run" + string([]byte{0xff})},
		{name: "overlong", id: "private-run" + string([]byte{0xc0, 0xaf})},
		{name: "surrogate", id: "private-run" + string([]byte{0xed, 0xa0, 0x80})},
		{name: "ascii", id: "run-123", valid: true},
		{name: "unicode", id: "运行/α", valid: true},
		{name: "replacement-character", id: "运行/\ufffd", valid: true},
	}
	for _, backend := range []string{"memory", "filestore"} {
		for _, tc := range cases {
			t.Run(backend+"/"+tc.name, func(t *testing.T) {
				base := recoveryBackend(t, backend)
				store := &startupFaultStore{Store: base}
				rt := recoveryRuntime(t, store, durable.NewCodecs())
				g := recoveryGraph(t, "run-id")
				var body, observations, events atomic.Int32
				recoveryAdd(t, g, pulse.NewNode("work", "", nil, nil, func(*pulse.RunCtx) error { body.Add(1); return nil }))
				opts := recoveryOptions(tc.id)
				opts.OnEvent = func(durable.Event) error { events.Add(1); return nil }
				opts.ExecOptions = []pulse.ExecOption{pulse.WithExecObserver(pulse.ObserverFunc{
					GraphStarted: func(pulse.Site) { observations.Add(1) },
				})}
				for _, action := range []string{"Run", "Resume"} {
					t.Run(action, func(t *testing.T) {
						before := store.acquires.Load()
						var execution *pulse.Execution
						var err error
						if action == "Run" {
							execution, err = rt.Run(context.Background(), g, pulse.Inputs{}, opts)
						} else {
							execution, err = rt.Resume(context.Background(), g, pulse.Inputs{}, opts)
						}
						if tc.valid {
							if err != nil || execution == nil || execution.Snapshot().Graph.Status != pulse.GraphStatusCompleted || store.acquires.Load() != before+1 || body.Load() != 1 {
								t.Fatalf("valid RunID rejected or confirmed business reran: execution=%v acquires=%d body=%d err=%v", execution != nil, store.acquires.Load(), body.Load(), err)
							}
							return
						}
						var checkpoint *pulse.CheckpointError
						if err == nil || errors.As(err, &checkpoint) || execution != nil || store.acquires.Load() != 0 || body.Load() != 0 || observations.Load() != 0 || events.Load() != 0 {
							t.Fatalf("invalid RunID was not rejected before Store/Execution: execution=%v acquires=%d body=%d observations=%d events=%d err=%v", execution != nil, store.acquires.Load(), body.Load(), observations.Load(), events.Load(), err)
						}
						if !utf8.ValidString(err.Error()) || strings.Contains(err.Error(), "private-run") {
							t.Fatalf("argument error exposed the invalid RunID: %q", err.Error())
						}
					})
				}
				if tc.valid {
					if record := recoveryRecord(t, base, tc.id); record.RunID != tc.id || events.Load() != 1 {
						t.Fatal("valid RunID or restoration fact changed")
					}
				}
			})
		}
	}
}

func TestRuntimeRunIDPreflightPreservesStoreFailures(t *testing.T) {
	for _, backend := range []string{"memory", "filestore"} {
		for _, action := range []string{"Run", "Resume"} {
			for _, op := range []string{"acquire", "load"} {
				t.Run(backend+"/"+action+"/"+op, func(t *testing.T) {
					cause := &recoveryCauseError{cause: errors.New("backend sentinel")}
					store := &startupFaultStore{Store: recoveryBackend(t, backend), op: op, cause: cause}
					rt := recoveryRuntime(t, store, durable.NewCodecs())
					g := recoveryGraph(t, "backend-failure")
					var body atomic.Int32
					recoveryAdd(t, g, pulse.NewNode("work", "", nil, nil, func(*pulse.RunCtx) error { body.Add(1); return nil }))
					var execution *pulse.Execution
					var err error
					if action == "Run" {
						execution, err = rt.Run(context.Background(), g, pulse.Inputs{}, recoveryOptions("运行/α"))
					} else {
						execution, err = rt.Resume(context.Background(), g, pulse.Inputs{}, recoveryOptions("运行/α"))
					}
					checkRecoveryFailure(t, err, cause)
					if execution != nil || body.Load() != 0 || store.acquires.Load() != 1 {
						t.Fatal("actual backend operation bypassed or business ran after failure")
					}
				})
			}
		}
	}
}
