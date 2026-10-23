package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Luo-root/pulse"
	"reflect"
	"sync/atomic"
	"testing"
)

func TestPreflightRejectsMissingCodecAndChannelsBeforeBusiness(t *testing.T) {
	for _, mode := range []string{"unused-input", "pass-through", "output", "channel", "child-input"} {
		t.Run(mode, func(t *testing.T) {
			type unregistered struct{ N int }
			g := testGraph(t, "graph")
			var body atomic.Int32
			testAdd(t, g, pulse.NewNode("business", "", nil, nil, func(*pulse.RunCtx) error { body.Add(1); return nil }))
			key := pulse.NewKey[unregistered]("unregistered")
			switch mode {
			case "unused-input", "pass-through":
				if err := g.DeclareInput(pulse.NewPort(key)); err != nil {
					t.Fatal(err)
				}
				if mode == "pass-through" {
					if err := g.DeclareOutput(pulse.NewPort(key)); err != nil {
						t.Fatal(err)
					}
				}
			case "output":
				testAdd(t, g, pulse.NewNode("source", "", nil, pulse.Provides(key), func(rc *pulse.RunCtx) error { body.Add(1); return pulse.Set(rc, key, unregistered{}) }))
				if err := g.DeclareOutput(pulse.NewPort(key)); err != nil {
					t.Fatal(err)
				}
			case "channel":
				channel := pulse.NewKey[chan int]("channel")
				if err := g.DeclareInput(pulse.NewPort(channel)); err != nil {
					t.Fatal(err)
				}
			case "child-input":
				child := testGraph(t, "child")
				if err := child.DeclareInput(pulse.NewPort(key)); err != nil {
					t.Fatal(err)
				}
				valid := pulse.NewKey[int]("valid")
				bridge := pulse.NewKey[int]("bridge")
				testAdd(t, child, pulse.NewNode("source", "", nil, pulse.Provides(valid), func(rc *pulse.RunCtx) error { body.Add(1); return pulse.Set(rc, valid, 1) }))
				if err := child.DeclareOutput(pulse.NewPort(valid)); err != nil {
					t.Fatal(err)
				}
				if err := pulse.Sub(g, "mount", "", []pulse.SubBind{pulse.Out(valid, bridge)}, child); err != nil {
					t.Fatal(err)
				}
			}
			store := NewMemoryStore()
			if _, err := testRuntime(t, store, NewCodecs()).Run(context.Background(), g, pulse.Inputs{}, testOptions("run")); err == nil {
				t.Fatal("unsafe schema accepted")
			}
			if body.Load() != 0 {
				t.Fatal("preflight ran business")
			}
			s, err := store.Acquire(context.Background(), "run")
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if _, err := s.Load(context.Background()); !errors.Is(err, ErrNotFound) {
				t.Fatal("preflight wrote a run record")
			}
		})
	}
}

func TestResumeRejectsInputVersionDefinitionAndCodecDrift(t *testing.T) {
	type value int
	input, output := pulse.NewKey[value]("in"), pulse.NewKey[value]("out")
	var calls atomic.Int32
	makeGraph := func(description string) *pulse.Graph {
		g := testGraph(t, "graph")
		if err := g.DeclareInput(pulse.NewPort(input)); err != nil {
			t.Fatal(err)
		}
		if err := g.DeclareOutput(pulse.NewPort(output)); err != nil {
			t.Fatal(err)
		}
		testAdd(t, g, pulse.NewNode("work", description, pulse.Requires(input), pulse.Provides(output), func(rc *pulse.RunCtx) error {
			calls.Add(1)
			v, err := pulse.Get(rc, input)
			if err != nil {
				return err
			}
			return pulse.Set(rc, output, v)
		}))
		return g
	}
	makeCodecs := func(id, version string) *Codecs {
		c := &Codecs{}
		if err := RegisterJSON[value](c, id, version); err != nil {
			t.Fatal(err)
		}
		return c
	}
	g := makeGraph("original")
	store := NewMemoryStore()
	rt := testRuntime(t, store, makeCodecs("value", "1"))
	var in pulse.Inputs
	if err := pulse.Seed(&in, input, value(1)); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Run(context.Background(), g, in, testOptions("run")); err != nil {
		t.Fatal(err)
	}
	var changed pulse.Inputs
	if err := pulse.Seed(&changed, input, value(2)); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Resume(context.Background(), g, changed, testOptions("run")); !errors.Is(err, ErrConflict) {
		t.Fatalf("input drift: %v", err)
	}
	opts := testOptions("run")
	opts.Version = "business/2"
	if _, err := rt.Resume(context.Background(), g, in, opts); !errors.Is(err, ErrConflict) {
		t.Fatalf("version drift: %v", err)
	}
	if _, err := rt.Resume(context.Background(), makeGraph("changed"), in, testOptions("run")); !errors.Is(err, ErrConflict) {
		t.Fatalf("definition drift: %v", err)
	}
	for _, c := range []*Codecs{makeCodecs("value", "2"), makeCodecs("other-value", "1")} {
		if _, err := testRuntime(t, store, c).Resume(context.Background(), g, in, testOptions("run")); !errors.Is(err, ErrConflict) {
			t.Fatalf("codec drift: %v", err)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("drift executed business")
	}
	opts.Version = ""
	if _, err := rt.Run(context.Background(), g, in, opts); err == nil {
		t.Fatal("missing business version accepted")
	}
}

func TestCorruptRecordsFailBeforeEventsOrBusiness(t *testing.T) {
	for _, corruption := range []string{"format", "missing-bytes", "checksum", "scalar-null", "reason", "missing-output", "wrong-codec", "wrong-attempt", "closure", "cycle", "index"} {
		t.Run(corruption, func(t *testing.T) {
			store := NewMemoryStore()
			rt := testRuntime(t, store, NewCodecs())
			g := testGraph(t, "graph")
			first, last := pulse.NewKey[int]("first"), pulse.NewKey[int]("last")
			var body, observations, events atomic.Int32
			testAdd(t, g, pulse.NewNode("first", "", nil, pulse.Provides(first), func(rc *pulse.RunCtx) error { body.Add(1); return pulse.Set(rc, first, 1) }))
			testAdd(t, g, pulse.NewNode("last", "", pulse.Requires(first), pulse.Provides(last), func(rc *pulse.RunCtx) error { body.Add(1); return pulse.Set(rc, last, 2) }))
			if _, err := rt.Run(context.Background(), g, pulse.Inputs{}, testOptions("run")); err != nil {
				t.Fatal(err)
			}
			rewriteRecord(t, store, "run", func(r *RunRecord) {
				s := r.Scopes[r.RootScope]
				n := s.Nodes["last"]
				switch corruption {
				case "format":
					r.Format++
				case "missing-bytes":
					n.Outputs[0].Data = nil
				case "checksum":
					n.Outputs[0].Data = []byte("9")
				case "scalar-null":
					n.Outputs[0].Data = []byte("null")
					n.Outputs[0].Digest = digest(n.Outputs[0].Data)
				case "reason":
					n.Reason = "failed"
				case "missing-output":
					n.Outputs = nil
				case "wrong-codec":
					n.Outputs[0].TypeID = "different"
				case "wrong-attempt":
					n.Attempt = s.Nodes["first"].Attempt
				case "closure":
					firstAttempt := s.Nodes["first"].Attempt
					delete(s.Nodes, "first")
					a := r.Attempts[firstAttempt]
					a.State = AttemptActive
					r.Attempts[firstAttempt] = a
				case "cycle":
					a := r.Attempts[n.Attempt]
					a.Parent = n.Attempt
					r.Attempts[n.Attempt] = a
				case "index":
					a := r.Attempts[n.Attempt]
					a.Index = 2
					r.Attempts[n.Attempt] = a
				}
				s.Nodes["last"] = n
				r.Scopes[r.RootScope] = s
			})
			opts := testOptions("run")
			opts.OnEvent = func(Event) error { events.Add(1); return nil }
			opts.ExecOptions = []pulse.ExecOption{pulse.WithExecObserver(pulse.ObserverFunc{GraphStarted: func(pulse.Site) { observations.Add(1) }})}
			if _, err := rt.Resume(context.Background(), g, pulse.Inputs{}, opts); err == nil {
				t.Fatal("corruption accepted")
			}
			if body.Load() != 2 || observations.Load() != 0 || events.Load() != 0 {
				t.Fatalf("invalid restore side effects: body=%d observations=%d events=%d", body.Load(), observations.Load(), events.Load())
			}
		})
	}
}

func childChain(t *testing.T, calls *[3]atomic.Int32) (*pulse.Graph, pulse.Key[int]) {
	t.Helper()
	child := testGraph(t, "child", pulse.WithoutAspects())
	one, two, three := pulse.NewKey[int]("one"), pulse.NewKey[int]("two"), pulse.NewKey[int]("three")
	testAdd(t, child, pulse.NewNode("C1", "", nil, pulse.Provides(one), func(rc *pulse.RunCtx) error { calls[0].Add(1); return pulse.Set(rc, one, 1) }))
	testAdd(t, child, pulse.NewNode("C2", "", pulse.Requires(one), pulse.Provides(two), func(rc *pulse.RunCtx) error { calls[1].Add(1); return pulse.Set(rc, two, 2) }))
	testAdd(t, child, pulse.NewNode("C3", "", pulse.Requires(two), pulse.Provides(three), func(rc *pulse.RunCtx) error { calls[2].Add(1); return pulse.Set(rc, three, 3) }))
	if err := child.DeclareOutput(pulse.NewPort(three)); err != nil {
		t.Fatal(err)
	}
	return child, three
}

func TestChildScopesRecoverPartialAndParentUnconfirmed(t *testing.T) {
	for _, interrupt := range []string{"C2", "parent"} {
		t.Run(interrupt, func(t *testing.T) {
			var calls [3]atomic.Int32
			child, output := childChain(t, &calls)
			parent := testGraph(t, "parent")
			bridged := pulse.NewKey[int]("bridged")
			if err := pulse.Sub(parent, "mount", "", []pulse.SubBind{pulse.Out(output, bridged)}, child); err != nil {
				t.Fatal(err)
			}
			if err := parent.DeclareOutput(pulse.NewPort(bridged)); err != nil {
				t.Fatal(err)
			}
			store := &faultStore{Store: NewMemoryStore(), after: interrupt == "C2", failure: errors.New("stop at checkpoint"), match: func(r RunRecord) bool {
				if interrupt == "parent" {
					return recordHasNode(r, "parent", "mount")
				}
				return recordHasNode(r, "child", "C2")
			}}
			rt := testRuntime(t, store, NewCodecs())
			if _, err := rt.Run(context.Background(), parent, pulse.Inputs{}, testOptions("run")); err == nil {
				t.Fatal("fault not observed")
			}
			before := readRecord(t, store, "run")
			var childScope string
			for id, s := range before.Scopes {
				if s.Graph == "child" {
					childScope = id
				}
			}
			var events atomic.Int32
			opts := testOptions("run")
			opts.OnEvent = func(e Event) error {
				if e.Graph == "child" {
					events.Add(1)
				}
				return nil
			}
			e, err := rt.Resume(context.Background(), parent, pulse.Inputs{}, opts)
			if err != nil {
				t.Fatal(err)
			}
			for i := range calls {
				if calls[i].Load() != 1 {
					t.Fatalf("child C%d reran: %d", i+1, calls[i].Load())
				}
			}
			wantEvents := int32(2)
			if interrupt == "parent" {
				wantEvents = 3
			}
			if events.Load() != wantEvents {
				t.Fatalf("restored child events: %d", events.Load())
			}
			value, state, err := pulse.ReadOutput(e, bridged)
			if value != 3 || state != pulse.SlotStateReady || err != nil {
				t.Fatalf("bridge: %v %v %v", value, state, err)
			}
			after := readRecord(t, store, "run")
			if _, ok := after.Scopes[childScope]; !ok || len(after.Scopes) != 2 {
				t.Fatal("active child scope was not recognized")
			}
		})
	}
}

func TestParentRetryFailedScopeCannotReviveChildRecords(t *testing.T) {
	var calls [3]atomic.Int32
	child, output := childChain(t, &calls)
	parent := testGraph(t, "parent")
	bridged := pulse.NewKey[int]("bridged")
	var wrappers atomic.Int32
	outerFailure := func(rc *pulse.RunCtx, next func(*pulse.RunCtx) error) error {
		if err := next(rc); err != nil {
			return err
		}
		if wrappers.Add(1) == 1 {
			return errors.New("outer failed after child success")
		}
		return nil
	}
	if err := pulse.Sub(parent, "mount", "", []pulse.SubBind{pulse.Out(output, bridged)}, child, pulse.Retry(2, 0), outerFailure); err != nil {
		t.Fatal(err)
	}
	store := NewMemoryStore()
	rt := testRuntime(t, store, NewCodecs())
	if _, err := rt.Run(context.Background(), parent, pulse.Inputs{}, testOptions("run")); err != nil {
		t.Fatal(err)
	}
	for i := range calls {
		if calls[i].Load() != 2 {
			t.Fatalf("failed scope child C%d was reused", i+1)
		}
	}
	r := readRecord(t, store, "run")
	if len(r.Scopes) != 3 {
		t.Fatalf("child scopes merged: %d", len(r.Scopes))
	}
	var failedScope bool
	for id, scope := range r.Scopes {
		if scope.Graph == "child" && r.Attempts[id].State == AttemptFailed {
			failedScope = true
		}
	}
	if !failedScope {
		t.Fatal("old failed child scope disappeared or remained active")
	}
	if _, err := rt.Resume(context.Background(), parent, pulse.Inputs{}, testOptions("run")); err != nil {
		t.Fatal(err)
	}
	for i := range calls {
		if calls[i].Load() != 2 {
			t.Fatal("confirmed parent reran child")
		}
	}
}

func TestResumeFailedRootStopsBeforeRestoreOrStart(t *testing.T) {
	store := NewMemoryStore()
	g := testGraph(t, "failed-root")
	output := pulse.NewKey[int]("out")
	if err := g.DeclareOutput(pulse.NewPort(output)); err != nil {
		t.Fatal(err)
	}
	var body, events, starts atomic.Int32
	testAdd(t, g, pulse.NewNode("work", "", nil, pulse.Provides(output), func(*pulse.RunCtx) error {
		body.Add(1)
		return errors.New("final business failure")
	}))
	rt, opts := testRuntime(t, store, NewCodecs()), testOptions("run")
	if _, err := rt.Run(context.Background(), g, pulse.Inputs{}, opts); err == nil {
		t.Fatal("business failure lost")
	}
	before := readRecord(t, store, "run")
	opts.OnEvent = func(Event) error { events.Add(1); return nil }
	opts.BeforeStart = func(*pulse.Execution) error { starts.Add(1); return nil }
	opts.ExecOptions = []pulse.ExecOption{pulse.WithExecObserver(pulse.ObserverFunc{GraphStarted: func(pulse.Site) { starts.Add(1) }})}
	for i := 0; i < 2; i++ {
		e, err := rt.Resume(context.Background(), g, pulse.Inputs{}, opts)
		var signal *pulse.CheckpointScopeFailure
		var checkpoint *pulse.CheckpointError
		if !errors.As(err, &signal) || signal.Scope != before.RootScope || errors.As(err, &checkpoint) ||
			e == nil || e.Snapshot().Graph.Status != pulse.GraphStatusPending || events.Load() != 0 || starts.Load() != 0 || body.Load() != 1 {
			t.Fatalf("failed root invented Start/restore/retry: err=%v execution=%v", err, e != nil)
		}
		if _, state, err := pulse.ReadOutput(e, output); err == nil || state != "" {
			t.Fatal("unstarted execution exposed output")
		}
		if !reflect.DeepEqual(before, readRecord(t, store, "run")) {
			t.Fatal("exhausted Resume changed record")
		}
	}
}

func TestLegacyFailedStreamValidationPrecedesRepair(t *testing.T) {
	for _, corruption := range []string{"none", "format-one", "association", "sibling-canceled"} {
		t.Run(corruption, func(t *testing.T) {
			p := newStreamProtocol(t)
			key := streamKey(streamID(p.input.Stream.ID))
			stream := p.s.record.Streams[key]
			stream.Stream.Terminal = "failed"
			p.s.record.Streams[key] = stream
			if corruption == "format-one" {
				p.s.record.Format = 1
			}
			if corruption == "association" {
				stream.Stream.Attempt = p.processor.Scope
				p.s.record.Streams[key] = stream
			}
			if corruption == "sibling-canceled" {
				sibling := stream
				sibling.Stream.ID.Group, sibling.Stream.Terminal = "other", "canceled"
				p.s.record.Streams[streamKey(sibling.Stream.ID)] = sibling
			}
			// Test the Runtime entry, not a helper capable of repairing unvalidated data.
			stored := cloneRecord(p.s.record)
			store := NewMemoryStore()
			store.records = map[string]RunRecord{"run": stored}
			g := testGraph(t, "streams")
			input, output, value := pulse.NewKey[pulse.Stream[int]]("input"), pulse.NewKey[pulse.Stream[int]]("output"), pulse.NewKey[int]("value")
			var calls atomic.Int32
			body := func(*pulse.RunCtx) error { calls.Add(1); return nil }
			testAdd(t, g, pulse.NewNode("source", "", nil, pulse.Provides(input), body))
			testAdd(t, g, pulse.NewNode("processor", "", pulse.Requires(input), pulse.Deps(pulse.Provides(output), pulse.Provides(value)), body))
			testAdd(t, g, pulse.NewNode("sink", "", pulse.Requires(output), nil, body))
			var events atomic.Int32
			opts := RunOptions{RunID: "run", Version: "1", OnEvent: func(Event) error { events.Add(1); return nil }, BeforeStart: func(*pulse.Execution) error { calls.Add(1); return nil }}
			_, err := testRuntime(t, store, NewCodecs()).Resume(context.Background(), g, pulse.Inputs{}, opts)
			var checkpoint *pulse.CheckpointError
			if corruption != "none" {
				if !errors.As(err, &checkpoint) || checkpoint.Operation != "validate restore" || !reflect.DeepEqual(stored, readRecord(t, store, "run")) {
					t.Fatalf("damaged history was guessed/repaired: %v", err)
				}
			} else {
				var signal *pulse.CheckpointScopeFailure
				if !errors.As(err, &signal) || errors.As(err, &checkpoint) {
					t.Fatalf("legacy failure not restored: %v", err)
				}
			}
			if calls.Load() != 0 || events.Load() != 0 {
				t.Fatal("failed scope emitted events or entered business")
			}
		})
	}
}

func TestWholeChainOuterFailureDoesNotCommit(t *testing.T) {
	store := NewMemoryStore()
	g := testGraph(t, "graph")
	key := pulse.NewKey[int]("output")
	testAdd(t, g, pulse.NewNode("work", "", nil, pulse.Provides(key), func(rc *pulse.RunCtx) error { return pulse.Set(rc, key, 1) },
		func(rc *pulse.RunCtx, next func(*pulse.RunCtx) error) error {
			if err := next(rc); err != nil {
				return err
			}
			return errors.New("outside failed")
		}))
	if _, err := testRuntime(t, store, NewCodecs()).Run(context.Background(), g, pulse.Inputs{}, testOptions("run")); err == nil {
		t.Fatal("outer failure lost")
	}
	r := readRecord(t, store, "run")
	if len(r.Scopes[r.RootScope].Nodes) != 0 {
		t.Fatal("outer failure committed a node")
	}
}

func TestRecoveryEventPanicIsRedactedAndStops(t *testing.T) {
	store := NewMemoryStore()
	rt := testRuntime(t, store, NewCodecs())
	g := testGraph(t, "graph")
	var calls atomic.Int32
	testAdd(t, g, pulse.NewNode("work", "", nil, nil, func(*pulse.RunCtx) error { calls.Add(1); return nil }))
	if _, err := rt.Run(context.Background(), g, pulse.Inputs{}, testOptions("run")); err != nil {
		t.Fatal(err)
	}
	opts := testOptions("run")
	opts.OnEvent = func(Event) error { panic("secret") }
	_, err := rt.Resume(context.Background(), g, pulse.Inputs{}, opts)
	var checkpoint *pulse.CheckpointError
	if !errors.As(err, &checkpoint) || checkpoint.Err.Error() != "durable: recovery event callback panicked" {
		t.Fatalf("callback panic: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatal("callback failure ran business")
	}
	if _, err := rt.Resume(context.Background(), g, pulse.Inputs{}, testOptions("run")); err != nil {
		t.Fatal(err)
	}
}

func TestCodecDecodeWrongTypeFailsClosedAtCommit(t *testing.T) {
	codecs := &Codecs{}
	if err := codecs.Register(codecFunc{typeOf: reflect.TypeFor[int](), id: "broken", ver: "1",
		encodeFn: func(any) ([]byte, error) { return []byte("1"), nil }, decodeFn: func([]byte) (any, error) { return nil, nil }}); err != nil {
		t.Fatal(err)
	}
	store := NewMemoryStore()
	g := testGraph(t, "graph")
	key := pulse.NewKey[int]("out")
	testAdd(t, g, pulse.NewNode("work", "", nil, pulse.Provides(key), func(rc *pulse.RunCtx) error { return pulse.Set(rc, key, 1) }))
	if _, err := testRuntime(t, store, codecs).Run(context.Background(), g, pulse.Inputs{}, testOptions("run")); err == nil {
		t.Fatal("nil scalar decode accepted")
	}
	if recordHasNode(readRecord(t, store, "run"), "graph", "work") {
		t.Fatal("invalid codec committed business output")
	}
}

type loadErrorStore struct {
	Store
	err       error
	panicLoad bool
}
type loadErrorSession struct {
	Session
	owner *loadErrorStore
}

func (s loadErrorStore) Acquire(ctx context.Context, id string) (Session, error) {
	session, err := s.Store.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	return loadErrorSession{Session: session, owner: &s}, nil
}
func (s loadErrorSession) Load(context.Context) (RunRecord, error) {
	if s.owner.panicLoad {
		panic("private record bytes")
	}
	return RunRecord{}, s.owner.err
}

func TestLoadErrorsAreNotNotFoundAndReleaseSession(t *testing.T) {
	for _, panicLoad := range []bool{false, true} {
		t.Run(fmt.Sprint(panicLoad), func(t *testing.T) {
			base := NewMemoryStore()
			store := loadErrorStore{Store: base, err: errors.New("private record bytes"), panicLoad: panicLoad}
			g := testGraph(t, "graph")
			var calls atomic.Int32
			testAdd(t, g, pulse.NewNode("business", "", nil, nil, func(*pulse.RunCtx) error { calls.Add(1); return nil }))
			if _, err := testRuntime(t, store, NewCodecs()).Run(context.Background(), g, pulse.Inputs{}, testOptions("run")); err == nil {
				t.Fatal("load error ignored")
			}
			if calls.Load() != 0 {
				t.Fatal("load failure ran business")
			}
			session, err := base.Acquire(context.Background(), "run")
			if err != nil {
				t.Fatal("session leaked", err)
			}
			defer session.Close()
			if _, err := session.Load(context.Background()); !errors.Is(err, ErrNotFound) {
				t.Fatal("load failure initialized a record")
			}
		})
	}
}

// 固定脱敏样例由 Durable b4805d5 + Pulse 1f67045d6c42 生成。
// 不由当前编码器动态制造“旧记录”；description 也属于旧定义指纹。
const legacyFormatOne = `{
  "format": 1,
  "run_id": "legacy-format-one",
  "revision": 3,
  "version": "fixture/1",
  "definition": "353c64e7cb227c50143a57036bf485f94d710d9512f9ca026791aeda82eb31dc",
  "inputs": "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945",
  "root_scope": "c2ca14f87b4791abd5df80676ca63060",
  "scopes": {
    "c2ca14f87b4791abd5df80676ca63060": {
      "graph": "legacy-format-one",
      "path": "",
      "inputs": "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945",
      "nodes": {
        "source": {
          "node_id": "source",
          "attempt": "599f9e4683c2b75a4e9a864bd36af9c3",
          "reason": "completed",
          "outputs": [{"name":"answer","type_id":"go/int","version":"1","state":"ready","digest":"73475cb40a568e8da8a045ced110137e159f890ac4da883b6b17dc651b3a8049","data":"NDI="}]
        }
      }
    }
  },
  "attempts": {
    "599f9e4683c2b75a4e9a864bd36af9c3": {"parent":"c2ca14f87b4791abd5df80676ca63060","node_id":"source","boundary":"","index":1,"limit":1,"state":"confirmed"}
  }
}`

func TestResumeFixedFormatOneRecord(t *testing.T) {
	var record RunRecord
	if err := json.Unmarshal([]byte(legacyFormatOne), &record); err != nil {
		t.Fatal(err)
	}
	store := &MemoryStore{records: map[string]RunRecord{record.RunID: record}}
	g, err := pulse.New("legacy-format-one", "旧格式完整节点恢复")
	if err != nil {
		t.Fatal(err)
	}
	answer := pulse.NewKey[int]("answer")
	if err := g.DeclareOutput(pulse.NewPort(answer)); err != nil {
		t.Fatal(err)
	}
	testAdd(t, g, pulse.NewNode("source", "生成原始格式fixture", nil, pulse.Provides(answer), func(*pulse.RunCtx) error { t.Error("confirmed legacy source reran"); return nil }))
	e, err := testRuntime(t, store, NewCodecs()).Resume(context.Background(), g, pulse.Inputs{}, RunOptions{RunID: record.RunID, Version: record.Version})
	if err != nil {
		t.Fatal(err)
	}
	if value, state, err := pulse.ReadOutput(e, answer); err != nil || value != 42 || state != pulse.SlotStateReady {
		t.Fatalf("legacy value=%d state=%s err=%v", value, state, err)
	}
}

func TestFormatOneRejectsIncrementalFields(t *testing.T) {
	p := newStreamProtocol(t)
	p.s.record.Format = 1
	if err := p.s.validateLoaded(); err == nil {
		t.Fatal("format 1 accepted incremental stream fields")
	}
}
