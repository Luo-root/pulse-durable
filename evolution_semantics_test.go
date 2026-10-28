package durable

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
)

// Retain the actual Save arguments, not cloneRecord copies: cloning here would
// hide runtime aliasing. JSON is only an independent historical oracle.
type evolutionSave struct {
	record RunRecord
	bytes  []byte
}

type evolutionStore struct {
	Store
	mu      sync.Mutex
	history []evolutionSave
	loads   int
	after   func(RunRecord) error
}

type evolutionSession struct {
	Session
	owner *evolutionStore
}

func (s *evolutionStore) Acquire(ctx context.Context, id string) (Session, error) {
	session, err := s.Store.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	return &evolutionSession{Session: session, owner: s}, nil
}

func (s *evolutionSession) Load(ctx context.Context) (RunRecord, error) {
	s.owner.mu.Lock()
	s.owner.loads++
	s.owner.mu.Unlock()
	return s.Session.Load(ctx)
}

func (s *evolutionSession) Initialize(ctx context.Context, r RunRecord) error {
	if err := s.Session.Initialize(ctx, r); err != nil {
		return err
	}
	return s.observe(r)
}
func (s *evolutionSession) Apply(ctx context.Context, b ChangeBatch) (CommitAck, error) {
	ack, err := s.Session.Apply(ctx, b)
	if err != nil {
		return CommitAck{}, err
	}
	r, err := s.Session.Load(ctx)
	if err != nil {
		return CommitAck{}, err
	}
	return ack, s.observe(r)
}
func (s *evolutionSession) observe(r RunRecord) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	s.owner.mu.Lock()
	defer s.owner.mu.Unlock()
	s.owner.history = append(s.owner.history, evolutionSave{record: r, bytes: data})
	if s.owner.after != nil {
		return s.owner.after(r)
	}
	return nil
}

func (s *evolutionStore) assertHistory(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, saved := range s.history {
		data, err := json.Marshal(saved.record)
		if err != nil || string(data) != string(saved.bytes) {
			t.Fatalf("Save argument %d (revision %d) changed after return: %v", i, saved.record.Revision, err)
		}
		if i > 0 && saved.record.Revision != s.history[i-1].record.Revision+1 {
			t.Fatalf("non-contiguous Save history at %d", i)
		}
	}
}

func TestEvolutionSaveHistorySurvivesCallerMutationAndResume(t *testing.T) {
	store := &evolutionStore{Store: NewMemoryStore()}
	codecs := NewCodecs()
	if err := RegisterJSON[[]int](codecs, "slice", "1"); err != nil {
		t.Fatal(err)
	}
	g := testGraph(t, "history")
	first, final := pulse.NewKey[[]int]("first"), pulse.NewKey[[]int]("final")
	caller := []int{11, 12}
	var calls [2]atomic.Int32
	testAdd(t, g, pulse.NewNode("first", "", nil, pulse.Provides(first), func(rc *pulse.RunCtx) error {
		calls[0].Add(1)
		return pulse.Set(rc, first, caller)
	}))
	testAdd(t, g, pulse.NewNode("final", "", pulse.Requires(first), pulse.Provides(final), func(rc *pulse.RunCtx) error {
		calls[1].Add(1)
		v, err := pulse.Get(rc, first)
		if err != nil {
			return err
		}
		return pulse.Set(rc, final, append([]int(nil), v...))
	}))
	if err := g.DeclareOutput(pulse.NewPort(final)); err != nil {
		t.Fatal(err)
	}
	if _, err := testRuntime(t, store, codecs).Run(t.Context(), g, pulse.Inputs{}, testOptions("history")); err != nil {
		t.Fatal(err)
	}
	caller[0] = 99
	loaded := readRecord(t, store, "history")
	loaded.Scopes[loaded.RootScope].Nodes["first"].Outputs[0].Data[0] = 'X'
	delete(loaded.Attempts, loaded.Scopes[loaded.RootScope].Nodes["first"].Attempt)
	store.assertHistory(t)
	e, err := testRuntime(t, store, codecs).Resume(t.Context(), g, pulse.Inputs{}, testOptions("history"))
	if err != nil {
		t.Fatal(err)
	}
	v, state, err := pulse.ReadOutput(e, final)
	if err != nil || state != pulse.SlotStateReady || !reflect.DeepEqual(v, []int{11, 12}) || calls[0].Load() != 1 || calls[1].Load() != 1 {
		t.Fatalf("caller mutation contaminated recovery: value=%v state=%v err=%v calls=%d/%d", v, state, err, calls[0].Load(), calls[1].Load())
	}
	r := readRecord(t, store, "history")
	for _, node := range r.Scopes[loaded.RootScope].Nodes {
		if r.Attempts[node.Attempt].State != AttemptConfirmed {
			t.Fatal("node lost its confirmed root")
		}
	}
	store.assertHistory(t)
}

func TestEvolutionMultiRouteMessageHistoryAndStopBoundary(t *testing.T) {
	p := newStreamProtocol(t)
	ctx := t.Context()
	store := &evolutionStore{}
	p.s.session = &evolutionSession{Session: p.s.session, owner: store}
	// Two independently addressed output groups of the processor's output.
	req := p.openRequest(true)
	req.Group = "audit"
	audit, err := p.s.openStream(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	for sequence := uint64(1); sequence <= 2; sequence++ {
		if err := p.append(sequence); err != nil {
			t.Fatal(err)
		}
	}
	a, err := p.s.openMessage(ctx, p.messageRequest(1, 2))
	if err != nil {
		t.Fatal(err)
	}
	commit := pulse.CheckpointMessageCommit{Site: p.site, Scope: p.s.record.RootScope, Attempt: a.Attempt.Scope,
		Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: p.input.Stream.ID, Sequence: 1}},
		State:  []pulse.CheckpointState{{Name: "sum", Type: reflect.TypeFor[int](), Value: 7}},
		Values: []pulse.CheckpointOutput{{Key: p.value, State: pulse.SlotStateReady, Value: 42}},
		Outputs: []pulse.CheckpointStreamAppend{
			{Site: p.site, Attempt: p.processor.Scope, Stream: p.output.Stream.ID, Message: pulse.CheckpointStreamMessage{Sequence: 1, Value: 70}},
			{Site: p.site, Attempt: p.processor.Scope, Stream: audit.Stream.ID, Message: pulse.CheckpointStreamMessage{Sequence: 1, Value: 700}},
		}}
	before := cloneRecord(p.s.record)
	saves := len(store.history)
	commit.Stops = []pulse.CheckpointStreamID{p.input.Stream.ID}
	if err := p.s.commitMessage(ctx, commit); err == nil || len(store.history) != saves || !reflect.DeepEqual(before, p.s.record) {
		t.Fatalf("managed Stop must reject without Save or partial effects: %v", err)
	}
	commit.Stops = nil
	// A bad second route must not publish the otherwise-valid first route.
	commit.Outputs[1].Message.Value = "wrong type"
	if err := p.s.commitMessage(ctx, commit); err == nil || len(store.history) != saves || !reflect.DeepEqual(before, p.s.record) {
		t.Fatalf("invalid second route partially committed: %v", err)
	}
	commit.Outputs[1].Message.Value = 700
	if err := p.s.commitMessage(ctx, commit); err != nil {
		t.Fatal(err)
	}
	if len(store.history) != saves+1 {
		t.Fatal("one message transaction used more than one Save")
	}
	r := store.history[saves].record
	message := r.Messages[a.Attempt.Scope]
	managed := r.Managed[managedKey(r.RootScope, p.processor.Scope, "processor")]
	input := r.Streams[streamKey(streamID(p.input.Stream.ID))]
	if r.Revision != before.Revision+1 || message.State != AttemptConfirmed || message.Parent != managed.Parent || message.Scope != managed.Scope || message.NodeID != managed.NodeID || message.Sequence != 1 || message.Input != streamID(p.input.Stream.ID) || input.AckedThrough != 1 || len(input.Pending) != 1 || input.Pending[0].Sequence != 2 || input.Stream.Stopped {
		t.Fatal("message identity, managed ownership and input ACK did not commit together")
	}
	if len(managed.State) != 1 || string(managed.State[0].Data) != "7" || len(managed.Values) != 1 || string(managed.Values[0].Data) != "42" {
		t.Fatalf("wrong committed State/Values: %+v", managed)
	}
	for i, id := range []pulse.CheckpointStreamID{p.output.Stream.ID, audit.Stream.ID} {
		output := r.Streams[streamKey(streamID(id))]
		want := []string{"70", "700"}[i]
		if output.Stream.Attempt != message.Parent || output.LastSequence != 1 || len(output.Pending) != 1 || string(output.Pending[0].Value.Data) != want {
			t.Fatalf("route %d was not in the same commit: %+v", i, output)
		}
	}
	commit.State[0].Value = 99
	commit.Outputs[0].Message.Value = 99
	// Manual ACK+Stop is legal and must preserve the managed transaction.
	if err := p.s.commitMessage(ctx, pulse.CheckpointMessageCommit{Site: p.site, Scope: r.RootScope,
		Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: p.input.Stream.ID, Sequence: 2}}, Stops: []pulse.CheckpointStreamID{audit.Stream.ID}}); err != nil {
		t.Fatal(err)
	}
	if len(store.history) != saves+2 || p.s.record.Streams[streamKey(streamID(p.input.Stream.ID))].AckedThrough != 2 || !p.s.record.Streams[streamKey(streamID(audit.Stream.ID))].Stream.Stopped || len(p.s.record.Streams[streamKey(streamID(audit.Stream.ID))].Pending) != 0 {
		t.Fatal("manual ACK+Stop did not share one Save")
	}
	if err := p.s.validateLoaded(); err != nil {
		t.Fatal(err)
	}
	store.assertHistory(t)
}

func TestEvolutionNestedSiblingAndRetryBudgetAcrossUncertainSaves(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	store := &evolutionStore{Store: NewMemoryStore()}
	goodSaved := make(chan struct{})
	var once sync.Once
	var interrupted int
	lost := errors.New("committed failure acknowledgement lost")
	store.after = func(r RunRecord) error {
		if recordHasNode(r, "leaf", "good") {
			once.Do(func() { close(goodSaved) })
		}
		failed := 0
		for _, a := range r.Attempts {
			if a.NodeID == "bad" && a.Boundary != "" && a.State == AttemptFailed {
				failed++
			}
		}
		if failed > interrupted {
			interrupted = failed
			return lost
		}
		return nil
	}
	leaf := testGraph(t, "leaf")
	good := pulse.NewKey[int]("good")
	var goodCalls, badCalls atomic.Int32
	testAdd(t, leaf, pulse.NewNode("good", "", nil, pulse.Provides(good), func(rc *pulse.RunCtx) error {
		goodCalls.Add(1)
		return pulse.Set(rc, good, 17)
	}))
	testAdd(t, leaf, pulse.NewNode("bad", "", nil, nil, func(rc *pulse.RunCtx) error {
		if err := rc.WaitStream(func(ctx context.Context) error {
			select {
			case <-goodSaved:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}); err != nil {
			return err
		}
		badCalls.Add(1)
		return errors.New("business failure")
	}, pulse.Retry(3, 0)))
	if err := leaf.DeclareOutput(pulse.NewPort(good)); err != nil {
		t.Fatal(err)
	}
	middle, root := testGraph(t, "middle"), testGraph(t, "root")
	middleOut, rootOut := pulse.NewKey[int]("middle-out"), pulse.NewKey[int]("root-out")
	if err := pulse.Sub(middle, "inner", "", []pulse.SubBind{pulse.Out(good, middleOut)}, leaf); err != nil {
		t.Fatal(err)
	}
	if err := middle.DeclareOutput(pulse.NewPort(middleOut)); err != nil {
		t.Fatal(err)
	}
	if err := pulse.Sub(root, "outer", "", []pulse.SubBind{pulse.Out(middleOut, rootOut)}, middle); err != nil {
		t.Fatal(err)
	}
	var goodScope, goodAttempt string
	for execution := 1; execution <= 5; execution++ {
		store.mu.Lock()
		loadsBefore := store.loads
		store.mu.Unlock()
		runtime := testRuntime(t, store, NewCodecs())
		var err error
		if execution == 1 {
			_, err = runtime.Run(ctx, root, pulse.Inputs{}, testOptions("nested"))
		} else {
			_, err = runtime.Resume(ctx, root, pulse.Inputs{}, testOptions("nested"))
		}
		if err == nil {
			t.Fatal("failed graph unexpectedly succeeded")
		}
		store.mu.Lock()
		loadsAfter := store.loads
		store.mu.Unlock()
		if loadsAfter != loadsBefore+1 {
			t.Fatalf("execution %d did not Load authority exactly once: before=%d after=%d", execution, loadsBefore, loadsAfter)
		}
		wantCalls := execution
		if wantCalls > 3 {
			wantCalls = 3
		}
		if goodCalls.Load() != 1 || badCalls.Load() != int32(wantCalls) {
			t.Fatalf("execution %d replayed sibling or refilled Retry: good=%d bad=%d", execution, goodCalls.Load(), badCalls.Load())
		}
		r := readRecord(t, store, "nested")
		failed := make(map[int]bool)
		for id, scope := range r.Scopes {
			if scope.Graph == "leaf" {
				node := scope.Nodes["good"]
				if goodScope == "" {
					goodScope, goodAttempt = id, node.Attempt
				}
				if id != goodScope || node.Attempt != goodAttempt || string(node.Outputs[0].Data) != "17" || r.Attempts[node.Attempt].State != AttemptConfirmed {
					t.Fatal("nested sibling's scope, root or business output changed")
				}
			}
		}
		for _, a := range r.Attempts {
			if a.NodeID == "bad" && a.Boundary != "" {
				if a.Parent == "" || r.Attempts[a.Parent].NodeID != "bad" || a.Limit != 3 || a.State != AttemptFailed || failed[a.Index] {
					t.Fatalf("invalid persisted Retry relationship: %+v", a)
				}
				failed[a.Index] = true
			}
		}
		if len(failed) != wantCalls || len(r.Scopes) != 3 || recordHasNode(r, "middle", "inner") || recordHasNode(r, "root", "outer") {
			t.Fatalf("failed nested mounts confirmed or retry history lost: failed=%v scopes=%d", failed, len(r.Scopes))
		}
		if execution <= 3 && !errors.Is(err, lost) {
			t.Fatalf("committed Save failure not propagated: %v", err)
		}
		store.assertHistory(t)
	}
	if interrupted != 3 || store.loads < 5 {
		t.Fatalf("did not exercise three uncertain commits with authoritative reloads: interruptions=%d loads=%d", interrupted, store.loads)
	}
}
