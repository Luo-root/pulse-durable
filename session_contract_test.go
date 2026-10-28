package durable

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/Luo-root/pulse"
)

func memorySeedDigest(r RunRecord) [32]byte { data, _ := json.Marshal(r); return sha256.Sum256(data) }

func contractSeed(id string) RunRecord {
	return RunRecord{Format: RecordFormat, RunID: id, Revision: 1, Version: "1", Definition: digest([]byte("definition")), Inputs: digest([]byte("inputs")), RootScope: "root", Scopes: map[string]ScopeRecord{"root": {Graph: "graph", Inputs: digest([]byte("inputs")), Nodes: map[string]NodeRecord{}}}}
}

func TestUnifiedMemoryInitializeAndApply(t *testing.T) {
	ctx := t.Context()
	store := NewMemoryStore()
	session, err := store.Acquire(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	seed := contractSeed("run")
	for _, edit := range []func(*RunRecord){
		func(r *RunRecord) { r.Revision = 2 }, func(r *RunRecord) { r.RunID = "other" },
		func(r *RunRecord) { r.Format = 1 }, func(r *RunRecord) { r.Version = "" },
		func(r *RunRecord) { r.Attempts = map[string]AttemptRecord{"a": {}} },
		func(r *RunRecord) { s := r.Scopes["root"]; s.Nodes["n"] = NodeRecord{}; r.Scopes["root"] = s },
	} {
		bad := cloneRecord(seed)
		edit(&bad)
		if err := session.Initialize(ctx, bad); !errors.Is(err, ErrConflict) {
			t.Fatalf("invalid seed accepted: %v", err)
		}
		if _, err := session.Load(ctx); !errors.Is(err, ErrNotFound) {
			t.Fatalf("invalid seed published: %v", err)
		}
	}
	if err := session.Initialize(ctx, seed); err != nil {
		t.Fatal(err)
	}
	if err := session.Initialize(ctx, seed); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate: %v", err)
	}
	seed.Scopes["root"].Nodes["external"] = NodeRecord{}
	before, err := session.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Scopes["root"].Nodes) != 0 {
		t.Fatal("Initialize retained caller map")
	}
	tx := newRecordTransaction(before.Revision)
	if err := tx.putNode("root", "n", nil, NodeRecord{NodeID: "n", Outputs: []ValueRecord{{Data: []byte("payload")}}}); err != nil {
		t.Fatal(err)
	}
	batch := ChangeBatch{changeHeader(before), tx.copy()}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ack, err := session.Apply(ctx, batch)
			if err == nil && ack.Header != batch.Header() {
				err = errors.New("wrong ack")
			}
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	successes, conflicts := 0, 0
	for err := range results {
		if err == nil {
			successes++
		} else if errors.Is(err, ErrConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("CAS: success=%d conflict=%d", successes, conflicts)
	}
	loaded, _ := session.Load(ctx)
	loaded.Scopes["root"].Nodes["n"].Outputs[0].Data[0] = 'X'
	again, _ := session.Load(ctx)
	if string(again.Scopes["root"].Nodes["n"].Outputs[0].Data) != "payload" {
		t.Fatal("Load alias")
	}
	chain := store.chains["run"]
	digest, err := ChangeBatchDigest(batch)
	if err != nil {
		t.Fatal(err)
	}
	// Memory uses the same physical chain transition as persistent backends.
	expected, err := NextChain(SeedChain(memorySeedDigest(contractSeed("run"))), digest, 2)
	if err != nil || chain != expected {
		t.Fatalf("chain mismatch: %v", err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Apply(ctx, batch); err == nil {
		t.Fatal("closed Apply")
	}
	if err := session.Initialize(ctx, contractSeed("run")); err == nil {
		t.Fatal("closed Initialize")
	}
	reopened, err := store.Acquire(ctx, "run")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := reopened.Apply(ctx, batch); !errors.Is(err, ErrConflict) {
		t.Fatal("unloaded session accepted Apply")
	}
	restored, err := reopened.Load(ctx)
	if err != nil || !reflect.DeepEqual(restored, again) {
		t.Fatalf("reopen: %v", err)
	}
}

type contractFaultSession struct {
	Session
	initializeErr error
	applyErr      error
	panicApply    bool
	badAck        bool
	calls         int
}

func (s *contractFaultSession) Initialize(ctx context.Context, r RunRecord) error {
	if err := s.Session.Initialize(ctx, r); err != nil {
		return err
	}
	return s.initializeErr
}
func (s *contractFaultSession) Apply(ctx context.Context, b ChangeBatch) (CommitAck, error) {
	s.calls++
	ack, err := s.Session.Apply(ctx, b)
	if err != nil {
		return ack, err
	}
	if s.panicApply {
		panic(nil)
	}
	if s.badAck {
		ack.Header.Revision++
	}
	return ack, s.applyErr
}

func TestUnifiedUnknownAndAckSeal(t *testing.T) {
	for _, mode := range []string{"initialize unknown", "apply unknown", "apply panic nil", "ack"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			store := NewMemoryStore()
			raw, _ := store.Acquire(ctx, "run")
			defer raw.Close()
			unknown := errors.New("unknown commit")
			wrapper := &contractFaultSession{Session: raw}
			seed := contractSeed("run")
			seed.Revision = 0
			state := &runState{session: wrapper, record: seed}
			if mode == "initialize unknown" {
				wrapper.initializeErr = unknown
			}
			err := state.initialize(ctx, seed)
			if mode == "initialize unknown" {
				if !errors.Is(err, unknown) || state.stopped != err || state.record.Revision != 0 {
					t.Fatal("initialize unknown published")
				}
				authority, loadErr := raw.Load(ctx)
				if loadErr != nil || authority.Revision != 1 {
					t.Fatal("missing committed authority")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			before := cloneRecord(state.record)
			switch mode {
			case "apply unknown":
				wrapper.applyErr = unknown
			case "apply panic nil":
				wrapper.panicApply = true
			case "ack":
				wrapper.badAck = true
			}
			tx := newRecordTransaction(1)
			_ = tx.putAttempt("a", nil, AttemptRecord{Parent: "root", NodeID: "work", Index: 1, Limit: 1, State: AttemptActive})
			err = state.commitRecordTransaction(ctx, tx)
			if err == nil || state.stopped != err || !reflect.DeepEqual(before, state.record) {
				t.Fatal("unknown/ack published")
			}
			if again := state.commitRecordTransaction(ctx, tx); again != err || wrapper.calls != 1 {
				t.Fatal("sealed session retried")
			}
			authority, loadErr := raw.Load(ctx)
			if loadErr != nil || authority.Revision != 2 || authority.Attempts["a"].NodeID != "work" {
				t.Fatal("committed Apply not authoritative")
			}
		})
	}
}

func TestCloneRecordPreservesExactShape(t *testing.T) {
	for _, empty := range []bool{false, true} {
		var values []ValueRecord
		var data []byte
		var pending []StreamMessageRecord
		if empty {
			values = []ValueRecord{}
			data = []byte{}
			pending = []StreamMessageRecord{}
		}
		r := contractSeed("clone")
		r.Scopes["root"] = ScopeRecord{Nodes: map[string]NodeRecord{"n": {Outputs: values}, "data": {Outputs: []ValueRecord{{Data: data, Stream: &StreamDescriptorRecord{SourceID: "original"}}}}}}
		r.Streams = map[string]StreamRecord{"empty": {Pending: pending}, "cursor": {Cursor: &ValueRecord{Data: data}}, "pending": {Pending: []StreamMessageRecord{{Value: ValueRecord{Data: data}}}}}
		r.Managed = map[string]ManagedRecord{"m": {State: values, Values: []ValueRecord{{Data: data}}}}
		out := CloneRecord(r)
		if !reflect.DeepEqual(out, r) {
			t.Fatalf("clone normalized empty=%t", empty)
		}
		out.Scopes["root"].Nodes["data"].Outputs[0].Stream.SourceID = "changed"
		if r.Scopes["root"].Nodes["data"].Outputs[0].Stream.SourceID != "original" {
			t.Fatal("descriptor alias")
		}
		out.Streams["cursor"].Cursor.Name = "changed"
		if r.Streams["cursor"].Cursor.Name != "" {
			t.Fatal("cursor alias")
		}
		out.Managed["m"].Values[0].Name = "changed"
		if r.Managed["m"].Values[0].Name != "" {
			t.Fatal("managed alias")
		}
	}
}

func TestUnifiedRuntimeMemoryManaged500(t *testing.T) {
	store := NewMemoryStore()
	rt, err := New(store, NewCodecs())
	if err != nil {
		t.Fatal(err)
	}
	g, err := pulse.New("unified", "")
	if err != nil {
		t.Fatal(err)
	}
	key := pulse.NewKey[pulse.Stream[int]]("numbers")
	err = pulse.ProduceStream(g, "source", "", key, func(_ *pulse.RunCtx, w *pulse.StreamWriter[int]) error {
		for i := int(w.Position().Sequence) + 1; i <= 500; i++ {
			if err := w.SendAt(i, i); err != nil {
				return err
			}
		}
		return nil
	}, pulse.WithBuffer(1), pulse.WithStreamSource("integers", true))
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	err = pulse.ConsumeStream(g, "sum", "", key, func(rc *pulse.RunCtx, v int) error {
		old, _, err := pulse.GetMessageState[int](rc, "sum")
		if err != nil {
			return err
		}
		total = old + v
		return pulse.SetMessageState(rc, "sum", total)
	}, pulse.WithMessageHandling(pulse.WithMessageRetry(2, 0)))
	if err != nil {
		t.Fatal(err)
	}
	opts := RunOptions{RunID: "500", Version: "1"}
	if _, err := rt.Run(context.Background(), g, pulse.Inputs{}, opts); err != nil {
		t.Fatal(err)
	}
	if total != 125250 {
		t.Fatalf("sum=%d", total)
	}
	if _, err := rt.Resume(context.Background(), g, pulse.Inputs{}, opts); err != nil {
		t.Fatal(err)
	}
	if total != 125250 {
		t.Fatal("confirmed state repeated")
	}
}
