package durable

import (
	"context"
	"sync/atomic"
	"testing"

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
