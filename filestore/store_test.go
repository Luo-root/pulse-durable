package filestore

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	durable "github.com/Luo-root/pulse-durable"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, e := New(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func acquireTestSession(t *testing.T, s *Store, id string) durable.Session {
	t.Helper()
	ss, e := s.Acquire(t.Context(), id)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := ss.Close(); e != nil {
			t.Error(e)
		}
	})
	return ss
}
func createTestTemporary(t *testing.T, s *Store, id string) string {
	t.Helper()
	f, e := os.CreateTemp(s.dir, fileBase(id)+"-*.tmp")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = f.WriteString("uncommitted"); e != nil {
		t.Fatal(e)
	}
	if e = f.Close(); e != nil {
		t.Fatal(e)
	}
	return f.Name()
}
func requireBusy(t *testing.T, s *Store, id string) {
	t.Helper()
	ch := make(chan error, 1)
	go func() {
		ss, e := s.Acquire(context.Background(), id)
		if ss != nil {
			ss.Close()
		}
		ch <- e
	}()
	select {
	case e := <-ch:
		if !errors.Is(e, durable.ErrBusy) {
			t.Fatal(e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Acquire blocked")
	}
}

// Complete metadata fixture is for recordio validation, not Initialize import.
func testRecord(id string) durable.RunRecord {
	return durable.RunRecord{Format: durable.RecordFormat, RunID: id, Revision: 1, Version: "业务版本/α", Definition: strings.Repeat("a", 64), Inputs: strings.Repeat("b", 64), RootScope: "scope", Scopes: map[string]durable.ScopeRecord{"scope": {Graph: "图", Inputs: strings.Repeat("b", 64), Nodes: map[string]durable.NodeRecord{"node": {NodeID: "node", Attempt: "attempt", Reason: "opaque", Outputs: []durable.ValueRecord{{Name: "结果", TypeID: "binary", Version: "v1", State: "opaque", Data: []byte{0, 1, 255}}}}}}}, Attempts: map[string]durable.AttemptRecord{"attempt": {Parent: "scope", NodeID: "node", Index: 2, Limit: 5, State: durable.AttemptActive}}}
}
func TestNewAndInvalidArguments(t *testing.T) {
	if _, e := New(""); e == nil {
		t.Fatal("empty accepted")
	}
	p := filepath.Join(t.TempDir(), "file")
	os.WriteFile(p, []byte("existing"), 0600)
	if _, e := New(p); e == nil {
		t.Fatal("file accepted")
	}
	s := newTestStore(t)
	if !filepath.IsAbs(s.dir) {
		t.Fatal("relative")
	}
	for _, id := range []string{"", string([]byte{255})} {
		if _, e := s.Acquire(t.Context(), id); e == nil {
			t.Fatal("invalid id accepted")
		}
	}
	if _, e := s.Acquire(nil, "r"); e == nil {
		t.Fatal("nil ctx accepted")
	}
	if _, e := new(Store).Acquire(t.Context(), "r"); e == nil {
		t.Fatal("zero store accepted")
	}
}
func TestAcquireIndependentStoresAndRuns(t *testing.T) {
	s := newTestStore(t)
	other, e := New(s.dir)
	if e != nil {
		t.Fatal(e)
	}
	a := acquireTestSession(t, s, "a")
	requireBusy(t, s, "a")
	requireBusy(t, other, "a")
	b := acquireTestSession(t, other, "b")
	if e = b.Initialize(t.Context(), incrementalSeed("b")); e != nil {
		t.Fatal(e)
	}
	if _, e = a.Load(t.Context()); !errors.Is(e, durable.ErrNotFound) {
		t.Fatal(e)
	}
	b.Close()
	requireBusy(t, other, "a")
	a.Close()
	acquireTestSession(t, other, "a")
}
func TestAcquireCleansOnlyRunTemporaries(t *testing.T) {
	s := newTestStore(t)
	ss := acquireTestSession(t, s, "run")
	if e := ss.Initialize(t.Context(), incrementalSeed("run")); e != nil {
		t.Fatal(e)
	}
	ss.Close()
	base := fileBase("run")
	lock := filepath.Join(s.dir, base+".lock")
	before, e := os.Stat(lock)
	if e != nil {
		t.Fatal(e)
	}
	acquireTestSession(t, s, "other")
	other := createTestTemporary(t, s, "other")
	removed := []string{createTestTemporary(t, s, "run")}
	for _, n := range []string{"0", "1", "4294967295"} {
		p := filepath.Join(s.dir, base+"-"+n+".tmp")
		os.WriteFile(p, []byte("stale"), 0600)
		removed = append(removed, p)
	}
	preserved := map[string][]byte{}
	for _, p := range []string{filepath.Join(s.dir, base+".head"), other} {
		b, e := os.ReadFile(p)
		if e != nil {
			t.Fatal(e)
		}
		preserved[p] = b
	}
	for _, n := range []string{"-.tmp", "--1.tmp", "-+1.tmp", "-01.tmp", "-4294967296.tmp", "-name.tmp", "-1.tmp.bak", "-75.TMP", "-1.json"} {
		p := filepath.Join(s.dir, base+n)
		b := []byte("caller" + n)
		os.WriteFile(p, b, 0600)
		preserved[p] = b
	}
	directory := filepath.Join(s.dir, base+"-4444.tmp")
	os.Mkdir(directory, 0700)
	nested := filepath.Join(directory, "nested")
	os.WriteFile(nested, []byte("nested"), 0600)
	preserved[nested] = []byte("nested")
	acquireTestSession(t, s, "run")
	for _, p := range removed {
		if _, e := os.Stat(p); !errors.Is(e, os.ErrNotExist) {
			t.Fatal("temporary survived", e)
		}
	}
	for p, b := range preserved {
		got, e := os.ReadFile(p)
		if e != nil || !bytes.Equal(got, b) {
			t.Fatal("caller file changed", p, e)
		}
	}
	after, e := os.Stat(lock)
	if e != nil || !os.SameFile(before, after) {
		t.Fatal("lock replaced", e)
	}
}
func TestApplyCASAndCompleteRecord(t *testing.T) {
	s := newTestStore(t)
	ss := acquireTestSession(t, s, "run")
	seed := incrementalSeed("run")
	b := revisionBatch(t, seed)
	if _, e := ss.Apply(t.Context(), b); !errors.Is(e, durable.ErrNotFound) {
		t.Fatal(e)
	}
	if e := ss.Initialize(t.Context(), seed); e != nil {
		t.Fatal(e)
	}
	ack, e := ss.Apply(t.Context(), b)
	if e != nil || ack.Header != b.Header() {
		t.Fatal(e)
	}
	path := ss.(*session).path
	before, e := os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = ss.Apply(t.Context(), b); !errors.Is(e, durable.ErrConflict) {
		t.Fatal("stale accepted", e)
	}
	after, e := os.ReadFile(path)
	if e != nil || !bytes.Equal(before, after) {
		t.Fatal("conflict changed head", e)
	}
	r, e := ss.Load(t.Context())
	if e != nil || r.Revision != 2 || len(r.Attempts) != 1 {
		t.Fatal(e)
	}
	delete(r.Attempts, "attempt-1")
	again, e := ss.Load(t.Context())
	if e != nil || len(again.Attempts) != 1 {
		t.Fatal("load aliased", e)
	}
	ss.Close()
	r, e = acquireTestSession(t, s, "run").Load(t.Context())
	if e != nil || r.Revision != 2 || len(r.Attempts) != 1 {
		t.Fatal("reopen", e)
	}
}
func TestCorruptRecordIsNotMissingOrOverwritten(t *testing.T) {
	for _, data := range []string{"", "{", "null", "{}", `{"format":77}`, `{} {}`} {
		t.Run(data, func(t *testing.T) {
			s := newTestStore(t)
			ss := acquireTestSession(t, s, "run")
			p := ss.(*session).path
			os.WriteFile(p, []byte(data), 0600)
			if _, e := ss.Load(t.Context()); e == nil || errors.Is(e, durable.ErrNotFound) {
				t.Fatal(e)
			}
			if e := ss.Initialize(t.Context(), incrementalSeed("run")); e == nil {
				t.Fatal("corrupt overwritten")
			}
			after, e := os.ReadFile(p)
			if e != nil || string(after) != data {
				t.Fatal("corrupt modified", e)
			}
		})
	}
}
func TestReadFailureIsNotMissing(t *testing.T) {
	s := newTestStore(t)
	ss := acquireTestSession(t, s, "run")
	os.Mkdir(ss.(*session).path, 0700)
	if _, e := ss.Load(t.Context()); e == nil || errors.Is(e, durable.ErrNotFound) {
		t.Fatal(e)
	}
	if e := ss.Initialize(t.Context(), incrementalSeed("run")); e == nil || errors.Is(e, durable.ErrNotFound) {
		t.Fatal(e)
	}
}
func TestRunIDCannotEscapeDirectory(t *testing.T) {
	parent := t.TempDir()
	s, e := New(filepath.Join(parent, "store"))
	if e != nil {
		t.Fatal(e)
	}
	ids := []string{"..", "../../escaped", `D:\outside\record`, `\\server\share`, "con/aux", "中文/../❄️", "\x00/record", "a\nb", "é", "e\u0301"}
	for _, id := range ids {
		ss := acquireTestSession(t, s, id)
		if e = ss.Initialize(t.Context(), incrementalSeed(id)); e != nil {
			t.Fatal(e)
		}
		r, e := ss.Load(t.Context())
		if e != nil || r.RunID != id {
			t.Fatal(e)
		}
		ss.Close()
	}
	entries, e := os.ReadDir(s.dir)
	if e != nil || len(entries) != len(ids)*3 {
		t.Fatal("unexpected layout", e)
	}
	for _, entry := range entries {
		base := strings.SplitN(entry.Name(), ".", 2)[0]
		if entry.IsDir() || len(base) != 64 {
			t.Fatal("raw id path")
		}
		if _, e = hex.DecodeString(base); e != nil {
			t.Fatal(e)
		}
	}
	outside, e := os.ReadDir(parent)
	if e != nil || len(outside) != 1 || outside[0].Name() != "store" {
		t.Fatal("escaped", e)
	}
}
func TestContextAndClose(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, e := s.Acquire(ctx, "r"); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	ss := acquireTestSession(t, s, "r")
	if _, e := ss.Load(ctx); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if e := ss.Initialize(ctx, incrementalSeed("r")); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if _, e := ss.Load(nil); e == nil {
		t.Fatal("nil accepted")
	}
	if e := ss.Initialize(nil, incrementalSeed("r")); e == nil {
		t.Fatal("nil accepted")
	}
	if _, e := ss.Load(t.Context()); !errors.Is(e, durable.ErrNotFound) {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if e := ss.Close(); e != nil {
				t.Error(e)
			}
		})
	}
	wg.Wait()
	if _, e := ss.Load(t.Context()); !errors.Is(e, os.ErrClosed) {
		t.Fatal(e)
	}
	if e := ss.Initialize(t.Context(), incrementalSeed("r")); !errors.Is(e, os.ErrClosed) {
		t.Fatal(e)
	}
}
func TestSessionConcurrentCAS(t *testing.T) {
	s := newTestStore(t)
	ss := acquireTestSession(t, s, "run")
	if e := ss.Initialize(t.Context(), incrementalSeed("run")); e != nil {
		t.Fatal(e)
	}
	var writes atomic.Uint64
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 10 {
				r, e := ss.Load(t.Context())
				if e != nil {
					t.Error(e)
					return
				}
				if _, e = ss.Apply(t.Context(), revisionBatch(t, r)); e == nil {
					writes.Add(1)
				} else if !errors.Is(e, durable.ErrConflict) {
					t.Error(e)
					return
				}
			}
		})
	}
	wg.Wait()
	r, e := ss.Load(t.Context())
	if e != nil || r.Revision != 1+writes.Load() {
		t.Fatal("CAS lost write", e)
	}
}

// Real process publication and lock-release coverage lives in
// TestIncrementalProcessPublication, including before-batch/before-head/after-head.
func TestStoredJSONIsComplete(t *testing.T) {
	s := newTestStore(t)
	ss := acquireTestSession(t, s, "run")
	if e := ss.Initialize(t.Context(), incrementalSeed("run")); e != nil {
		t.Fatal(e)
	}
	if _, e := ss.Load(t.Context()); e != nil {
		t.Fatal(e)
	}
	inner := ss.(*session)
	b, e := os.ReadFile(inner.snapshotPath(inner.head.SnapshotID))
	if e != nil || len(b) == 0 {
		t.Fatal(e)
	}
	entries, e := os.ReadDir(s.dir)
	if e != nil || len(entries) != 3 {
		t.Fatal("unexpected temporary", e)
	}
}
