package sqlitestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/internal/runlock"
)

var ctx = context.Background()

func openStore(t *testing.T, path string) *Store {
	t.Helper()
	s, e := New(path)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		if e := s.Close(); e != nil {
			t.Error(e)
		}
	})
	return s
}
func acquire(t *testing.T, s *Store, id string) durable.Session {
	t.Helper()
	ss, e := s.Acquire(ctx, id)
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
func record(id string, rev uint64) durable.RunRecord {
	d := fmt.Sprintf("%x", sha256.Sum256([]byte("inputs")))
	return durable.RunRecord{Format: durable.RecordFormat, RunID: id, Revision: rev, Version: "v1", Definition: d, Inputs: d, RootScope: "root", Scopes: map[string]durable.ScopeRecord{"root": {Graph: "graph", Inputs: d, Nodes: map[string]durable.NodeRecord{}}}}
}

type captureStore struct {
	durable.Store
	initial durable.RunRecord
	batches []durable.ChangeBatch
}
type captureSession struct {
	durable.Session
	owner *captureStore
}

func (s *captureStore) Acquire(c context.Context, id string) (durable.Session, error) {
	ss, e := s.Store.Acquire(c, id)
	return &captureSession{ss, s}, e
}
func (s *captureSession) Initialize(c context.Context, r durable.RunRecord) error {
	s.owner.initial = r
	return s.Session.Initialize(c, r)
}
func (s *captureSession) Apply(c context.Context, b durable.ChangeBatch) (durable.CommitAck, error) {
	s.owner.batches = append(s.owner.batches, b)
	return s.Session.Apply(c, b)
}

// 测试变更由真实 Runtime 产生，而非捏造的线格式或 Save 桥。
func runtimeBatches(t *testing.T, id string) (durable.RunRecord, []durable.ChangeBatch) {
	t.Helper()
	g, e := pulse.New("capture", "")
	if e != nil {
		t.Fatal(e)
	}
	if e = g.Add(pulse.NewNode("node", "", nil, nil, func(*pulse.RunCtx) error { return nil })); e != nil {
		t.Fatal(e)
	}
	c := &captureStore{Store: durable.NewMemoryStore()}
	rt, e := durable.New(c, durable.NewCodecs())
	if e != nil {
		t.Fatal(e)
	}
	if _, e = rt.Run(ctx, g, pulse.Inputs{}, durable.RunOptions{RunID: id, Version: "v1"}); e != nil {
		t.Fatal(e)
	}
	if len(c.batches) < 2 {
		t.Fatal("missing runtime batches")
	}
	return c.initial, c.batches
}
func TestContract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.sqlite")
	s := openStore(t, path)
	ss := acquire(t, s, "运行/🌈")
	if _, e := ss.Load(ctx); !errors.Is(e, durable.ErrNotFound) {
		t.Fatal(e)
	}
	r, batches := runtimeBatches(t, "运行/🌈")
	if e := ss.Initialize(ctx, r); e != nil {
		t.Fatal(e)
	}
	for _, bad := range []durable.RunRecord{r, record("wrong", 2), record(r.RunID, 0), record(r.RunID, ^uint64(0))} {
		if e := ss.Initialize(ctx, bad); !errors.Is(e, durable.ErrConflict) {
			t.Fatal(e)
		}
	}
	for _, b := range batches {
		ack, e := ss.Apply(ctx, b)
		if e != nil || ack.Header != b.Header() {
			t.Fatalf("ack: %v", e)
		}
		if _, e = b.ApplyTo(&r); e != nil {
			t.Fatal(e)
		}
	}
	if _, e := ss.Apply(ctx, batches[0]); !errors.Is(e, durable.ErrConflict) {
		t.Fatal(e)
	}
	got, e := ss.Load(ctx)
	if e != nil || !reflect.DeepEqual(got, r) {
		t.Fatalf("roundtrip: %v", e)
	}
	delete(got.Scopes, r.RootScope)
	got, e = ss.Load(ctx)
	if e != nil || !reflect.DeepEqual(got, r) {
		t.Fatal("load alias", e)
	}
	if e = ss.Close(); e != nil {
		t.Fatal(e)
	}
	got, e = acquire(t, openStore(t, path), r.RunID).Load(ctx)
	if e != nil || !reflect.DeepEqual(got, r) {
		t.Fatal("reopen", e)
	}
}
func TestOwnershipAndParallelRuns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.sqlite")
	a, b := openStore(t, path), openStore(t, path)
	held := acquire(t, a, "same")
	start := time.Now()
	if _, e := b.Acquire(ctx, "same"); !errors.Is(e, durable.ErrBusy) {
		t.Fatal(e)
	}
	if time.Since(start) > time.Second {
		t.Fatal("Acquire waited")
	}
	if e := acquire(t, b, "other").Initialize(ctx, record("other", 1)); e != nil {
		t.Fatal(e)
	}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprint(i)
			ss, e := a.Acquire(ctx, id)
			if e != nil {
				t.Error(e)
				return
			}
			defer ss.Close()
			if e = ss.Initialize(ctx, record(id, 1)); e != nil {
				t.Error(e)
			}
		}(i)
	}
	wg.Wait()
	held.Close()
	acquire(t, b, "same")
}
func TestMaxRevision(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "r.sqlite"))
	ss := acquire(t, s, "max")
	if e := ss.Initialize(ctx, record("max", 1)); e != nil {
		t.Fatal(e)
	}
	if _, e := s.db.Exec(`UPDATE durable_heads SET revision='18446744073709551615'`); e != nil {
		t.Fatal(e)
	}
	ss.Close()
	if _, e := acquire(t, s, "max").Load(ctx); e == nil {
		t.Fatal("unreachable/gapped maximum revision accepted")
	}
	if e := ss.Initialize(ctx, record("max", 0)); !errors.Is(e, os.ErrClosed) {
		t.Fatal(e)
	}
}
func TestInvalidRecords(t *testing.T) {
	for _, mutation := range []string{
		`UPDATE durable_snapshots SET record=x'7b'`, `UPDATE durable_snapshots SET digest=zeroblob(32)`, `UPDATE durable_snapshots SET chain=zeroblob(32)`,
		`UPDATE durable_heads SET revision='01'`, `UPDATE durable_heads SET revision='0'`, `UPDATE durable_heads SET revision='18446744073709551616'`, `UPDATE durable_heads SET chain=zeroblob(32)`,
		`DELETE FROM durable_batches WHERE revision='2'`, `UPDATE durable_batches SET digest=zeroblob(32) WHERE revision='2'`, `UPDATE durable_batches SET parent_chain=zeroblob(32) WHERE revision='2'`, `UPDATE durable_batches SET chain=zeroblob(32) WHERE revision='2'`, `UPDATE durable_batches SET wire=x'7b' WHERE revision='2'`,
	} {
		t.Run(mutation, func(t *testing.T) {
			s := openStore(t, filepath.Join(t.TempDir(), "r.sqlite"))
			ss := acquire(t, s, "id")
			r, bs := runtimeBatches(t, "id")
			if e := ss.Initialize(ctx, r); e != nil {
				t.Fatal(e)
			}
			for _, b := range bs {
				if _, e := ss.Apply(ctx, b); e != nil {
					t.Fatal(e)
				}
			}
			ss.Close()
			if _, e := s.db.Exec(mutation); e != nil {
				t.Fatal(e)
			}
			ss = acquire(t, s, "id")
			if _, e := ss.Load(ctx); e == nil || errors.Is(e, durable.ErrNotFound) {
				t.Fatal("corruption accepted", e)
			}
			if e := ss.Initialize(ctx, r); !errors.Is(e, durable.ErrConflict) {
				t.Fatal("overwrote corruption", e)
			}
			if _, e := ss.Apply(ctx, bs[0]); e == nil {
				t.Fatal("applied over corruption")
			}
		})
	}
}
func TestValidationAndClose(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "r.sqlite"))
	for _, id := range []string{"", string([]byte{255})} {
		if _, e := s.Acquire(ctx, id); e == nil {
			t.Fatal("invalid id")
		}
	}
	ss := acquire(t, s, "id")
	c, cancel := context.WithCancel(ctx)
	cancel()
	for _, c := range []context.Context{nil, c} {
		if _, e := s.Acquire(c, "x"); e == nil {
			t.Fatal("acquire context")
		}
		if _, e := ss.Load(c); e == nil {
			t.Fatal("load context")
		}
		if e := ss.Initialize(c, record("id", 1)); e == nil {
			t.Fatal("initialize context")
		}
		if _, e := ss.Apply(c, durable.ChangeBatch{}); e == nil {
			t.Fatal("apply context")
		}
	}
	r := record("id", 1)
	r.Version = string([]byte{255})
	if e := ss.Initialize(ctx, r); e == nil {
		t.Fatal("metadata")
	}
	r = record("id", 1)
	r.Managed = map[string]durable.ManagedRecord{string([]byte{255}): {}}
	if e := ss.Initialize(ctx, r); e == nil {
		t.Fatal("invalid key")
	}
	s.Close()
	s.Close()
	ss.Close()
	if _, e := ss.Load(ctx); !errors.Is(e, os.ErrClosed) {
		t.Fatal(e)
	}
	if e := ss.Initialize(ctx, record("id", 1)); !errors.Is(e, os.ErrClosed) {
		t.Fatal(e)
	}
	if _, e := ss.Apply(ctx, durable.ChangeBatch{}); !errors.Is(e, os.ErrClosed) {
		t.Fatal(e)
	}
	if _, e := s.Acquire(ctx, "id"); !errors.Is(e, os.ErrClosed) {
		t.Fatal(e)
	}
	acquire(t, openStore(t, strings.TrimSuffix(s.locks, ".runlocks")), "id")
}
func TestSQLiteConfiguration(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "r.sqlite"))
	for i := 0; i < 3; i++ {
		var mode, version, source string
		var sync, busy int
		s.db.QueryRow("PRAGMA journal_mode").Scan(&mode)
		s.db.QueryRow("PRAGMA synchronous").Scan(&sync)
		s.db.QueryRow("PRAGMA busy_timeout").Scan(&busy)
		if mode != "wal" || sync != 2 || busy != 5000 {
			t.Fatalf("config %s/%d/%d", mode, sync, busy)
		}
		if e := s.db.QueryRow("SELECT sqlite_version(),sqlite_source_id()").Scan(&version, &source); e != nil || version == "" || source == "" {
			t.Fatal(e)
		}
		t.Log(version, source)
		s.db.SetMaxIdleConns(i % 2)
	}
}
func TestConcurrentInitialization(t *testing.T) {
	path := filepath.Join(t.TempDir(), "r.sqlite")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, e := New(path)
			if e != nil {
				t.Error(e)
				return
			}
			defer s.Close()
		}()
	}
	wg.Wait()
}
func TestNewContextCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "r.sqlite")
	c, cancel := context.WithCancel(ctx)
	cancel()
	if _, e := NewContext(c, path); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if _, e := os.Stat(path); !errors.Is(e, os.ErrNotExist) {
		t.Fatal("created file", e)
	}
	if _, e := NewContext(nil, path); e == nil {
		t.Fatal("nil context")
	}
	lock, e := runlock.Acquire(ctx, path+".init.lock")
	if e != nil {
		t.Fatal(e)
	}
	defer lock.Close()
	c, cancel = context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if _, e := NewContext(c, path); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal(e)
	}
}

const legacySchema = `CREATE TABLE durable_runs (run_id TEXT PRIMARY KEY NOT NULL, revision TEXT NOT NULL CHECK(typeof(revision) = 'text'), record BLOB NOT NULL CHECK(typeof(record) = 'blob')) WITHOUT ROWID`

func TestRejectUnrelatedAndMalformedSchema(t *testing.T) {
	for _, tc := range []struct {
		name, ddl string
		version   int
	}{{"old", legacySchema, 1}, {"unrelated", "CREATE TABLE unrelated(value TEXT)", 0}, {"unversioned_module", snapshotSQL, 0}, {"no_primary_key", strings.Replace(strings.Replace(snapshotSQL, ",\n PRIMARY KEY(run_id, revision)", "", 1), " WITHOUT ROWID", "", 1), 3}, {"nullable_revision", strings.Replace(headSQL, "revision TEXT NOT NULL", "revision TEXT", 1), 3}, {"missing_check", strings.Replace(snapshotSQL, " CHECK(typeof(record) = 'blob')", "", 1), 3}, {"wrong_type", strings.Replace(headSQL, "revision TEXT", "revision INTEGER", 1), 3}, {"extra_table", snapshotSQL + "; CREATE TABLE extra(value TEXT)", 2}} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "r.sqlite")
			db, e := sql.Open("sqlite", filepath.ToSlash(path))
			if e != nil {
				t.Fatal(e)
			}
			if _, e = db.Exec(tc.ddl); e != nil {
				t.Fatal(e)
			}
			if _, e = db.Exec(fmt.Sprintf("PRAGMA user_version=%d", tc.version)); e != nil {
				t.Fatal(e)
			}
			db.Close()
			before, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			if s, e := New(path); e == nil {
				s.Close()
				t.Fatal("adopted schema")
			}
			after, e := os.ReadFile(path)
			if e != nil || !bytes.Equal(before, after) {
				t.Fatal("rejected DB bytes modified", e)
			}
		})
	}
}
func TestSchemaAndDatabase(t *testing.T) {
	for _, path := range []string{"", ":memory:"} {
		if _, e := New(path); e == nil {
			t.Fatal("bad path")
		}
	}
	path := filepath.Join(t.TempDir(), "r.sqlite")
	s := openStore(t, path)
	if _, e := s.db.Exec("PRAGMA user_version=99"); e != nil {
		t.Fatal(e)
	}
	if _, e := New(path); e == nil {
		t.Fatal("unknown version")
	}
	path = filepath.Join(t.TempDir(), "corrupt")
	os.WriteFile(path, []byte("not a database"), 0600)
	if _, e := New(path); e == nil {
		t.Fatal("corrupt db")
	}
}
func TestCrossProcess(t *testing.T) {
	if path := os.Getenv("SQLITESTORE_CHILD_DB"); path != "" {
		s, e := New(path)
		if e != nil {
			t.Fatal(e)
		}
		defer s.Close()
		ss, e := s.Acquire(ctx, "process")
		if os.Getenv("SQLITESTORE_CHILD_BUSY") == "1" {
			if !errors.Is(e, durable.ErrBusy) {
				t.Fatal(e)
			}
			return
		}
		if e != nil {
			t.Fatal(e)
		}
		defer ss.Close()
		return
	}
	path := filepath.Join(t.TempDir(), "r.sqlite")
	s := openStore(t, path)
	acquire(t, s, "process")
	for _, busy := range []bool{true, false} {
		flag := "1"
		if !busy {
			s.Close()
			flag = "0"
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestCrossProcess$", "-test.timeout=20s")
		cmd.Env = append(os.Environ(), "SQLITESTORE_CHILD_DB="+path, "SQLITESTORE_CHILD_BUSY="+flag)
		if out, e := cmd.CombinedOutput(); e != nil {
			t.Fatalf("%v\n%s", e, out)
		}
	}
}
