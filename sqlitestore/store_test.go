package sqlitestore

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/Luo-root/pulse-durable/internal/runlock"
	"sync"
	"testing"
	"time"

	durable "github.com/Luo-root/pulse-durable"
)

var ctx = context.Background()

func openStore(t *testing.T, path string) *Store {
	t.Helper()
	s, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}
func acquire(t *testing.T, s *Store, id string) durable.Session {
	t.Helper()
	session, err := s.Acquire(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := session.Close(); err != nil {
			t.Error(err)
		}
	})
	return session
}
func record(id string, rev uint64) durable.RunRecord {
	return durable.RunRecord{Format: durable.RecordFormat, RunID: id, Revision: rev,
		Version: "v1", Definition: "definition", Inputs: "inputs", RootScope: "root",
		Scopes: map[string]durable.ScopeRecord{"root": {Graph: "graph", Path: "/", Inputs: "inputs", Bindings: true,
			Nodes: map[string]durable.NodeRecord{"node": {NodeID: "node", Attempt: "attempt", Reason: "completed", Outputs: []durable.ValueRecord{{Name: "blob", TypeID: "bytes", Version: "1", State: "ready", Data: []byte{0, 255, 254, 128, 13, 10, 0}}}}}}},
		Attempts: map[string]durable.AttemptRecord{"attempt": {Parent: "root", NodeID: "node", Index: 1, Limit: 1, State: durable.AttemptConfirmed}},
		Managed:  map[string]durable.ManagedRecord{"managed": {Scope: "root", NodeID: "node", State: []durable.ValueRecord{{Data: []byte{255, 0}}}, Values: []durable.ValueRecord{{Data: []byte{128, 1}}}}},
		Streams:  map[string]durable.StreamRecord{"stream": {Stream: durable.StreamDescriptorRecord{ID: durable.StreamIDRecord{Scope: "root", NodeID: "node", Key: "stream", Group: "group", Generation: math.MaxUint64}, Graph: "graph", Resumable: true, Capacity: 8}, LastSequence: math.MaxUint64, Pending: []durable.StreamMessageRecord{{Sequence: math.MaxUint64, Value: durable.ValueRecord{Data: []byte{255, 0, 1}}}}, Cursor: &durable.ValueRecord{Data: []byte{128, 0}}}},
		Messages: map[string]durable.MessageAttemptRecord{"message": {Scope: "root", Parent: "attempt", NodeID: "node", Input: durable.StreamIDRecord{Key: "stream", Generation: math.MaxUint64}, Sequence: math.MaxUint64, Index: 1, Limit: 3, State: durable.AttemptActive}},
	}
}

func TestContract(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.sqlite")
	s := openStore(t, path)
	session := acquire(t, s, "运行/🌈")
	if _, err := session.Load(ctx); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	r := record("运行/🌈", 1)
	if err := session.Save(ctx, r, 0); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name     string
		r        durable.RunRecord
		expected uint64
	}{
		{"stale", r, 0}, {"future", record(r.RunID, 4), 3}, {"identity", record("wrong", 2), 1},
		{"zero", record(r.RunID, 0), 0}, {"overflow", record(r.RunID, 0), math.MaxUint64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := session.Save(ctx, tc.r, tc.expected); !errors.Is(err, durable.ErrConflict) {
				t.Fatalf("got %v", err)
			}
		})
	}
	got, err := session.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, r) {
		t.Fatalf("roundtrip mismatch: %#v", got)
	}
	got.Scopes["root"].Nodes["node"].Outputs[0].Data[0] = 99
	delete(got.Attempts, "attempt")
	again, err := session.Load(ctx)
	if err != nil || !reflect.DeepEqual(again, r) {
		t.Fatalf("load isolation: %v", err)
	}
	r.Revision = 2
	if err := session.Save(ctx, r, 1); err != nil {
		t.Fatal(err)
	}
	r.Scopes["root"].Nodes["node"].Outputs[0].Data[0] = 77
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := acquire(t, openStore(t, path), r.RunID)
	got, err = reopened.Load(ctx)
	if err != nil || got.Revision != 2 || got.Scopes["root"].Nodes["node"].Outputs[0].Data[0] != 0 {
		t.Fatalf("persistence/save isolation: %v", err)
	}
}

func TestOwnershipAndParallelRuns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.sqlite")
	a, b := openStore(t, path), openStore(t, path)
	held := acquire(t, a, "same")
	start := time.Now()
	if _, err := b.Acquire(ctx, "same"); !errors.Is(err, durable.ErrBusy) {
		t.Fatalf("same run: %v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("Acquire waited")
	}
	// Merely holding a session must not hold a SQLite write transaction.
	other := acquire(t, b, "other")
	if err := other.Save(ctx, record("other", 1), 0); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			store := a
			if i%2 == 0 {
				store = b
			}
			id := fmt.Sprint("run-", i)
			ss, err := store.Acquire(ctx, id)
			if err != nil {
				t.Error(err)
				return
			}
			defer ss.Close()
			if err := ss.Save(ctx, record(id, 1), 0); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	acquire(t, b, "same")
}

func TestMaxRevision(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "runs.sqlite"))
	ss := acquire(t, s, "max")
	r := record("max", math.MaxUint64-1)
	data, _ := json.Marshal(r)
	// Seed the otherwise unreachable high revision, then exercise public CAS.
	if _, err := s.db.Exec("INSERT INTO durable_runs VALUES (?, ?, ?)", r.RunID, fmt.Sprint(r.Revision), data); err != nil {
		t.Fatal(err)
	}
	r.Revision = math.MaxUint64
	if err := ss.Save(ctx, r, math.MaxUint64-1); err != nil {
		t.Fatal(err)
	}
	got, err := ss.Load(ctx)
	if err != nil || got.Revision != math.MaxUint64 {
		t.Fatalf("max roundtrip: %v %#v", err, got)
	}
	if err := ss.Save(ctx, record("max", 0), math.MaxUint64); !errors.Is(err, durable.ErrConflict) {
		t.Fatalf("overflow: %v", err)
	}
}

func TestInvalidRecords(t *testing.T) {
	for _, tc := range []struct {
		name, rev string
		data      []byte
	}{
		{"syntax", "1", []byte("{")},
		{"identity", "1", []byte(`{"run_id":"wrong","revision":1}`)},
		{"revision_mismatch", "2", []byte(`{"run_id":"id","revision":1}`)},
		{"zero", "0", []byte(`{"run_id":"id","revision":0}`)},
		{"out_of_range", "18446744073709551616", []byte(`{"run_id":"id","revision":1}`)},
		{"json_out_of_range", "1", []byte(`{"run_id":"id","revision":18446744073709551616}`)},
		{"noncanonical", "01", []byte(`{"run_id":"id","revision":1}`)},
		{"surrogate", "1", []byte(`{"run_id":"id","revision":1,"version":"\ud800"}`)},
		{"invalid_utf8", "1", append([]byte(`{"run_id":"id","revision":1,"version":"`), 255, '"', '}')},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := openStore(t, filepath.Join(t.TempDir(), "runs.sqlite"))
			ss := acquire(t, s, "id")
			if _, err := s.db.Exec("INSERT INTO durable_runs VALUES (?, ?, ?)", "id", tc.rev, tc.data); err != nil {
				t.Fatal(err)
			}
			if _, err := ss.Load(ctx); err == nil || errors.Is(err, durable.ErrNotFound) {
				t.Fatalf("corruption accepted: %v", err)
			}
			if err := ss.Save(ctx, record("id", 2), 1); err == nil {
				t.Fatal("overwrote corruption")
			}
			var data []byte
			if err := s.db.QueryRow("SELECT record FROM durable_runs").Scan(&data); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(data, tc.data) {
				t.Fatal("corrupt record changed")
			}
		})
	}
}

func TestValidationAndClose(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "runs.sqlite"))
	for _, id := range []string{"", string([]byte{255})} {
		if _, err := s.Acquire(ctx, id); err == nil {
			t.Fatal("invalid run ID accepted")
		}
	}
	ss := acquire(t, s, "id")
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	for _, c := range []context.Context{nil, cancelled} {
		if _, err := s.Acquire(c, "x"); err == nil {
			t.Fatal("Acquire context")
		}
		if _, err := ss.Load(c); err == nil {
			t.Fatal("Load context")
		}
		if err := ss.Save(c, record("id", 1), 0); err == nil {
			t.Fatal("Save context")
		}
	}
	r := record("id", 1)
	r.Version = string([]byte{255})
	if err := ss.Save(ctx, r, 0); err == nil {
		t.Fatal("invalid metadata")
	}
	r = record("id", 1)
	r.Managed[string([]byte{255})] = durable.ManagedRecord{}
	if err := ss.Save(ctx, r, 0); err == nil {
		t.Fatal("invalid map key")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ss.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := ss.Load(ctx); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed load: %v", err)
	}
	if err := ss.Save(ctx, record("id", 1), 0); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed save: %v", err)
	}
	if _, err := s.Acquire(ctx, "id"); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed acquire: %v", err)
	}
	acquire(t, openStore(t, s.locks[:len(s.locks)-len(".runlocks")]), "id")
}

func TestSQLiteConfiguration(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "runs.sqlite"))
	check := func() {
		t.Helper()
		var mode, version, source string
		var synchronous, busy int
		if err := s.db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
			t.Fatal(err)
		}
		if err := s.db.QueryRow("PRAGMA synchronous").Scan(&synchronous); err != nil {
			t.Fatal(err)
		}
		if err := s.db.QueryRow("PRAGMA busy_timeout").Scan(&busy); err != nil {
			t.Fatal(err)
		}
		if mode != "wal" || synchronous != 2 || busy != 5000 {
			t.Fatalf("configuration: %s/%d/%d", mode, synchronous, busy)
		}
		if err := s.db.QueryRow("SELECT sqlite_version(), sqlite_source_id()").Scan(&version, &source); err != nil {
			t.Fatal(err)
		}
		if version == "" || source == "" {
			t.Fatal("missing SQLite identity")
		}
		t.Logf("sqlite_version=%s sqlite_source_id=%s", version, source)
	}
	check()
	// Discard the current connection, forcing DSN pragmas onto a new connection.
	s.db.SetMaxIdleConns(0)
	check()
	s.db.SetMaxIdleConns(1)
	check()
}

func TestConcurrentInitialization(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.sqlite")
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			s, err := NewContext(ctx, path)
			if err != nil {
				t.Error(err)
				return
			}
			defer s.Close()
			var mode string
			if err := s.db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil || mode != "wal" {
				t.Errorf("mode=%s err=%v", mode, err)
			}
		}()
	}
	close(start)
	wg.Wait()
}

func TestNewContextCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.sqlite")
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := NewContext(cancelled, path); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cancelled construction created file: %v", err)
	}
	if _, err := NewContext(nil, path); err == nil {
		t.Fatal("nil context")
	}
	lock, err := runlock.Acquire(ctx, path+".init.lock")
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	deadline, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if _, err := NewContext(deadline, path); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting constructor: %v", err)
	}
}

func TestRejectUnrelatedAndMalformedSchema(t *testing.T) {
	for _, tc := range []struct {
		name, ddl string
		version   int
	}{
		{"unrelated", "CREATE TABLE unrelated(value TEXT)", 0},
		{"unversioned_module", schemaSQL, 0},
		{"no_primary_key", strings.Replace(schemaSQL, " PRIMARY KEY", "", 1), 1},
		{"nullable_revision", strings.Replace(schemaSQL, "revision TEXT NOT NULL", "revision TEXT", 1), 1},
		{"missing_check", strings.Replace(schemaSQL, " CHECK(typeof(record) = 'blob')", "", 1), 1},
		{"wrong_type", strings.Replace(schemaSQL, "revision TEXT", "revision INTEGER", 1), 1},
		{"extra_table", schemaSQL + "; CREATE TABLE extra(value TEXT)", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Removing the PK also makes WITHOUT ROWID invalid, so remove that too.
			ddl := tc.ddl
			if tc.name == "no_primary_key" {
				ddl = strings.Replace(ddl, " WITHOUT ROWID", "", 1)
			}
			path := filepath.Join(t.TempDir(), "runs.sqlite")
			db, err := sql.Open("sqlite", filepath.ToSlash(path))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			if _, err := db.Exec(ddl); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(fmt.Sprintf("PRAGMA user_version=%d", tc.version)); err != nil {
				t.Fatal(err)
			}
			var before string
			if err := db.QueryRow("PRAGMA journal_mode").Scan(&before); err != nil {
				t.Fatal(err)
			}
			if s, err := NewContext(ctx, path); err == nil {
				s.Close()
				t.Fatal("adopted incompatible schema")
			}
			var version int
			var after string
			if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
				t.Fatal(err)
			}
			if err := db.QueryRow("PRAGMA journal_mode").Scan(&after); err != nil {
				t.Fatal(err)
			}
			if version != tc.version || after != before {
				t.Fatalf("rejected DB modified: %d %s", version, after)
			}
		})
	}
}

func TestSchemaAndDatabase(t *testing.T) {
	if _, err := New(""); err == nil {
		t.Fatal("empty path")
	}
	if _, err := New(":memory:"); err == nil {
		t.Fatal("memory path")
	}
	path := filepath.Join(t.TempDir(), "runs.sqlite")
	s := openStore(t, path)
	if _, err := s.db.Exec("PRAGMA user_version=99"); err != nil {
		t.Fatal(err)
	}
	if _, err := New(path); err == nil {
		t.Fatal("unsupported schema")
	}
	corrupt := filepath.Join(t.TempDir(), "corrupt.sqlite")
	if err := os.WriteFile(corrupt, []byte("not a database"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(corrupt); err == nil {
		t.Fatal("corrupt database")
	}
}

// A real child process must observe the parent's lock, and be able to acquire it
// after Store.Close. The helper uses the same race-instrumented test executable.
func TestCrossProcess(t *testing.T) {
	if path := os.Getenv("SQLITESTORE_CHILD_DB"); path != "" {
		s, err := New(path)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		ss, err := s.Acquire(ctx, "process")
		if os.Getenv("SQLITESTORE_CHILD_BUSY") == "1" {
			if !errors.Is(err, durable.ErrBusy) {
				t.Fatalf("cross-process busy: %v", err)
			}
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		defer ss.Close()
		return
	}
	path := filepath.Join(t.TempDir(), "runs.sqlite")
	s := openStore(t, path)
	acquire(t, s, "process")
	for _, busy := range []bool{true, false} {
		if !busy {
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		}
		cmd := exec.Command(os.Args[0], "-test.run=^TestCrossProcess$", "-test.timeout=20s")
		flag := "0"
		if busy {
			flag = "1"
		}
		cmd.Env = append(os.Environ(), "SQLITESTORE_CHILD_DB="+path, "SQLITESTORE_CHILD_BUSY="+flag)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("child: %v\n%s", err, output)
		}
	}
}
