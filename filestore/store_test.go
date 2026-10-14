package filestore

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	durable "github.com/Luo-root/pulse-durable"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func acquireTestSession(t *testing.T, store *Store, runID string) durable.Session {
	t.Helper()
	session, err := store.Acquire(context.Background(), runID)
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

func requireBusy(t *testing.T, store *Store, runID string) {
	t.Helper()
	result := make(chan error, 1)
	go func() {
		session, err := store.Acquire(context.Background(), runID)
		if session != nil {
			_ = session.Close()
		}
		result <- err
	}()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case err := <-result:
		if !errors.Is(err, durable.ErrBusy) {
			t.Fatalf("competing Acquire error = %v; want ErrBusy", err)
		}
	case <-timer.C:
		t.Fatal("competing Acquire blocked instead of returning ErrBusy")
	}
}

// 业务字段刻意包含后端不负责判断的格式与状态；只验证完整 JSON 保留及副本隔离。
func testRecord(runID string) durable.RunRecord {
	return durable.RunRecord{
		Format: 77, RunID: runID, Revision: 1, Version: "业务版本/α",
		Definition: "opaque-definition", Inputs: "opaque-inputs", RootScope: "scope",
		Scopes: map[string]durable.ScopeRecord{
			"scope": {
				Graph: "图", Path: "a/b", Inputs: "opaque-child-inputs",
				Nodes: map[string]durable.NodeRecord{
					"node": {NodeID: "node", Attempt: "attempt", Reason: "opaque-reason",
						Outputs: []durable.ValueRecord{{Name: "结果", TypeID: "binary", Version: "v1",
							State: "opaque-state", Data: []byte{0, 1, 255}}}},
				},
			},
		},
		Attempts: map[string]durable.AttemptRecord{
			"attempt": {Parent: "scope", NodeID: "node", Boundary: "opaque", Index: 2,
				Limit: 5, State: durable.AttemptState("opaque-attempt")},
		},
	}
}

func TestNewAndInvalidArguments(t *testing.T) {
	if _, err := New(""); err == nil {
		t.Fatal("New accepted empty directory")
	}
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(path); err == nil {
		t.Fatal("New accepted regular file as directory")
	}
	store, err := New(filepath.Join(t.TempDir(), "nested", "store"))
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(store.dir) {
		t.Fatal("New did not resolve directory to absolute path")
	}
	for _, runID := range []string{"", string([]byte{255})} {
		if _, err := store.Acquire(context.Background(), runID); err == nil {
			t.Fatal("Acquire accepted invalid run ID")
		}
	}
	if _, err := store.Acquire(nil, "run"); err == nil {
		t.Fatal("Acquire accepted nil context")
	}
	if _, err := new(Store).Acquire(context.Background(), "run"); err == nil {
		t.Fatal("Acquire accepted uninitialized Store")
	}
}

func TestAcquireIndependentStoresAndRuns(t *testing.T) {
	first := newTestStore(t)
	second, err := New(first.dir)
	if err != nil {
		t.Fatal(err)
	}
	a := acquireTestSession(t, first, "a")
	requireBusy(t, first, "a")
	requireBusy(t, second, "a")
	b := acquireTestSession(t, second, "b")
	if err := b.Save(context.Background(), testRecord("b"), 0); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Load(context.Background()); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("unwritten independent run error = %v; want ErrNotFound", err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	requireBusy(t, second, "a")
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	acquireTestSession(t, second, "a")
}

func TestSaveCASAndCompleteRecord(t *testing.T) {
	store := newTestStore(t)
	session := acquireTestSession(t, store, "run")
	ctx := context.Background()
	initial := testRecord("run")
	future := initial
	future.Revision = 2
	if err := session.Save(ctx, future, 1); !errors.Is(err, durable.ErrConflict) {
		t.Fatalf("first Save with nonzero expected error = %v; want ErrConflict", err)
	}
	if _, err := session.Load(ctx); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("Load after rejected first Save error = %v; want ErrNotFound", err)
	}
	if err := session.Save(ctx, initial, 0); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(store.dir, fileBase("run")+".json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name     string
		runID    string
		revision uint64
		expected uint64
	}{
		{"stale", "run", 1, 0},
		{"future", "run", 4, 3},
		{"wrong-next", "run", 3, 1},
		{"zero", "run", 0, 1},
		{"wrong-run", "other", 2, 1},
		{"overflow", "run", 0, ^uint64(0)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			record := initial
			record.RunID, record.Revision = tc.runID, tc.revision
			if err := session.Save(ctx, record, tc.expected); !errors.Is(err, durable.ErrConflict) {
				t.Fatalf("rejected Save error = %v; want ErrConflict", err)
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("rejected Save changed existing record")
			}
		})
	}
	loaded, err := session.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded, initial) {
		t.Fatal("complete record did not round-trip")
	}
	loaded.Scopes["scope"].Nodes["node"].Outputs[0].Data[0] = 99
	delete(loaded.Attempts, "attempt")
	again, err := session.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(again, initial) {
		t.Fatal("modifying a loaded record changed disk record")
	}
	initial.Revision = 2
	if err := session.Save(ctx, initial, 1); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	loaded, err = acquireTestSession(t, store, "run").Load(ctx)
	if err != nil || !reflect.DeepEqual(loaded, initial) {
		t.Fatalf("updated record did not survive reopening: %v", err)
	}
}

func TestCorruptRecordIsNotMissingOrOverwritten(t *testing.T) {
	for _, data := range []string{"", "{", "null", "{}", `{"run_id":"another","revision":1}`,
		`{"run_id":"run","revision":0}`, `{"run_id":"run","revision":1} {}`} {
		t.Run(data, func(t *testing.T) {
			store := newTestStore(t)
			session := acquireTestSession(t, store, "run")
			path := filepath.Join(store.dir, fileBase("run")+".json")
			if err := os.WriteFile(path, []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := session.Load(context.Background()); err == nil || errors.Is(err, durable.ErrNotFound) {
				t.Fatalf("corrupt Load error = %v; want read failure", err)
			}
			if err := session.Save(context.Background(), testRecord("run"), 0); err == nil {
				t.Fatal("Save overwrote corrupt record")
			}
			after, err := os.ReadFile(path)
			if err != nil || string(after) != data {
				t.Fatalf("failed Save changed corrupt record: %v", err)
			}
		})
	}
}

func TestReadFailureIsNotMissing(t *testing.T) {
	store := newTestStore(t)
	session := acquireTestSession(t, store, "run")
	if err := os.Mkdir(filepath.Join(store.dir, fileBase("run")+".json"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Load(context.Background()); err == nil || errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("directory Load error = %v; want read failure", err)
	}
	if err := session.Save(context.Background(), testRecord("run"), 0); err == nil || errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("Save over unreadable record error = %v; want read failure", err)
	}
}

func TestRunIDCannotEscapeDirectory(t *testing.T) {
	parent := t.TempDir()
	store, err := New(filepath.Join(parent, "store"))
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{"..", "../../escaped", `D:\outside\record`, `\\server\share`, "con/aux",
		"中文/../❄️", "\x00/record", "a\nb", "é", "e\u0301"}
	for _, id := range ids {
		session := acquireTestSession(t, store, id)
		if err := session.Save(context.Background(), testRecord(id), 0); err != nil {
			t.Fatal(err)
		}
		loaded, err := session.Load(context.Background())
		if err != nil || loaded.RunID != id {
			t.Fatalf("run ID did not round-trip: %v", err)
		}
		if err := session.Close(); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(store.dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != len(ids)*2 {
		t.Fatalf("store has %d files; want %d", len(entries), len(ids)*2)
	}
	for _, entry := range entries {
		base := strings.TrimSuffix(strings.TrimSuffix(entry.Name(), ".lock"), ".json")
		if entry.IsDir() || len(base) != 64 {
			t.Fatal("run ID was used as a path instead of a SHA-256 basename")
		}
		if _, err := hex.DecodeString(base); err != nil {
			t.Fatal(err)
		}
	}
	outside, err := os.ReadDir(parent)
	if err != nil || len(outside) != 1 || outside[0].Name() != "store" {
		t.Fatalf("files escaped caller directory: %v", err)
	}
}

func TestContextAndClose(t *testing.T) {
	store := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Acquire(ctx, "run"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Acquire error = %v", err)
	}
	session := acquireTestSession(t, store, "run")
	if _, err := session.Load(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Load error = %v", err)
	}
	if err := session.Save(ctx, testRecord("run"), 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Save error = %v", err)
	}
	if _, err := session.Load(nil); err == nil {
		t.Fatal("Load accepted nil context")
	}
	if err := session.Save(nil, testRecord("run"), 0); err == nil {
		t.Fatal("Save accepted nil context")
	}
	if _, err := session.Load(context.Background()); !errors.Is(err, durable.ErrNotFound) {
		t.Fatalf("canceled Save created a record: %v", err)
	}
	lockPath := filepath.Join(store.dir, fileBase("run")+".lock")
	before, err := os.Stat(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := session.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if _, err := session.Load(context.Background()); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed Load error = %v; want ErrClosed", err)
	}
	if err := session.Save(context.Background(), testRecord("run"), 0); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed Save error = %v; want ErrClosed", err)
	}
	acquireTestSession(t, store, "run")
	after, err := os.Stat(lockPath)
	if err != nil || !os.SameFile(before, after) {
		t.Fatalf("Close or reacquire replaced lock file: %v", err)
	}
}

func TestSessionConcurrentCAS(t *testing.T) {
	store := newTestStore(t)
	session := acquireTestSession(t, store, "run")
	ctx := context.Background()
	if err := session.Save(ctx, testRecord("run"), 0); err != nil {
		t.Fatal(err)
	}
	var writes atomic.Uint64
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 10 {
				record, err := session.Load(ctx)
				if err != nil {
					t.Error(err)
					return
				}
				expected := record.Revision
				record.Revision++
				if err := session.Save(ctx, record, expected); err == nil {
					writes.Add(1)
				} else if !errors.Is(err, durable.ErrConflict) {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	record, err := session.Load(ctx)
	if err != nil || record.Revision != 1+writes.Load() {
		t.Fatalf("concurrent CAS lost or duplicated a write: %v", err)
	}
}

const (
	helperMode = "PULSE_FILESTORE_HELPER_MODE"
	helperDir  = "PULSE_FILESTORE_HELPER_DIR"
	helperRun  = "PULSE_FILESTORE_HELPER_RUN"
	crashCode  = 73
)

func helperCommand(ctx context.Context, executable, mode, dir, runID string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestFilestoreProcessHelper$", "-test.count=1")
	cmd.Env = append(os.Environ(), helperMode+"="+mode, helperDir+"="+dir, helperRun+"="+runID)
	return cmd
}

func TestProcessExitReleasesLockAndKeepsRecord(t *testing.T) {
	store := newTestStore(t)
	runID := "真实进程/../运行"
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	writer := helperCommand(ctx, executable, "hold-and-exit", store.dir, runID)
	input, err := writer.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := writer.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	writer.Stderr = &stderr
	if err := writer.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	defer func() {
		cancel()
		_ = input.Close()
		if !waited {
			_ = writer.Wait()
		}
	}()
	ready, err := bufio.NewReader(output).ReadString('\n')
	if err != nil || ready != "locked-and-saved\n" {
		t.Fatalf("writer did not confirm held lock and saved record: %v", err)
	}
	// 在子进程仍持有锁时竞争，防止只有进程内互斥的实现冒充操作系统锁。
	requireBusy(t, store, runID)
	independent := acquireTestSession(t, store, "other-process-run")
	if err := independent.Close(); err != nil {
		t.Fatal(err)
	}
	if err := input.Close(); err != nil {
		t.Fatal(err)
	}
	err = writer.Wait()
	waited = true
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != crashCode {
		t.Fatalf("writer exit = %v; want os.Exit(%d); stderr: %s", err, crashCode, stderr.String())
	}
	reader := helperCommand(ctx, executable, "load-and-save", store.dir, runID)
	if output, err := reader.CombinedOutput(); err != nil {
		t.Fatalf("new process could not acquire and load: %v; output: %s", err, output)
	}
	// 第二个进程也不调用 Close，再次确认退出释放句柄，并保留替换后的记录。
	record, err := acquireTestSession(t, store, runID).Load(context.Background())
	want := testRecord(runID)
	want.Revision = 2
	if err != nil || !reflect.DeepEqual(record, want) {
		t.Fatalf("record did not survive process exit and replacement: %v", err)
	}
}

func TestFilestoreProcessHelper(t *testing.T) {
	mode := os.Getenv(helperMode)
	if mode == "" {
		t.Skip("only run in a dedicated subprocess")
	}
	store, err := New(os.Getenv(helperDir))
	if err != nil {
		os.Exit(51)
	}
	runID := os.Getenv(helperRun)
	session, err := store.Acquire(context.Background(), runID)
	if err != nil {
		os.Exit(52)
	}
	switch mode {
	case "hold-and-exit":
		if err := session.Save(context.Background(), testRecord(runID), 0); err != nil {
			os.Exit(53)
		}
		if _, err := io.WriteString(os.Stdout, "locked-and-saved\n"); err != nil {
			os.Exit(54)
		}
		if _, err := io.Copy(io.Discard, os.Stdin); err != nil {
			os.Exit(55)
		}
		os.Exit(crashCode) // 刻意不 Close，证明操作系统在进程退出时释放锁。
	case "load-and-save":
		record, err := session.Load(context.Background())
		if err != nil || !reflect.DeepEqual(record, testRecord(runID)) {
			os.Exit(56)
		}
		record.Revision = 2
		if err := session.Save(context.Background(), record, 1); err != nil {
			os.Exit(57)
		}
		os.Exit(0) // 再次不 Close。
	default:
		os.Exit(58)
	}
}

func TestStoredJSONIsComplete(t *testing.T) {
	store := newTestStore(t)
	session := acquireTestSession(t, store, "run")
	record := testRecord("run")
	if err := session.Save(context.Background(), record, 0); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(store.dir, fileBase("run")+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var decoded durable.RunRecord
	if err := json.Unmarshal(data, &decoded); err != nil || !reflect.DeepEqual(decoded, record) {
		t.Fatalf("target was not one complete JSON record: %v", err)
	}
	entries, err := os.ReadDir(store.dir)
	if err != nil || len(entries) != 2 {
		t.Fatalf("successful Save left temporary files: %v", err)
	}
}
