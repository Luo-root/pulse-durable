package sqlitestore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)

const recoveryExit = 73

var uncertainApply = errors.New("test: SQLite committed but acknowledgement was lost")

// These are Runtime tests against a real SQLite database, not lock tests.
// Every Run and Resume reconstructs the graph in a separate process. Audit files
// observe business entries only; business and recovery never read them.
func TestSQLiteRuntimeRecovery(t *testing.T) {
	for _, scenario := range []string{"node-before", "node-after", "sub-before", "sub-after", "retry-failed", "retry-exhausted", "uncertain-node", "stream-append-after", "message-before", "message-after", "uncertain-message"} {
		t.Run(scenario, func(t *testing.T) {
			dir := t.TempDir()
			runRecoveryChild(t, dir, scenario, "run", recoveryExit)
			before := readRecoveryRecord(t, dir)
			if before.RootScope == "" || before.Revision == 0 {
				t.Fatal("missing durable run identity")
			}
			if strings.Contains(scenario, "message") || strings.HasPrefix(scenario, "stream-") {
				assertStreamBoundary(t, before, scenario)
			} else if scenario == "retry-failed" || scenario == "retry-exhausted" {
				want := 1
				if scenario == "retry-exhausted" {
					want = 2
				}
				if got := retryIndex(before, durable.AttemptFailed); got != want {
					t.Fatalf("failed retry index=%d, want %d", got, want)
				}
			} else {
				confirmed := false
				for _, s := range before.Scopes {
					_, ok := s.Nodes["work"]
					confirmed = confirmed || ok
				}
				if confirmed != !strings.HasSuffix(scenario, "before") {
					t.Fatalf("wrong checkpoint side: confirmed=%t", confirmed)
				}
			}
			runRecoveryChild(t, dir, scenario, "resume", 0)
			after := readRecoveryRecord(t, dir)
			if after.RootScope != before.RootScope || after.RunID != before.RunID || after.Version != before.Version || after.Definition != before.Definition || after.Inputs != before.Inputs {
				t.Fatal("Resume replaced durable run identity")
			}
			if scenario == "retry-exhausted" {
				if len(before.Attempts) != len(after.Attempts) || retryIndex(after, durable.AttemptFailed) != 2 {
					t.Fatal("exhausted Resume refreshed Retry budget")
				}
				for id, old := range before.Attempts {
					next := after.Attempts[id]
					if old.Parent != next.Parent || old.Index != next.Index || old.Limit != next.Limit {
						t.Fatal("exhausted Resume replaced persisted attempt identity")
					}
				}
				assertAudit(t, dir, "work", "call\ncall\n")
			} else {
				if after.Revision <= before.Revision {
					t.Fatal("Resume did not advance revision")
				}
				for id, old := range before.Attempts {
					next, ok := after.Attempts[id]
					if !ok || old.Parent != next.Parent || old.NodeID != next.NodeID || old.Index != next.Index || old.Limit != next.Limit || old.Boundary != next.Boundary {
						t.Fatalf("persisted attempt identity/budget changed: %s", id)
					}
				}
				if strings.Contains(scenario, "message") || strings.HasPrefix(scenario, "stream-") {
					want := "1\n2\n3\n"
					if scenario == "message-before" {
						want = "1\n1\n2\n3\n"
					}
					assertAudit(t, dir, "message", want)
					if scenario == "stream-append-after" {
						assertAudit(t, dir, "cursor", "0\n1\n")
						assertAudit(t, dir, "source", "call\ncall\n")
					}
					assertFinishedStream(t, after)
				} else {
					calls := "call\n"
					if strings.HasSuffix(scenario, "before") || scenario == "retry-failed" {
						calls += "call\n"
					}
					assertAudit(t, dir, "work", calls)
					assertAudit(t, dir, "first", callsIfRetry(scenario))
					assertAudit(t, dir, "tail", "call\n")
					if scenario == "retry-failed" {
						latest := 0
						for _, a := range after.Attempts {
							if a.NodeID == "mount" && a.Boundary != "" {
								if a.Limit != 2 {
									t.Fatalf("Retry limit=%d, want 2", a.Limit)
								}
								if a.Index > latest {
									latest = a.Index
								}
							}
						}
						if latest != 2 {
							t.Fatalf("Retry index=%d, want 2", latest)
						}
					}
				}
			}
			// A third independent process must not replay any confirmed business.
			trace := recoveryAudits(t, dir)
			runRecoveryChild(t, dir, scenario, "resume", 0)
			final := readRecoveryRecord(t, dir)
			if !reflect.DeepEqual(trace, recoveryAudits(t, dir)) {
				t.Fatal("repeated Resume reran confirmed business")
			}
			if len(final.Streams) == 0 && !reflect.DeepEqual(after, final) {
				t.Fatal("repeated ordinary Resume changed history")
			}
			t.Logf("real SQLite exit/restart: revision %d -> %d -> %d, scopes=%d", before.Revision, after.Revision, final.Revision, len(final.Scopes))
		})
	}
}

func callsIfRetry(s string) string {
	if s == "retry-failed" {
		return "call\ncall\n"
	}
	return "call\n"
}
func retryIndex(r durable.RunRecord, state durable.AttemptState) int {
	latest := 0
	for _, a := range r.Attempts {
		if a.NodeID == "mount" && a.Boundary != "" && a.State == state && a.Index > latest {
			latest = a.Index
		}
	}
	return latest
}

func runRecoveryChild(t *testing.T, dir, scenario, phase string, code int) {
	t.Helper()
	c, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(c, os.Args[0], "-test.run=^TestSQLiteRecoveryHelper$", "-test.count=1", "-test.timeout=15s")
	cmd.Env = append(os.Environ(), "SQLITE_RECOVERY_DIR="+dir, "SQLITE_RECOVERY_CASE="+scenario, "SQLITE_RECOVERY_PHASE="+phase, "GORACE=atexit_sleep_ms=0")
	out, err := cmd.CombinedOutput()
	got := 0
	if err != nil {
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			t.Fatalf("child startup: %v\n%s", err, out)
		}
		got = exit.ExitCode()
	}
	if c.Err() != nil || got != code {
		t.Fatalf("%s/%s exit=%d want=%d: %v\n%s", scenario, phase, got, code, c.Err(), out)
	}
}

func readRecoveryRecord(t *testing.T, dir string) durable.RunRecord {
	t.Helper()
	s, err := New(filepath.Join(dir, "runs.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ss, err := s.Acquire(t.Context(), "recovery")
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	r, err := ss.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func audit(dir, name, text string) error {
	f, err := os.OpenFile(filepath.Join(dir, name+".audit"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.WriteString(text)
	return errors.Join(err, f.Sync(), f.Close())
}
func recoveryAudits(t *testing.T, dir string) map[string]string {
	t.Helper()
	result := make(map[string]string)
	for _, name := range []string{"first", "work", "tail", "source", "message", "cursor"} {
		data, err := os.ReadFile(filepath.Join(dir, name+".audit"))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		result[name] = string(data)
	}
	return result
}
func assertAudit(t *testing.T, dir, name, want string) {
	t.Helper()
	if got := recoveryAudits(t, dir)[name]; got != want {
		t.Fatalf("%s entries=%q, want %q", name, got, want)
	}
}

// The wrapper only selects a boundary from the candidate record. All persistence
// is performed by the real SQLite Session.Apply; no product fault hooks are used.
type recoveryStore struct {
	durable.Store
	scenario string
}

func (s recoveryStore) Acquire(c context.Context, id string) (durable.Session, error) {
	ss, err := s.Store.Acquire(c, id)
	if err != nil {
		return nil, err
	}
	return &recoverySession{Session: ss, scenario: s.scenario}, nil
}

type recoverySession struct {
	durable.Session
	scenario string
	fired    bool
}

func (s *recoverySession) Apply(c context.Context, b durable.ChangeBatch) (durable.CommitAck, error) {
	r, err := s.Session.Load(c)
	if err != nil {
		return durable.CommitAck{}, err
	}
	if _, err = b.ApplyTo(&r); err != nil {
		return durable.CommitAck{}, err
	}
	match := false
	switch s.scenario {
	case "retry-failed", "retry-exhausted":
		want := 1
		if s.scenario == "retry-exhausted" {
			want = 2
		}
		match = retryIndex(r, durable.AttemptFailed) == want
	case "stream-append-after":
		for _, stream := range r.Streams {
			match = match || stream.LastSequence == 1 && stream.Cursor != nil
		}
	case "message-before", "message-after", "uncertain-message":
		for _, m := range r.Messages {
			match = match || m.Sequence == 1 && m.State == durable.AttemptConfirmed
		}
	default:
		for _, scope := range r.Scopes {
			_, ok := scope.Nodes["work"]
			match = match || ok
		}
	}
	match = match && !s.fired
	if match && strings.HasSuffix(s.scenario, "before") {
		os.Exit(recoveryExit)
	}
	ack, err := s.Session.Apply(c, b)
	if err != nil {
		return durable.CommitAck{}, err
	}
	if match {
		s.fired = true
		if strings.HasPrefix(s.scenario, "uncertain-") {
			return durable.CommitAck{}, uncertainApply
		}
		os.Exit(recoveryExit)
	}
	return ack, nil
}

func TestSQLiteRecoveryHelper(t *testing.T) {
	dir := os.Getenv("SQLITE_RECOVERY_DIR")
	if dir == "" {
		return
	}
	scenario, phase := os.Getenv("SQLITE_RECOVERY_CASE"), os.Getenv("SQLITE_RECOVERY_PHASE")
	s, err := New(filepath.Join(dir, "runs.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var backend durable.Store = s
	if phase == "run" {
		backend = recoveryStore{Store: s, scenario: scenario}
	}
	rt, err := durable.New(backend, durable.NewCodecs())
	if err != nil {
		t.Fatal(err)
	}
	g, key := ordinaryRecoveryGraph(t, dir, scenario, phase)
	if strings.Contains(scenario, "message") || strings.HasPrefix(scenario, "stream-") {
		g = streamRecoveryGraph(t, dir)
	}
	c, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	opts := durable.RunOptions{RunID: "recovery", Version: "sqlite-recovery/1"}
	var e *pulse.Execution
	if phase == "run" {
		e, err = rt.Run(c, g, pulse.Inputs{}, opts)
	} else {
		e, err = rt.Resume(c, g, pulse.Inputs{}, opts)
	}
	if phase == "run" && strings.HasPrefix(scenario, "uncertain-") {
		var checkpoint *pulse.CheckpointError
		if !errors.Is(err, uncertainApply) || !errors.As(err, &checkpoint) || e == nil || e.Snapshot().Graph.Status != pulse.GraphStatusFailed {
			t.Fatalf("committed Apply error was swallowed: %v", err)
		}
		os.Exit(recoveryExit)
	}
	if scenario == "retry-exhausted" && phase == "resume" {
		if err == nil {
			t.Fatal("exhausted Retry succeeded")
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(scenario) > 0 && !strings.Contains(scenario, "message") && !strings.HasPrefix(scenario, "stream-") {
		value, state, err := pulse.ReadOutput(e, key)
		if err != nil || state != pulse.SlotStateReady || value != 12 {
			t.Fatalf("output=%d state=%s error=%v", value, state, err)
		}
	}
	if phase == "run" {
		t.Fatal("fault boundary was not reached")
	}
}

func ordinaryRecoveryGraph(t testing.TB, dir, scenario, phase string) (*pulse.Graph, pulse.Key[int]) {
	t.Helper()
	g, err := pulse.New("sqlite-chain", "")
	if err != nil {
		t.Fatal(err)
	}
	one, two, result := pulse.NewKey[int]("one"), pulse.NewKey[int]("two"), pulse.NewKey[int]("result")
	add := func(n *pulse.Node) {
		if err := g.Add(n); err != nil {
			t.Fatal(err)
		}
	}
	add(pulse.NewNode("first", "", nil, pulse.Provides(one), func(rc *pulse.RunCtx) error {
		if err := audit(dir, "first", "call\n"); err != nil {
			return err
		}
		return pulse.Set(rc, one, 10)
	}))
	add(pulse.NewNode("work", "", pulse.Requires(one), pulse.Provides(two), func(rc *pulse.RunCtx) error {
		if err := audit(dir, "work", "call\n"); err != nil {
			return err
		}
		if (scenario == "retry-failed" && phase == "run") || scenario == "retry-exhausted" {
			return errors.New("retryable business failure")
		}
		v, err := pulse.Get(rc, one)
		if err != nil {
			return err
		}
		return pulse.Set(rc, two, v+1)
	}))
	add(pulse.NewNode("tail", "", pulse.Requires(two), pulse.Provides(result), func(rc *pulse.RunCtx) error {
		if err := audit(dir, "tail", "call\n"); err != nil {
			return err
		}
		v, err := pulse.Get(rc, two)
		if err != nil {
			return err
		}
		return pulse.Set(rc, result, v+1)
	}))
	if err := g.DeclareOutput(pulse.NewPort(result)); err != nil {
		t.Fatal(err)
	}
	if strings.HasPrefix(scenario, "sub-") || strings.HasPrefix(scenario, "retry-") {
		parent, err := pulse.New("sqlite-parent", "")
		if err != nil {
			t.Fatal(err)
		}
		out := pulse.NewKey[int]("out")
		var aspects []pulse.Aspect
		if strings.HasPrefix(scenario, "retry-") {
			aspects = append(aspects, pulse.Retry(2, 0))
		}
		if err := pulse.Sub(parent, "mount", "", []pulse.SubBind{pulse.Out(result, out)}, g, aspects...); err != nil {
			t.Fatal(err)
		}
		if err := parent.DeclareOutput(pulse.NewPort(out)); err != nil {
			t.Fatal(err)
		}
		return parent, out
	}
	return g, result
}

func streamRecoveryGraph(t testing.TB, dir string) *pulse.Graph {
	t.Helper()
	g, err := pulse.New("sqlite-stream", "")
	if err != nil {
		t.Fatal(err)
	}
	input := pulse.NewKey[pulse.Stream[int]]("input")
	if err := g.Add(pulse.NewNode("source", "", nil, pulse.Provides(input), func(rc *pulse.RunCtx) error {
		if err := audit(dir, "source", "call\n"); err != nil {
			return err
		}
		w, err := pulse.OpenStream(rc, input, pulse.WithBuffer(4), pulse.WithStreamSource("integers", true))
		if err != nil {
			return err
		}
		pos := w.Position()
		if pos.Sequence > 0 && pos.Cursor != int(pos.Sequence) {
			return fmt.Errorf("lost cursor: %+v", pos)
		}
		if err := audit(dir, "cursor", fmt.Sprintf("%d\n", pos.Sequence)); err != nil {
			return err
		}
		for v := int(pos.Sequence) + 1; v <= 3; v++ {
			if err := w.SendAt(v, v); err != nil {
				return err
			}
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := g.Add(pulse.NewNode("processor", "", pulse.Requires(input), nil, func(rc *pulse.RunCtx) error {
		stream, err := pulse.Get(rc, input)
		if err != nil {
			return err
		}
		for {
			delivery, err := stream.Receive(rc)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			if err := pulse.HandleMessage(rc, delivery, func(rc *pulse.RunCtx, value int) error {
				if err := audit(dir, "message", fmt.Sprintf("%d\n", value)); err != nil {
					return err
				}
				sum, _, err := pulse.GetMessageState[int](rc, "sum")
				if err != nil {
					return err
				}
				return pulse.SetMessageState(rc, "sum", sum+value)
			}); err != nil {
				return err
			}
		}
	})); err != nil {
		t.Fatal(err)
	}
	return g
}
func assertStreamBoundary(t *testing.T, r durable.RunRecord, scenario string) {
	t.Helper()
	if len(r.Streams) != 1 {
		t.Fatalf("streams=%d", len(r.Streams))
	}
	for _, s := range r.Streams {
		if s.Cursor == nil || string(s.Cursor.Data) != fmt.Sprint(s.LastSequence) {
			t.Fatal("source cursor and append were not atomic")
		}
		if scenario == "stream-append-after" && (s.LastSequence != 1 || s.AckedThrough != 0 || len(s.Pending) != 1) {
			t.Fatalf("wrong append boundary: %+v", s)
		}
		if strings.Contains(scenario, "message") {
			ack := uint64(1)
			if scenario == "message-before" {
				ack = 0
			}
			if s.AckedThrough != ack {
				t.Fatalf("ack=%d want %d", s.AckedThrough, ack)
			}
			if ack == 1 {
				if len(r.Managed) != 1 {
					t.Fatal("confirmed message lost managed state")
				}
				for _, m := range r.Managed {
					if len(m.State) != 1 || string(m.State[0].Data) != "1" {
						t.Fatal("message state and ack were not atomic")
					}
				}
			}
		}
	}
}
func assertFinishedStream(t *testing.T, r durable.RunRecord) {
	t.Helper()
	for _, s := range r.Streams {
		if s.LastSequence != 3 || s.AckedThrough != 3 || len(s.Pending) != 0 || s.Stream.Terminal != "completed" || s.Cursor == nil || string(s.Cursor.Data) != "3" {
			t.Fatalf("incomplete recovered stream: %+v", s)
		}
	}
	if len(r.Managed) != 1 {
		t.Fatal("missing managed state")
	}
	for _, m := range r.Managed {
		if len(m.State) != 1 || string(m.State[0].Data) != "6" {
			t.Fatalf("sum was lost: %+v", m)
		}
	}
	for _, m := range r.Messages {
		if m.State != durable.AttemptConfirmed {
			t.Fatal("unconfirmed message after recovery")
		}
	}
}

// Observe entry into Apply without a scheduler-delay guess. The concrete session
// mutex is held by Apply, and its Store connection is checked out for BEGIN.
func awaitSQLiteApply(t *testing.T, ss *session) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		locked := !ss.mu.TryLock()
		if !locked {
			ss.mu.Unlock()
		}
		if locked && ss.store.db.Stats().InUse == 1 {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("Apply did not enter its database operation")
		default:
			runtime.Gosched()
		}
	}
}

func TestSQLiteBusyApplyCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.sqlite")
	writer, blocker := openStore(t, path), openStore(t, path)
	ss := acquire(t, writer, "cancelled-run")
	old, batches := runtimeBatches(t, "cancelled-run")
	if err := ss.Initialize(t.Context(), old); err != nil {
		t.Fatal(err)
	}
	// A separate RunID owns this connection's independent write transaction;
	// durable run ownership is not the source of contention.
	acquire(t, blocker, "blocking-run")
	conn, err := blocker.db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(t.Context(), "BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	held := true
	release := func() {
		if held {
			_, err := conn.ExecContext(context.Background(), "ROLLBACK")
			if err != nil {
				t.Error(err)
			}
			held = false
		}
	}
	defer release()
	c, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := ss.Apply(c, batches[0]); done <- err }()
	awaitSQLiteApply(t, ss.(*session))
	started := time.Now()
	cancel()
	var saveErr error
	select {
	case saveErr = <-done:
	case <-time.After(7 * time.Second):
		// Always release the SQL lock before teardown so even a cancellation
		// defect cannot leave Close waiting indefinitely on this test's lock.
		release()
		select {
		case saveErr = <-done:
		case <-time.After(7 * time.Second):
			t.Fatal("Apply remained stuck after releasing SQLite write lock")
		}
		t.Errorf("cancelled busy Apply exceeded 7s finite bound (configured busy timeout is 5s)")
	}
	elapsed := time.Since(started)
	t.Logf("busy Apply cancellation returned in %s: %v", elapsed, saveErr)
	if !errors.Is(saveErr, context.Canceled) || errors.Is(saveErr, durable.ErrBusy) || errors.Is(saveErr, durable.ErrConflict) {
		t.Errorf("busy cancellation classification: %v", saveErr)
	}
	// Check authority before releasing the blocking transaction or issuing a new
	// write. A failed/uncertain Apply is never blindly retried.
	got, err := ss.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, old) {
		t.Fatal("cancelled Apply changed authoritative record")
	}
	release()
	if _, err := ss.Apply(t.Context(), batches[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := batches[0].ApplyTo(&old); err != nil {
		t.Fatal(err)
	}
	got, err = ss.Load(t.Context())
	if err != nil || !reflect.DeepEqual(got, old) {
		t.Fatalf("new Apply after proven noncommit: revision=%d err=%v", got.Revision, err)
	}
}

func TestSQLiteConcurrentOperationsAndClose(t *testing.T) {
	for iteration := 0; iteration < 20; iteration++ {
		path := filepath.Join(t.TempDir(), "runs.sqlite")
		s := openStore(t, path)
		ss := acquire(t, s, "close-run")
		initial, batches := runtimeBatches(t, "close-run")
		if err := ss.Initialize(t.Context(), initial); err != nil {
			t.Fatal(err)
		}
		// Occupy the only connection as a deterministic barrier: Apply has taken
		// its session mutex before Load and both Close operations are released.
		conn, err := s.db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		c, cancel := context.WithCancel(t.Context())
		defer cancel()
		type result struct {
			name     string
			err      error
			revision uint64
		}
		done := make(chan result, 4)
		go func() { _, err := ss.Apply(c, batches[0]); done <- result{name: "apply", err: err} }()
		awaitSQLiteApply(t, ss.(*session))
		start := make(chan struct{})
		ready := make(chan struct{}, 3)
		go func() {
			ready <- struct{}{}
			<-start
			r, err := ss.Load(c)
			done <- result{name: "load", err: err, revision: r.Revision}
		}()
		go func() { ready <- struct{}{}; <-start; done <- result{name: "session-close", err: ss.Close()} }()
		go func() { ready <- struct{}{}; <-start; done <- result{name: "store-close", err: s.Close()} }()
		for n := 0; n < 3; n++ {
			<-ready
		}
		close(start)
		if iteration%2 == 0 {
			cancel()
		}
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
		deadline := time.NewTimer(5 * time.Second)
		for n := 0; n < 4; n++ {
			select {
			case r := <-done:
				if strings.HasSuffix(r.name, "close") {
					if r.err != nil {
						t.Errorf("%s: %v", r.name, r.err)
					}
				} else if r.err != nil && !errors.Is(r.err, os.ErrClosed) && !errors.Is(r.err, context.Canceled) {
					t.Errorf("%s unexpected result: %v", r.name, r.err)
				}
				if r.name == "load" && r.err == nil && r.revision != 1 && r.revision != 2 {
					t.Errorf("successful Load returned revision %d", r.revision)
				}
			case <-deadline.C:
				cancel()
				t.Fatal("Load/Apply/Session.Close/Store.Close deadlocked")
			}
		}
		deadline.Stop()
		cancel()
		if _, err := ss.Load(t.Context()); !errors.Is(err, os.ErrClosed) {
			t.Errorf("Load after Close: %v", err)
		}
		if _, err := ss.Apply(t.Context(), batches[1]); !errors.Is(err, os.ErrClosed) {
			t.Errorf("Apply after Close: %v", err)
		}
	}
}
