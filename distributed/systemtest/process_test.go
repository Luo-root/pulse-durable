package systemtest

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/Luo-root/pulse-durable/distributed/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestWorkerProcess(t *testing.T) {
	if os.Getenv("PULSE_SYSTEMTEST_CHILD") != "yes" {
		t.Skip("subprocess entry only")
	}
	var c Config
	if json.Unmarshal([]byte(os.Getenv("PULSE_SYSTEMTEST_CONFIG")), &c) != nil {
		t.Fatal("invalid child config")
	}
	if err := Run(context.Background(), c, os.Stdin, os.Stdout); err != nil {
		t.Fatal("child worker failed")
	}
}

type process struct {
	cmd    *exec.Cmd
	input  io.WriteCloser
	mu     sync.Mutex
	events []Event
	done   chan error
}

func (p *process) send(t *testing.T, s string) {
	t.Helper()
	if err := json.NewEncoder(p.input).Encode(s); err != nil {
		t.Fatal("child control failed")
	}
}
func (p *process) snapshot() []Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]Event(nil), p.events...)
}
func (p *process) has(kind string) bool {
	for _, e := range p.snapshot() {
		if e.Kind == kind {
			return true
		}
	}
	return false
}
func start(t *testing.T, dsn, ns, id, version string, capacity int, hold bool) *process {
	t.Helper()
	cfg, _ := json.Marshal(Config{Namespace: ns, ID: id, Version: version, Capacity: capacity, Duration: 90 * time.Second, HoldFinish: hold})
	cmd := exec.Command(os.Args[0], "-test.run=^TestWorkerProcess$", "-test.timeout=100s")
	// Remove inherited credential/control keys rather than relying on duplicate env precedence.
	for _, v := range os.Environ() {
		if !strings.HasPrefix(v, "PULSE_SYSTEMTEST_") && !strings.HasPrefix(v, "PULSE_DISTRIBUTED_TEST_DSN=") {
			cmd.Env = append(cmd.Env, v)
		}
	}
	cmd.Env = append(cmd.Env, "PULSE_SYSTEMTEST_CHILD=yes", "PULSE_SYSTEMTEST_CONFIG="+string(cfg), "PULSE_DISTRIBUTED_TEST_DSN="+dsn, "PULSE_DISTRIBUTED_TEST_ALLOW=yes")
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	p := &process{cmd: cmd, input: input, done: make(chan error, 1)}
	if err = cmd.Start(); err != nil {
		t.Fatal("cannot start child")
	}
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		s := bufio.NewScanner(output)
		for s.Scan() {
			var e Event
			if json.Unmarshal(s.Bytes(), &e) == nil && e.Kind != "" {
				p.mu.Lock()
				p.events = append(p.events, e)
				p.mu.Unlock()
			}
		}
	}()
	go func() { err := cmd.Wait(); <-readDone; p.done <- err }()
	t.Cleanup(func() {
		_ = input.Close()
		_ = cmd.Process.Kill()
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			t.Error("child did not exit")
		}
	})
	await(t, func() bool { return p.has("ready") }, "child ready")
	return p
}
func await(t *testing.T, f func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if f() {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatalf("timeout: %s", what)
}

type fixture struct {
	c       *postgres.Coordinator
	admin   *pgxpool.Pool
	dsn, ns string
}

func provision(t *testing.T) fixture {
	t.Helper()
	dsn := os.Getenv("PULSE_DISTRIBUTED_TEST_DSN")
	if dsn == "" || os.Getenv("PULSE_DISTRIBUTED_TEST_ALLOW") != "yes" {
		t.Skip("not run: PULSE_DISTRIBUTED_TEST_DSN and PULSE_DISTRIBUTED_TEST_ALLOW=yes required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid test DSN")
	}
	// Administrative DDL is limited to the explicitly authorized Docker database.
	c := cfg.ConnConfig
	allowedDatabase := c.Database == "durable_distributed_test" || c.Database == "durable_dist_security_20261025"
	if c.Host != "127.0.0.1" || c.Port != 55432 || !allowedDatabase || c.User != "durable_test" {
		t.Fatal("provisioning restricted to authorized local Docker database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	admin, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal("admin connection failed")
	}
	t.Cleanup(admin.Close)
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	role := "pulse_sys_" + suffix
	ns := "systemtest-" + suffix
	if _, err = admin.Exec(ctx, "CREATE ROLE "+role+" LOGIN PASSWORD 'systemtest-local-only'"); err != nil {
		t.Fatal("isolated role creation failed")
	}
	// Deliberately no DROP/DELETE cleanup: preserve evidence, never alter shared grants.
	if err = postgres.Migrate(ctx, dsn, postgres.Grant{Role: role, Namespace: ns, Permissions: []string{"query", "submit", "worker", "cancel"}}); err != nil {
		t.Fatal(err)
	}
	u := url.URL{Scheme: "postgres", Host: net.JoinHostPort(c.Host, strconv.Itoa(int(c.Port))), Path: "/" + c.Database, User: url.UserPassword(role, "systemtest-local-only")}
	q := u.Query()
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()
	backend, err := postgres.New(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(backend.Close)
	return fixture{backend, admin, u.String(), ns}
}
func (f fixture) submit(t *testing.T, id, version string) {
	t.Helper()
	task, err := Task(context.Background(), f.ns, id, version)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.c.Submit(context.Background(), task); err != nil {
		t.Fatal(err)
	}
}
func (f fixture) status(t *testing.T, id string) d.Snapshot {
	t.Helper()
	s, err := f.c.Get(context.Background(), d.Identity{Namespace: f.ns, RunID: id})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func (f fixture) success(t *testing.T, id string) {
	t.Helper()
	await(t, func() bool { return f.status(t, id).Status == d.Succeeded }, "authoritative success "+id)
}
func mutation(s string) d.Mutation {
	h := sha256.Sum256([]byte(s))
	return d.Mutation{ID: s, Digest: hex.EncodeToString(h[:])}
}

// TestMultiProcess deliberately fills every slot with a blocked suffix before
// releasing any process. This makes worker participation deterministic instead
// of depending on a fast polling worker not monopolizing short tasks.
func TestMultiProcess(t *testing.T) {
	for _, count := range []int{2, 4, 8} {
		t.Run(fmt.Sprintf("workers-%d", count), func(t *testing.T) {
			f := provision(t)
			const capacity = 2
			var ps []*process
			for i := 0; i < count*capacity; i++ {
				f.submit(t, fmt.Sprintf("run-%03d", i), "v1")
			}
			for i := 0; i < count; i++ {
				ps = append(ps, start(t, f.dsn, f.ns, fmt.Sprintf("w%d", i), "v1", capacity, false))
			}
			for _, p := range ps {
				p.send(t, "start")
			}
			await(t, func() bool {
				for _, p := range ps {
					n := 0
					for _, e := range p.snapshot() {
						if e.Kind == "suffix" {
							n++
						}
					}
					if n != capacity {
						return false
					}
				}
				return true
			}, "every process fills capacity")
			pids := map[int]bool{}
			boots := map[string]bool{}
			for _, p := range ps {
				for _, e := range p.snapshot() {
					if e.Kind == "ready" {
						if pids[e.PID] || boots[e.Worker.BootID] || e.Worker.BootID == "" {
							t.Fatal("process/boot identity not independent")
						}
						pids[e.PID] = true
						boots[e.Worker.BootID] = true
						t.Logf("worker evidence: pid=%d boot=%s", e.PID, e.Worker.BootID)
					}
				}
			}
			// A further queued task must remain unclaimed while all business slots block.
			f.submit(t, "overflow", "v1")
			time.Sleep(300 * time.Millisecond)
			if s := f.status(t, "overflow"); s.Status != d.Queued || s.Epoch != 0 {
				t.Fatal("worker hoarded a task beyond capacity")
			}
			auditCtx, auditCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer auditCancel()
			rows, err := f.admin.Query(auditCtx, `SELECT worker_id,count(*) FROM pulse_durable_distributed.runs WHERE namespace=$1 AND status='running' GROUP BY worker_id`, f.ns)
			if err != nil {
				t.Fatal("capacity audit query failed")
			}
			defer rows.Close() // also closes rows on a fatal scan/assertion
			seen := 0
			for rows.Next() {
				var id string
				var n int
				if err = rows.Scan(&id, &n); err != nil {
					t.Fatal(err)
				}
				if n != capacity {
					t.Fatalf("worker %s owns %d", id, n)
				}
				seen++
			}
			rows.Close()
			if rows.Err() != nil || seen != count {
				t.Fatal("authoritative worker capacity mismatch")
			}
			for _, p := range ps {
				p.send(t, "release")
			}
			for i := 0; i < count*capacity; i++ {
				f.success(t, fmt.Sprintf("run-%03d", i))
			}
			f.success(t, "overflow")
			for _, p := range ps {
				p.send(t, "drain")
				await(t, func() bool { return p.has("drained") }, "drain")
			}
			calls := map[string]int{}
			finishes := map[string]int{}
			for _, p := range ps {
				for _, e := range p.snapshot() {
					if e.Kind == "prefix" || e.Kind == "suffix" {
						calls[e.RunID+"/"+e.Kind]++
					}
					if e.Kind == "finish" && e.Reason == "ok" {
						finishes[e.RunID]++
					}
				}
			}
			for i := 0; i < count*capacity; i++ {
				id := fmt.Sprintf("run-%03d", i)
				if calls[id+"/prefix"] != 1 || calls[id+"/suffix"] != 1 || finishes[id] != 1 {
					t.Fatalf("non-single result audit for %s: prefix=%d suffix=%d finish=%d", id, calls[id+"/prefix"], calls[id+"/suffix"], finishes[id])
				}
			}
		})
	}
}

func TestKillRecovery(t *testing.T) {
	for _, window := range []string{"suffix", "before_finish"} {
		t.Run(window, func(t *testing.T) {
			f := provision(t)
			f.submit(t, "recover", "v1")
			a := start(t, f.dsn, f.ns, "a", "v1", 1, window == "before_finish")
			a.send(t, "start")
			await(t, func() bool { return a.has("suffix") }, "prefix checkpoint and suffix entry")
			if window == "before_finish" {
				a.send(t, "release")
				await(t, func() bool { return a.has("before_finish") }, "final checkpoint before Finish")
			}
			old := f.status(t, "recover")
			if old.Status != d.Running || old.Revision == 0 {
				t.Fatal("missing persisted checkpoint under lease")
			}
			if window == "before_finish" {
				// A second controller round trip after the authority query proves
				// Finish is still held, not merely that it was entered earlier.
				a.send(t, "probe_finish")
				await(t, func() bool { return a.has("finish_held") }, "persistent Finish kill-window handshake")
			}
			if err := a.cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			b := start(t, f.dsn, f.ns, "b", "v1", 1, false)
			b.send(t, "release")
			b.send(t, "start")
			f.success(t, "recover")
			now := f.status(t, "recover")
			if now.Epoch <= old.Epoch {
				t.Fatal("takeover did not increment fencing epoch")
			}
			for _, e := range b.snapshot() {
				if e.Kind == "prefix" {
					t.Fatal("persisted prefix executed again")
				}
				if window == "before_finish" && e.Kind == "suffix" {
					t.Fatal("completed checkpoint executed business again")
				}
			}
			prefixes, suffixes := 0, 0
			for _, p := range []*process{a, b} {
				for _, e := range p.snapshot() {
					if e.Kind == "prefix" {
						prefixes++
					}
					if e.Kind == "suffix" {
						suffixes++
					}
					if e.RunID != "" {
						t.Logf("audit window=%s pid=%d kind=%s epoch=%d", window, e.PID, e.Kind, e.Epoch)
					}
				}
			}
			want := 2
			if window == "before_finish" {
				want = 1
			}
			if prefixes != 1 || suffixes != want {
				t.Fatalf("business attempts prefix=%d suffix=%d want suffix=%d", prefixes, suffixes, want)
			}
			b.send(t, "drain")
			await(t, func() bool { return b.has("drained") }, "recovery drain")
		})
	}
}
func TestVersionCancelDrain(t *testing.T) {
	f := provision(t)
	f.submit(t, "wrong-version", "v2")
	p := start(t, f.dsn, f.ns, "control", "v1", 1, false)
	p.send(t, "start")
	time.Sleep(300 * time.Millisecond)
	if s := f.status(t, "wrong-version"); s.Status != d.Queued || s.Epoch != 0 {
		t.Fatal("incompatible version claimed")
	}
	for _, e := range p.snapshot() {
		if e.Kind == "prefix" || e.Kind == "suffix" {
			t.Fatal("incompatible business invoked")
		}
	}
	f.submit(t, "cancel", "v1")
	await(t, func() bool { return p.has("suffix") }, "cancel task active")
	_, err := f.c.Cancel(context.Background(), d.Identity{Namespace: f.ns, RunID: "cancel"}, mutation("cancel"))
	if err != nil {
		t.Fatal(err)
	}
	p.send(t, "drain")
	p.send(t, "release")
	await(t, func() bool { return p.has("drained") }, "cancel drain")
	if f.status(t, "cancel").Status != d.Canceled {
		t.Fatal("Cancel overwritten by Finish/Drain")
	}
	if f.status(t, "wrong-version").Status != d.Queued {
		t.Fatal("Drain performed business cancel")
	}
}

// Drain must retain a live, non-canceled task beyond one full lease TTL while
// rejecting compatible work, then finish it without claiming the queued task.
func TestDrainRetainsLease(t *testing.T) {
	f := provision(t)
	f.submit(t, "active", "v1")
	p := start(t, f.dsn, f.ns, "drain-live", "v1", 1, false)
	p.send(t, "start")
	await(t, func() bool { return p.has("suffix") }, "non-canceled active task")
	before := f.status(t, "active")
	p.send(t, "drain")
	await(t, func() bool { return p.has("draining") }, "admission stopped ACK")
	for _, e := range p.snapshot() {
		if e.Kind == "draining" && e.Active != 1 {
			t.Fatal("drain ACK without active reservation")
		}
	}
	f.submit(t, "compatible-queued", "v1")
	until := time.Now().Add(2500 * time.Millisecond) // greater than configured 2s TTL
	for time.Now().Before(until) {
		if p.has("drained") {
			t.Fatal("Drain returned before active task completed")
		}
		s := f.status(t, "active")
		if s.Status != d.Running || s.Epoch != before.Epoch || !s.LeaseUntil.After(time.Now()) {
			t.Fatal("Drain lost active ownership")
		}
		q := f.status(t, "compatible-queued")
		if q.Status != d.Queued || q.Epoch != 0 {
			t.Fatal("Drain admitted compatible queued task")
		}
		time.Sleep(50 * time.Millisecond)
	}
	after := f.status(t, "active")
	if !after.LeaseUntil.After(before.LeaseUntil.Add(2 * time.Second)) {
		t.Fatal("no authoritative renewal across TTL during Drain")
	}
	p.send(t, "release")
	f.success(t, "active")
	await(t, func() bool { return p.has("drained") }, "successful live Drain")
	for _, e := range p.snapshot() {
		if e.Kind == "drained" && e.Reason != "ok" {
			t.Fatal("Drain did not return ok")
		}
	}
	q := f.status(t, "compatible-queued")
	if q.Status != d.Queued || q.Epoch != 0 {
		t.Fatal("Drain claimed pending work on completion")
	}
}

func TestTaskAssembly(t *testing.T) {
	task, err := Task(context.Background(), "unit", "run", "v1")
	if err != nil {
		t.Fatal(err)
	}
	r := &d.Registry{}
	if _, err = r.Register("systemtest", "v1", func() (d.Assembly, error) { return Assembly(nil) }); err != nil {
		t.Fatal(err)
	}
	if _, _, err = r.Resolve(context.Background(), task); err != nil {
		t.Fatal(err)
	}
}
