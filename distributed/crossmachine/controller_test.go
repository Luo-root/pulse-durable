package crossmachine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/Luo-root/pulse-durable/distributed/systemtest"
)

type fakeStore struct {
	tasks       map[string]TaskEvidence
	owners      map[string]string
	ps          []*fakeProcess
	overflowBad bool
	replay      bool
}
type fakeProcess struct {
	db               *fakeStore
	id               string
	pid              int
	events           []systemtest.Event
	released, exited bool
	exitErr          error
	cap              int
	commands         []string
}

func (p *fakeProcess) emit(k, id string, epoch int64) {
	p.events = append(p.events, systemtest.Event{Kind: k, PID: p.pid, Worker: d.Worker{ID: p.id, BootID: "boot-" + p.id}, RunID: id, Epoch: epoch, Reason: "ok"})
}
func (p *fakeProcess) Send(cmd string) error {
	p.commands = append(p.commands, cmd)
	switch cmd {
	case "start":
		n := 0
		// 稳定的任务顺序，与确定性的模拟数据库领取队列匹配。
		for i := 0; i < 100; i++ {
			id := fmt.Sprintf("run-%04d", i)
			s, ok := p.db.tasks[id]
			if !ok || s.Status != d.Queued {
				continue
			}
			s.Status = d.Running
			s.Epoch = 1
			s.Revision = 1
			p.db.tasks[id] = s
			p.db.owners[id] = p.id
			p.emit("owned", id, 1)
			p.emit("prefix", id, 1)
			p.emit("suffix", id, 1)
			n++
			if n == p.cap {
				break
			}
		}
	case "release":
		p.released = true
		for id, s := range p.db.tasks {
			owner := p.db.owners[id]
			dead := false
			for _, other := range p.db.ps {
				if other.id == owner && other.exited {
					dead = true
				}
			}
			if s.Status == d.Queued || owner == p.id || dead {
				if dead {
					s.Epoch++
					p.emit("owned", id, s.Epoch)
					if p.db.replay {
						p.emit("prefix", id, s.Epoch)
					}
					p.emit("suffix", id, s.Epoch)
				}
				if s.Epoch == 0 {
					s.Epoch = 1
					p.emit("prefix", id, 1)
					p.emit("suffix", id, 1)
				}
				s.Status = d.Succeeded
				s.Revision = 2
				p.db.tasks[id] = s
				p.emit("finish", id, s.Epoch)
			}
		}
	case "drain":
		p.emit("drained", "", 0)
		p.exited = true
	case "probe_finish":
		p.emit("finish_held", "", 0)
	default:
		return errors.New("bad command")
	}
	return nil
}
func (p *fakeProcess) Events() []systemtest.Event {
	return append([]systemtest.Event(nil), p.events...)
}
func (p *fakeProcess) Exited() (bool, error) { return p.exited, p.exitErr }
func (p *fakeProcess) Close() error          { p.exited = true; return nil }
func (s *fakeStore) Submit(_ context.Context, id string) error {
	s.tasks[id] = TaskEvidence{ID: id, Status: d.Queued}
	return nil
}
func (s *fakeStore) Get(_ context.Context, id string) (TaskEvidence, error) {
	v, ok := s.tasks[id]
	if !ok {
		return v, errors.New("missing")
	}
	if s.overflowBad && id == "overflow" {
		v.Status = d.Running
		v.Epoch = 1
	}
	return v, nil
}
func (s *fakeStore) Capacity(context.Context) (map[string]int, error) {
	m := map[string]int{}
	for id, v := range s.tasks {
		if v.Status == d.Running {
			m[s.owners[id]]++
		}
	}
	return m, nil
}
func testConfig(n int) Config {
	return Config{Namespace: "unit-only", BuildSHA: "parent-sha", Version: "v1", Capacity: 2, Timeout: time.Second, Poll: time.Millisecond, Scenario: "normal", Hosts: []Host{{Label: "windows", OS: "windows", Workers: n, Command: Command{Path: "fake"}}, {Label: "vps", OS: "linux", Workers: n, Command: Command{Path: "fake"}}}}
}
func fakeController(db *fakeStore) Controller {
	return Controller{Launch: func(_ context.Context, _ Command, args, env []string) (Process, error) {
		id := ""
		for i, a := range args {
			if a == "-worker-id" {
				id = args[i+1]
			}
		}
		// 不同主机的 PID 相同是合法的。启动标识仍须唯一。
		pid := 100 + len(db.ps)%(len(db.ps)/2+1)
		p := &fakeProcess{db: db, id: id, pid: pid, cap: 2}
		p.emit("ready", "", 0)
		db.ps = append(db.ps, p)
		return p, nil
	}, Kill: func(_ context.Context, w WorkerEvidence) error {
		for _, p := range db.ps {
			if p.id == w.ID && "boot-"+p.id == w.Boot && p.pid == w.PID {
				p.exited = true
				return nil
			}
		}
		return errors.New("not found")
	}}
}
func newFake() *fakeStore {
	return &fakeStore{tasks: map[string]TaskEvidence{}, owners: map[string]string{}}
}
func TestNormalMatrix(t *testing.T) {
	for _, n := range []int{1, 2, 4} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			db := newFake()
			ctl := fakeController(db)
			// PID 只在主机内唯一；有意在两端使用相同的工作进程偏移。
			base := ctl.Launch
			ctl.Launch = func(ctx context.Context, c Command, args, env []string) (Process, error) {
				p, e := base(ctx, c, args, env)
				fp := p.(*fakeProcess)
				fp.pid = 100 + (len(db.ps)-1)%n
				fp.events[0].PID = fp.pid
				return p, e
			}
			r := ctl.Run(context.Background(), testConfig(n), db)
			if r.Result != "PASS" {
				t.Fatalf("%+v", r)
			}
			if len(r.Workers) != 2*n || len(r.Tasks) != 4*n+1 {
				t.Fatalf("missing evidence: %+v", r)
			}
			for _, p := range db.ps {
				if strings.Join(p.commands, ",") != "start,release,drain" {
					t.Fatal(p.commands)
				}
			}
			if r.Checks["checkpoint_recovery"] != "NOT_RUN" {
				t.Fatal("normal run fabricated kill evidence")
			}
		})
	}
}
func TestCheckpointRecovery(t *testing.T) {
	db := newFake()
	c := testConfig(1)
	c.Scenario = "checkpoint-kill"
	c.KillHost = "vps"
	r := fakeController(db).Run(context.Background(), c, db)
	if r.Result != "PASS" || r.Checks["checkpoint_recovery"] != "PASS" {
		t.Fatalf("%+v", r)
	}
}
func TestRejectPrefixReplay(t *testing.T) {
	db := newFake()
	db.replay = true
	c := testConfig(1)
	c.Scenario = "checkpoint-kill"
	c.KillHost = "vps"
	r := fakeController(db).Run(context.Background(), c, db)
	if r.Result != "FAIL" || r.Reason != "prefix_replayed" {
		t.Fatalf("%+v", r)
	}
}
func TestCapacityFailure(t *testing.T) {
	db := newFake()
	db.overflowBad = true
	r := fakeController(db).Run(context.Background(), testConfig(1), db)
	if r.Result != "FAIL" || r.Reason != "overflow_claimed" {
		t.Fatalf("%+v", r)
	}
}
func TestKillRequiresCallback(t *testing.T) {
	db := newFake()
	c := testConfig(1)
	c.Scenario = "checkpoint-kill"
	c.KillHost = "vps"
	ctl := fakeController(db)
	ctl.Kill = nil
	r := ctl.Run(context.Background(), c, db)
	if r.Result != "NOT_RUN" || len(db.ps) != 0 {
		t.Fatalf("%+v", r)
	}
}
func TestDuplicateBootRejected(t *testing.T) {
	db := newFake()
	ctl := fakeController(db)
	base := ctl.Launch
	ctl.Launch = func(ctx context.Context, c Command, args, env []string) (Process, error) {
		p, e := base(ctx, c, args, env)
		p.(*fakeProcess).events[0].Worker.BootID = "duplicate"
		return p, e
	}
	r := ctl.Run(context.Background(), testConfig(1), db)
	if r.Reason != "duplicate_identity" {
		t.Fatalf("%+v", r)
	}
}
func TestEnvironmentAndReportRedaction(t *testing.T) {
	t.Setenv("CM_ADMIN_SECRET", "postgres://admin:secret")
	t.Setenv("CM_WORKER_SECRET", "postgres://worker:secret")
	t.Setenv("PULSE_DISTRIBUTED_TEST_DSN", "inherited-secret")
	env, err := commandEnvironment(Command{Env: map[string]string{"PULSE_DISTRIBUTED_TEST_DSN": "CM_WORKER_SECRET"}}, []string{"CM_ADMIN_SECRET", "CM_WORKER_SECRET"})
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range env {
		if strings.HasPrefix(v, "CM_ADMIN_SECRET=") || strings.HasPrefix(v, "CM_WORKER_SECRET=") || strings.Contains(v, "inherited-secret") {
			t.Fatal("inherited credential")
		}
	}
	found := false
	for _, v := range env {
		if v == "PULSE_DISTRIBUTED_TEST_DSN=postgres://worker:secret" {
			found = true
		}
	}
	if !found {
		t.Fatal("worker injection missing")
	}
	db := newFake()
	r := fakeController(db).Run(context.Background(), testConfig(1), db)
	wire, _ := json.Marshal(r)
	if strings.Contains(string(wire), "secret") || strings.Contains(string(wire), "payload") {
		t.Fatal("report leaked data")
	}
}

// 使用真实子进程测试标准输入输出 JSON 管道，不使用 SSH 或数据库。
func TestFakeProcessEntry(t *testing.T) {
	if os.Getenv("CM_FAKE_PROCESS") != "yes" {
		return
	}
	defer os.Exit(0) // 抑制 Go 测试运行器附加的非 JSON PASS 尾行
	_ = json.NewEncoder(os.Stdout).Encode(systemtest.Event{Kind: "ready", PID: os.Getpid(), Worker: d.Worker{ID: "fake", BootID: "fake-boot"}})
	dec := json.NewDecoder(os.Stdin)
	for {
		var cmd string
		if dec.Decode(&cmd) != nil {
			return
		}
		if cmd == "probe_finish" {
			_ = json.NewEncoder(os.Stdout).Encode(systemtest.Event{Kind: "finish_held"})
		}
		if cmd == "drain" {
			_ = json.NewEncoder(os.Stdout).Encode(systemtest.Event{Kind: "drained", Reason: "ok"})
			return
		}
	}
}
func TestExecJSONPipe(t *testing.T) {
	t.Setenv("CM_FAKE_PARENT", "yes")
	env, e := commandEnvironment(Command{Env: map[string]string{"CM_FAKE_PROCESS": "CM_FAKE_PARENT"}}, nil)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p, e := Launch(ctx, Command{Path: os.Args[0], Args: []string{"-test.run=^TestFakeProcessEntry$"}}, nil, env)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	wait := func(kind string) {
		t.Helper()
		for {
			for _, v := range p.Events() {
				if v.Kind == kind {
					return
				}
			}
			if done, _ := p.Exited(); done {
				t.Fatal("early exit")
			}
			select {
			case <-ctx.Done():
				t.Fatal("event timeout")
			case <-time.After(time.Millisecond):
			}
		}
	}
	wait("ready")
	if ProbeFinish(p) != nil {
		t.Fatal("probe")
	}
	wait("finish_held")
	if p.Send("drain") != nil {
		t.Fatal("drain")
	}
	wait("drained")
}
