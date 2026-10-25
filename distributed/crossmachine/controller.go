package crossmachine

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	d "github.com/Luo-root/pulse-durable/distributed"
)

type Host struct {
	Label   string  `json:"label"`
	OS      string  `json:"os"` // Operator assertion, not remote attestation.
	Workers int     `json:"workers"`
	Command Command `json:"command"`
}
type Config struct {
	Namespace        string        `json:"namespace"`
	BuildSHA         string        `json:"build_sha"`
	Version          string        `json:"version"`
	Hosts            []Host        `json:"hosts"`
	Capacity         int           `json:"capacity"`
	Timeout          time.Duration `json:"timeout"` // JSON nanoseconds; CLI also accepts -timeout.
	Poll             time.Duration `json:"poll,omitempty"`
	Scenario         string        `json:"scenario"` // normal or checkpoint-kill
	KillHost         string        `json:"kill_host,omitempty"`
	AdminDSNEnv      string        `json:"admin_dsn_env"`
	WorkerDSNEnv     string        `json:"worker_dsn_env"`
	Prepare          bool          `json:"prepare"` // Explicit DDL authorization, never implied by run.
	TTL              time.Duration `json:"ttl,omitempty"`
	RenewInterval    time.Duration `json:"renew_interval,omitempty"`
	RenewTimeout     time.Duration `json:"renew_timeout,omitempty"`
	OperationTimeout time.Duration `json:"operation_timeout,omitempty"`
	PollMin          time.Duration `json:"poll_min,omitempty"`
	PollMax          time.Duration `json:"poll_max,omitempty"`
}

// Store observes only sanitized task metadata; implementations must not return payloads.
type Store interface {
	Submit(context.Context, string) error
	Get(context.Context, string) (TaskEvidence, error)
	Capacity(context.Context) (map[string]int, error)
}
type TaskEvidence struct {
	ID       string   `json:"run_id"`
	Status   d.Status `json:"status"`
	Epoch    int64    `json:"epoch"`
	Revision uint64   `json:"revision"`
}
type WorkerEvidence struct {
	Host   string          `json:"host_label"`
	OS     string          `json:"os"`
	ID     string          `json:"worker_id"`
	PID    int             `json:"pid"`
	Boot   string          `json:"boot_id"`
	Events []EpochEvidence `json:"events,omitempty"`
}
type EpochEvidence struct {
	Kind  string `json:"kind"`
	RunID string `json:"run_id"`
	Epoch int64  `json:"epoch"`
}
type Report struct {
	Result      string                   `json:"result"`
	Reason      string                   `json:"reason,omitempty"`
	BuildSHA    string                   `json:"build_sha"`
	Namespace   string                   `json:"namespace"`
	Scenario    string                   `json:"scenario"`
	Started     time.Time                `json:"started"`
	Duration    time.Duration            `json:"duration_ns"`
	Phases      map[string]time.Duration `json:"durations_ns"`
	Workers     []WorkerEvidence         `json:"workers"`
	Tasks       []TaskEvidence           `json:"tasks"`
	Checks      map[string]string        `json:"checks"`
	Limitations []string                 `json:"limitations"`
}

// Kill must kill only the observed test worker, and return after death is confirmed.
// For SSH never implement this by merely killing the local ssh client.
type Kill func(context.Context, WorkerEvidence) error
type Controller struct {
	Launch Launcher
	Kill   Kill
}
type running struct {
	p        Process
	evidence WorkerEvidence
	killed   bool
}

var safeArgument = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

func (c Config) Validate() error {
	if !safeArgument.MatchString(c.Namespace) || !safeArgument.MatchString(c.BuildSHA) || !safeArgument.MatchString(c.Version) || c.Capacity < 2 || c.Capacity > 1024 || c.Timeout <= 0 || len(c.Hosts) != 2 {
		return errors.New("invalid configuration")
	}
	if c.Scenario != "normal" && c.Scenario != "checkpoint-kill" {
		return errors.New("invalid scenario")
	}
	if c.Hosts[0].Workers != c.Hosts[1].Workers {
		return errors.New("expected symmetric worker counts")
	}
	n := c.Hosts[0].Workers
	if n != 1 && n != 2 && n != 4 {
		return errors.New("worker count must be 1, 2 or 4 per host")
	}
	if c.Hosts[0].Label == c.Hosts[1].Label {
		return errors.New("host labels must differ")
	}
	found := false
	for _, h := range c.Hosts {
		if !safeArgument.MatchString(h.Label) || !safeArgument.MatchString(h.OS) || h.Command.Path == "" {
			return errors.New("incomplete host")
		}
		for child, parent := range h.Command.Env {
			if c.AdminDSNEnv != "" && (strings.EqualFold(parent, c.AdminDSNEnv) || strings.EqualFold(child, c.AdminDSNEnv)) {
				return errors.New("administrator environment cannot be forwarded")
			}
		}
		if h.Label == c.KillHost {
			found = true
		}
	}
	if c.Scenario == "checkpoint-kill" && !found {
		return errors.New("kill host required")
	}
	return nil
}

func (ctl Controller) Run(parent context.Context, c Config, db Store) (report Report) {
	report = Report{Result: "NOT_RUN", BuildSHA: c.BuildSHA, Namespace: c.Namespace, Scenario: c.Scenario, Started: time.Now().UTC(), Phases: map[string]time.Duration{}, Checks: map[string]string{"capacity": "NOT_RUN", "all_succeeded": "NOT_RUN", "checkpoint_recovery": "NOT_RUN", "drain": "NOT_RUN"}, Limitations: []string{"Host labels, OS and build SHA are operator-supplied, not remotely attested.", "Harness timing settings are configurable; omitted settings use local defaults (TTL=2s, renew timeout=400ms), unsuitable for unmeasured high RTT.", "Failed SSH runs may need explicit remote PID/boot cleanup; closing SSH is not proof of remote exit."}}
	var ps []*running
	var ids []string
	defer func() {
		for _, p := range ps {
			_ = p.p.Close()
			for _, e := range p.p.Events() {
				if e.RunID != "" && (e.Kind == "prefix" || e.Kind == "suffix" || e.Kind == "owned" || e.Kind == "finish") {
					p.evidence.Events = append(p.evidence.Events, EpochEvidence{e.Kind, e.RunID, e.Epoch})
				}
			}
			report.Workers = append(report.Workers, p.evidence)
		}
		report.Duration = time.Since(report.Started)
	}()
	fail := func(code string) Report { report.Result = "FAIL"; report.Reason = code; return report }
	if c.Validate() != nil || db == nil {
		report.Reason = "invalid_configuration"
		return
	}
	if c.Scenario == "checkpoint-kill" && ctl.Kill == nil {
		report.Reason = "kill_callback_required"
		return
	}
	ctx, cancel := context.WithTimeout(parent, c.Timeout)
	defer cancel()
	poll := c.Poll
	if poll <= 0 {
		poll = 25 * time.Millisecond
	}
	await := func(f func() (bool, error)) error {
		ticker := time.NewTicker(poll)
		defer ticker.Stop()
		for {
			ok, err := f()
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
			select {
			case <-ctx.Done():
				return errors.New("deadline")
			case <-ticker.C:
			}
		}
	}
	has := func(p *running, kind string) bool {
		for _, e := range p.p.Events() {
			if e.Kind == kind {
				return true
			}
		}
		return false
	}
	waitEvent := func(p *running, kind string) error {
		return await(func() (bool, error) {
			if has(p, kind) {
				return true, nil
			}
			if done, _ := p.p.Exited(); done {
				return false, errors.New("early exit")
			}
			return false, nil
		})
	}
	launch := ctl.Launch
	if launch == nil {
		launch = Launch
	}
	phase := time.Now()
	for _, h := range c.Hosts {
		for i := 0; i < h.Workers; i++ {
			id := fmt.Sprintf("cm-h%d-w%d", len(ps)/h.Workers, i)
			env, err := commandEnvironment(h.Command, []string{c.AdminDSNEnv, c.WorkerDSNEnv})
			if err != nil {
				return fail("command_environment")
			}
			args := []string{"-namespace", c.Namespace, "-worker-id", id, "-version", c.Version, "-capacity", fmt.Sprint(c.Capacity), "-duration", c.Timeout.String()}
			for _, timing := range []struct {
				name  string
				value time.Duration
			}{
				{"ttl", c.TTL}, {"renew-interval", c.RenewInterval}, {"renew-timeout", c.RenewTimeout}, {"operation-timeout", c.OperationTimeout}, {"poll-min", c.PollMin}, {"poll-max", c.PollMax},
			} {
				if timing.value != 0 {
					args = append(args, "-"+timing.name, timing.value.String())
				}
			}
			p, err := launch(ctx, h.Command, args, env)
			if err != nil {
				return fail("launch")
			}
			r := &running{p: p, evidence: WorkerEvidence{Host: h.Label, OS: h.OS, ID: id}}
			ps = append(ps, r)
			if waitEvent(r, "ready") != nil {
				return fail("ready")
			}
			for _, e := range p.Events() {
				if e.Kind == "ready" {
					r.evidence.PID = e.PID
					r.evidence.Boot = e.Worker.BootID
					if e.Worker.ID != id {
						return fail("worker_identity")
					}
					break
				}
			}
			if r.evidence.PID <= 0 || r.evidence.Boot == "" {
				return fail("worker_identity")
			}
			for _, other := range ps[:len(ps)-1] {
				if other.evidence.Boot == r.evidence.Boot || (other.evidence.Host == h.Label && other.evidence.PID == r.evidence.PID) {
					return fail("duplicate_identity")
				}
			}
		}
	}
	report.Phases["ready"] = time.Since(phase)
	phase = time.Now()
	// Submit at least two tasks per worker, then fill workers one at a time. Earlier
	// workers remain blocked at suffix and cannot monopolize subsequent slots.
	for i := 0; i < len(ps)*c.Capacity; i++ {
		id := fmt.Sprintf("run-%04d", i)
		ids = append(ids, id)
		if db.Submit(ctx, id) != nil {
			return fail("submit")
		}
	}
	for _, p := range ps {
		if p.p.Send("start") != nil {
			return fail("start")
		}
		if await(func() (bool, error) {
			runs := map[string]bool{}
			for _, e := range p.p.Events() {
				if e.Kind == "suffix" {
					runs[e.RunID] = true
				}
			}
			if len(runs) > c.Capacity {
				return false, errors.New("capacity")
			}
			if len(runs) == c.Capacity {
				return true, nil
			}
			if done, _ := p.p.Exited(); done {
				return false, errors.New("early exit")
			}
			return false, nil
		}) != nil {
			return fail("fill_slots")
		}
	}
	ids = append(ids, "overflow")
	if db.Submit(ctx, "overflow") != nil {
		return fail("submit_overflow")
	}
	// Hold all suffix gates while checking DB ownership over several polls.
	for i := 0; i < 3; i++ {
		if i > 0 {
			select {
			case <-ctx.Done():
				return fail("capacity_timeout")
			case <-time.After(poll):
			}
		}
		counts, err := db.Capacity(ctx)
		if err != nil {
			return fail("capacity_query")
		}
		if len(counts) != len(ps) {
			return fail("capacity_count")
		}
		for _, p := range ps {
			if counts[p.evidence.ID] != c.Capacity {
				return fail("capacity_exceeded_or_underfilled")
			}
		}
		s, err := db.Get(ctx, "overflow")
		if err != nil || s.Status != d.Queued || s.Epoch != 0 {
			return fail("overflow_claimed")
		}
	}
	report.Checks["capacity"] = "PASS"
	report.Phases["fill_and_capacity"] = time.Since(phase)
	phase = time.Now()
	var killed *running
	old := map[string]TaskEvidence{}
	if c.Scenario == "checkpoint-kill" {
		for _, p := range ps {
			if p.evidence.Host == c.KillHost {
				killed = p
				break
			}
		}
		for _, e := range killed.p.Events() {
			if e.Kind == "suffix" {
				s, err := db.Get(ctx, e.RunID)
				if err != nil || s.Status != d.Running || s.Revision == 0 || s.Epoch != e.Epoch {
					return fail("checkpoint_not_persisted")
				}
				old[e.RunID] = s
			}
		}
		if len(old) != c.Capacity {
			return fail("kill_window")
		}
		if ctl.Kill(ctx, killed.evidence) != nil {
			return fail("kill_confirmation")
		}
		killed.killed = true
	}
	for _, p := range ps {
		if !p.killed && p.p.Send("release") != nil {
			return fail("release")
		}
	}
	if await(func() (bool, error) {
		report.Tasks = nil
		all := true
		for _, id := range ids {
			s, err := db.Get(ctx, id)
			if err != nil {
				return false, errors.New("status")
			}
			report.Tasks = append(report.Tasks, s)
			if s.Status == d.Failed || s.Status == d.Blocked || s.Status == d.Canceled {
				return false, errors.New("terminal failure")
			}
			if s.Status != d.Succeeded {
				all = false
			}
		}
		return all, nil
	}) != nil {
		return fail("all_succeeded")
	}
	report.Checks["all_succeeded"] = "PASS"
	// First drain every live process and collect every event, including the
	// killed process's trailing pipe bytes. DB success alone is not a replay oracle.
	report.Phases["release_and_recovery"] = time.Since(phase)
	phase = time.Now()
	for _, p := range ps {
		if !p.killed {
			if p.p.Send("drain") != nil || waitEvent(p, "drained") != nil {
				return fail("drain")
			}
		}
		if await(func() (bool, error) {
			done, err := p.p.Exited()
			if !done {
				return false, nil
			}
			if p.killed && errors.Is(err, ErrProcessFailed) {
				return true, nil
			}
			return true, err
		}) != nil {
			return fail("process_exit_or_incomplete_events")
		}
		if !p.killed {
			for _, e := range p.p.Events() {
				if e.Kind == "drained" && e.Reason != "ok" {
					return fail("drain_timeout")
				}
			}
		}
	}
	for id, before := range old {
		now, err := db.Get(ctx, id)
		if err != nil || now.Epoch <= before.Epoch {
			return fail("recovery_epoch")
		}
		prefixes := 0
		for _, p := range ps {
			for _, e := range p.p.Events() {
				if e.RunID == id && e.Kind == "prefix" {
					prefixes++
				}
			}
		}
		if prefixes != 1 {
			return fail("prefix_replayed")
		}
	}
	if killed != nil {
		report.Checks["checkpoint_recovery"] = "PASS"
	}
	report.Checks["drain"] = "PASS"
	report.Phases["drain"] = time.Since(phase)
	report.Result = "PASS"
	return
}

// ProbeFinish exposes the harness handshake without weakening its persistent gate.
func ProbeFinish(p Process) error { return p.Send("probe_finish") }
