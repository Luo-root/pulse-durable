package soaktest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"os"
	"sync"
	"time"

	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/Luo-root/pulse-durable/distributed/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Histogram struct {
	Count   int64     `json:"count"`
	TotalNS int64     `json:"total_ns"`
	MaxNS   int64     `json:"max_ns"`
	Buckets [64]int64 `json:"power2_microsecond_buckets"`
}

func (h *Histogram) add(v time.Duration) {
	n := int64(v)
	if n < 0 {
		n = 0
	}
	h.Count++
	h.TotalNS += n
	if n > h.MaxNS {
		h.MaxNS = n
	}
	i := bits.Len64(uint64(n / 1000))
	h.Buckets[i]++
}
func (h Histogram) quantile(q float64) int64 {
	if h.Count == 0 {
		return 0
	}
	target := int64(math.Ceil(float64(h.Count) * q))
	if target < 1 {
		target = 1
	}
	var n int64
	for i, v := range h.Buckets {
		n += v
		if n >= target {
			if i >= 53 {
				return h.MaxNS
			}
			return int64(uint64(1)<<i) * 1000
		}
	}
	return h.MaxNS
}

type Phase struct {
	Name        string        `json:"name"`
	RequestedNS time.Duration `json:"requested_ns"`
	SubmitNS    time.Duration `json:"actual_submit_ns"`
	DrainNS     time.Duration `json:"actual_drain_ns"`
	Accepted    int64         `json:"accepted"`
	Completed   int64         `json:"completed"`
	Throughput  float64       `json:"completed_per_second_including_drain"`
	Latency     Histogram     `json:"end_to_end_observed_latency"`
	P50NS       int64         `json:"p50_upper_ns"`
	P95NS       int64         `json:"p95_upper_ns"`
	P99NS       int64         `json:"p99_upper_ns"`
}
type WorkerStats struct {
	PID      int    `json:"pid"`
	Boot     string `json:"boot_id"`
	Finishes int64  `json:"finish_events"`
}
type Report struct {
	Result                 string                  `json:"result"`
	Reason                 string                  `json:"reason,omitempty"`
	Config                 Config                  `json:"config"`
	Started                time.Time               `json:"started"`
	Duration               time.Duration           `json:"actual_total_ns"`
	Accepted               int64                   `json:"accepted"`
	Terminal               int64                   `json:"terminal"`
	AuditComplete          bool                    `json:"audit_complete"`
	Pending                int64                   `json:"pending"`
	Failed                 int64                   `json:"failed"`
	Blocked                int64                   `json:"blocked"`
	Invalid                int64                   `json:"invalid_terminal_or_checksum"`
	UnexpectedEvents       int64                   `json:"unexpected_worker_events"`
	RuntimeInfrastructure  int64                   `json:"runtime_infrastructure_events"`
	ShutdownInfrastructure int64                   `json:"shutdown_infrastructure_events"`
	Events                 map[string]int64        `json:"event_counts"`
	Workers                map[string]*WorkerStats `json:"workers"`
	Phases                 []Phase                 `json:"phases"`
	Resources              string                  `json:"cpu_rss"`
	LatencyMethod          string                  `json:"latency_method"`
	TotalLatency           Histogram               `json:"all_phases_latency"`
	P50NS                  int64                   `json:"all_phases_p50_upper_ns"`
	P95NS                  int64                   `json:"all_phases_p95_upper_ns"`
	P99NS                  int64                   `json:"all_phases_p99_upper_ns"`
}

// Prepare is the only DDL/permission-changing entrypoint. Existing roles are never modified.
func Prepare(ctx context.Context, c Config) error {
	if e := c.Validate(); e != nil {
		return e
	}
	a, e := pgxpool.New(ctx, os.Getenv(c.AdminEnv))
	if e != nil {
		return errors.New("prepare connection failed")
	}
	defer a.Close()
	w, _ := pgx.ParseConfig(os.Getenv(c.WorkerEnv))
	var exists bool
	if w.Password == "" || a.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", w.User).Scan(&exists) != nil || exists {
		return errors.New("prepare requires a fresh worker role with password")
	}
	if e = postgres.Migrate(ctx, os.Getenv(c.AdminEnv)); e != nil {
		return errors.New("prepare migration failed")
	}
	var used bool
	if a.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pulse_durable_distributed.runs WHERE namespace=$1) OR EXISTS(SELECT 1 FROM pulse_durable_distributed.access WHERE namespace=$1)`, c.Namespace).Scan(&used) != nil || used {
		return errors.New("prepare requires unused namespace")
	}
	tx, e := a.Begin(ctx)
	if e != nil {
		return errors.New("prepare transaction failed")
	}
	defer tx.Rollback(context.Background())
	if _, e = tx.Exec(ctx, "SET LOCAL standard_conforming_strings=on"); e != nil {
		return errors.New("prepare quoting failed")
	}
	// Quote password as a SQL literal without emitting SQL or backend errors.
	password := "'"
	for _, r := range w.Password {
		if r == '\'' {
			password += "'"
		}
		password += string(r)
	}
	password += "'"
	if _, e = tx.Exec(ctx, "CREATE ROLE "+pgx.Identifier{w.User}.Sanitize()+" LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS PASSWORD "+password); e != nil {
		return errors.New("prepare role creation failed")
	}
	if tx.Commit(ctx) != nil {
		return errors.New("prepare commit failed")
	}
	if postgres.Migrate(ctx, os.Getenv(c.AdminEnv), postgres.Grant{Role: w.User, Namespace: c.Namespace, Permissions: []string{"query", "submit", "worker"}}) != nil {
		return errors.New("prepare grant failed (role may already exist)")
	}
	return nil
}

// Run exclusively creates raw and summary files; existing evidence is never overwritten.
// The caller executable must route its worker subcommand to Child.
func Run(ctx context.Context, c Config, prefix string) (report Report, err error) {
	if e := c.Validate(); e != nil {
		return report, e
	}
	raw, e := os.OpenFile(prefix+".events.jsonl", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return report, errors.New("raw output exists or cannot be created")
	}
	defer raw.Close()
	summary, e := os.OpenFile(prefix+".summary.json", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return report, errors.New("summary output exists or cannot be created (raw preserved)")
	}
	defer summary.Close()
	report = Report{Result: "FAIL", Config: c, Started: time.Now().UTC(), Events: map[string]int64{}, Workers: map[string]*WorkerStats{}, Resources: "unavailable: portable per-process CPU/RSS sampling not implemented", LatencyMethod: "local submit-start to terminal poll observation, includes queue/checkpoints/poll delay; fixed power-of-two microsecond histogram, quantiles approximate upper bounds; exact count/total/max"}
	var mu sync.Mutex
	var outputErr bool
	var children []*child
	childrenStopped := false
	stopChildren := func() error {
		if childrenStopped {
			return nil
		}
		childrenStopped = true
		var failed bool
		for _, p := range children {
			if p.stop() != nil {
				failed = true
			}
		}
		if failed {
			return errors.New("worker shutdown failed")
		}
		return nil
	}
	defer func() {
		if !childrenStopped {
			if e := stopChildren(); e != nil {
				err = errors.Join(err, e)
			}
		}
		report.Duration = time.Since(report.Started)
		if outputErr {
			err = errors.Join(err, errors.New("raw evidence write failed"))
		}
		if raw.Sync() != nil {
			err = errors.Join(err, errors.New("raw evidence sync failed"))
		}
		err = finalizeReport(&report, err)
		if json.NewEncoder(summary).Encode(report) != nil {
			err = errors.Join(err, errors.New("summary write failed"))
		}
		if summary.Sync() != nil {
			err = errors.Join(err, errors.New("summary evidence sync failed"))
		}
		if err != nil {
			report.Result, report.Reason = "FAIL", err.Error()
		}
	}()
	admin, e := pgxpool.New(ctx, os.Getenv(c.AdminEnv))
	if e != nil {
		return report, errors.New("admin connection failed")
	}
	defer admin.Close()
	var used bool
	if admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pulse_durable_distributed.runs WHERE namespace=$1)`, c.Namespace).Scan(&used) != nil || used {
		return report, errors.New("run requires empty namespace and installed schema")
	}
	backend, e := postgres.New(ctx, os.Getenv(c.WorkerEnv))
	if e != nil {
		return report, errors.New("submit backend unavailable")
	}
	defer backend.Close()
	observe := func(v Event) {
		mu.Lock()
		defer mu.Unlock()
		if json.NewEncoder(raw).Encode(v) != nil {
			outputErr = true
		}
		report.Events[v.Kind+":"+v.Reason]++
		if badEvent(v) {
			report.UnexpectedEvents++
			if v.Reason == "infrastructure" {
				if v.Stage == "shutdown" {
					report.ShutdownInfrastructure++
				} else {
					report.RuntimeInfrastructure++
				}
			}
		}
		s := report.Workers[v.Worker.ID]
		if s == nil {
			s = &WorkerStats{}
			report.Workers[v.Worker.ID] = s
		}
		if v.Kind == "ready" {
			s.PID = v.PID
			s.Boot = v.Worker.BootID
		}
		if v.Kind == "finish" && v.Reason == "ok" {
			s.Finishes++
		}
	}
	for i := 0; i < c.Workers; i++ {
		p, e := launch(c, workerID(i), observe)
		if e != nil {
			return report, e
		}
		children = append(children, p)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		mu.Lock()
		ready := 0
		for _, s := range report.Workers {
			if s.PID > 0 && s.Boot != "" {
				ready++
			}
		}
		mu.Unlock()
		if ready == c.Workers {
			break
		}
		if time.Now().After(deadline) {
			return report, errors.New("workers not ready within 30 seconds")
		}
		select {
		case <-ctx.Done():
			return report, errors.New("run canceled during startup")
		case <-time.After(20 * time.Millisecond):
		}
	}
	var sequence int64
	for r := 0; r < c.Repeat; r++ {
		for _, p := range []struct {
			name     string
			duration time.Duration
		}{{"warm", c.Warm}, {"steady", c.Steady}} {
			if p.duration == 0 {
				continue
			}
			phase, e := drive(ctx, c, backend, admin, fmt.Sprintf("%d-%s", r+1, p.name), p.duration, &sequence)
			report.Phases = append(report.Phases, phase)
			report.TotalLatency.Count += phase.Latency.Count
			report.TotalLatency.TotalNS += phase.Latency.TotalNS
			if phase.Latency.MaxNS > report.TotalLatency.MaxNS {
				report.TotalLatency.MaxNS = phase.Latency.MaxNS
			}
			for i, n := range phase.Latency.Buckets {
				report.TotalLatency.Buckets[i] += n
			}
			report.P50NS = report.TotalLatency.quantile(.50)
			report.P95NS = report.TotalLatency.quantile(.95)
			report.P99NS = report.TotalLatency.quantile(.99)
			report.Accepted += phase.Accepted
			if e != nil {
				err = e
				break
			}
		}
		if err != nil {
			break
		}
	}
	// Admission is already exhausted by drive; now stop workers. child.stop waits
	// for the process and its event scanner, and Child's lifecycle waits for Drain
	// (including all in-flight Finish/Save calls) before exiting.
	if e := stopChildren(); e != nil {
		err = errors.Join(err, e)
	}
	// A single query can race a committed Finish becoming visible to the admin
	// snapshot. Require a bounded stable reconciliation after all workers/events
	// have stopped, rather than treating sleep as synchronization.
	auditCtx, cancel := context.WithTimeout(ctx, c.auditTimeout())
	defer cancel()
	snap, e := stableAudit(auditCtx, report.Accepted, func(queryCtx context.Context) (auditSnapshot, error) {
		return queryAudit(queryCtx, admin, c)
	})
	if e != nil {
		return report, errors.Join(err, e)
	}
	report.AuditComplete = true
	report.Terminal = snap.Terminal
	report.Pending = snap.Pending
	report.Failed = snap.Failed
	report.Blocked = snap.Blocked
	report.Invalid = snap.Invalid
	mu.Lock()
	bad := report.UnexpectedEvents
	mu.Unlock()
	if snap.Total != report.Accepted || report.Accepted != report.Terminal+report.Pending || report.Pending != 0 || report.Failed != 0 || report.Blocked != 0 || report.Invalid != 0 || bad != 0 {
		err = errors.Join(err, errors.New("reconciliation or unexpected worker event assertion failed"))
	}
	if err == nil {
		report.Result = "PASS"
	}
	return report, err
}

// finalizeReport compares finish events only against a fully verified audit.
func finalizeReport(report *Report, err error) error {
	if !report.AuditComplete {
		err = errors.Join(err, errors.New("final audit incomplete"))
	} else {
		var finishes int64
		for _, s := range report.Workers {
			finishes += s.Finishes
		}
		if report.Accepted == 0 || finishes != report.Terminal {
			err = errors.Join(err, errors.New("finish event count does not match nonzero accepted terminal count"))
		}
	}
	if report.UnexpectedEvents != 0 {
		err = errors.Join(err, errors.New("unexpected worker events, including shutdown"))
	}
	if err != nil {
		report.Result, report.Reason = "FAIL", err.Error()
	}
	return err
}

type auditSnapshot struct {
	Total, Terminal, Pending, Failed, Blocked, Invalid int64
}

func stableAudit(ctx context.Context, accepted int64, query func(context.Context) (auditSnapshot, error)) (auditSnapshot, error) {
	var snap auditSnapshot
	var previous *auditSnapshot
	stable := 0
	for {
		if err := ctx.Err(); err != nil {
			return snap, fmt.Errorf("final reconciliation interrupted: %w", err)
		}
		current, err := query(ctx)
		if err != nil {
			return snap, err
		}
		if err := ctx.Err(); err != nil {
			return snap, fmt.Errorf("final reconciliation interrupted: %w", err)
		}
		if previous != nil && *previous == current {
			stable++
		} else {
			stable = 1
		}
		previous = &current
		snap = current
		if stable >= 2 || (current.Terminal == accepted && current.Pending == 0) {
			return snap, nil
		}
		select {
		case <-ctx.Done():
			return snap, fmt.Errorf("final reconciliation interrupted: %w", ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func queryAudit(ctx context.Context, admin *pgxpool.Pool, c Config) (auditSnapshot, error) {
	var snap auditSnapshot
	rows, err := admin.Query(ctx, `SELECT run_id,status,record FROM pulse_durable_distributed.runs WHERE namespace=$1`, c.Namespace)
	if err != nil {
		return snap, errors.Join(errors.New("final reconciliation query failed"), ctx.Err())
	}
	defer rows.Close()
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return snap, err
		}
		var id, status string
		var record []byte
		if rows.Scan(&id, &status, &record) != nil {
			return snap, errors.New("final reconciliation scan failed")
		}
		snap.Total++
		switch d.Status(status) {
		case d.Succeeded:
			snap.Terminal++
			valid, err := validRecordContext(ctx, record, id, c.CPURounds)
			if err != nil {
				return snap, err
			}
			if !valid {
				snap.Invalid++
			}
		case d.Failed:
			snap.Terminal++
			snap.Failed++
		case d.Canceled:
			snap.Terminal++
			snap.Invalid++
		case d.Blocked:
			snap.Pending++
			snap.Blocked++
		case d.Queued, d.Running:
			snap.Pending++
		default:
			snap.Pending++
			snap.Invalid++
		}
	}
	if err := rows.Err(); err != nil {
		return snap, errors.Join(errors.New("final reconciliation rows failed"), ctx.Err())
	}
	return snap, ctx.Err()
}
func drive(ctx context.Context, c Config, backend *postgres.Coordinator, admin *pgxpool.Pool, name string, duration time.Duration, seq *int64) (p Phase, err error) {
	p.Name = name
	p.RequestedNS = duration
	start := time.Now()
	end := start.Add(duration)
	drainEnd := end.Add(60 * time.Second)
	pending := map[string]time.Time{}
	submitting := true
	defer func() {
		if submitting {
			p.SubmitNS = time.Since(start)
		}
		p.P50NS = p.Latency.quantile(.50)
		p.P95NS = p.Latency.quantile(.95)
		p.P99NS = p.Latency.quantile(.99)
		elapsed := time.Since(start)
		p.DrainNS = elapsed - p.SubmitNS
		if elapsed > 0 {
			p.Throughput = float64(p.Completed) / elapsed.Seconds()
		}
	}()
	for time.Now().Before(end) || len(pending) > 0 {
		if ctx.Err() != nil {
			return p, errors.New("run canceled")
		}
		if time.Now().After(drainEnd) {
			return p, errors.New("phase drain exceeded 60 seconds")
		}
		// Bounded batches allow observations between submission bursts.
		for batch := 0; batch < 32 && len(pending) < c.MaxOutstanding && time.Now().Before(end); batch++ {
			*seq++
			id := fmt.Sprintf("task-%012d", *seq)
			t, e := task(ctx, c, id)
			if e != nil {
				return p, errors.New("task construction failed")
			}
			began := time.Now()
			op, cancel := context.WithTimeout(ctx, 5*time.Second)
			result, e := backend.Submit(op, t)
			cancel()
			if e != nil {
				return p, errors.New("submission failed; final audit detects unknown commits")
			}
			if result.Deduplicated {
				return p, errors.New("unexpected deduplicated submission")
			}
			pending[id] = began
			p.Accepted++
		}
		if submitting {
			p.SubmitNS = time.Since(start)
			if !time.Now().Before(end) {
				submitting = false
			}
		}
		if len(pending) > 0 {
			ids := make([]string, 0, len(pending))
			for id := range pending {
				ids = append(ids, id)
			}
			op, cancel := context.WithTimeout(ctx, 5*time.Second)
			rows, e := admin.Query(op, `SELECT run_id,status FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=ANY($2::text[]) AND status NOT IN ('queued','running')`, c.Namespace, ids)
			if e != nil {
				cancel()
				return p, errors.New("pending query failed")
			}
			invalid := false
			for rows.Next() {
				var id, status string
				if rows.Scan(&id, &status) != nil {
					invalid = true
					break
				}
				if status != "succeeded" {
					invalid = true
				}
				p.Latency.add(time.Since(pending[id]))
				delete(pending, id)
				p.Completed++
			}
			e = rows.Err()
			rows.Close()
			cancel()
			if e != nil || invalid {
				return p, errors.New("phase encountered non-success terminal/blocked or observation failure")
			}
		}
		select {
		case <-ctx.Done():
			return p, errors.New("run canceled")
		case <-time.After(10 * time.Millisecond):
		}
	}
	return p, nil
}
