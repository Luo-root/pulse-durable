package soaktest

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	d "github.com/Luo-root/pulse-durable/distributed"
)

func TestLocalEndpoints(t *testing.T) {
	t.Setenv("PULSE_DISTRIBUTED_TEST_ALLOW", "yes")
	t.Setenv("PGHOST", "127.0.0.1")
	c := Default()
	c.Namespace = "regression"
	cases := []struct {
		name, admin, worker string
		valid               bool
	}{
		{"local", "host=127.0.0.1 port=55432 dbname=soak user=admin sslmode=disable", "host=127.0.0.1 port=55432 dbname=soak user=worker sslmode=disable", true},
		{"remote fallback", "host=127.0.0.1,remote.invalid port=55432 dbname=soak user=admin sslmode=disable", "host=127.0.0.1 port=55432 dbname=soak user=worker sslmode=disable", false},
		{"different port", "host=127.0.0.1 port=55432 dbname=soak user=admin sslmode=disable", "host=127.0.0.1 port=55433 dbname=soak user=worker sslmode=disable", false},
		{"local other endpoint fallback", "host=127.0.0.1,localhost port=55432 dbname=soak user=admin sslmode=disable", "host=127.0.0.1 port=55432 dbname=soak user=worker sslmode=disable", false},
		{"TLS fallback", "host=localhost port=55432 dbname=soak user=admin sslmode=prefer", "host=localhost port=55432 dbname=soak user=worker sslmode=prefer", true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(c.AdminEnv, tt.admin)
			t.Setenv(c.WorkerEnv, tt.worker)
			if got := c.Validate() == nil; got != tt.valid {
				t.Fatalf("accepted=%v want %v", got, tt.valid)
			}
		})
	}
}
func TestStableAuditWaitsForDelayedFinishSnapshot(t *testing.T) {
	accepted := int64(1)
	snapshots := []auditSnapshot{
		{Total: 1, Pending: 1},
		{Total: 1, Terminal: 1},
	}
	var calls int
	got, err := stableAudit(t.Context(), accepted, func(context.Context) (auditSnapshot, error) {
		v := snapshots[calls]
		calls++
		return v, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || got.Terminal != accepted || got.Pending != 0 {
		t.Fatalf("calls=%d snapshot=%+v", calls, got)
	}
}

func TestStableAuditRequiresTwoStableSnapshotsWhenNotComplete(t *testing.T) {
	accepted := int64(2)
	snapshots := []auditSnapshot{{Total: 2, Terminal: 1, Pending: 1}, {Total: 2, Terminal: 1, Pending: 1}}
	var calls int
	got, err := stableAudit(t.Context(), accepted, func(context.Context) (auditSnapshot, error) {
		v := snapshots[calls]
		calls++
		return v, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || got.Terminal != 1 || got.Pending != 1 {
		t.Fatalf("calls=%d snapshot=%+v", calls, got)
	}
}

func TestInfrastructureEventRetainsRedactedContext(t *testing.T) {
	e := Event{Kind: "renew", Operation: "renew", Stage: "runtime", Reason: "infrastructure", Worker: d.Worker{ID: "worker-01", BootID: "boot-01"}, RunID: "run-01", Epoch: 7}
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, want := range []string{`"operation":"renew"`, `"stage":"runtime"`, `"reason":"infrastructure"`, `"run_id":"run-01"`, `"epoch":7`, `"worker-01"`, `"boot-01"`} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing redacted context %q in %s", want, text)
		}
	}
	for _, forbidden := range []string{"password", "dbname", "host=", "payload", "raw error"} {
		if strings.Contains(strings.ToLower(text), forbidden) {
			t.Fatalf("sensitive context leaked: %q in %s", forbidden, text)
		}
	}
}

func TestInfrastructureStageClassification(t *testing.T) {
	if !badEvent(Event{Kind: "renew", Operation: "renew", Stage: "runtime", Reason: "infrastructure"}) || !badEvent(Event{Kind: "finish", Operation: "finish", Stage: "shutdown", Reason: "infrastructure"}) {
		t.Fatal("infrastructure events must remain unexpected in both stages")
	}
}

func TestAuditBudgetAndCancellation(t *testing.T) {
	if (Config{}).auditTimeout() != Default().AuditTimeout || Default().AuditTimeout <= 0 {
		t.Fatal("zero config must use the bounded default audit budget")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 17*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	calls := 0
	_, err := stableAudit(ctx, 2, func(queryCtx context.Context) (auditSnapshot, error) {
		calls++
		if got, ok := queryCtx.Deadline(); !ok || !got.Equal(deadline) {
			t.Fatalf("query has a separate budget: %v, want %v", got, deadline)
		}
		if calls == 2 {
			cancel()
		}
		return auditSnapshot{Total: 2, Terminal: 1, Pending: 1}, nil
	})
	if calls != 2 || !errors.Is(err, context.Canceled) {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

// 在 CPU 工作期间确定性取消，不依赖调度器时序。
type cancelAfterChecks struct {
	context.Context
	checks int
}

func (c *cancelAfterChecks) Err() error {
	c.checks++
	if c.checks >= 3 {
		return context.Canceled
	}
	return nil
}

func TestChecksumCooperativeCancellationAndCompatibility(t *testing.T) {
	ctx := &cancelAfterChecks{Context: t.Context()}
	if _, err := checksumContext(ctx, "task", 10000); !errors.Is(err, context.Canceled) {
		t.Fatalf("checksum ignored cancellation: %v", err)
	}
	if got := checksum("", 0); got != "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" {
		t.Fatalf("checksum encoding changed: %s", got)
	}
	got, err := checksumContext(t.Context(), "task", 10000)
	if err != nil || got != checksum("task", 10000) {
		t.Fatalf("checksum compatibility: %q, %v", got, err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := validRecordContext(canceled, nil, "task", 10000); !errors.Is(err, context.Canceled) {
		t.Fatalf("record validation ignored cancellation: %v", err)
	}
}

func TestAuditDeadlineCannotReturnSuccess(t *testing.T) {
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := stableAudit(ctx, 1, func(context.Context) (auditSnapshot, error) {
		t.Fatal("expired budget must not start a query")
		return auditSnapshot{}, nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline lost: %v", err)
	}
}

func TestIncompleteAuditPreservesPrimaryErrorWithoutFinishMismatch(t *testing.T) {
	primary := errors.New("original phase failure")
	shutdown := errors.New("worker shutdown failed")
	r := Report{Result: "PASS", Accepted: 1, Workers: map[string]*WorkerStats{"one": {Finishes: 1}}, UnexpectedEvents: 1}
	err := finalizeReport(&r, errors.Join(primary, shutdown))
	if !errors.Is(err, primary) || !errors.Is(err, shutdown) || r.Result != "FAIL" || !strings.Contains(r.Reason, "audit incomplete") || !strings.Contains(r.Reason, "unexpected worker events") || strings.Contains(r.Reason, "finish event count") {
		t.Fatalf("misleading incomplete audit: report=%+v err=%v", r, err)
	}
	r = Report{Result: "PASS"}
	if finalizeReport(&r, nil) == nil || r.Result != "FAIL" {
		t.Fatal("incomplete audit passed without a primary error")
	}
	r = Report{AuditComplete: true, Accepted: 1, Terminal: 1}
	if err := finalizeReport(&r, nil); err == nil || !strings.Contains(err.Error(), "finish event count") {
		t.Fatal("complete audit must still enforce finish count")
	}
}

func TestFailuresNotGloballySuppressed(t *testing.T) {
	for _, e := range []Event{{Kind: "claim", Reason: "unknown"}, {Kind: "claim", Reason: "interrupted"}, {Kind: "renew", Reason: "lease_lost"}} {
		if !badEvent(e) {
			t.Fatalf("fault suppressed: %+v", e)
		}
	}
}
