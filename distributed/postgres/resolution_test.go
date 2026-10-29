package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func waitBlocked(t *testing.T, admin *pgxpool.Pool, blocker uint32) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		var blocked bool
		if e := admin.QueryRow(context.Background(), `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1::integer=ANY(pg_blocking_pids(pid)))`, int32(blocker)).Scan(&blocked); e != nil {
			t.Fatal(scrub(e, false))
		}
		if blocked {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("operation never reached database lock barrier")
		}
		time.Sleep(time.Millisecond)
	}
}

func txCall(t *testing.T, tx pgx.Tx, op string, a map[string]any) []byte {
	t.Helper()
	b, e := json.Marshal(a)
	if e != nil {
		t.Fatal(e)
	}
	var out []byte
	if e = tx.QueryRow(context.Background(), `SELECT pulse_durable_distributed.api($1,$2::jsonb)`, op, b).Scan(&out); e != nil {
		t.Fatal(scrub(e, false))
	}
	return out
}

// 先前读取的回执是后续所有租约观测的持久下界。
// 管理员夹具模拟未来的权威观测，不改变
// 主机或数据库时钟；ResolveClaim 必须在领取或关闭前失败。
func TestResolveClaimReceiptClockRegression(t *testing.T) {
	c, admin, ns := integration(t)
	ctx := context.Background()
	task := taskFixture(t, ns, "resolve-receipt-regression")
	if _, err := c.Submit(ctx, task); err != nil {
		t.Fatal(err)
	}
	claim := claimOne(t, c, task, "resolve-regression", time.Minute)
	m := mut("resolve-clock-receipt")
	q := receiptQuery(t, admin, task.Identity(), m, "cancel", nil, nil)
	if r, err := c.FindMutation(ctx, q); err != nil || r.State != d.AbsentWithinWindow {
		t.Fatal(r, err)
	}
	if _, err := admin.Exec(ctx, `UPDATE pulse_durable_distributed.runs SET receipt_observed_through=clock_timestamp()+interval '10 minutes', lease_until=clock_timestamp()+interval '20 minutes' WHERE namespace=$1 AND run_id=$2`, ns, task.Identity().RunID); err != nil {
		t.Fatal(err)
	}
	before := func() (bool, int) {
		t.Helper()
		var closed bool
		var events int
		if err := admin.QueryRow(ctx, `SELECT l.closed,(SELECT count(*) FROM pulse_durable_distributed.events e WHERE e.namespace=$1 AND e.run_id=$2 AND e.operation='resolve') FROM pulse_durable_distributed.leases l WHERE l.namespace=$1 AND l.lease_id=$3`, ns, task.Identity().RunID, claim.Token.LeaseID).Scan(&closed, &events); err != nil {
			t.Fatal(err)
		}
		return closed, events
	}
	closed, events := before()
	if closed {
		t.Fatal("fixture lease already closed")
	}
	if r, err := c.ResolveClaim(ctx, ns, claim.Token.Worker, claim.Token.LeaseID); !errors.Is(err, d.ErrClockRegression) || r.Claim != nil {
		t.Fatalf("regressed resolve returned claim: %+v %v", r, err)
	}
	afterClosed, afterEvents := before()
	if afterClosed != closed || afterEvents != events {
		t.Fatal("regressed resolve published closure/event")
	}
	// 同样的时钟回退也不得发布永久关闭记录。
	if _, err := admin.Exec(ctx, `UPDATE pulse_durable_distributed.runs SET lease_until=clock_timestamp()-interval '1 second' WHERE namespace=$1 AND run_id=$2`, ns, task.Identity().RunID); err != nil {
		t.Fatal(err)
	}
	if r, err := c.ResolveClaim(ctx, ns, claim.Token.Worker, claim.Token.LeaseID); !errors.Is(err, d.ErrClockRegression) || r.Claim != nil || r.Closed {
		t.Fatalf("regressed resolve returned closure: %+v %v", r, err)
	}
	afterClosed, afterEvents = before()
	if afterClosed != closed || afterEvents != events {
		t.Fatal("regressed expired resolve published closure/event")
	}
}

func TestResolveClaimBarriers(t *testing.T) {
	c, admin, ns := integration(t)
	ctx := context.Background()
	w := d.Worker{ID: "barrier-worker", BootID: "barrier-boot"}
	for _, claimFirst := range []bool{true, false} {
		name := "resolve-first"
		if claimFirst {
			name = "claim-first"
		}
		t.Run(name, func(t *testing.T) {
			task := taskFixture(t, ns, name)
			if _, e := c.Submit(ctx, task); e != nil {
				t.Fatal(e)
			}
			id := digest([]byte(ns + name))[:32]
			q := d.ClaimRequest{Namespace: ns, Worker: w, Capabilities: []d.Route{task.Route()}, Limit: 1, TTL: time.Minute, LeaseIDs: []string{id}}
			a := map[string]any{"namespace": ns, "worker_id": w.ID, "boot_id": w.BootID, "lease_id": id, "lease_ids": q.LeaseIDs, "capabilities": q.Capabilities, "limit": 1, "ttl_us": q.TTL.Microseconds()}
			tx, e := c.pool.Begin(ctx)
			if e != nil {
				t.Fatal(scrub(e, false))
			}
			defer tx.Rollback(ctx)
			if claimFirst {
				// 执行真实 API，在提交前暂停其事务，
				// 并有意丢弃返回的领取结果（模拟响应丢失）。
				txCall(t, tx, "claim", a)
				type result struct {
					r d.ClaimResolution
					e error
				}
				done := make(chan result, 1)
				go func() { r, e := c.ResolveClaim(ctx, ns, w, id); done <- result{r, e} }()
				waitBlocked(t, admin, tx.Conn().PgConn().PID())
				if e = tx.Commit(ctx); e != nil {
					t.Fatal(scrub(e, true))
				}
				got := <-done
				if got.e != nil || got.r.Closed || got.r.Claim == nil || got.r.Claim.Token.LeaseID != id || got.r.Claim.Token.Identity != task.Identity() {
					t.Fatalf("committed claim resolution: %+v error=%v", got.r, got.e)
				}
			} else {
				txCall(t, tx, "resolve", a)
				done := make(chan error, 1)
				go func() { _, e := c.Claim(ctx, q); done <- e }()
				waitBlocked(t, admin, tx.Conn().PgConn().PID())
				if e = tx.Commit(ctx); e != nil {
					t.Fatal(scrub(e, true))
				}
				if e = <-done; !errors.Is(e, d.ErrInvalid) {
					t.Fatalf("late claim must fail after closure: %v", e)
				}
				r, e := c.ResolveClaim(ctx, ns, w, id)
				if e != nil || !r.Closed || r.Claim != nil {
					t.Fatalf("closure not permanent: %+v %v", r, e)
				}
			}
			if _, e = c.Cancel(ctx, task.Identity(), mut(name+"-cancel")); e != nil {
				t.Fatal(e)
			}
		})
	}
	t.Run("unused slots stay closed", func(t *testing.T) {
		ids := []string{digest([]byte(ns + "slot1"))[:32], digest([]byte(ns + "slot2"))[:32]}
		task := taskFixture(t, ns, "slots")
		q := d.ClaimRequest{Namespace: ns, Worker: w, Capabilities: []d.Route{task.Route()}, Limit: 2, TTL: time.Minute, LeaseIDs: ids}
		rows, e := c.Claim(ctx, q)
		if e != nil || len(rows) != 0 {
			t.Fatalf("empty batch count=%d err=%v", len(rows), e)
		}
		if _, e = c.Submit(ctx, task); e != nil {
			t.Fatal(e)
		}
		for _, id := range ids {
			r, e := c.ResolveClaim(ctx, ns, w, id)
			if e != nil || !r.Closed || r.Claim != nil {
				t.Fatalf("unused slot not closed: %+v %v", r, e)
			}
			q.Limit, q.LeaseIDs = 1, []string{id}
			if _, e = c.Claim(ctx, q); !errors.Is(e, d.ErrInvalid) {
				t.Fatalf("unused slot reused: %v", e)
			}
		}
		if _, e = c.Cancel(ctx, task.Identity(), mut("slots-cancel")); e != nil {
			t.Fatal(e)
		}
	})
}
