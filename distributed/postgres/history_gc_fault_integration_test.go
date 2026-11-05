// 本文件补充历史 GC 的真实 PostgreSQL 竞争与故障回滚验收。
// 测试只使用 dedicated DSN 创建的一次性数据库；故障注入通过临时
// AFTER DELETE trigger 完成，生产 SQL 与数据布局不增加测试开关。
package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const gcRunAdvisorySeed int64 = 5788618723189269578

// holdGCRunLock 用管理员连接持有与 pin/GC 相同的 identity advisory lock，
// 让两个真实入口先排队，再按 PostgreSQL 的锁等待顺序释放。它不直接
// 修改业务表，因此竞争窗口是数据库锁本身，而不是测试时序猜测。
func holdGCRunLock(t *testing.T, admin *pgxpool.Pool, identity d.Identity) pgx.Tx {
	t.Helper()
	tx, err := admin.Begin(context.Background())
	if err != nil {
		t.Fatal("begin advisory lock transaction", err)
	}
	if _, err = tx.Exec(context.Background(), `SELECT pg_advisory_xact_lock(hashtextextended(jsonb_build_array($1::text,$2::text)::text,$3::bigint))`, identity.Namespace, identity.RunID, gcRunAdvisorySeed); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal("hold GC run advisory lock", err)
	}
	return tx
}

// waitGCAdvisoryWaiters 等待真实入口已经进入 advisory lock 等待队列。
// 轮询的是 PostgreSQL 权威锁表，不用固定 sleep 推断入口是否到达。
func waitGCAdvisoryWaiters(t *testing.T, admin *pgxpool.Pool, minimum int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		var count int
		if err := admin.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted`).Scan(&count); err != nil {
			t.Fatal("query advisory waiters", scrub(err, false))
		}
		if count >= minimum {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("advisory waiters did not reach %d", minimum)
		case <-tick.C:
		}
	}
}

func gcPinChain(t *testing.T, f gcIntegrationFixture, revision string) string {
	t.Helper()
	var chain string
	if err := f.admin.QueryRow(context.Background(), `SELECT next_chain FROM pulse_durable_distributed.batches WHERE namespace=$1 AND run_id=$2 AND revision=$3`, f.request.Identity.Namespace, f.request.Identity.RunID, revision).Scan(&chain); err != nil {
		t.Fatal("read pin chain", scrub(err, false))
	}
	return chain
}

func gcActivePin(t *testing.T, f gcIntegrationFixture, pinID, mutationID, chain string) d.PinRequest {
	t.Helper()
	ttl := time.Hour
	pin := d.PinRequest{Identity: f.request.Identity, PinID: pinID, Revision: 3, Chain: chain, HolderDigest: digest([]byte("gc-race-holder")), TTL: &ttl, Mutation: d.Mutation{ID: mutationID}}
	pin.Mutation.Digest = pin.Digest()
	return pin
}

func TestPostgresHistoryGCCompetesWithPinCreate(t *testing.T) {
	f := newGCIntegrationFixture(t)
	ctx := context.Background()
	plan, err := f.c.PrepareHistoryGC(ctx, f.request)
	if err != nil {
		t.Fatal("prepare GC plan", err)
	}
	pin := gcActivePin(t, f, "gc-create-race", "gc-create-race-mutation", gcPinChain(t, f, "3"))
	execute := d.HistoryGCExecute{Identity: f.request.Identity, PlanID: f.request.PlanID, PlanDigest: plan.PlanDigest}
	tx := holdGCRunLock(t, f.admin, f.request.Identity)
	pinDone := make(chan error, 1)
	go func() { _, pinErr := f.c.PinHistory(ctx, pin); pinDone <- pinErr }()
	waitGCAdvisoryWaiters(t, f.admin, 1)
	gcDone := make(chan error, 1)
	go func() { _, gcErr := f.c.ExecuteHistoryGC(ctx, execute); gcDone <- gcErr }()
	waitGCAdvisoryWaiters(t, f.admin, 2)
	if err = tx.Commit(ctx); err != nil {
		t.Fatal("release pin/GC race lock", err)
	}
	if err = <-pinDone; err != nil {
		t.Fatalf("pin_create lost the lock race: %v", err)
	}
	if err = <-gcDone; !errors.Is(err, d.ErrPinned) {
		t.Fatalf("GC did not recheck the newly committed pin: %v", err)
	}
	var floor, through string
	if err = f.admin.QueryRow(ctx, `SELECT history_floor::text,history_gc_through::text FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2`, f.request.Identity.Namespace, f.request.Identity.RunID).Scan(&floor, &through); err != nil {
		t.Fatal(err)
	}
	if floor != "1" || through != "1" {
		t.Fatalf("pinned refusal changed GC authority: floor=%s through=%s", floor, through)
	}
}

func TestPostgresHistoryGCCompetesWithPinRenew(t *testing.T) {
	f := newGCIntegrationFixture(t)
	ctx := context.Background()
	plan, err := f.c.PrepareHistoryGC(ctx, f.request)
	if err != nil {
		t.Fatal("prepare GC plan", err)
	}
	pin := gcActivePin(t, f, "gc-renew-race", "gc-renew-create", gcPinChain(t, f, "3"))
	if _, err = f.c.PinHistory(ctx, pin); err != nil {
		t.Fatal("create pin before renew race", err)
	}
	renew := d.RenewPinRequest{Identity: pin.Identity, PinID: pin.PinID, ExpectedVersion: 1, HolderDigest: pin.HolderDigest, Mutation: d.Mutation{ID: "gc-renew-race-mutation"}}
	renew.Mutation.Digest = renew.Digest()
	execute := d.HistoryGCExecute{Identity: f.request.Identity, PlanID: f.request.PlanID, PlanDigest: plan.PlanDigest}
	tx := holdGCRunLock(t, f.admin, f.request.Identity)
	renewDone := make(chan error, 1)
	go func() { _, renewErr := f.c.RenewPin(ctx, renew); renewDone <- renewErr }()
	waitGCAdvisoryWaiters(t, f.admin, 1)
	gcDone := make(chan error, 1)
	go func() { _, gcErr := f.c.ExecuteHistoryGC(ctx, execute); gcDone <- gcErr }()
	waitGCAdvisoryWaiters(t, f.admin, 2)
	if err = tx.Commit(ctx); err != nil {
		t.Fatal("release renew/GC race lock", err)
	}
	if err = <-renewDone; err != nil {
		t.Fatalf("pin_renew lost the lock race: %v", err)
	}
	if err = <-gcDone; !errors.Is(err, d.ErrPinned) {
		t.Fatalf("GC did not preserve the renewed pin: %v", err)
	}
	state, err := f.c.FindPin(ctx, d.PinQuery{Identity: pin.Identity, PinID: pin.PinID})
	if err != nil || state.State == nil || state.State.Version != 2 {
		t.Fatalf("renewed pin state was not committed: state=%+v err=%v", state, err)
	}
}

// installGCDeleteFailure 在当前布局内临时增加一个 AFTER DELETE trigger，
// 并同步 catalog signature，使正式 check_schema 仍能通过。清理时先删
// trigger/function，再重新计算原布局签名，避免测试把故障对象遗留到专用库。
func installGCDeleteFailure(t *testing.T, admin *pgxpool.Pool) func() {
	t.Helper()
	ctx := context.Background()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	functionSQL := pgx.Identifier{"gc_fault_" + suffix}.Sanitize()
	triggerSQL := pgx.Identifier{"gc_fault_trigger_" + suffix}.Sanitize()
	tx, err := admin.Begin(ctx)
	if err != nil {
		t.Fatal("begin GC trigger transaction", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err = tx.Exec(ctx, fmt.Sprintf(`CREATE FUNCTION pulse_durable_distributed.%s() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $fault$ BEGIN RAISE EXCEPTION USING ERRCODE='PZ001',MESSAGE='test GC delete failure'; END $fault$`, functionSQL)); err != nil {
		t.Fatal("create GC delete fault function", scrub(err, false))
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf(`REVOKE ALL ON FUNCTION pulse_durable_distributed.%s() FROM PUBLIC`, functionSQL)); err != nil {
		t.Fatal("revoke GC delete fault function", scrub(err, false))
	}
	if _, err = tx.Exec(ctx, fmt.Sprintf(`CREATE TRIGGER %s AFTER DELETE ON pulse_durable_distributed.batches FOR EACH ROW EXECUTE FUNCTION pulse_durable_distributed.%s()`, triggerSQL, functionSQL)); err != nil {
		t.Fatal("create GC delete fault trigger", scrub(err, false))
	}
	sig, err := signature(ctx, tx)
	if err != nil {
		t.Fatal("sign temporary GC fault layout", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE pulse_durable_distributed.meta SET signature=$1`, sig); err != nil {
		t.Fatal("install temporary GC fault signature", scrub(err, false))
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal("commit temporary GC fault", scrub(err, false))
	}
	return func() {
		cleanupTx, cleanupErr := admin.Begin(ctx)
		if cleanupErr != nil {
			t.Errorf("begin GC fault cleanup: %v", cleanupErr)
			return
		}
		defer func() { _ = cleanupTx.Rollback(ctx) }()
		if _, cleanupErr = cleanupTx.Exec(ctx, fmt.Sprintf(`DROP TRIGGER IF EXISTS %s ON pulse_durable_distributed.batches`, triggerSQL)); cleanupErr != nil {
			t.Errorf("drop GC fault trigger: %v", scrub(cleanupErr, false))
			return
		}
		if _, cleanupErr = cleanupTx.Exec(ctx, fmt.Sprintf(`DROP FUNCTION IF EXISTS pulse_durable_distributed.%s()`, functionSQL)); cleanupErr != nil {
			t.Errorf("drop GC fault function: %v", scrub(cleanupErr, false))
			return
		}
		cleanSig, sigErr := signature(ctx, cleanupTx)
		if sigErr != nil {
			t.Errorf("restore GC fault layout signature: %v", sigErr)
			return
		}
		if _, cleanupErr = cleanupTx.Exec(ctx, `UPDATE pulse_durable_distributed.meta SET signature=$1`, cleanSig); cleanupErr != nil {
			t.Errorf("persist restored GC layout signature: %v", scrub(cleanupErr, false))
			return
		}
		if cleanupErr = cleanupTx.Commit(ctx); cleanupErr != nil {
			t.Errorf("commit GC fault cleanup: %v", scrub(cleanupErr, false))
		}
	}
}

func TestPostgresHistoryGCDeleteFailureRollsBackAllState(t *testing.T) {
	f := newGCIntegrationFixture(t)
	plan, err := f.c.PrepareHistoryGC(context.Background(), f.request)
	if err != nil {
		t.Fatal("prepare GC plan", err)
	}
	cleanupFault := installGCDeleteFailure(t, f.admin)
	t.Cleanup(cleanupFault)
	before := f.retainedState(t)
	execute := d.HistoryGCExecute{Identity: f.request.Identity, PlanID: f.request.PlanID, PlanDigest: plan.PlanDigest}
	if _, err = f.c.ExecuteHistoryGC(context.Background(), execute); err == nil {
		t.Fatal("DELETE trigger failure was swallowed")
	}
	if got := f.retainedState(t); got != before {
		t.Fatalf("GC failure changed retained state: before=%s after=%s", before, got)
	}
	var floor, through string
	var batches, receipts int
	var executed bool
	if err = f.admin.QueryRow(context.Background(), `SELECT r.history_floor::text,r.history_gc_through::text,(SELECT count(*) FROM pulse_durable_distributed.batches WHERE namespace=$1 AND run_id=$2),(SELECT count(*) FROM pulse_durable_distributed.history_gc_receipts WHERE namespace=$1 AND run_id=$2),g.executed FROM pulse_durable_distributed.runs r JOIN pulse_durable_distributed.history_gc_plans g ON g.namespace=r.namespace AND g.run_id=r.run_id AND g.plan_id=$3 WHERE r.namespace=$1 AND r.run_id=$2`, f.request.Identity.Namespace, f.request.Identity.RunID, f.request.PlanID).Scan(&floor, &through, &batches, &receipts, &executed); err != nil {
		t.Fatal("read rollback state", scrub(err, false))
	}
	if floor != "1" || through != "1" || batches != 5 || receipts != 0 || executed {
		t.Fatalf("GC rollback incomplete: floor=%s through=%s batches=%d receipts=%d executed=%v", floor, through, batches, receipts, executed)
	}
}
