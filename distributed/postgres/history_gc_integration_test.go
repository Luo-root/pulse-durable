// 本文件使用专用 PostgreSQL 测试库验证历史 GC 的真实事务边界。
// 夹具通过正式 checkpoint/合成/Finish API 创建；仅由管理员将 apply 时间
// 移到过去以触发 TTL，不调整主机时钟，不使用业务库。
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/jackc/pgx/v5/pgxpool"
)

// gcIntegrationFixture 的 active base 是 revision 5，head 是 revision 6。
// batch 2..4 是可回收前缀，batch 5 为边界，batch 6 是必须保留的后缀。
type gcIntegrationFixture struct {
	c       *Coordinator
	admin   *pgxpool.Pool
	run     *snapshotTestRun
	request d.HistoryGCRequest
	compact CompactStorageRequest
}

func newGCIntegrationFixture(t *testing.T) gcIntegrationFixture {
	t.Helper()
	c, admin, ns := integration(t)
	ctx := context.Background()
	var role string
	if err := c.pool.QueryRow(ctx, "SELECT session_user").Scan(&role); err != nil {
		t.Fatal(err)
	}
	if err := Migrate(ctx, os.Getenv("PULSE_DISTRIBUTED_TEST_DSN"), Grant{Role: role, Namespace: ns, Permissions: []string{"maintain", "replay", "retain"}}); err != nil {
		t.Fatal("grant GC fixture capabilities", err)
	}
	h := newSnapshotTestRun(t, c, admin, ns, "history-gc")
	for i := 0; i < 4; i++ {
		h.apply(t, snapshotNodeBatch(t, h.r, snapshotDataNode(fmt.Sprintf("gc-node-%d", i), i+1), nil))
	}
	compact := CompactStorageRequest{Identity: h.token.Identity, ExpectedGeneration: 1, ThroughRevision: 5, Mutation: d.Mutation{ID: "gc-base"}}
	compact.Mutation.Digest = CompactStorageDigest(compact)
	if _, err := c.CompactStorage(ctx, compact); err != nil {
		t.Fatal("publish GC base", err)
	}
	h.apply(t, snapshotNodeBatch(t, h.r, snapshotDataNode("gc-suffix", 5), nil))
	h.reload(t)
	if _, err := c.Finish(ctx, d.FinishRequest{Token: h.token, ExpectedRevision: h.r.Revision, Outcome: durable.OutcomeCompleted, Mutation: mut("gc-finish")}); err != nil {
		t.Fatal("finish GC fixture", err)
	}
	// 权威 apply guard 保留，每条 batch 的 TTL 均从相应提交时间起算。
	if _, err := admin.Exec(ctx, `UPDATE pulse_durable_distributed.mutation_guards SET occurred_at=clock_timestamp()-interval '2 hours' WHERE namespace=$1 AND run_id=$2 AND operation='apply'`, ns, h.r.RunID); err != nil {
		t.Fatal("age only GC fixture apply times", scrub(err, false))
	}
	policy, err := c.GetRetentionPolicy(ctx, ns)
	if err != nil {
		t.Fatal(err)
	}
	ttl := time.Hour
	proposed := d.RetentionPolicy{Enabled: true, ReplayHistoryTTL: &ttl}
	ver, err := strconv.ParseUint(policy.PolicyVersion, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	preview, err := c.PreviewRetentionPolicy(ctx, "gc-enable-"+ns, ns, ver, proposed, d.RetentionCursor{}, 10, now.Add(-24*time.Hour), now)
	if err != nil {
		t.Fatal("preview GC policy", err)
	}
	policy, err = c.UpdateRetentionPolicy(ctx, d.RetentionPolicyUpdate{Namespace: ns, ExpectedVersion: ver, Policy: proposed, DryRunID: preview.DryRunID})
	if err != nil {
		t.Fatal("enable GC policy", err)
	}
	ver, err = strconv.ParseUint(policy.PolicyVersion, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return gcIntegrationFixture{c: c, admin: admin, run: h, request: d.HistoryGCRequest{Identity: h.token.Identity, PlanID: "gc-plan", PolicyVersion: ver, PolicyDigest: policy.PolicyDigest, MaxRows: 2, MaxBytes: d.HistoryGCMaxBytes}, compact: compact}
}

// image 用真实 SQL 读取当前快照及后缀，再由生产重放代码冷物化，不依赖 worker
// 内存投影。终态 Run 不能伪造 token 使用 LoadOwned，故采用管理员取证读取。
func (f gcIntegrationFixture) image(t *testing.T) (durable.RunRecord, string) {
	t.Helper()
	var wire []byte
	if err := f.admin.QueryRow(context.Background(), `SELECT pulse_durable_distributed.storage_image(r,r.revision,true) FROM pulse_durable_distributed.runs r WHERE namespace=$1 AND run_id=$2`, f.request.Identity.Namespace, f.request.Identity.RunID).Scan(&wire); err != nil {
		t.Fatal("read cold recovery image", scrub(err, false))
	}
	r, chain, err := buildMaintenanceImage(context.Background(), CompactStorageRequest{Identity: f.request.Identity, ThroughRevision: f.run.r.Revision}, wire)
	if err != nil {
		t.Fatal("replay cold recovery image", err)
	}
	return r, chain
}

func (f gcIntegrationFixture) retainedState(t *testing.T) string {
	t.Helper()
	var out string
	if err := f.admin.QueryRow(context.Background(), `SELECT jsonb_build_object(
 'run',to_jsonb(r)-'history_floor'-'history_gc_through'-'maintenance_watermark',
 'guards',(SELECT jsonb_agg(to_jsonb(g) ORDER BY mutation_id) FROM pulse_durable_distributed.mutation_guards g WHERE namespace=$1 AND run_id=$2),
 'receipts',(SELECT jsonb_agg(to_jsonb(g) ORDER BY mutation_id) FROM pulse_durable_distributed.mutation_receipts g WHERE namespace=$1 AND run_id=$2),
 'maintenance_guards',(SELECT jsonb_agg(to_jsonb(g) ORDER BY mutation_id) FROM pulse_durable_distributed.maintenance_guards g WHERE namespace=$1 AND run_id=$2),
 'maintenance_receipts',(SELECT jsonb_agg(to_jsonb(g) ORDER BY mutation_id) FROM pulse_durable_distributed.maintenance_receipts g WHERE namespace=$1 AND run_id=$2),
 'snapshots',(SELECT jsonb_agg(to_jsonb(s) ORDER BY generation) FROM pulse_durable_distributed.storage_snapshots s WHERE namespace=$1 AND run_id=$2),
 'build_pins',(SELECT jsonb_agg(to_jsonb(g) ORDER BY mutation_id) FROM pulse_durable_distributed.build_pins g WHERE namespace=$1 AND run_id=$2),
 'pins',(SELECT jsonb_agg(to_jsonb(g) ORDER BY pin_id) FROM pulse_durable_distributed.history_pins g WHERE namespace=$1 AND run_id=$2),
 'pin_guards',(SELECT jsonb_agg(to_jsonb(g) ORDER BY mutation_id) FROM pulse_durable_distributed.pin_guards g WHERE namespace=$1 AND run_id=$2),
 'pin_receipts',(SELECT jsonb_agg(to_jsonb(g) ORDER BY mutation_id) FROM pulse_durable_distributed.pin_receipts g WHERE namespace=$1 AND run_id=$2),
 'events',(SELECT jsonb_agg(to_jsonb(g) ORDER BY event_seq) FROM pulse_durable_distributed.events g WHERE namespace=$1 AND run_id=$2),
 'objects',(SELECT jsonb_agg(to_jsonb(o) ORDER BY kind,key1,key2,key3) FROM pulse_durable_distributed.snapshot_objects o WHERE namespace=$1 AND run_id=$2),
 'containers',(SELECT jsonb_agg(to_jsonb(c) ORDER BY kind,key1) FROM pulse_durable_distributed.snapshot_containers c WHERE namespace=$1 AND run_id=$2))::text
 FROM pulse_durable_distributed.runs r WHERE namespace=$1 AND run_id=$2`, f.request.Identity.Namespace, f.request.Identity.RunID).Scan(&out); err != nil {
		t.Fatal("capture permanently retained state", scrub(err, false))
	}
	return out
}

func TestPostgresHistoryGCBoundedRecoveryAndReceipts(t *testing.T) {
	f := newGCIntegrationFixture(t)
	ctx := context.Background()
	before, beforeChain := f.image(t)
	retained := f.retainedState(t)
	plan, err := f.c.PrepareHistoryGC(ctx, f.request)
	if err != nil {
		t.Fatal("prepare bounded prefix", err)
	}
	if plan.FirstRevision != 2 || plan.ThroughRevision != 3 || plan.Rows != 2 || plan.SnapshotRevision != 5 || plan.AfterFloor != 5 || plan.Validate(f.request) != nil {
		t.Fatalf("unexpected bounded plan: %+v", plan)
	}
	query := d.HistoryGCQuery{Identity: f.request.Identity, PlanID: f.request.PlanID, RequestDigest: f.request.Digest()}
	observed, err := f.c.FindHistoryGC(ctx, query)
	if err != nil || observed.State != d.HistoryGCPrepared || observed.Plan == nil || observed.Plan.PlanDigest != plan.PlanDigest {
		t.Fatal("query unknown prepare using request digest", observed.State, err)
	}
	again, err := f.c.PrepareHistoryGC(ctx, f.request)
	if err != nil || !reflect.DeepEqual(again, plan) {
		t.Fatal("prepare retry changed immutable plan", err)
	}
	changed := f.request
	changed.MaxRows++
	if _, err := f.c.PrepareHistoryGC(ctx, changed); !errors.Is(err, d.ErrConflict) {
		t.Fatal("same plan ID allowed a changed request", err)
	}
	execute := d.HistoryGCExecute{Identity: f.request.Identity, PlanID: f.request.PlanID, PlanDigest: plan.PlanDigest}
	receipt, err := f.c.ExecuteHistoryGC(ctx, execute)
	if err != nil || receipt.Validate(plan) != nil {
		t.Fatal("execute bounded prefix", err)
	}
	var floor, deletedThrough string
	var batchCount int
	if err := f.admin.QueryRow(ctx, `SELECT history_floor::text,history_gc_through::text,(SELECT count(*) FROM pulse_durable_distributed.batches WHERE namespace=$1 AND run_id=$2) FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2`, f.request.Identity.Namespace, f.request.Identity.RunID).Scan(&floor, &deletedThrough, &batchCount); err != nil || floor != "5" || deletedThrough != "3" || batchCount != 3 {
		t.Fatal("GC floor, budget or deletion accounting", floor, deletedThrough, batchCount, scrub(err, false))
	}
	after, afterChain := f.image(t)
	if !reflect.DeepEqual(before, after) || beforeChain != afterChain || !reflect.DeepEqual(after, f.run.r) {
		t.Fatal("GC changed cold recovery projection or logical chain")
	}
	if f.retainedState(t) != retained {
		t.Fatal("GC changed identity, current budget, snapshots, guard, receipt, pin or event")
	}
	retried, err := f.c.ExecuteHistoryGC(ctx, execute)
	if err != nil || !reflect.DeepEqual(retried, receipt) {
		t.Fatal("execute retry did not return exact permanent receipt", err)
	}
	observed, err = f.c.FindHistoryGC(ctx, query)
	if err != nil || observed.State != d.HistoryGCExecuted || observed.Receipt == nil || !reflect.DeepEqual(*observed.Receipt, receipt) {
		t.Fatal("query committed deletion", observed.State, err)
	}
	// 用第二个计划回收同一 base 前仍余下的 batch 4；base 5 与 suffix 6 保留。
	next := f.request
	next.PlanID = "gc-plan-next"
	p2, err := f.c.PrepareHistoryGC(ctx, next)
	if err != nil || p2.FirstRevision != 4 || p2.ThroughRevision != 4 || p2.BeforeFloor != 5 {
		t.Fatal("prepare remaining prefix", err)
	}
	if _, err = f.c.ExecuteHistoryGC(ctx, d.HistoryGCExecute{Identity: next.Identity, PlanID: next.PlanID, PlanDigest: p2.PlanDigest}); err != nil {
		t.Fatal("execute remaining prefix", err)
	}
	if _, err = f.c.CompactStorage(ctx, f.compact); err != nil {
		t.Fatal("exact old compact receipt became unavailable", err)
	}
	newCompact := f.compact
	newCompact.ExpectedGeneration = 2
	newCompact.Mutation.ID = "gc-recompact-base"
	newCompact.Mutation.Digest = CompactStorageDigest(newCompact)
	if _, err = f.c.CompactStorage(ctx, newCompact); err != nil {
		t.Fatal("compact at preserved GC base", err)
	}
	// 永久执行标记存在但回执被管理员破坏，不能谎称 prepared 或再次删除。
	if _, err = f.admin.Exec(ctx, `DELETE FROM pulse_durable_distributed.history_gc_receipts WHERE namespace=$1 AND run_id=$2 AND plan_id=$3`, query.Identity.Namespace, query.Identity.RunID, query.PlanID); err != nil {
		t.Fatal(err)
	}
	if _, err = f.c.FindHistoryGC(ctx, query); !errors.Is(err, d.ErrUnknown) {
		t.Fatal("missing committed GC receipt downgraded", err)
	}
	if _, err = f.c.ExecuteHistoryGC(ctx, execute); !errors.Is(err, d.ErrUnknown) {
		t.Fatal("missing committed GC receipt allowed retry", err)
	}
}

func TestPostgresHistoryGCPinsAndRevalidation(t *testing.T) {
	f := newGCIntegrationFixture(t)
	ctx := context.Background()
	plan, err := f.c.PrepareHistoryGC(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	var chain string
	if err = f.admin.QueryRow(ctx, `SELECT next_chain FROM pulse_durable_distributed.batches WHERE namespace=$1 AND run_id=$2 AND revision=3`, f.request.Identity.Namespace, f.request.Identity.RunID).Scan(&chain); err != nil {
		t.Fatal(err)
	}
	ttl := time.Hour
	pin := d.PinRequest{Identity: f.request.Identity, PinID: "gc-protect", Revision: 3, Chain: chain, HolderDigest: digest([]byte("gc-holder")), TTL: &ttl, Mutation: d.Mutation{ID: "gc-pin-create"}}
	pin.Mutation.Digest = pin.Digest()
	if _, err = f.c.PinHistory(ctx, pin); err != nil {
		t.Fatal("pin after prepare", err)
	}
	before := f.run.publication(t)
	execute := d.HistoryGCExecute{Identity: f.request.Identity, PlanID: f.request.PlanID, PlanDigest: plan.PlanDigest}
	if _, err = f.c.ExecuteHistoryGC(ctx, execute); !errors.Is(err, d.ErrPinned) {
		t.Fatal("GC ignored newly created pin", err)
	}
	if f.run.publication(t) != before {
		t.Fatal("pinned refusal published deletion or changed floor")
	}
	release := d.ReleasePinRequest{Identity: pin.Identity, PinID: pin.PinID, ExpectedVersion: 1, HolderDigest: pin.HolderDigest, Mutation: d.Mutation{ID: "gc-pin-release"}}
	release.Mutation.Digest = release.Digest()
	if err = f.c.ReleasePin(ctx, release); err != nil {
		t.Fatal(err)
	}
	if _, err = f.c.ExecuteHistoryGC(ctx, execute); err != nil {
		t.Fatal("released pin should no longer prevent prefix GC", err)
	}
	// batch 4 仍在，但 floor 已提升到5；不能因单条链存在创建不可恢复的引用。
	if err = f.admin.QueryRow(ctx, `SELECT next_chain FROM pulse_durable_distributed.batches WHERE namespace=$1 AND run_id=$2 AND revision=4`, pin.Identity.Namespace, pin.Identity.RunID).Scan(&chain); err != nil {
		t.Fatal(err)
	}
	pin.PinID = "gc-below-floor"
	pin.Revision = 4
	pin.Chain = chain
	pin.Mutation.ID = "gc-pin-below-floor"
	pin.Mutation.Digest = pin.Digest()
	if _, err = f.c.PinHistory(ctx, pin); !errors.Is(err, d.ErrHistoryExpired) {
		t.Fatal("retained target batch bypassed history floor", err)
	}
	if err = f.admin.QueryRow(ctx, `SELECT chain FROM pulse_durable_distributed.storage_snapshots WHERE namespace=$1 AND run_id=$2 AND generation=2`, pin.Identity.Namespace, pin.Identity.RunID).Scan(&chain); err != nil {
		t.Fatal(err)
	}
	pin.PinID = "gc-at-floor"
	pin.Revision = 5
	pin.Chain = chain
	pin.Mutation.ID = "gc-pin-at-floor"
	pin.Mutation.Digest = pin.Digest()
	if _, err = f.c.PinHistory(ctx, pin); err != nil {
		t.Fatal("published snapshot base should remain pinnable", err)
	}
}

func TestPostgresHistoryGCRejectsChangedPolicyAndClock(t *testing.T) {
	f := newGCIntegrationFixture(t)
	ctx := context.Background()
	plan, err := f.c.PrepareHistoryGC(ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	execute := d.HistoryGCExecute{Identity: f.request.Identity, PlanID: f.request.PlanID, PlanDigest: plan.PlanDigest}
	var original *time.Time
	if err = f.admin.QueryRow(ctx, `SELECT maintenance_watermark FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2`, f.request.Identity.Namespace, f.request.Identity.RunID).Scan(&original); err != nil {
		t.Fatal(err)
	}
	if _, err = f.admin.Exec(ctx, `UPDATE pulse_durable_distributed.runs SET maintenance_watermark=clock_timestamp()+interval '1 hour' WHERE namespace=$1 AND run_id=$2`, f.request.Identity.Namespace, f.request.Identity.RunID); err != nil {
		t.Fatal(err)
	}
	before := f.run.publication(t)
	if _, err = f.c.ExecuteHistoryGC(ctx, execute); !errors.Is(err, d.ErrClockRegression) {
		t.Fatal("GC ignored future database watermark", err)
	}
	if f.run.publication(t) != before {
		t.Fatal("clock rejection changed GC authority")
	}
	if _, err = f.admin.Exec(ctx, `UPDATE pulse_durable_distributed.runs SET maintenance_watermark=$3 WHERE namespace=$1 AND run_id=$2`, f.request.Identity.Namespace, f.request.Identity.RunID, original); err != nil {
		t.Fatal(err)
	}
	policy, err := f.c.GetRetentionPolicy(ctx, f.request.Identity.Namespace)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.c.UpdateRetentionPolicy(ctx, d.RetentionPolicyUpdate{Namespace: f.request.Identity.Namespace, ExpectedVersion: f.request.PolicyVersion, Policy: d.RetentionPolicy{Enabled: false, ReplayHistoryTTL: policy.Policy.ReplayHistoryTTL}}); err != nil {
		t.Fatal("disable retention policy", err)
	}
	before = f.run.publication(t)
	if _, err = f.c.ExecuteHistoryGC(ctx, execute); !errors.Is(err, d.ErrConflict) {
		t.Fatal("GC executed stale/disabled policy", err)
	}
	if f.run.publication(t) != before {
		t.Fatal("policy conflict published deletion")
	}
}

// 原始 SQL 也必须验证摘要及整数信封，不能只依赖可信 Go 适配器前置校验。
func TestPostgresHistoryGCRejectsRawArguments(t *testing.T) {
	f := newGCIntegrationFixture(t)
	ctx := context.Background()
	a := args(f.request.Identity)
	a["plan_id"] = f.request.PlanID
	a["policy_version"] = strconv.FormatUint(f.request.PolicyVersion, 10)
	a["policy_digest"] = f.request.PolicyDigest
	a["max_rows"] = strconv.FormatUint(f.request.MaxRows, 10)
	a["max_bytes"] = strconv.FormatUint(f.request.MaxBytes, 10)
	a["request_digest"] = strings.Repeat("f", 64)
	before := f.run.publication(t)
	if _, err := f.c.historyGCCall(ctx, "prepare", a, true); !errors.Is(err, d.ErrInvalid) {
		t.Fatal("SQL accepted forged request digest", err)
	}
	a["request_digest"] = f.request.Digest()
	a["max_rows"] = "02"
	if _, err := f.c.historyGCCall(ctx, "prepare", a, true); !errors.Is(err, d.ErrInvalid) {
		t.Fatal("SQL accepted noncanonical integer", err)
	}
	if f.run.publication(t) != before {
		t.Fatal("malformed GC request published state")
	}
	var count int
	if err := f.admin.QueryRow(ctx, `SELECT count(*) FROM pulse_durable_distributed.history_gc_plans WHERE namespace=$1 AND run_id=$2`, f.request.Identity.Namespace, f.request.Identity.RunID).Scan(&count); err != nil || count != 0 {
		t.Fatal("malformed request reserved plan", count, scrub(err, false))
	}
	// 保存的计划或回执不是任意 JSON 容器，响应仍需严格字段校验。
	if _, err := json.Marshal(f.request); err != nil {
		t.Fatal(err)
	}
}
