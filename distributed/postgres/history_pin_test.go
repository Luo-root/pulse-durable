package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestHistoryPinStrictDecode(t *testing.T) {
	q := d.PinQuery{Identity: d.Identity{Namespace: "ns", RunID: "run"}, PinID: "pin"}
	for _, wire := range []string{`{}`, `{"status":"absent"}`, `{"status":"unknown","state":null}`, `{"status":"absent","state":null,"extra":1}`, `{"status":"absent","status":"active","state":null}`, `{"status":null,"state":null}`} {
		if _, e := decodePinResult([]byte(wire), q.Identity, q.PinID); !errors.Is(e, d.ErrPinUnknown) {
			t.Fatal(wire, e)
		}
	}
	if out, e := decodePinResult([]byte(`{"status":"absent","state":null}`), q.Identity, q.PinID); e != nil || out.Status != d.PinAbsent {
		t.Fatal(out, e)
	}
	mq := d.PinMutationQuery{Identity: q.Identity, PinID: q.PinID, Mutation: mut("m"), Operation: d.PinCreateOperation}
	for _, wire := range []string{`{}`, `{"status":"absent"}`, `{"status":"unknown","receipt":null}`, `{"status":"found","receipt":null}`} {
		if _, e := decodePinMutation([]byte(wire), mq); !errors.Is(e, d.ErrPinUnknown) {
			t.Fatal(wire, e)
		}
	}
	for code, want := range map[string]error{"PH001": d.ErrPinInvalid, "PH002": d.ErrPinConflict, "PH003": d.ErrPinNotFound, "PH004": d.ErrPinExpired, "PH005": d.ErrPinUnknown, "PD007": d.ErrClockRegression} {
		if e := pinError(&pgconn.PgError{Code: code, Message: "secret"}, true); !errors.Is(e, want) || strings.Contains(e.Error(), "secret") {
			t.Fatal(code, e)
		}
	}
	if !errors.Is(pinError(errors.New("lost response"), true), d.ErrPinUnknown) {
		t.Fatal("unknown write")
	}
	c := new(Coordinator)
	if _, e := c.pinCall(context.Background(), "pin_lock", nil, true); !errors.Is(e, d.ErrPinInvalid) {
		t.Fatal(e)
	}
	ttl := d.PinTTLMax
	a := pinArgs(q.Identity, q.PinID, mut("m"), digest(nil), ^uint64(0), ^uint64(0), digest(nil), &ttl)
	b, e := json.Marshal(a)
	if e != nil || !strings.Contains(string(b), `"revision":"18446744073709551615"`) || !strings.Contains(string(b), `"ttl_ns":"9223372036000000000"`) {
		t.Fatal(string(b), e)
	}
}

func TestPostgresHistoryPin(t *testing.T) {
	c, admin, ns := integration(t)
	ctx := context.Background()
	var role string
	if e := c.pool.QueryRow(ctx, "SELECT session_user").Scan(&role); e != nil {
		t.Fatal(e)
	}
	permission := func(perm string, enabled bool) {
		t.Helper()
		var e error
		if enabled {
			_, e = admin.Exec(ctx, "INSERT INTO pulse_durable_distributed.access VALUES($1,$2,$3) ON CONFLICT DO NOTHING", role, ns, perm)
		} else {
			_, e = admin.Exec(ctx, "DELETE FROM pulse_durable_distributed.access WHERE role_name=$1 AND namespace=$2 AND permission=$3", role, ns, perm)
		}
		if e != nil {
			t.Fatal(e)
		}
	}
	permission("replay", true)
	task := taskFixture(t, ns, "pin<>&\u2028\u2029")
	if _, e := c.Submit(ctx, task); e != nil {
		t.Fatal(e)
	}
	ttl := time.Hour
	q := d.PinRequest{Identity: task.Identity(), PinID: "pin<>&\u2028\u2029", Revision: 1, Chain: digest(nil), HolderDigest: digest([]byte("holder")), TTL: &ttl, Mutation: d.Mutation{ID: "create"}}
	sign := func() { q.Mutation.Digest = q.Digest() }
	sign()
	if _, e := c.PinHistory(ctx, q); !errors.Is(e, d.ErrPinNotFound) {
		t.Fatal("queued pin", e)
	}
	claim := claimOne(t, c, task, "pin-init", time.Minute)
	if e := c.InitializeOwned(ctx, d.InitializeRequest{Token: claim.Token, Record: record(task, 1), Mutation: mut("init")}); e != nil {
		t.Fatal(e)
	}
	if e := admin.QueryRow(ctx, "SELECT head_chain FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2", ns, q.Identity.RunID).Scan(&q.Chain); e != nil {
		t.Fatal(e)
	}
	sign()
	// SQL 必须重算摘要，失败不能留下 guard 或 pin。
	bad := pinArgs(q.Identity, q.PinID, q.Mutation, q.HolderDigest, 1, 0, q.Chain, q.TTL)
	bad["mutation_digest"] = digest(nil)
	if _, e := c.pinCall(ctx, "create", bad, true); !errors.Is(e, d.ErrPinInvalid) {
		t.Fatal("raw digest", e)
	}
	var count int
	if e := admin.QueryRow(ctx, "SELECT count(*) FROM pulse_durable_distributed.pin_guards WHERE namespace=$1", ns).Scan(&count); e != nil || count != 0 {
		t.Fatal(count, e)
	}
	// 仅 worker/query/submit/cancel 不隐式获得 pin 写权限。
	permission("replay", false)
	if _, err := c.PinHistory(ctx, q); err == nil {
		t.Fatal("worker gained replay permission")
	}
	permission("maintain", true)
	if _, err := c.PinHistory(ctx, q); err == nil {
		t.Fatal("maintain substituted for finite create replay")
	}
	permission("maintain", false)
	permission("replay", true)
	created, e := c.PinHistory(ctx, q)
	if e != nil {
		t.Fatal(e)
	}
	if created.State.LongLived || created.State.Version != 1 {
		t.Fatal(created)
	}
	renew := d.RenewPinRequest{Identity: q.Identity, PinID: q.PinID, Mutation: d.Mutation{ID: "renew"}, ExpectedVersion: 1, HolderDigest: q.HolderDigest}
	renew.Mutation.Digest = renew.Digest()
	renewed, e := c.RenewPin(ctx, renew)
	if e != nil || renewed.State.Version != 2 || renewed.State.ExpiresAt.Before(created.State.ExpiresAt) {
		t.Fatal(renewed, e)
	}
	// 物理合成不使原历史引用不可查，创建新 pin 也继续使用原链。
	permission("maintain", true)
	compact := CompactStorageRequest{Identity: q.Identity, ExpectedGeneration: 1, ThroughRevision: 1, Mutation: d.Mutation{ID: "pin-compact"}}
	compact.Mutation.Digest = CompactStorageDigest(compact)
	if _, err := c.CompactStorage(ctx, compact); err != nil {
		t.Fatal(err)
	}
	permission("maintain", false)
	if got, err := c.FindPin(ctx, d.PinQuery{Identity: q.Identity, PinID: q.PinID}); err != nil || got.State.Chain != q.Chain {
		t.Fatal(got, err)
	}
	// nil 保留原有限策略；旧 receipt 先于当前版本 CAS。
	again, e := c.RenewPin(ctx, renew)
	if e != nil || again.State.Version != 2 || !again.State.ExpiresAt.Equal(renewed.State.ExpiresAt) {
		t.Fatal(again, e)
	}
	again, e = c.PinHistory(ctx, q)
	if e != nil || again.State.Version != 1 {
		t.Fatal(again, e)
	}
	mq := d.PinMutationQuery{Identity: q.Identity, PinID: q.PinID, Mutation: renew.Mutation, Operation: d.PinRenewOperation}
	found, e := c.FindPinMutation(ctx, mq)
	if e != nil || found.Status != d.PinMutationFound || found.Receipt.Version != 2 {
		t.Fatal(found, e)
	}
	conflict := renew
	conflict.Mutation.ID = q.Mutation.ID
	conflict.Mutation.Digest = conflict.Digest()
	if _, e = c.RenewPin(ctx, conflict); !errors.Is(e, d.ErrPinConflict) {
		t.Fatal("cross operation", e)
	}
	permission("replay", false)
	if _, e = c.RenewPin(ctx, renew); e == nil {
		t.Fatal("revoked receipt replay")
	}
	if _, e = c.PinHistory(ctx, q); e == nil {
		t.Fatal("revoked create replay")
	}
	permission("query", false)
	if _, e = c.FindPinMutation(ctx, mq); e == nil {
		t.Fatal("revoked receipt query")
	}
	permission("query", true)
	permission("replay", true)
	foreign := q
	foreign.Identity.Namespace = ns + "-other"
	foreign.Mutation.Digest = foreign.Digest()
	if _, e = c.PinHistory(ctx, foreign); e == nil {
		t.Fatal("cross namespace")
	}
	// 即使知道 holder 摘要，另一个真实会话主体也不能续期。
	if _, e = admin.Exec(ctx, "UPDATE pulse_durable_distributed.history_pins SET holder=current_user WHERE namespace=$1", ns); e != nil {
		t.Fatal(e)
	}
	if _, e = c.RenewPin(ctx, renew); e == nil {
		t.Fatal("forged holder")
	}
	if _, e = admin.Exec(ctx, "UPDATE pulse_durable_distributed.history_pins SET holder=$2 WHERE namespace=$1", ns, role); e != nil {
		t.Fatal(e)
	}
	// 两个不同新 mutation 竞争相同版本，只有一个成功。
	var wg sync.WaitGroup
	answers := make(chan error, 2)
	for _, id := range []string{"cas-a", "cas-b"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			r := renew
			r.ExpectedVersion = 2
			r.Mutation.ID = id
			r.Mutation.Digest = r.Digest()
			_, err := c.RenewPin(ctx, r)
			answers <- err
		}(id)
	}
	wg.Wait()
	close(answers)
	success, conflicts := 0, 0
	for err := range answers {
		if err == nil {
			success++
		} else if errors.Is(err, d.ErrPinConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflicts != 1 {
		t.Fatal(success, conflicts)
	}
	release := d.ReleasePinRequest{Identity: q.Identity, PinID: q.PinID, ExpectedVersion: 3, HolderDigest: q.HolderDigest, Mutation: d.Mutation{ID: "release"}}
	release.Mutation.Digest = release.Digest()
	if e = c.ReleasePin(ctx, release); e != nil {
		t.Fatal(e)
	}
	if e = c.ReleasePin(ctx, release); e != nil {
		t.Fatal("release receipt", e)
	}
	renew.ExpectedVersion = 4
	renew.Mutation.ID = "revive"
	renew.Mutation.Digest = renew.Digest()
	if _, e = c.RenewPin(ctx, renew); !errors.Is(e, d.ErrPinConflict) {
		t.Fatal("released revived", e)
	}
	state, e := c.FindPin(ctx, d.PinQuery{Identity: q.Identity, PinID: q.PinID})
	if e != nil || state.Status != d.PinReleased {
		t.Fatal(state, e)
	}
	// PinID 永久不复用，跨 pin 的 mutation 也不能复用。
	reused := q
	reused.Mutation.ID = "reuse"
	reused.Mutation.Digest = reused.Digest()
	if _, e = c.PinHistory(ctx, reused); !errors.Is(e, d.ErrPinConflict) {
		t.Fatal(e)
	}
	reused.PinID = "different"
	reused.Mutation = q.Mutation
	reused.Mutation.Digest = reused.Digest()
	if _, e = c.PinHistory(ctx, reused); !errors.Is(e, d.ErrPinConflict) {
		t.Fatal(e)
	}
	// 缺损 receipt 必须未知，而非 absent。
	if _, e = admin.Exec(ctx, "DELETE FROM pulse_durable_distributed.pin_receipts WHERE namespace=$1 AND mutation_id=$2", ns, mq.Mutation.ID); e != nil {
		t.Fatal(e)
	}
	if _, e = c.FindPinMutation(ctx, mq); !errors.Is(e, d.ErrPinUnknown) {
		t.Fatal("missing receipt", e)
	}
	// 长期创建只能 maintain；撤销 maintain 不能借 replay 续长期 pin。
	long := q
	long.PinID = "long"
	long.TTL = nil
	long.Mutation.ID = "long"
	long.Mutation.Digest = long.Digest()
	if _, e = c.PinHistory(ctx, long); e == nil {
		t.Fatal("long needs maintain")
	}
	permission("maintain", true)
	lr, e := c.PinHistory(ctx, long)
	if e != nil || !lr.State.LongLived || !lr.State.ExpiresAt.IsZero() {
		t.Fatal(lr, e)
	}
	rr := d.RenewPinRequest{Identity: q.Identity, PinID: long.PinID, ExpectedVersion: 1, HolderDigest: q.HolderDigest, Mutation: d.Mutation{ID: "long-renew"}}
	rr.Mutation.Digest = rr.Digest()
	if _, e = c.RenewPin(ctx, rr); e != nil {
		t.Fatal(e)
	}
	permission("maintain", false)
	if _, e = c.RenewPin(ctx, rr); e == nil {
		t.Fatal("long receipt bypassed revocation")
	}
	permission("maintain", true)
	// 显式有限 TTL 可以缩回长期策略；nil 本身不改变策略。
	rr.ExpectedVersion = 2
	rr.Mutation.ID = "long-finite"
	rr.TTL = &ttl
	rr.Mutation.Digest = rr.Digest()
	if got, err := c.RenewPin(ctx, rr); err != nil || got.State.LongLived {
		t.Fatal(got, err)
	}
	// 管理员仅构造专用 fixture 的过期/版本/回退状态，不睡眠、不改系统时钟。
	if _, e = admin.Exec(ctx, "UPDATE pulse_durable_distributed.history_pins SET expires_at=clock_timestamp()-interval '1 second' WHERE namespace=$1 AND pin_id=$2", ns, long.PinID); e != nil {
		t.Fatal(e)
	}
	rr.ExpectedVersion = 3
	rr.Mutation.ID = "expired"
	rr.Mutation.Digest = rr.Digest()
	if _, e = c.RenewPin(ctx, rr); !errors.Is(e, d.ErrPinExpired) {
		t.Fatal(e)
	}
	got, e := c.FindPin(ctx, d.PinQuery{Identity: q.Identity, PinID: long.PinID})
	if e != nil || got.Status != d.PinExpired {
		t.Fatal(got, e)
	}
	if _, e = admin.Exec(ctx, "UPDATE pulse_durable_distributed.history_pins SET expires_at=clock_timestamp()+interval '1 hour',version=18446744073709551615 WHERE namespace=$1 AND pin_id=$2", ns, long.PinID); e != nil {
		t.Fatal(e)
	}
	rr.ExpectedVersion = ^uint64(0)
	rr.Mutation.ID = "max"
	rr.Mutation.Digest = rr.Digest()
	if _, e = c.RenewPin(ctx, rr); !errors.Is(e, d.ErrPinConflict) {
		t.Fatal(e)
	}
	// 版本耗尽不影响精确旧 receipt。
	oldLong := d.RenewPinRequest{Identity: q.Identity, PinID: long.PinID, ExpectedVersion: 1, HolderDigest: q.HolderDigest, Mutation: d.Mutation{ID: "long-renew"}}
	oldLong.Mutation.Digest = oldLong.Digest()
	if got, err := c.RenewPin(ctx, oldLong); err != nil || got.State.Version != 2 {
		t.Fatal(got, err)
	}
	// maintain 代释放仍严格执行 CAS，不允许跳版本。
	delegated := d.ReleasePinRequest{Identity: q.Identity, PinID: long.PinID, ExpectedVersion: 1, HolderDigest: digest([]byte("other")), Mutation: d.Mutation{ID: "delegate"}}
	delegated.Mutation.Digest = delegated.Digest()
	if err := c.ReleasePin(ctx, delegated); !errors.Is(err, d.ErrPinConflict) {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "UPDATE pulse_durable_distributed.history_pins SET version=8 WHERE namespace=$1 AND pin_id=$2", ns, long.PinID); err != nil {
		t.Fatal(err)
	}
	delegated.ExpectedVersion = 8
	delegated.Mutation.Digest = delegated.Digest()
	if err := c.ReleasePin(ctx, delegated); err != nil {
		t.Fatal(err)
	}
	// 最大 revision 使用真实存在的历史行验证，不接受仅伪造 head。
	maxPin := q
	maxPin.PinID = "max-revision"
	maxPin.Revision = ^uint64(0)
	maxPin.TTL = &ttl
	maxPin.Mutation.ID = "max-revision"
	maxPin.Mutation.Digest = maxPin.Digest()
	if _, err := admin.Exec(ctx, "UPDATE pulse_durable_distributed.runs SET revision=18446744073709551615 WHERE namespace=$1 AND run_id=$2", ns, q.Identity.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "INSERT INTO pulse_durable_distributed.batches VALUES($1,$2,18446744073709551615,$3,$4,$4,$4)", ns, q.Identity.RunID, []byte("fixture"), q.Chain); err != nil {
		t.Fatal(err)
	}
	if got, err := c.PinHistory(ctx, maxPin); err != nil || got.State.Revision != ^uint64(0) {
		t.Fatal(got, err)
	}
	// TTL 整秒上界不会经 JSON 浮点丢失。
	maxTTL := d.PinTTLMax
	maxPin.PinID = "max-ttl"
	maxPin.TTL = &maxTTL
	maxPin.Mutation.ID = "max-ttl"
	maxPin.Mutation.Digest = maxPin.Digest()
	if _, err := c.PinHistory(ctx, maxPin); err != nil {
		t.Fatal(err)
	}
	// 专用 fixture 在事务中直接验证数据库可表示时间的溢出映射。
	tx, err := admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, "SELECT pulse_durable_distributed.pin_deadline('294276-12-31 23:59:59+00'::timestamptz,9223372036)")
	if !errors.Is(pinError(err, true), d.ErrPinInvalid) {
		t.Fatal("DB deadline boundary", err)
	}
	_ = tx.Rollback(ctx)
	// 所有新入口可调用，内部 helper 不授予应用角色，PUBLIC 无执行权。
	rows, e := admin.Query(ctx, "SELECT p.proname,has_function_privilege($1,p.oid,'EXECUTE'),EXISTS(SELECT 1 FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a WHERE a.grantee=0) FROM pg_proc p WHERE pronamespace='pulse_durable_distributed'::regnamespace AND proname LIKE 'pin_%'", role)
	if e != nil {
		t.Fatal(e)
	}
	for rows.Next() {
		var name string
		var allowed, public bool
		if e = rows.Scan(&name, &allowed, &public); e != nil {
			t.Fatal(e)
		}
		entry := name == "pin_create" || name == "pin_renew" || name == "pin_release" || name == "pin_find" || name == "pin_find_mutation"
		if allowed != entry || public {
			t.Fatal(name, allowed, public)
		}
	}
	if e = rows.Err(); e != nil {
		t.Fatal(e)
	}
	rows.Close()
	if _, e = admin.Exec(ctx, "UPDATE pulse_durable_distributed.runs SET maintenance_watermark=clock_timestamp()+interval '1 hour' WHERE namespace=$1", ns); e != nil {
		t.Fatal(e)
	}
	if _, e = c.FindPin(ctx, d.PinQuery{Identity: q.Identity, PinID: q.PinID}); !errors.Is(e, d.ErrClockRegression) {
		t.Fatal(e)
	}
}
