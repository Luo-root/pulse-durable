package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/Luo-root/pulse-durable/recordio"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func taskFixture(t *testing.T, ns, id string) d.Task {
	t.Helper()
	g, e := pulse.New("distributed-test", "no side effects")
	if e != nil {
		t.Fatal(e)
	}
	if e = g.Add(pulse.NewNode("one", "one", nil, nil, func(*pulse.RunCtx) error { return nil })); e != nil {
		t.Fatal(e)
	}
	task, e := d.NewTask(context.Background(), d.Identity{Namespace: ns, RunID: id}, "test", "v1", g, pulse.Inputs{}, durable.NewCodecs())
	if e != nil {
		t.Fatal(e)
	}
	return task
}
func record(t d.Task, rev uint64) durable.RunRecord {
	return durable.RunRecord{RunID: t.Identity().RunID, Revision: rev, Version: t.Route().BusinessVersion, Definition: t.Route().Definition, Inputs: t.InputsFingerprint(), RootScope: "root", Scopes: map[string]durable.ScopeRecord{"root": {Graph: "test", Inputs: t.InputsFingerprint(), Nodes: map[string]durable.NodeRecord{}}}, Attempts: map[string]durable.AttemptRecord{}}
}
func mut(id string) d.Mutation { return d.Mutation{ID: id, Digest: digest([]byte(id))} }
func TestValidationAndLosslessRevision(t *testing.T) {
	task := taskFixture(t, "tenant", "run")
	r := record(task, math.MaxUint64)
	b, e := recordio.EncodeSnapshot(r)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = decodeRecord(task, math.MaxUint64, b); e != nil {
		t.Fatal(e)
	}
	r.Inputs = "wrong"
	b, _ = recordio.EncodeSnapshot(r)
	if _, e = decodeRecord(task, math.MaxUint64, b); !errors.Is(e, d.ErrInvalid) {
		t.Fatal(e)
	}
	if lease(strings.Repeat("a", 31)) || !lease(strings.Repeat("a", 32)) || ttl(0) || validText("x\x00") {
		t.Fatal("validation")
	}
	if !errors.Is(scrub(errors.New("secret password"), true), d.ErrUnknown) {
		t.Fatal("lost response")
	}
	if strings.Contains(scrub(errors.New("secret password"), false).Error(), "secret") {
		t.Fatal("leaked details")
	}
}

func TestClockRegressionIsSanitizedAndStable(t *testing.T) {
	e := scrub(&pgconn.PgError{Code: "PD007", Message: "clock regression with private details"}, false)
	if !errors.Is(e, d.ErrClockRegression) || strings.Contains(e.Error(), "private") {
		t.Fatalf("clock regression classification leaked or was lost: %v", e)
	}
}

// 集成测试同时要求专用 DSN 和显式启用破坏性操作。
// 它们创建角色并安装或验证专用模式，但绝不 DROP
// 任何模式（旧版或分布式）。使用一次性数据库；DDL 漂移时安全拒绝。
func integration(t *testing.T) (*Coordinator, *pgxpool.Pool, string) {
	t.Helper()
	dsn := os.Getenv("PULSE_DISTRIBUTED_TEST_DSN")
	if dsn == "" || os.Getenv("PULSE_DISTRIBUTED_TEST_ALLOW_DESTRUCTIVE") != "yes" {
		t.Skip("dedicated DSN and PULSE_DISTRIBUTED_TEST_ALLOW_DESTRUCTIVE=yes required")
	}
	ctx := context.Background()
	admin, e := connect(ctx, dsn)
	if e != nil {
		raw, rawErr := pgxpool.New(ctx, dsn)
		if rawErr == nil {
			defer raw.Close()
			rawErr = raw.Ping(ctx)
		}
		if rawErr != nil {
			t.Fatalf("raw test connection error type %T", rawErr)
		}
		t.Fatal("configured connection failed", e)
	}
	t.Cleanup(admin.Close)
	var exists bool
	if e = admin.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='pulse_durable_distributed')").Scan(&exists); e != nil {
		t.Fatal(scrub(e, false))
	}
	if exists {
		// 测试失败可能留下明确指定的专用模式。由 Migrate
		// 验证该模式，绝不悄然替换或删除。
	}
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	role := "pulse_dist_test_" + suffix
	if _, e = admin.Exec(ctx, "CREATE ROLE "+role+" LOGIN PASSWORD 'distributed-test-only'"); e != nil {
		t.Fatal(scrub(e, false))
	}
	ns := "isolated-test-" + suffix
	if e = Migrate(ctx, dsn, Grant{Role: role, Namespace: ns, Permissions: []string{"query", "worker", "submit", "cancel"}}); e != nil {
		t.Fatalf("migrate: %v", e)
	}
	t.Logf("migrate ok ns=%s role=%s", ns, role)
	var layoutMarker string
	if e = admin.QueryRow(ctx, "SELECT layout_marker FROM pulse_durable_distributed.meta").Scan(&layoutMarker); e != nil || layoutMarker != "pulse-durable-distributed-layout-v1" {
		t.Fatalf("migration layout marker=%q err=%v", layoutMarker, e)
	}
	cfg, e := pgxpool.ParseConfig(dsn)
	if e != nil {
		t.Fatal("DSN config")
	}
	u := url.URL{Scheme: "postgres", Host: net.JoinHostPort(cfg.ConnConfig.Host, strconv.Itoa(int(cfg.ConnConfig.Port))), Path: "/" + cfg.ConnConfig.Database, User: url.UserPassword(role, "distributed-test-only")}
	q := u.Query()
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()
	workerDSN := u.String()
	c, e := New(ctx, workerDSN)
	if e != nil {
		var ok bool
		var ce error
		ce = admin.QueryRow(ctx, "SELECT pulse_durable_distributed.check_schema()").Scan(&ok)
		t.Logf("admin check_schema=%v err=%v", ok, ce)
		var def string
		ce = admin.QueryRow(ctx, "SELECT pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='pulse_durable_distributed' AND p.proname='check_schema'").Scan(&def)
		t.Logf("check_schema def prefix=%q err=%v", func() string {
			if len(def) > 120 {
				return def[:120]
			}
			return def
		}(), ce)
		t.Fatal(e)
	}
	t.Cleanup(c.Close)
	if _, e = New(ctx, dsn); e == nil {
		t.Fatal("administrator accepted as worker")
	}
	if _, e = c.pool.Exec(ctx, "UPDATE pulse_durable_distributed.runs SET epoch=0"); e == nil {
		t.Fatal("worker has direct UPDATE")
	}
	return c, admin, ns
}

// setTestPermission 同时维护测试角色的业务权限行和 PostgreSQL 函数 ACL。
//
// 生产入口 Migrate 会一次性完成这两步；部分集成测试为了验证权限
// 动态撤销，会直接改变 access 表。若只改 access 而不同步 ACL，测试
// 看到的角色状态就不再是生产中的合法状态，后续入口会被 PostgreSQL
// 在进入业务权限判断前直接拒绝。该辅助函数只服务于一次性测试角色，
// 不属于产品授权路径，也不授予任何表权限。
func setTestPermission(t *testing.T, admin *pgxpool.Pool, ctx context.Context, role, ns, permission string, enabled bool) {
	t.Helper()
	if enabled {
		if _, err := admin.Exec(ctx, "INSERT INTO pulse_durable_distributed.access VALUES($1,$2,$3) ON CONFLICT DO NOTHING", role, ns, permission); err != nil {
			t.Fatal(err)
		}
	} else if _, err := admin.Exec(ctx, "DELETE FROM pulse_durable_distributed.access WHERE role_name=$1 AND namespace=$2 AND permission=$3", role, ns, permission); err != nil {
		t.Fatal(err)
	}

	var api, query, maintain, replay, retain bool
	if err := admin.QueryRow(ctx, `
SELECT
 EXISTS(SELECT 1 FROM pulse_durable_distributed.access WHERE role_name=$1 AND namespace=$2 AND permission IN ('query','worker','submit','cancel')),
 EXISTS(SELECT 1 FROM pulse_durable_distributed.access WHERE role_name=$1 AND namespace=$2 AND permission='query'),
 EXISTS(SELECT 1 FROM pulse_durable_distributed.access WHERE role_name=$1 AND namespace=$2 AND permission='maintain'),
 EXISTS(SELECT 1 FROM pulse_durable_distributed.access WHERE role_name=$1 AND namespace=$2 AND permission='replay'),
 EXISTS(SELECT 1 FROM pulse_durable_distributed.access WHERE role_name=$1 AND namespace=$2 AND permission='retain')`, role, ns).Scan(&api, &query, &maintain, &replay, &retain); err != nil {
		t.Fatal(err)
	}
	roleSQL := pgx.Identifier{role}.Sanitize()
	setFunctions := func(functions string, granted bool) {
		t.Helper()
		verb, suffix := "REVOKE EXECUTE ON FUNCTION ", " FROM "
		if granted {
			verb, suffix = "GRANT EXECUTE ON FUNCTION ", " TO "
		}
		if _, err := admin.Exec(ctx, verb+functions+suffix+roleSQL); err != nil {
			t.Fatal(err)
		}
	}
	setFunctions("pulse_durable_distributed.api(text,jsonb)", api)
	setFunctions("pulse_durable_distributed.storage_inspect(jsonb),pulse_durable_distributed.storage_compact_begin(jsonb),pulse_durable_distributed.storage_compact_publish(jsonb),pulse_durable_distributed.storage_compact_abort(jsonb),pulse_durable_distributed.storage_compact_find(jsonb),pulse_durable_distributed.semantic_compress(jsonb)", maintain)
	setFunctions("pulse_durable_distributed.pin_create(jsonb),pulse_durable_distributed.pin_renew(jsonb),pulse_durable_distributed.pin_release(jsonb)", maintain || replay)
	setFunctions("pulse_durable_distributed.pin_find(jsonb),pulse_durable_distributed.pin_find_mutation(jsonb)", query || maintain || replay)
	setFunctions("pulse_durable_distributed.retention_policy_get(jsonb),pulse_durable_distributed.retention_policy_preview(jsonb),pulse_durable_distributed.retention_candidate_preview(jsonb),pulse_durable_distributed.retention_policy_update(jsonb),pulse_durable_distributed.history_gc_prepare(jsonb),pulse_durable_distributed.history_gc_execute(jsonb),pulse_durable_distributed.history_gc_find(jsonb)", retain)
}

func claimOne(t *testing.T, c *Coordinator, task d.Task, id string, ttl time.Duration) d.Claim {
	t.Helper()
	rows, e := c.Claim(context.Background(), d.ClaimRequest{Namespace: task.Identity().Namespace, Worker: d.Worker{ID: "worker", BootID: "boot"}, Capabilities: []d.Route{task.Route()}, Limit: 1, TTL: ttl, LeaseIDs: []string{digest([]byte(task.Identity().Namespace + id))[:32]}})
	if e != nil || len(rows) != 1 {
		t.Fatalf("claim count=%d error=%v", len(rows), e)
	}
	return rows[0]
}
func TestPostgresProtocol(t *testing.T) {
	c, admin, ns := integration(t)
	ctx := context.Background()
	var addr string
	var port int
	var db, ver string
	var pid int
	var layoutMarker string
	if e := admin.QueryRow(ctx, "SELECT coalesce(inet_server_addr()::text,''),inet_server_port(),current_database(),version(),pg_backend_pid(),(SELECT layout_marker FROM pulse_durable_distributed.meta)").Scan(&addr, &port, &db, &ver, &pid, &layoutMarker); e != nil {
		t.Fatal(e)
	}
	t.Logf("db instance addr=%s port=%d db=%s version=%s pid=%d layout_marker=%s", addr, port, db, ver, pid, layoutMarker)
	t.Run("concurrent claims and uint64 revision", func(t *testing.T) {
		task := taskFixture(t, ns, "concurrent")
		if _, e := c.Submit(ctx, task); e != nil {
			t.Fatal(e)
		}
		start := make(chan struct{})
		type res struct {
			rows []d.Claim
			err  error
		}
		done := make(chan res, 2)
		for _, id := range []string{"f", "0"} {
			go func(id string) {
				<-start
				rows, e := c.Claim(ctx, d.ClaimRequest{Namespace: ns, Worker: d.Worker{ID: id, BootID: "boot"}, Capabilities: []d.Route{task.Route()}, Limit: 1, TTL: time.Minute, LeaseIDs: []string{digest([]byte(ns + id))[:32]}})
				done <- res{rows, e}
			}(id)
		}
		close(start)
		var got []d.Claim
		for range 2 {
			r := <-done
			if r.err != nil {
				t.Fatal(r.err)
			}
			got = append(got, r.rows...)
		}
		if len(got) != 1 {
			t.Fatal("duplicate claim", len(got))
		}
		a := got[0]
		// 以管理员身份设置较大的权威修订号，再测试真实
		// 工作进程 CAS 及超出 float64 整数精度的实际 JSON 数字路径。
		r := record(task, math.MaxUint64-1)
		b, _ := json.Marshal(r)
		if _, e := admin.Exec(ctx, "UPDATE pulse_durable_distributed.runs SET revision=$1::numeric,record=$2 WHERE namespace=$3 AND run_id=$4", strconv.FormatUint(r.Revision, 10), b, ns, "concurrent"); e != nil {
			t.Fatal(scrub(e, false))
		}
		if _, e := c.LoadOwned(ctx, a.Token); !errors.Is(e, d.ErrInvalid) {
			t.Fatalf("forged checkpoint without authoritative chain accepted: %v", e)
		}
		var exact string
		if e := admin.QueryRow(ctx, "SELECT revision::text FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2", ns, "concurrent").Scan(&exact); e != nil || exact != strconv.FormatUint(math.MaxUint64-1, 10) {
			t.Fatal("revision precision", e)
		}
		cancelled, e := c.Cancel(ctx, task.Identity(), mut("cancel-concurrent"))
		if e != nil || cancelled.Status != d.Canceled || cancelled.Revision != math.MaxUint64-1 {
			t.Fatalf("cannot cancel corrupt checkpoint: status=%s revision=%d err=%v", cancelled.Status, cancelled.Revision, e)
		}
		var retained []byte
		if e = admin.QueryRow(ctx, "SELECT record FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2", ns, "concurrent").Scan(&retained); e != nil || string(retained) != string(b) {
			t.Fatal("Cancel changed corrupt checkpoint evidence", scrub(e, false))
		}
	})
	t.Run("submit and stale owner same revision", func(t *testing.T) {
		task := taskFixture(t, ns, "stale")
		first, e := c.Submit(ctx, task)
		if e != nil || first.Deduplicated {
			t.Fatal(e)
		}
		again, e := c.Submit(ctx, task)
		if e != nil || !again.Deduplicated {
			t.Fatal(e)
		}
		a := claimOne(t, c, task, "a", time.Minute)
		if e = c.InitializeOwned(ctx, d.InitializeRequest{Token: a.Token, Record: record(task, 1), Mutation: mut("save-a")}); e != nil {
			t.Fatal(e)
		}
		// 管理员屏障有意使 A 过期，但不修改修订号。
		if _, e = admin.Exec(ctx, "UPDATE pulse_durable_distributed.runs SET lease_until=clock_timestamp()-interval '1 second' WHERE namespace=$1 AND run_id=$2", ns, "stale"); e != nil {
			t.Fatal(scrub(e, false))
		}
		b := claimOne(t, c, task, "b", time.Minute)
		if b.Snapshot.Revision != 1 || b.Token.Epoch != 2 {
			t.Fatal("takeover changed revision")
		}
		if e = c.Block(ctx, a.Token, "invalid_record", mut("stale-block")); !errors.Is(e, d.ErrLeaseLost) {
			t.Fatal(e)
		}
		if _, e = c.LoadOwned(ctx, a.Token); !errors.Is(e, d.ErrLeaseLost) {
			t.Fatal(e)
		}
		batch := nextBatch(t, record(task, 1))
		digest, e := d.DeltaMutationDigest(a.Token, batch)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = c.ApplyOwned(ctx, a.Token, d.Mutation{ID: "stale-apply", Digest: digest}, batch); !errors.Is(e, d.ErrLeaseLost) {
			t.Fatal(e)
		}
		if _, e = c.Renew(ctx, a.Token, time.Minute); !errors.Is(e, d.ErrLeaseLost) {
			t.Fatal(e)
		}
		if e = c.Release(ctx, a.Token, mut("stale-release")); !errors.Is(e, d.ErrLeaseLost) {
			t.Fatal(e)
		}
		if _, e = c.Finish(ctx, d.FinishRequest{Token: a.Token, ExpectedRevision: 1, Outcome: durable.OutcomeCompleted, Mutation: mut("stale-finish")}); !errors.Is(e, d.ErrLeaseLost) {
			t.Fatal(e)
		}
		invalid := record(task, 1)
		invalid.Inputs = strings.Repeat("f", 64)
		badBatch := nextBatch(t, invalid)
		badDigest, _ := d.DeltaMutationDigest(b.Token, badBatch)
		if _, e = c.ApplyOwned(ctx, b.Token, d.Mutation{ID: "bad-record", Digest: badDigest}, badBatch); e == nil {
			t.Fatal("bad batch identity accepted")
		}
		found, e := c.FindClaim(ctx, ns, b.Token.Worker, b.Token.LeaseID)
		if e != nil || len(found) != 1 {
			t.Fatal(e)
		}
		receipts, e := c.FindMutation(ctx, receiptQuery(t, admin, task.Identity(), mut("save-a"), "initialize", &a.Token.Epoch, uint64ptr(1)))
		if e != nil || receipts.State != d.Found || receipts.Receipt.Revision != 1 {
			t.Fatal("mutation receipt", e)
		}
		if _, e = c.Cancel(ctx, task.Identity(), mut("cancel")); e != nil {
			t.Fatal(e)
		}
		if _, e = c.Finish(ctx, d.FinishRequest{Token: b.Token, ExpectedRevision: 1, Outcome: durable.OutcomeCompleted, Mutation: mut("finish-after-cancel")}); !errors.Is(e, d.ErrTerminal) {
			t.Fatal(e)
		}
	})
	t.Run("finish before cancel", func(t *testing.T) {
		task := taskFixture(t, ns, "finished")
		if _, e := c.Submit(ctx, task); e != nil {
			t.Fatal(e)
		}
		a := claimOne(t, c, task, "c", time.Minute)
		if e := c.InitializeOwned(ctx, d.InitializeRequest{Token: a.Token, Record: record(task, 1), Mutation: mut("finish-save")}); e != nil {
			t.Fatal(e)
		}
		s, e := c.Finish(ctx, d.FinishRequest{Token: a.Token, ExpectedRevision: 1, Outcome: durable.OutcomeCompleted, Mutation: mut("finish")})
		if e != nil || s.Status != d.Succeeded {
			t.Fatal(e)
		}
		s, e = c.Cancel(ctx, task.Identity(), mut("late-cancel"))
		if e != nil || s.Status != d.Succeeded {
			t.Fatal(e)
		}
	})
	t.Run("lock wait crosses expiry", func(t *testing.T) {
		task := taskFixture(t, ns, "locked")
		if _, e := c.Submit(ctx, task); e != nil {
			t.Fatal(e)
		}
		a := claimOne(t, c, task, "d", time.Minute)
		tx, e := admin.Begin(ctx)
		if e != nil {
			t.Fatal(scrub(e, false))
		}
		defer tx.Rollback(ctx)
		// 锁住该行，启动 Renew，再等待 pg_stat_activity 证明其
		// 已阻塞。在释放前，于持锁事务内使该行过期。
		if _, e = tx.Exec(ctx, "SELECT 1 FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2 FOR UPDATE", ns, "locked"); e != nil {
			t.Fatal(scrub(e, false))
		}
		done := make(chan error, 1)
		go func() { _, e := c.Renew(ctx, a.Token, time.Minute); done <- e }()
		deadline := time.Now().Add(3 * time.Second)
		for {
			var waiting bool
			e = admin.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE application_name='pulse-durable-distributed' AND wait_event_type='Lock')").Scan(&waiting)
			if e != nil {
				t.Fatal(scrub(e, false))
			}
			if waiting {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("renew never reached row-lock barrier")
			}
			time.Sleep(time.Millisecond)
		}
		if _, e = tx.Exec(ctx, "UPDATE pulse_durable_distributed.runs SET lease_until=clock_timestamp()-interval '1 millisecond' WHERE namespace=$1 AND run_id=$2", ns, "locked"); e != nil {
			t.Fatal(scrub(e, false))
		}
		if e = tx.Commit(ctx); e != nil {
			t.Fatal(scrub(e, false))
		}
		if e = <-done; !errors.Is(e, d.ErrLeaseLost) {
			t.Fatal(e)
		}
		if _, e = c.Cancel(ctx, task.Identity(), mut("cancel-locked")); e != nil {
			t.Fatal(e)
		}
	})
	t.Run("safe isolated fencing mutation controls", func(t *testing.T) {
		// 变异版本绝不替换生产函数。事务内的临时表
		// 模拟相同的修订号与旧令牌交错。
		tx, e := admin.Begin(ctx)
		if e != nil {
			t.Fatal(scrub(e, false))
		}
		defer tx.Rollback(ctx)
		if _, e = tx.Exec(ctx, `CREATE TEMP TABLE mutation_run(revision numeric(20,0),epoch bigint,lease_until timestamptz,status text) ON COMMIT DROP; INSERT INTO mutation_run VALUES(7,2,clock_timestamp()-interval '1 second','running')`); e != nil {
			t.Fatal(scrub(e, false))
		}
		tag, e := tx.Exec(ctx, "UPDATE mutation_run SET revision=8 WHERE revision=7 AND epoch=1 AND status='running' AND lease_until>clock_timestamp()")
		if e != nil || tag.RowsAffected() != 0 {
			t.Fatal("fenced control failed")
		}
		tag, e = tx.Exec(ctx, "UPDATE mutation_run SET revision=8 WHERE revision=7")
		if e != nil || tag.RowsAffected() != 1 {
			t.Fatal("CAS-only mutation did not expose stale write")
		}
		if _, e = tx.Exec(ctx, "UPDATE mutation_run SET revision=7"); e != nil {
			t.Fatal(scrub(e, false))
		}
		// 先前的成功读取不能保护后续未受栅栏保护的写入。
		var readRevision int
		if e = tx.QueryRow(ctx, "SELECT revision FROM mutation_run").Scan(&readRevision); e != nil {
			t.Fatal(scrub(e, false))
		}
		if _, e = tx.Exec(ctx, "UPDATE mutation_run SET epoch=3"); e != nil {
			t.Fatal(scrub(e, false))
		}
		tag, e = tx.Exec(ctx, "UPDATE mutation_run SET revision=8 WHERE revision=$1", readRevision)
		if e != nil || tag.RowsAffected() != 1 {
			t.Fatal("read-then-write mutation not detected")
		}
		if _, e = tx.Exec(ctx, "UPDATE mutation_run SET lease_until=clock_timestamp()+interval '2 milliseconds'"); e != nil {
			t.Fatal(scrub(e, false))
		}
		// 等待数据库时间，而非主机时钟。now() 停留在事务
		// 开始时，会错误授权此已过期行。
		if _, e = tx.Exec(ctx, "SELECT pg_sleep(0.01)"); e != nil {
			t.Fatal(scrub(e, false))
		}
		var frozen, live bool
		if e = tx.QueryRow(ctx, "SELECT lease_until>now(),lease_until>clock_timestamp() FROM mutation_run").Scan(&frozen, &live); e != nil {
			t.Fatal(scrub(e, false))
		}
		if !frozen || live {
			t.Fatal("frozen transaction-clock mutation not exposed")
		}
	})
	t.Run("direct SQL rejects malformed records", func(t *testing.T) {
		task := taskFixture(t, ns, "direct")
		if _, e := c.Submit(ctx, task); e != nil {
			t.Fatal(e)
		}
		a := claimOne(t, c, task, "e", time.Minute)
		good, err := recordio.EncodeSnapshot(record(task, 1))
		if err != nil {
			t.Fatal(err)
		}
		for i, b := range [][]byte{[]byte("not json"), []byte{0xff}, []byte(strings.Replace(string(good), `"revision":1`, `"revision":"1"`, 1)), []byte(strings.Replace(string(good), `"run_id":"direct"`, `"run_id":"different"`, 1)), []byte(strings.Replace(string(good), `"inputs":"`+task.InputsFingerprint()+`"`, `"inputs":"different"`, 1)), []byte(strings.Replace(string(good), `"version":2`, `"version":2,"version":2`, 1)), []byte(strings.Replace(string(good), `"version":2`, `"version":1`, 1))} {
			args := owned(a.Token)
			mutate(args, mut("direct-"+strconv.Itoa(i)))
			args["expected"] = "0"
			args["record"] = b
			if _, e := c.call(ctx, "initialize", args, true); !errors.Is(e, d.ErrInvalid) {
				t.Fatalf("malformed record %d accepted or rejected for an unrelated reason: %v", i, e)
			}
		}
		s, e := c.Get(ctx, task.Identity())
		if e != nil || s.Revision != 0 {
			t.Fatal("invalid writes changed authority", e)
		}
		// 正控命中同一个原始 SQL 入口，排除所有负控因为失效操作名
		// 或缺失权限而一起失败的假验收。
		args := owned(a.Token)
		mutate(args, mut("direct-valid"))
		args["expected"], args["record"] = "0", good
		if _, e = c.call(ctx, "initialize", args, true); e != nil {
			t.Fatalf("valid raw initialize rejected: %v", e)
		}
		if s, e = c.Get(ctx, task.Identity()); e != nil || s.Revision != 1 {
			t.Fatal("valid raw initialize was not published", e)
		}
		if _, e = c.Cancel(ctx, task.Identity(), mut("cancel-direct")); e != nil {
			t.Fatal(e)
		}
	})
	t.Run("release block and epoch exhaustion", func(t *testing.T) {
		task := taskFixture(t, ns, "release")
		if _, e := c.Submit(ctx, task); e != nil {
			t.Fatal(e)
		}
		a := claimOne(t, c, task, "1", time.Minute)
		if e := c.Release(ctx, a.Token, mut("release-ok")); e != nil {
			t.Fatal(e)
		}
		b := claimOne(t, c, task, "2", time.Minute)
		if b.Token.Epoch != 2 {
			t.Fatal("release reset epoch")
		}
		if e := c.Block(ctx, b.Token, "invalid_record", mut("block-ok")); e != nil {
			t.Fatal(e)
		}
		s, e := c.Get(ctx, task.Identity())
		if e != nil || s.Status != d.Blocked {
			t.Fatal(e)
		}
		if _, e = c.Cancel(ctx, task.Identity(), mut("cancel-blocked")); e != nil {
			t.Fatal(e)
		}
		task = taskFixture(t, ns, "exhausted")
		if _, e = c.Submit(ctx, task); e != nil {
			t.Fatal(e)
		}
		if _, e = admin.Exec(ctx, "UPDATE pulse_durable_distributed.runs SET epoch=9223372036854775807 WHERE namespace=$1 AND run_id=$2", ns, "exhausted"); e != nil {
			t.Fatal(scrub(e, false))
		}
		_, e = c.Claim(ctx, d.ClaimRequest{Namespace: ns, Worker: d.Worker{ID: "w", BootID: "b"}, Capabilities: []d.Route{task.Route()}, Limit: 1, TTL: time.Minute, LeaseIDs: []string{digest([]byte(ns + "3"))[:32]}})
		if !errors.Is(e, d.ErrInvalid) {
			t.Fatal("epoch overflow not rejected", e)
		}
		if _, e = c.Cancel(ctx, task.Identity(), mut("cancel-exhausted")); e != nil {
			t.Fatal(e)
		}
	})
	t.Run("resolve delayed claim and late claim", func(t *testing.T) {
		task := taskFixture(t, ns, "resolve-delayed")
		if _, e := c.Submit(ctx, task); e != nil {
			t.Fatal(e)
		}
		worker := d.Worker{ID: "resolve-worker", BootID: "resolve-boot"}
		// 先 Resolve 会永久关闭未使用 ID；后续 Claim 不得
		// 复用它，即使存在匹配的排队运行。
		unused := digest([]byte(ns + "unused"))[:32]
		r, e := c.ResolveClaim(ctx, ns, worker, unused)
		if e != nil || !r.Closed || r.Claim != nil {
			t.Fatalf("unused resolve closed=%v err=%v", r.Closed, e)
		}
		lateTask := taskFixture(t, ns, "resolve-late")
		if _, e = c.Submit(ctx, lateTask); e != nil {
			t.Fatal(e)
		}
		if _, e = c.Claim(ctx, d.ClaimRequest{Namespace: ns, Worker: worker, Capabilities: []d.Route{lateTask.Route()}, Limit: 1, TTL: time.Minute, LeaseIDs: []string{unused}}); !errors.Is(e, d.ErrInvalid) {
			t.Fatalf("late claim accepted: %v", e)
		}
		// 先 Claim 表示已提交但响应可能丢失。Resolve
		// 必须等待或读取持久租约行，并返回相同领取结果。
		leaseID := digest([]byte(ns + "resolve-delayed"))[:32]
		if _, e = c.Claim(ctx, d.ClaimRequest{Namespace: ns, Worker: worker, Capabilities: []d.Route{task.Route()}, Limit: 1, TTL: time.Minute, LeaseIDs: []string{leaseID}}); e != nil {
			t.Fatal(e)
		}
		r, e = c.ResolveClaim(ctx, ns, worker, leaseID)
		if e != nil || r.Closed || r.Claim == nil {
			t.Fatalf("resolve committed claim closed=%v err=%v", r.Closed, e)
		}
		r, e = c.ResolveClaim(ctx, ns, worker, leaseID)
		if e != nil || r.Closed || r.Claim == nil {
			t.Fatalf("resolve idempotency closed=%v err=%v", r.Closed, e)
		}
	})
	t.Run("query ACL and lease history", func(t *testing.T) {
		if _, e := c.Get(ctx, d.Identity{Namespace: "unauthorized", RunID: "x"}); e == nil {
			t.Fatal("cross-namespace read")
		}
		_, e := c.Claim(ctx, d.ClaimRequest{Namespace: ns, Worker: d.Worker{ID: "worker", BootID: "boot"}, Capabilities: []d.Route{taskFixture(t, ns, "unused").Route()}, Limit: 1, TTL: time.Minute, LeaseIDs: []string{digest([]byte(ns + "a"))[:32]}})
		if !errors.Is(e, d.ErrInvalid) {
			t.Fatal("lease ID history reuse accepted", e)
		}
	})
}
