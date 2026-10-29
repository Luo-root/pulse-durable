package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"crypto/sha256"
	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/jackc/pgx/v5"
)

const mutationOwnerPredicate = "r.epoch IS DISTINCT FROM (a->>'epoch')::bigint OR r.lease_id IS DISTINCT FROM a->>'lease_id' OR r.worker_id IS DISTINCT FROM a->>'worker_id' OR r.boot_id IS DISTINCT FROM a->>'boot_id' OR "
const mutationExpiryPredicate = " OR r.lease_until<=t"
const mutationClockStatement = " t:=clock_timestamp();\n IF t<r.updated_at OR t<r.receipt_observed_through THEN"
const mutationRevisionCAS = "IF r.revision<>rev THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='revision conflict'; END IF;"

// 有意要求精确且唯一的替换：模式变化必须安全拒绝，
// 而不是悄然停止测试预期的生产保护条件。
func mutationReplace(source, old, replacement string) (string, error) {
	if strings.Count(source, old) != 1 {
		return "", fmt.Errorf("mutation target must occur exactly once: %q", old)
	}
	return strings.Replace(source, old, replacement, 1), nil
}

func mutationAPI(kind string) (string, error) {
	api, e := replaceCreate(schemaSQL, "api")
	if e != nil {
		return "", e
	}
	if strings.Count(api, mutationRevisionCAS) != 1 {
		return "", errors.New("production revision CAS changed")
	}
	switch kind {
	case "owner":
		api, e = mutationReplace(api, mutationOwnerPredicate, "")
	case "expiry":
		api, e = mutationReplace(api, mutationExpiryPredicate, "")
		if e == nil {
			api, e = mutationReplace(api, " IF op<>'cancel' AND r.lease_until<=t THEN RAISE EXCEPTION USING ERRCODE='PD004',MESSAGE='lease lost'; END IF;", "")
		}
	case "frozen-clock":
		api, e = mutationReplace(api, mutationClockStatement, " t:=now();\n IF t<r.updated_at THEN")
		if e == nil {
			api, e = mutationReplace(api, " t := clock_timestamp();", " t := now();")
		}
	default:
		return "", errors.New("unknown mutation")
	}
	if e == nil && strings.Count(api, mutationRevisionCAS) != 1 {
		return "", errors.New("mutation removed revision CAS")
	}
	return api, e
}

func TestMutationTargetsFailClosed(t *testing.T) {
	if strings.Count(schemaSQL, "IF FOUND AND (t<r.updated_at OR t<r.receipt_observed_through) THEN") != 1 {
		t.Fatal("resolve clock regression guard changed or missing")
	}
	for _, kind := range []string{"owner", "expiry", "frozen-clock"} {
		if _, e := mutationAPI(kind); e != nil {
			t.Fatal(e)
		}
	}
	for _, source := range []string{"missing", "target target"} {
		if _, e := mutationReplace(source, "target", "changed"); e == nil {
			t.Fatal("missing or ambiguous mutation accepted")
		}
	}
}

// 有意与 integration() 分离：该辅助函数会提交角色、
// 授权及夹具准备。正常模式初始化后，所有变异
// 改动，包括 CREATE OR REPLACE 及对应目录签名，
// 都留在一个管理员事务内。以受限 session_user 调用
// 真实 API，保留所有凭据和 ACL 检查；
// 绝不替换或削弱 check_schema 本身。
// 仅在明确授权的一次性测试数据库上串行运行。
// Migrate 安装缺失的正常模式并拒绝漂移，不依赖
// 测试执行顺序。超级用户仅用于切换
// 会话授权，避免为其他连接提交变异。
func TestPostgresProductionFencingMutations(t *testing.T) {
	dsn := os.Getenv("PULSE_DISTRIBUTED_TEST_DSN")
	if dsn == "" || os.Getenv("PULSE_DISTRIBUTED_TEST_ALLOW_DESTRUCTIVE") != "yes" {
		t.Skip("dedicated DSN and PULSE_DISTRIBUTED_TEST_ALLOW_DESTRUCTIVE=yes required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	admin, e := connect(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close()
	var super bool
	if e = admin.QueryRow(ctx, `SELECT rolsuper FROM pg_roles WHERE rolname=session_user`).Scan(&super); e != nil || !super {
		t.Fatal("transaction-local restricted session authorization requires a dedicated test superuser", scrub(e, false))
	}
	// 上方显式启用仅授权正常模式初始化，不授权修复漂移。
	// Migrate 在尝试任何变异前验证已有 DDL。
	if e = Migrate(ctx, dsn); e != nil {
		t.Fatal("initialize normal schema before mutation tests", e)
	}
	if e = validate(ctx, admin, false); e != nil {
		t.Fatal("normal schema invalid before mutation tests", e)
	}
	api, e := replaceCreate(schemaSQL, "api")
	if e != nil {
		t.Fatal(e)
	}
	parts := strings.Split(api, "$$")
	var installedBody string
	if len(parts) != 3 {
		t.Fatal("production API delimiter changed")
	}
	if e = admin.QueryRow(ctx, `SELECT prosrc FROM pg_proc WHERE oid='pulse_durable_distributed.api(text,jsonb)'::regprocedure`).Scan(&installedBody); e != nil || installedBody != parts[1] {
		t.Fatal("installed API body differs from mutation source; refusing replacement", scrub(e, false))
	}

	// 包含精确函数定义、所有权、ACL 以及
	// 已存储签名。每次回滚后比较，以检测持久漂移。
	state := func() string {
		t.Helper()
		var s string
		e := admin.QueryRow(ctx, `SELECT jsonb_build_array((`+catalogSQL+`),
 (SELECT signature FROM pulse_durable_distributed.meta),
 (SELECT jsonb_agg(jsonb_build_array(oid,proowner,proacl) ORDER BY oid) FROM pg_proc WHERE pronamespace='pulse_durable_distributed'::regnamespace),
 (SELECT jsonb_agg(to_jsonb(a) ORDER BY role_name,namespace,permission) FROM pulse_durable_distributed.access a))::text`).Scan(&s)
		if e != nil {
			t.Fatal(scrub(e, false))
		}
		return s
	}
	before := state()
	for _, kind := range []string{"owner", "expiry", "frozen-clock"} {
		t.Run(kind, func(t *testing.T) {
			for _, phase := range []string{"normal-before", "mutated", "normal-after-rollback"} {
				t.Run(phase, func(t *testing.T) {
					mutationTrial(t, ctx, admin, kind, phase == "mutated")
					if state() != before {
						t.Fatal("rollback did not restore exact catalog, signature and function ACLs/access")
					}
					if e := validate(ctx, admin, false); e != nil {
						t.Fatal("normal schema invalid after rollback", e)
					}
				})
			}
		})
		if t.Failed() {
			break
		}
	}
}

func mutationTrial(t *testing.T, ctx context.Context, admin interface {
	Begin(context.Context) (pgx.Tx, error)
}, kind string, changed bool) {
	t.Helper()
	tx, e := admin.Begin(ctx)
	if e != nil {
		t.Fatal(scrub(e, false))
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if e := tx.Rollback(cleanup); e != nil && !errors.Is(e, pgx.ErrTxClosed) {
			t.Errorf("mutation transaction rollback failed: %v", scrub(e, false))
		}
	}()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, e := tx.Exec(ctx, sql, args...); e != nil {
			t.Fatal(scrub(e, false))
		}
	}
	exec(`SELECT pg_advisory_xact_lock(5788618723189269577)`)
	// 单独的 nextval 不会回滚。事务性 RESTART 也隔离
	// 事件序列，回滚时恢复原状态（同时加锁防止
	// 并发使用）。不要替换为 setval。
	var restart string
	if e := tx.QueryRow(ctx, `SELECT format('ALTER SEQUENCE %s RESTART WITH 1',pg_get_serial_sequence('pulse_durable_distributed.events','event_seq'))`).Scan(&restart); e != nil {
		t.Fatal(scrub(e, false))
	}
	exec(restart)
	role := "pulse_mutation_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	roleSQL := pgx.Identifier{role}.Sanitize()
	ns := role
	exec("CREATE ROLE " + roleSQL + " NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS")
	exec("GRANT USAGE ON SCHEMA pulse_durable_distributed TO " + roleSQL)
	exec("GRANT EXECUTE ON FUNCTION pulse_durable_distributed.api(text,jsonb),pulse_durable_distributed.check_schema() TO " + roleSQL)
	exec(`INSERT INTO pulse_durable_distributed.access VALUES($1,$2,'worker')`, role, ns)

	task := taskFixture(t, ns, "mutation")
	rec, e := json.Marshal(record(task, 1))
	if e != nil {
		t.Fatal(e)
	}
	oldLease, newLease := strings.Repeat("a", 32), strings.Repeat("b", 32)
	currentEpoch, currentLease := int64(1), oldLease
	worker, boot := "old-worker", "old-boot"
	if kind == "owner" {
		currentEpoch, currentLease, worker, boot = 2, newLease, "new-worker", "new-boot"
	}
	// 夹具写入仅允许管理员执行。两个所有者的修订号均为 1；
	// 因此变异无须绕过独立 CAS 即可成功。
	exec(`INSERT INTO pulse_durable_distributed.runs(namespace,run_id,task,digest,route,status,epoch,revision,record,lease_id,worker_id,boot_id,lease_until,created_at,updated_at)
 VALUES($1,$2,$3,$4,$5,'running',$6,1,$7,$8,$9,$10,clock_timestamp()+interval '1 hour',transaction_timestamp()-interval '2 seconds',transaction_timestamp()-interval '1 second')`,
		ns, task.Identity().RunID, task.Bytes(), task.SubmissionDigest(), task.Route(), currentEpoch, rec, currentLease, worker, boot)
	seed := durable.SeedChain(sha256.Sum256(rec))
	exec(`UPDATE pulse_durable_distributed.runs SET snapshot_digest=$1,head_chain=$2,root_scope='root' WHERE namespace=$3`, digest(rec), chainText(seed), ns)
	if kind == "expiry" {
		exec(`UPDATE pulse_durable_distributed.runs SET lease_until=clock_timestamp()-interval '1 second' WHERE namespace=$1`, ns)
	}
	if kind == "frozen-clock" {
		exec(`UPDATE pulse_durable_distributed.runs SET lease_until=transaction_timestamp()+interval '10 milliseconds' WHERE namespace=$1`, ns)
		exec(`SELECT pg_sleep(0.02)`)
		var frozen, live bool
		if e := tx.QueryRow(ctx, `SELECT lease_until>now(),lease_until>clock_timestamp() FROM pulse_durable_distributed.runs WHERE namespace=$1`, ns).Scan(&frozen, &live); e != nil || !frozen || live {
			t.Fatal("database-time expiry barrier not established", scrub(e, false))
		}
	}
	if changed {
		api, e := mutationAPI(kind)
		if e != nil {
			t.Fatal(e)
		}
		exec(api)
		// 仅在此回滚事务内重新签名，让实际 API 对变异版本运行
		// 未改动的结构和权限验证器。
		sig, e := signature(ctx, tx)
		if e != nil {
			t.Fatal(e)
		}
		exec(`UPDATE pulse_durable_distributed.meta SET signature=$1`, sig)
	}
	exec("SET LOCAL SESSION AUTHORIZATION " + roleSQL)
	if e := validateWorker(ctx, tx); e != nil {
		t.Fatal("restricted session failed unchanged schema/permission checks", e)
	}
	args := owned(d.Token{Identity: task.Identity(), Epoch: 1, LeaseID: oldLease, Worker: d.Worker{ID: "old-worker", BootID: "old-boot"}})
	mutate(args, mut("mutation-save"))
	args["expected"] = "1"
	batch := nextBatch(t, record(task, 1))
	wire, e := durable.MarshalChangeBatch(batch)
	if e != nil {
		t.Fatal(e)
	}
	next, e := durable.NextChain(seed, sha256.Sum256(wire), 2)
	if e != nil {
		t.Fatal(e)
	}
	args["wire"] = wire
	args["batch_digest"] = digest(wire)
	args["parent_chain"] = chainText(seed)
	args["next_chain"] = chainText(next)
	call := func() ([]byte, error) {
		t.Helper()
		b, e := json.Marshal(args)
		if e != nil {
			t.Fatal(e)
		}
		exec("SAVEPOINT mutation_call")
		var out []byte
		e = tx.QueryRow(ctx, `SELECT pulse_durable_distributed.api('apply',$1::jsonb)`, b).Scan(&out)
		if e != nil {
			exec("ROLLBACK TO SAVEPOINT mutation_call")
		}
		exec("RELEASE SAVEPOINT mutation_call")
		return out, scrub(e, false)
	}
	if changed {
		args["expected"] = "0"
		if _, e := call(); !errors.Is(e, d.ErrRevisionConflict) {
			t.Fatalf("mutant must retain revision CAS: %v", e)
		}
		args["expected"] = "1"
	}
	out, saveErr := call()
	// 这正是正常的过期所有者拒绝判据。显式记录其
	// RED 结果，不将接受变异伪装成普通协议
	// PASS。意外 SQL 错误不算检测成功。
	rejected := errors.Is(saveErr, d.ErrLeaseLost)
	if !changed {
		if !rejected {
			t.Fatalf("normal stale/expired-owner rejection oracle failed: %v", saveErr)
		}
		exec("SET LOCAL SESSION AUTHORIZATION DEFAULT")
		var revision string
		var events int
		if e := tx.QueryRow(ctx, `SELECT revision::text,(SELECT count(*) FROM pulse_durable_distributed.events WHERE namespace=$1) FROM pulse_durable_distributed.runs WHERE namespace=$1`, ns).Scan(&revision, &events); e != nil || revision != "1" || events != 0 {
			t.Fatalf("rejected Save changed authority: revision=%s events=%d err=%v", revision, events, scrub(e, false))
		}
		t.Logf("normal %s rejection oracle: GREEN (ErrLeaseLost; revision=1, events=0)", kind)
		return
	}
	if saveErr != nil || rejected {
		t.Fatalf("mutation not detected: rejection=%v save error=%v", rejected, saveErr)
	}
	if string(out) != "{}" {
		t.Fatalf("unexpected Apply response: %s", out)
	}
	exec("SET LOCAL SESSION AUTHORIZATION DEFAULT")
	var revision string
	var events int
	if e := tx.QueryRow(ctx, `SELECT revision::text,(SELECT count(*) FROM pulse_durable_distributed.events WHERE namespace=$1 AND operation='apply' AND mutation_id='mutation-save') FROM pulse_durable_distributed.runs WHERE namespace=$1`, ns).Scan(&revision, &events); e != nil || revision != "2" || events != 1 {
		t.Fatalf("mutant did not persist actual SQL Save/event within rollback transaction: revision=%s events=%d err=%v", revision, events, scrub(e, false))
	}
	t.Logf("MUTATION DETECTED %s: stale/expired-owner rejection oracle RED (want ErrLeaseLost, got successful SQL Save; revision 1 -> 2, save events=1); revision CAS control GREEN", kind)
}
