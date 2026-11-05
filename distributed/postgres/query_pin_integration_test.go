// 本文件验证数据库角色的公开 pin 查询边界，相当于一组只通过正式
// Migrate 授权和 Coordinator 方法调用的端到端测试；不由测试辅助补 ACL。
package postgres

import (
	"context"
	"net"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresQueryOnlyHistoryPin(t *testing.T) {
	writer, admin, ns := integration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	dsn := os.Getenv("PULSE_DISTRIBUTED_TEST_DSN")
	var writerRole string
	if err := writer.pool.QueryRow(ctx, "SELECT session_user").Scan(&writerRole); err != nil {
		t.Fatal(scrub(err, false))
	}
	if err := Migrate(ctx, dsn, Grant{Role: writerRole, Namespace: ns, Permissions: []string{"replay"}}); err != nil {
		t.Fatal("grant writer replay", err)
	}
	task := taskFixture(t, ns, "query-only-pin")
	if _, err := writer.Submit(ctx, task); err != nil {
		t.Fatal(err)
	}
	claim := claimOne(t, writer, task, "query-only-pin", time.Minute)
	if err := writer.InitializeOwned(ctx, d.InitializeRequest{Token: claim.Token, Record: record(task, 1), Mutation: mut("query-pin-init")}); err != nil {
		t.Fatal(err)
	}
	var chain string
	if err := admin.QueryRow(ctx, "SELECT head_chain FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2", ns, task.Identity().RunID).Scan(&chain); err != nil {
		t.Fatal(scrub(err, false))
	}
	ttl := time.Hour
	pin := d.PinRequest{Identity: task.Identity(), PinID: "query-pin", Revision: 1, Chain: chain, HolderDigest: digest([]byte("query-pin-holder")), TTL: &ttl, Mutation: d.Mutation{ID: "query-pin-create"}}
	pin.Mutation.Digest = pin.Digest()
	if _, err := writer.PinHistory(ctx, pin); err != nil {
		t.Fatal("create reference pin", err)
	}

	readerRole := "pulse_pin_reader_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	quotedRole := pgx.Identifier{readerRole}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE ROLE "+quotedRole+" LOGIN PASSWORD 'distributed-test-only'"); err != nil {
		t.Fatal("create query-only role", scrub(err, false))
	}
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if _, err := admin.Exec(cleanup, "DELETE FROM pulse_durable_distributed.access WHERE role_name=$1", readerRole); err != nil {
			t.Error("remove query-only access", scrub(err, false))
			return
		}
		if _, err := admin.Exec(cleanup, "DROP OWNED BY "+quotedRole+"; DROP ROLE "+quotedRole); err != nil {
			t.Error("remove query-only role", scrub(err, false))
		}
	})
	if err := Migrate(ctx, dsn, Grant{Role: readerRole, Namespace: ns, Permissions: []string{"query"}}); err != nil {
		t.Fatal("grant query-only role", err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("parse dedicated test connection")
	}
	u := url.URL{Scheme: "postgres", Host: net.JoinHostPort(cfg.ConnConfig.Host, strconv.Itoa(int(cfg.ConnConfig.Port))), Path: "/" + cfg.ConnConfig.Database, User: url.UserPassword(readerRole, "distributed-test-only")}
	values := u.Query()
	values.Set("sslmode", "disable")
	u.RawQuery = values.Encode()
	reader, err := New(ctx, u.String())
	if err != nil {
		t.Fatal("open query-only coordinator", err)
	}
	defer reader.Close()

	query := d.PinQuery{Identity: pin.Identity, PinID: pin.PinID}
	got, err := reader.FindPin(ctx, query)
	if err != nil || got.Status != d.PinActive || got.State == nil || got.State.Chain != chain {
		t.Errorf("query-only FindPin: status=%s error=%v", got.Status, err)
	}
	mutationQuery := d.PinMutationQuery{Identity: pin.Identity, PinID: pin.PinID, Mutation: pin.Mutation, Operation: d.PinCreateOperation}
	receipt, err := reader.FindPinMutation(ctx, mutationQuery)
	if err != nil || receipt.Status != d.PinMutationFound || receipt.Receipt == nil || receipt.Receipt.Mutation != pin.Mutation {
		t.Errorf("query-only FindPinMutation: status=%s error=%v", receipt.Status, err)
	}
	// 公共查询授权不得顺带开放 pin 写入口、物理维护入口或内部 helper。
	for _, function := range []string{"pin_create", "pin_renew", "pin_release", "storage_inspect", "pin_lock"} {
		var allowed bool
		if err := admin.QueryRow(ctx, "SELECT has_function_privilege($1,$2,'EXECUTE')", readerRole, "pulse_durable_distributed."+function+"(jsonb)").Scan(&allowed); err != nil {
			t.Fatal("inspect query-only ACL", scrub(err, false))
		}
		if allowed {
			t.Errorf("query-only role can execute %s", function)
		}
	}
	foreign := query
	foreign.Identity.Namespace += "-foreign"
	if _, err := reader.FindPin(ctx, foreign); err == nil {
		t.Error("query-only role read a foreign namespace")
	}
}
