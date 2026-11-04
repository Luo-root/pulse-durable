package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
)

// 要求全新、可丢弃的模式版本 5 数据库，并显式启用标准破坏性
// 集成测试。离线验证只编译、不执行此测试。
func TestPostgresActiveSuffixBudgetCompaction(t *testing.T) {
	for _, boundary := range []string{"count", "bytes"} {
		t.Run(boundary, func(t *testing.T) {
			c, admin, ns := integration(t)
			ctx := context.Background()
			var role string
			if err := c.pool.QueryRow(ctx, "SELECT session_user").Scan(&role); err != nil {
				t.Fatal(err)
			}
			if _, err := admin.Exec(ctx, "INSERT INTO pulse_durable_distributed.access VALUES($1,$2,'maintain')", role, ns); err != nil {
				t.Fatal(err)
			}
			task := taskFixture(t, ns, "budget-"+boundary)
			id := task.Identity()
			if _, err := c.Submit(ctx, task); err != nil {
				t.Fatal(err)
			}
			owner := claimOne(t, c, task, "budget-"+boundary, time.Minute)
			if err := c.InitializeOwned(ctx, d.InitializeRequest{Token: owner.Token, Record: record(task, 1), Mutation: mut("budget-init")}); err != nil {
				t.Fatal(err)
			}
			appendBatch := func(name string) error {
				t.Helper()
				r, err := c.LoadOwned(ctx, owner.Token)
				if err != nil {
					t.Fatal(err)
				}
				batch := nextBatch(t, r)
				dg, err := d.DeltaMutationDigest(owner.Token, batch)
				if err != nil {
					t.Fatal(err)
				}
				_, err = c.ApplyOwned(ctx, owner.Token, d.Mutation{ID: name, Digest: dg}, batch)
				return err
			}
			if err := appendBatch("prefix"); err != nil {
				t.Fatal(err)
			}
			q := CompactStorageRequest{Identity: id, ExpectedGeneration: 1, ThroughRevision: 2, Mutation: d.Mutation{ID: "budget-build"}}
			q.Mutation.Digest = CompactStorageDigest(q)
			a, err := maintenanceArgs(q)
			if err != nil {
				t.Fatal(err)
			}
			begun, err := c.maintenanceCall(ctx, "begin", a, true)
			if err != nil {
				t.Fatal(err)
			}
			var begin struct {
				Image json.RawMessage `json:"image"`
			}
			if err = json.Unmarshal(begun, &begin); err != nil {
				t.Fatal(err)
			}
			r, chain, err := buildMaintenanceImage(ctx, q, begin.Image)
			if err != nil {
				t.Fatal(err)
			}
			// 合成边界注入避免写入 100000 行或 256 MiB。
			// 最后一次合法追加仍使用真实的受限工作进程 API。
			wire, err := durable.MarshalChangeBatch(nextBatch(t, r))
			if err != nil {
				t.Fatal(err)
			}
			if boundary == "count" {
				_, err = admin.Exec(ctx, "UPDATE pulse_durable_distributed.runs SET active_log_count=99999 WHERE namespace=$1 AND run_id=$2", ns, id.RunID)
			} else {
				_, err = admin.Exec(ctx, "UPDATE pulse_durable_distributed.runs SET active_log_bytes=268435456-$3 WHERE namespace=$1 AND run_id=$2", ns, id.RunID, len(wire))
			}
			if err != nil {
				t.Fatal(err)
			}
			// 业务追加与构建阶段竞争，而非与发布锁竞争。
			if err = appendBatch("concurrent-suffix"); err != nil {
				t.Fatal("last legal append", err)
			}
			var bytes, count int64
			readBudget := func() {
				t.Helper()
				if err := admin.QueryRow(ctx, "SELECT active_log_bytes,active_log_count FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2", ns, id.RunID).Scan(&bytes, &count); err != nil {
					t.Fatal(err)
				}
			}
			readBudget()
			if (boundary == "count" && count != 100000) || (boundary == "bytes" && bytes != 268435456) {
				t.Fatalf("did not reach exact boundary: bytes=%d count=%d", bytes, count)
			}
			if err = appendBatch("over-budget"); !errors.Is(err, d.ErrInvalid) {
				t.Fatal("over-budget append must fail", err)
			}
			// 除表示形式、预算和水位外，比较运行记录的每一列；
			// 包括业务头、租约、修订号和原始记录。
			businessState := func() string {
				t.Helper()
				var state string
				if err := admin.QueryRow(ctx, `SELECT (to_jsonb(r)-ARRAY['active_snapshot','representation_generation','active_log_bytes','active_log_count','maintenance_watermark'])::text FROM pulse_durable_distributed.runs r WHERE namespace=$1 AND run_id=$2`, ns, id.RunID).Scan(&state); err != nil {
					t.Fatal(err)
				}
				return state
			}
			retainedState := func() string {
				t.Helper()
				var state string
				if err := admin.QueryRow(ctx, `SELECT jsonb_build_object('batches',(SELECT jsonb_agg(to_jsonb(b) ORDER BY revision) FROM pulse_durable_distributed.batches b WHERE namespace=$1 AND run_id=$2),'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY event_seq) FROM pulse_durable_distributed.events e WHERE namespace=$1 AND run_id=$2))::text`, ns, id.RunID).Scan(&state); err != nil {
					t.Fatal(err)
				}
				return state
			}
			before, retained := businessState(), retainedState()
			snapshot, err := encodeMaintenanceRecord(r)
			if err != nil {
				t.Fatal(err)
			}
			a["record"], a["snapshot_digest"], a["chain"] = snapshot, digest(snapshot), chain
			published, err := c.maintenanceCall(ctx, "publish", a, true)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = decodeMaintenanceResult(published, q.Mutation); err != nil {
				t.Fatal(err)
			}
			if businessState() != before || retainedState() != retained {
				t.Fatal("publication changed business state, retained batches or events")
			}
			readBudget()
			if count != 1 || bytes != int64(len(wire)) {
				t.Fatalf("concurrent suffix not counted exactly: bytes=%d count=%d", bytes, count)
			}
			c.DropOwnedProjection(owner.Token)
			if err = appendBatch("after-compaction"); err != nil {
				t.Fatal("compaction did not release active budget", err)
			}
			// 也压缩到当前头：空后缀的开销为零。
			q = CompactStorageRequest{Identity: id, ExpectedGeneration: 2, ThroughRevision: 4, Mutation: d.Mutation{ID: "budget-head"}}
			q.Mutation.Digest = CompactStorageDigest(q)
			before, retained = businessState(), retainedState()
			if _, err = c.CompactStorage(ctx, q); err != nil {
				t.Fatal(err)
			}
			readBudget()
			if count != 0 || bytes != 0 || businessState() != before || retainedState() != retained {
				t.Fatal("head compaction must clear budget without changing business/history")
			}
		})
	}
}
