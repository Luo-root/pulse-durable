package postgres

import (
	"context"
	"errors"
	"os"
	"strconv"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// 在专用空库事务内编译实际 DDL；错误只带静态 DDL 的诊断，事务始终回滚。
func TestPostgresSnapshotDDL(t *testing.T) {
	dsn := os.Getenv("PULSE_DISTRIBUTED_TEST_DSN")
	if dsn == "" || os.Getenv("PULSE_DISTRIBUTED_TEST_ALLOW_DESTRUCTIVE") != "yes" {
		t.Skip("dedicated DSN and destructive opt-in required")
	}
	ctx := context.Background()
	admin, err := connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	var exists bool
	if err = admin.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='pulse_durable_distributed')").Scan(&exists); err != nil {
		t.Fatal(scrub(err, false))
	}
	if exists {
		t.Skip("DDL compilation requires an empty dedicated database")
	}
	tx, err := admin.Begin(ctx)
	if err != nil {
		t.Fatal(scrub(err, false))
	}
	defer tx.Rollback(ctx)
	for _, part := range []struct{ name, sql string }{{"schema", schemaSQL}, {"budget", snapshotBudgetSQL}, {"maintenance", maintenanceSQL}, {"check", checkSQL}} {
		if _, err = tx.Exec(ctx, part.sql); err != nil {
			var pe *pgconn.PgError
			if errors.As(err, &pe) {
				t.Fatalf("%s DDL: SQLSTATE %s position=%d internal_position=%d message=%s context=%s", part.name, pe.Code, pe.Position, pe.InternalPosition, pe.Message, pe.Where)
			}
			t.Fatal(scrub(err, false))
		}
	}
	snapshotDDLRollbackSmoke(t, ctx, tx)
}

func snapshotDDLRollbackSmoke(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	task := taskFixture(t, "ddl", "smoke")
	r := record(task, 1)
	initial, err := recordio.EncodeSnapshot(r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO pulse_durable_distributed.runs(namespace,run_id,task,digest,route) VALUES($1,$2,$3,$4,$5)`, "ddl", r.RunID, task.Bytes(), task.SubmissionDigest(), task.Route()); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `SELECT pulse_durable_distributed.snapshot_budget_initialize($1,$2,$3::jsonb,$4)`, "ddl", r.RunID, initial, len(initial)); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `UPDATE pulse_durable_distributed.runs SET record=$3,revision=1 WHERE namespace=$1 AND run_id=$2`, "ddl", r.RunID, initial); err != nil {
		t.Fatal(err)
	}
	key := "<>&\"\\\n\x01\u2028\u2029"
	v := durable.ValueRecord{Name: "value", TypeID: "bytes", Version: "v1", State: "ready", Data: []byte{0, 127, 255}}
	id := durable.StreamIDRecord{Scope: "child", Generation: 9}
	desc := durable.StreamDescriptorRecord{ID: id}
	header := snapshotTestStreamHeader{Stream: desc, LastSequence: 9}
	managed := snapshotTestManagedHeader{Scope: "child"}
	p := durable.StreamMessageRecord{Sequence: 9, Value: v}
	batches := [][]map[string]any{{
		snapshotTestOp("scope", "child", "", "", "", false, nil, durable.ScopeRecord{Graph: key, Inputs: r.Inputs}), snapshotTestOp("node", "", "child", key, "", false, nil, durable.NodeRecord{NodeID: key, Outputs: []durable.ValueRecord{v}}),
		snapshotTestOp("attempt", key, "", "", "", false, nil, durable.AttemptRecord{}), snapshotTestOp("message", key, "", "", "", false, nil, durable.MessageAttemptRecord{}), snapshotTestOp("stream", key, "", "", "", false, nil, header), snapshotTestOp("pending", "", key, "", "9", false, nil, p),
		snapshotTestOp("managed", key, "", "", "", false, nil, managed), snapshotTestOp("managed_entry", "", key, v.Name, "", true, nil, v), snapshotTestOp("managed_entry", "", key, v.Name, "", false, nil, v),
	}, {
		snapshotTestOp("pending", "", key, "", "9", false, p, nil), snapshotTestOp("managed", key, "", "", "", false, managed, managed), snapshotTestOp("managed_entry", "", key, v.Name, "", true, v, nil), snapshotTestOp("managed_entry", "", key, v.Name, "", false, v, nil),
	}}
	for i := 0; i < 10; i++ {
		var batch durable.ChangeBatch
		if i < len(batches) {
			batch = snapshotTestBatch(t, r, batches[i]...)
		} else {
			batch = nextBatch(t, r)
		}
		wire, err := durable.MarshalChangeBatch(batch)
		if err != nil {
			t.Fatal(err)
		}
		var got int64
		if err = tx.QueryRow(ctx, `SELECT pulse_durable_distributed.snapshot_budget_apply(r,$3::jsonb) FROM pulse_durable_distributed.runs r WHERE namespace=$1 AND run_id=$2`, "ddl", r.RunID, wire).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if _, err = batch.ApplyTo(&r); err != nil {
			t.Fatal(err)
		}
		expected, err := recordio.EncodeSnapshot(r)
		if err != nil {
			t.Fatal(err)
		}
		if got != int64(len(expected)) {
			t.Fatalf("revision=%d SQL=%d Go=%d", r.Revision, got, len(expected))
		}
		if _, err = tx.Exec(ctx, `UPDATE pulse_durable_distributed.runs SET revision=$3 WHERE namespace=$1 AND run_id=$2`, "ddl", r.RunID, strconv.FormatUint(r.Revision, 10)); err != nil {
			t.Fatal(err)
		}
		var ledger string
		var valid bool
		if err = tx.QueryRow(ctx, `SELECT s->>'snapshot_ledger_bytes',(s->>'snapshot_ledger_valid')::boolean FROM pulse_durable_distributed.runs r CROSS JOIN LATERAL pulse_durable_distributed.snapshot_budget_summary(r) s WHERE namespace=$1 AND run_id=$2`, "ddl", r.RunID).Scan(&ledger, &valid); err != nil {
			t.Fatal(err)
		}
		if !valid || ledger != strconv.Itoa(len(expected)) {
			t.Fatalf("revision=%d summary=%s valid=%v Go=%d", r.Revision, ledger, valid, len(expected))
		}
	}
}
