package postgres

import (
	"context"
	"errors"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
)

// 同容器内的等和损坏必须在 Load 时拒绝，不能等下次替换后再发现扣账错误。
func TestPostgresSnapshotLedgerRejectsEqualSumObjectCorruption(t *testing.T) {
	c, admin, ns := integration(t)
	h := newSnapshotTestRun(t, c, admin, ns, "ledger-equal-sum")
	h.apply(t, snapshotTestBatch(t, h.r,
		snapshotTestOp("node", "", h.r.RootScope, "a", "", false, nil, snapshotDataNode("a", 2)),
		snapshotTestOp("node", "", h.r.RootScope, "b", "", false, nil, snapshotDataNode("b", 3))))
	ctx := context.Background()
	changed, err := admin.Exec(ctx, `UPDATE pulse_durable_distributed.snapshot_objects SET bytes=bytes+CASE key2 WHEN 'a' THEN 1 ELSE -1 END WHERE namespace=$1 AND run_id=$2 AND kind='node' AND key1=$3 AND key2 IN ('a','b')`, ns, h.r.RunID, h.r.RootScope)
	if err != nil || changed.RowsAffected() != 2 {
		t.Fatalf("equal-sum corruption setup: rows=%d err=%v", changed.RowsAffected(), err)
	}
	// 旧版汇总检查仍应通过，证明这不是简单总数错误。
	h.check(t)
	before := h.publication(t)
	c.DropOwnedProjection(h.token)
	_, err = c.LoadOwned(ctx, h.token)
	if !errors.Is(err, ErrInvalidRecordBudget) || !errors.Is(err, d.ErrInvalidRecord) {
		t.Errorf("equal-sum object corruption escaped Load: want ErrInvalidRecordBudget/ErrInvalidRecord, got %v", err)
	}
	if after := h.publication(t); after != before {
		t.Fatal("Load published state or repaired the corrupt ledger")
	}
}

func TestPostgresSnapshotLedgerRejectsWrongKeysAndNullness(t *testing.T) {
	for _, kind := range []string{"object_key", "object_suffix", "missing_object", "nil_empty_compensated"} {
		t.Run(kind, func(t *testing.T) {
			c, admin, ns := integration(t)
			h := newSnapshotTestRun(t, c, admin, ns, "ledger-"+kind)
			h.apply(t, snapshotTestBatch(t, h.r,
				snapshotTestOp("node", "", h.r.RootScope, "a", "", false, nil, snapshotDataNode("a", 2)),
				snapshotTestOp("stream", "s", "", "", "", false, nil, snapshotTestStreamHeader{Stream: durable.StreamDescriptorRecord{ID: durable.StreamIDRecord{Scope: h.r.RootScope}}})))
			ctx := context.Background()
			var statements []string
			switch kind {
			case "object_key":
				statements = []string{`UPDATE pulse_durable_distributed.snapshot_objects SET key2='c' WHERE namespace=$1 AND run_id=$2 AND kind='node' AND key2='a'`}
			case "object_suffix":
				statements = []string{`UPDATE pulse_durable_distributed.snapshot_objects SET key3='unexpected' WHERE namespace=$1 AND run_id=$2 AND kind='node'`}
			case "missing_object":
				statements = []string{`DELETE FROM pulse_durable_distributed.snapshot_objects WHERE namespace=$1 AND run_id=$2 AND kind='node'`}
			case "nil_empty_compensated":
				statements = []string{
					`UPDATE pulse_durable_distributed.snapshot_containers SET is_nil=false WHERE namespace=$1 AND run_id=$2 AND kind='pending' AND key1='s'`,
					`UPDATE pulse_durable_distributed.snapshot_containers SET entry_bytes=entry_bytes-2 WHERE namespace=$1 AND run_id=$2 AND kind='streams' AND key1=''`,
					`UPDATE pulse_durable_distributed.runs SET snapshot_fixed_bytes=snapshot_fixed_bytes+2 WHERE namespace=$1 AND run_id=$2`,
				}
			}
			for _, statement := range statements {
				if changed, err := admin.Exec(ctx, statement, ns, h.r.RunID); err != nil || changed.RowsAffected() != 1 {
					t.Fatalf("corruption setup rows=%d err=%v", changed.RowsAffected(), err)
				}
			}
			if kind != "missing_object" {
				h.check(t)
			}
			before := h.publication(t)
			c.DropOwnedProjection(h.token)
			if _, err := c.LoadOwned(ctx, h.token); !errors.Is(err, ErrInvalidRecordBudget) || !errors.Is(err, d.ErrInvalidRecord) {
				t.Fatalf("corrupt ledger accepted: %v", err)
			}
			if h.publication(t) != before {
				t.Fatal("Load published state or repaired corrupt ledger")
			}
		})
	}
}
