package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	d "github.com/Luo-root/pulse-durable/distributed"
)

func TestPostgresIncrementalChainAndHeadCAS(t *testing.T) {
	c, admin, ns := integration(t)
	ctx := context.Background()
	task := taskFixture(t, ns, "incremental")
	if _, e := c.Submit(ctx, task); e != nil {
		t.Fatal(e)
	}
	claim := claimOne(t, c, task, "incremental-owner", time.Minute)
	initial := record(task, 1)
	if e := c.InitializeOwned(ctx, d.InitializeRequest{Token: claim.Token, Record: initial, Mutation: mut("initialize")}); e != nil {
		t.Fatal(e)
	}
	var original []byte
	if e := admin.QueryRow(ctx, `SELECT record FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2`, ns, task.Identity().RunID).Scan(&original); e != nil {
		t.Fatal(scrub(e, false))
	}
	for i := 0; i < 12; i++ {
		b := nextBatch(t, initial)
		digest, e := d.DeltaMutationDigest(claim.Token, b)
		if e != nil {
			t.Fatal(e)
		}
		ack, e := c.ApplyOwned(ctx, claim.Token, d.Mutation{ID: "apply-" + b.Header().RunID + "-" + string(rune('a'+i)), Digest: digest}, b)
		if e != nil || ack.Header != b.Header() {
			t.Fatalf("apply %d: %v", i, e)
		}
		initial.Revision++
	}
	var batches int
	var same bool
	if e := admin.QueryRow(ctx, `SELECT record=$3,(SELECT count(*) FROM pulse_durable_distributed.batches WHERE namespace=$1 AND run_id=$2) FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2`, ns, task.Identity().RunID, original).Scan(&same, &batches); e != nil || !same || batches != 12 {
		t.Fatalf("snapshot rewritten or batch lost: same=%v batches=%d err=%v", same, batches, scrub(e, false))
	}
	// 丢弃进程内投影，要求从权威链重放。
	c.projectionMu.Lock()
	delete(c.projections, claim.Token)
	c.projectionMu.Unlock()
	loaded, e := c.LoadOwned(ctx, claim.Token)
	if e != nil || loaded.Revision != 13 {
		t.Fatalf("replay: revision=%d err=%v", loaded.Revision, e)
	}
	delete(loaded.Scopes, "root")
	again, e := c.LoadOwned(ctx, claim.Token)
	if e != nil || len(again.Scopes) != 1 {
		t.Fatal("Load alias", e)
	}
	// 管理员制造的损坏保留修订号却改变权威链：
	// 缓存提案不得据此头授权写入。
	if _, e = admin.Exec(ctx, `UPDATE pulse_durable_distributed.runs SET head_chain=repeat('f',64) WHERE namespace=$1 AND run_id=$2`, ns, task.Identity().RunID); e != nil {
		t.Fatal(scrub(e, false))
	}
	b := nextBatch(t, again)
	digest, _ := d.DeltaMutationDigest(claim.Token, b)
	if _, e = c.ApplyOwned(ctx, claim.Token, d.Mutation{ID: "wrong-head", Digest: digest}, b); !errors.Is(e, d.ErrRevisionConflict) {
		t.Fatalf("head CAS bypass: %v", e)
	}
	if _, e = c.LoadOwned(ctx, claim.Token); !errors.Is(e, d.ErrInvalidRecord) {
		t.Fatalf("corrupt head accepted: %v", e)
	}
}
