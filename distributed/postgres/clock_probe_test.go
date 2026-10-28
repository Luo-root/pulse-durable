package postgres

import (
	"bytes"
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	d "github.com/Luo-root/pulse-durable/distributed"
)

// No schema, table or host-clock changes. Each query is sent only after the
// preceding connection's response has been received, establishing causality
// independently of concurrent response ordering.
func TestPostgresCrossConnectionClockCausality(t *testing.T) {
	dsn := os.Getenv("PULSE_DISTRIBUTED_TEST_DSN")
	if dsn == "" || os.Getenv("PULSE_DISTRIBUTED_TEST_ALLOW_DESTRUCTIVE") != "yes" {
		t.Skip("dedicated DSN and explicit opt-in required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pool, e := connect(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer pool.Close()
	a, e := pool.Acquire(ctx)
	if e != nil {
		t.Fatal(scrub(e, false))
	}
	defer a.Release()
	b, e := pool.Acquire(ctx)
	if e != nil {
		t.Fatal(scrub(e, false))
	}
	defer b.Release()
	type stats struct {
		samples, negative int
		min               int64
		set               bool
	}
	var directions [2]stats
	var previous time.Time
	if e = a.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&previous); e != nil {
		t.Fatal(scrub(e, false))
	}
	for i := 0; i < 4000; i++ {
		connection := b
		direction := 0
		if i%2 == 1 {
			connection = a
			direction = 1
		}
		var current time.Time
		var delta int64
		// MATERIALIZED ensures the returned timestamp and delta use one sample.
		e = connection.QueryRow(ctx, `WITH sampled AS MATERIALIZED (SELECT clock_timestamp() AS sampled_at) SELECT sampled_at,trunc(extract(epoch FROM (sampled_at-$1::timestamptz))*1000000)::bigint FROM sampled`, previous).Scan(&current, &delta)
		if e != nil {
			t.Fatalf("causal clock probe query %d: %v", i, scrub(e, false))
		}
		s := &directions[direction]
		s.samples++
		if !s.set || delta < s.min {
			s.min = delta
			s.set = true
		}
		if delta < 0 {
			s.negative++
		}
		previous = current
	}
	labels := [2]string{"A_to_B", "B_to_A"}
	failed := false
	for i, s := range directions {
		t.Logf("direction=%s samples=%d negative_count=%d min_delta_us=%d", labels[i], s.samples, s.negative, s.min)
		failed = failed || s.negative > 0
	}
	if failed {
		t.Fatal("database cross-connection clock causality violation observed; no host-clock cause inferred")
	}
}

func TestPostgresClockRegressionRejectsWithoutPublication(t *testing.T) {
	c, admin, ns := integration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	task := taskFixture(t, ns, "clock-zero-publication")
	if _, e := c.Submit(ctx, task); e != nil {
		t.Fatal(e)
	}
	claim := claimOne(t, c, task, "clock-owner", time.Minute)
	initial := record(task, 1)
	if e := c.InitializeOwned(ctx, d.InitializeRequest{Token: claim.Token, Record: initial, Mutation: mut("clock-init")}); e != nil {
		t.Fatal(e)
	}
	batch := nextBatch(t, initial)
	digest, e := d.DeltaMutationDigest(claim.Token, batch)
	if e != nil {
		t.Fatal(e)
	}
	mutation := d.Mutation{ID: "clock-rejected-apply", Digest: digest}
	type authority struct {
		revision, chain   string
		record            []byte
		batches, receipts int64
	}
	read := func() authority {
		t.Helper()
		var state authority
		e := admin.QueryRow(ctx, `SELECT revision::text,head_chain,record,(SELECT count(*) FROM pulse_durable_distributed.batches WHERE namespace=$1 AND run_id=$2),(SELECT count(*) FROM pulse_durable_distributed.events WHERE namespace=$1 AND run_id=$2 AND mutation_id IS NOT NULL) FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2`, ns, task.Identity().RunID).Scan(&state.revision, &state.chain, &state.record, &state.batches, &state.receipts)
		if e != nil {
			t.Fatal(scrub(e, false))
		}
		return state
	}
	before := read()
	var originalUpdated time.Time
	if e = admin.QueryRow(ctx, `SELECT updated_at FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2`, ns, task.Identity().RunID).Scan(&originalUpdated); e != nil {
		t.Fatal(scrub(e, false))
	}
	restore := func() error {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_, e := admin.Exec(cleanup, `UPDATE pulse_durable_distributed.runs SET updated_at=$3 WHERE namespace=$1 AND run_id=$2`, ns, task.Identity().RunID, originalUpdated)
		return e
	}
	t.Cleanup(func() {
		if e := restore(); e != nil {
			t.Error("restore isolated fixture failed", scrub(e, false))
		}
	})
	if _, e = admin.Exec(ctx, `UPDATE pulse_durable_distributed.runs SET updated_at=clock_timestamp()+interval '1 hour' WHERE namespace=$1 AND run_id=$2`, ns, task.Identity().RunID); e != nil {
		t.Fatal(scrub(e, false))
	}
	var diagnosticMu sync.Mutex
	var diagnostics []clockDiagnostic
	c.clockDiagnostic = func(value clockDiagnostic) {
		diagnosticMu.Lock()
		defer diagnosticMu.Unlock()
		if len(diagnostics) < 4 {
			diagnostics = append(diagnostics, value)
		}
	}
	if _, e = c.ApplyOwned(ctx, claim.Token, mutation, batch); !errors.Is(e, d.ErrClockRegression) {
		t.Fatalf("future authority not rejected as clock regression: %v", e)
	}
	diagnosticMu.Lock()
	got := append([]clockDiagnostic(nil), diagnostics...)
	diagnosticMu.Unlock()
	if len(got) != 1 || got[0].Phase != clockOwnedFirst || got[0].DeltaUS >= 0 || got[0].StepUS != 0 {
		t.Fatalf("missing fixed owned_first diagnostic: %+v", got)
	}
	after := read()
	if before.revision != after.revision || before.chain != after.chain || !bytes.Equal(before.record, after.record) || before.batches != after.batches || before.receipts != after.receipts {
		t.Fatal("PD007 published revision, chain, snapshot bytes, batch or receipt")
	}
	receipts, e := c.FindMutation(ctx, task.Identity(), mutation)
	if e != nil || len(receipts) != 0 {
		t.Fatal("rejected mutation has receipt", e)
	}
	if e = restore(); e != nil {
		t.Fatal(scrub(e, false))
	}
	c.DropOwnedProjection(claim.Token)
	loaded, e := c.LoadOwned(ctx, claim.Token)
	if e != nil || loaded.Revision != 1 || loaded.RunID != initial.RunID {
		t.Fatal("fresh authoritative load changed after rejected Apply", e)
	}
}
