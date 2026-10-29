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

func TestPostgresMaintenanceReceiptObservationRegression(t *testing.T) {
	c, admin, ns := integration(t)
	ctx := context.Background()
	var role string
	if err := c.pool.QueryRow(ctx, "SELECT session_user").Scan(&role); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "INSERT INTO pulse_durable_distributed.access VALUES($1,$2,'maintain')", role, ns); err != nil {
		t.Fatal(err)
	}
	task := taskFixture(t, ns, "maintenance-receipt-clock")
	if _, err := c.Submit(ctx, task); err != nil {
		t.Fatal(err)
	}
	claim := claimOne(t, c, task, "maintenance-clock", time.Minute)
	if err := c.InitializeOwned(ctx, d.InitializeRequest{Token: claim.Token, Record: record(task, 1), Mutation: mut("maintenance-clock-init")}); err != nil {
		t.Fatal(err)
	}
	q := CompactStorageRequest{Identity: task.Identity(), ExpectedGeneration: 1, ThroughRevision: 1, Mutation: d.Mutation{ID: "clock-build"}}
	q.Mutation.Digest = CompactStorageDigest(q)
	a, err := maintenanceArgs(q)
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.maintenanceCall(ctx, "begin", a, true)
	if err != nil {
		t.Fatal(err)
	}
	var begin struct {
		Image json.RawMessage `json:"image"`
	}
	if err = json.Unmarshal(b, &begin); err != nil {
		t.Fatal(err)
	}
	r, chain, err := buildMaintenanceImage(ctx, q, begin.Image)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := encodeMaintenanceRecord(r)
	if err != nil {
		t.Fatal(err)
	}
	a["record"] = wire
	a["snapshot_digest"] = digest(wire)
	a["chain"] = chain
	// The pin expires before the prior receipt observation, although it still
	// looks live to the regressed current clock. Never adjust the host clock.
	if _, err = admin.Exec(ctx, `UPDATE pulse_durable_distributed.runs SET receipt_observed_through=clock_timestamp()+interval '1 hour' WHERE namespace=$1 AND run_id=$2`, ns, task.Identity().RunID); err != nil {
		t.Fatal(err)
	}
	observe := func() string {
		t.Helper()
		var state string
		if err := admin.QueryRow(ctx, `SELECT jsonb_build_object('run',to_jsonb(r),'pins',(SELECT jsonb_agg(to_jsonb(p) ORDER BY mutation_id) FROM pulse_durable_distributed.build_pins p WHERE namespace=$1 AND run_id=$2),'guards',(SELECT jsonb_agg(to_jsonb(g) ORDER BY mutation_id) FROM pulse_durable_distributed.maintenance_guards g WHERE namespace=$1 AND run_id=$2),'receipts',(SELECT jsonb_agg(to_jsonb(m) ORDER BY mutation_id) FROM pulse_durable_distributed.maintenance_receipts m WHERE namespace=$1 AND run_id=$2),'snapshots',(SELECT jsonb_agg(to_jsonb(s) ORDER BY generation) FROM pulse_durable_distributed.storage_snapshots s WHERE namespace=$1 AND run_id=$2),'events',(SELECT jsonb_agg(to_jsonb(e) ORDER BY event_seq) FROM pulse_durable_distributed.events e WHERE namespace=$1 AND run_id=$2))::text FROM pulse_durable_distributed.runs r WHERE namespace=$1 AND run_id=$2`, ns, task.Identity().RunID).Scan(&state); err != nil {
			t.Fatal(err)
		}
		return state
	}
	before := observe()
	for _, op := range []string{"inspect", "find", "abort", "publish", "begin"} {
		t.Run(op, func(t *testing.T) {
			request := a
			if op == "begin" {
				fresh := q
				fresh.Mutation.ID = "clock-fresh-build"
				var err error
				request, err = maintenanceArgs(fresh)
				if err != nil {
					t.Fatal(err)
				}
			}
			if _, err := c.maintenanceCall(ctx, op, request, op != "inspect" && op != "find"); !errors.Is(err, d.ErrClockRegression) {
				t.Fatal("regressed maintenance not PD007", op, err)
			}
			if after := observe(); after != before {
				t.Fatal("regressed maintenance changed head/snapshot/pin/guard/receipt/event")
			}
		})
	}
}

// Not run by offline validation. Requires the same explicit disposable-DB opt-in
// as all existing integration tests. The helper's role remains a restricted login.
func TestPostgresMaintenancePreservesConcurrentSuffixAndReceipt(t *testing.T) {
	c, admin, ns := integration(t)
	ctx := context.Background()
	var role string
	if e := c.pool.QueryRow(ctx, "SELECT session_user").Scan(&role); e != nil {
		t.Fatal(e)
	}
	if _, e := admin.Exec(ctx, "INSERT INTO pulse_durable_distributed.access VALUES($1,$2,'maintain')", role, ns); e != nil {
		t.Fatal(e)
	}
	task := taskFixture(t, ns, "maintenance-suffix")
	if _, e := c.Submit(ctx, task); e != nil {
		t.Fatal(e)
	}
	owner := claimOne(t, c, task, "maintenance-suffix", time.Minute)
	if _, err := c.LoadOwned(ctx, owner.Token); !errors.Is(err, d.ErrNotFound) {
		t.Fatalf("fresh owned run must allow initialization: %v", err)
	}
	if e := c.InitializeOwned(ctx, d.InitializeRequest{Token: owner.Token, Record: record(task, 1), Mutation: mut("maintenance-init")}); e != nil {
		t.Fatal(e)
	}
	q := CompactStorageRequest{Identity: task.Identity(), ExpectedGeneration: 1, ThroughRevision: 1, Mutation: d.Mutation{ID: "maintenance-build"}}
	q.Mutation.Digest = CompactStorageDigest(q)
	a, e := maintenanceArgs(q)
	if e != nil {
		t.Fatal(e)
	}
	b, e := c.maintenanceCall(ctx, "begin", a, true)
	if e != nil {
		t.Fatal(e)
	}
	var begin struct {
		Image json.RawMessage `json:"image"`
	}
	if e = json.Unmarshal(b, &begin); e != nil {
		t.Fatal(e)
	}
	r, chain, e := buildMaintenanceImage(ctx, q, begin.Image)
	if e != nil {
		t.Fatal(e)
	}
	// Business writes after pin creation and before publication must survive.
	batch := nextBatch(t, r)
	dg, e := d.DeltaMutationDigest(owner.Token, batch)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.ApplyOwned(ctx, owner.Token, d.Mutation{ID: "maintenance-suffix-apply", Digest: dg}, batch); e != nil {
		t.Fatal(e)
	}
	wire, e := encodeMaintenanceRecord(r)
	if e != nil {
		t.Fatal(e)
	}
	a["record"] = wire
	a["snapshot_digest"] = digest(wire)
	a["chain"] = chain
	b, e = c.maintenanceCall(ctx, "publish", a, true)
	if e != nil {
		t.Fatal(e)
	}
	result, e := decodeMaintenanceResult(b, q.Mutation)
	if e != nil || result.Result.State.HeadRevision != "2" || result.Result.State.Generation != "2" {
		t.Fatal(result, e)
	}
	c.DropOwnedProjection(owner.Token)
	loaded, e := c.LoadOwned(ctx, owner.Token)
	if e != nil || loaded.Revision != 2 {
		t.Fatal(loaded.Revision, e)
	}
	// A fresh owner projection must append against the preserved business chain.
	post := nextBatch(t, loaded)
	postDigest, err := d.DeltaMutationDigest(owner.Token, post)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.ApplyOwned(ctx, owner.Token, d.Mutation{ID: "post-maintenance-apply", Digest: postDigest}, post); err != nil {
		t.Fatal("append after publication", err)
	}
	second := CompactStorageRequest{Identity: task.Identity(), ExpectedGeneration: 2, ThroughRevision: 2, Mutation: d.Mutation{ID: "maintenance-second"}}
	second.Mutation.Digest = CompactStorageDigest(second)
	secondResult, err := c.CompactStorage(ctx, second)
	if err != nil || secondResult.Status != "published" || secondResult.Result.State.BaseRevision != "2" || secondResult.Result.State.HeadRevision != "3" {
		t.Fatal("repeat synthesis", secondResult, err)
	}
	c.DropOwnedProjection(owner.Token)
	loaded, e = c.LoadOwned(ctx, owner.Token)
	if e != nil || loaded.Revision != 3 {
		t.Fatal("reload after repeat synthesis", loaded.Revision, e)
	}
	// Diagnostic deletion must have no effect on maintenance receipts or guards.
	if _, e = admin.Exec(ctx, "DELETE FROM pulse_durable_distributed.events WHERE namespace=$1", ns); e != nil {
		t.Fatal(e)
	}
	found, e := c.FindMaintenance(ctx, q.Identity, q.Mutation)
	if e != nil || found.Status != "published" {
		t.Fatal(found, e)
	}
	again, e := c.CompactStorage(ctx, q)
	if e != nil || again.Status != "published" || again.Result.State.Generation != "2" {
		t.Fatal(again, e)
	}
	// Replay cached business state is unchanged by representation publication.
	if loaded.Format != durable.RecordFormat {
		t.Fatal("logical format changed")
	}
}
