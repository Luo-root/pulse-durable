package postgres

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
)

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
