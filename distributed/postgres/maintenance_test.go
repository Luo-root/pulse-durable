package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/Luo-root/pulse-durable/recordio"
)

func TestMaintenanceRequestAndStrictReceipt(t *testing.T) {
	q := CompactStorageRequest{Identity: d.Identity{Namespace: "tenant", RunID: "run"}, ExpectedGeneration: 1, ThroughRevision: math.MaxUint64, Mutation: d.Mutation{ID: "build"}}
	q.Mutation.Digest = CompactStorageDigest(q)
	a, e := maintenanceArgs(q)
	if e != nil || a["through_revision"] != "18446744073709551615" {
		t.Fatal(a, e)
	}
	for _, key := range []string{"epoch", "lease_id", "worker_id", "boot_id"} {
		if _, ok := a[key]; ok {
			t.Fatal("owner token encoded")
		}
	}
	changed := q
	changed.ThroughRevision--
	if _, e = maintenanceArgs(changed); !errors.Is(e, d.ErrInvalid) {
		t.Fatal("digest did not bind plan")
	}
	state := StorageState{Generation: "2", BaseRevision: "9", BaseChain: strings.Repeat("a", 64), HeadRevision: "10", HeadChain: strings.Repeat("b", 64)}
	receipt := MaintenanceResult{Status: "published", Result: &MaintenanceReceipt{MutationID: q.Mutation.ID, MutationDigest: q.Mutation.Digest, Operation: "compact_storage", State: state, OccurredAt: time.Now().UTC()}}
	b, _ := json.Marshal(receipt)
	if _, e = decodeMaintenanceResult(b, q.Mutation); e != nil {
		t.Fatal(e)
	}
	for _, bad := range []string{strings.Replace(string(b), `"status":"published"`, `"status":"published","status":"published"`, 1), strings.Replace(string(b), `"status":`, `"Status":`, 1), strings.Replace(string(b), `"generation":"2"`, `"generation":"02"`, 1), strings.Replace(string(b), `"operation":"compact_storage"`, `"operation":"retain"`, 1), string(b) + `{}`, `{"status":"published","result":null}`, `{"status":"absent","result":null,"lease_id":"x"}`} {
		if _, e = decodeMaintenanceResult([]byte(bad), q.Mutation); e == nil {
			t.Fatal("invalid receipt accepted", bad)
		}
	}
	for _, status := range []string{"absent", "building", "closed", "expired"} {
		b, _ := json.Marshal(MaintenanceResult{Status: status})
		if _, e = decodeMaintenanceResult(b, q.Mutation); e != nil {
			t.Fatal(status, e)
		}
	}
}
func maintenanceImageFixture(t *testing.T) (CompactStorageRequest, []byte) {
	task := taskFixture(t, "tenant", "maintenance")
	r := record(task, 1)
	snapshot, _ := json.Marshal(r)
	chain := durable.SeedChain(sha256.Sum256(snapshot))
	batch := nextBatch(t, r)
	wire, _ := durable.MarshalChangeBatch(batch)
	next, _ := durable.NextChain(chain, sha256.Sum256(wire), 2)
	w := wireSnapshot{Task: task.Bytes(), Digest: task.SubmissionDigest(), Namespace: task.Identity().Namespace, RunID: task.Identity().RunID, Route: task.Route(), Status: d.Queued, Revision: "2", CreatedAt: time.Now(), UpdatedAt: time.Now(), Record: snapshot}
	log := storedLog{BaseRevision: "1", BaseChain: chainText(chain), Digest: digest(snapshot), Chain: chainText(next), Batches: []storedBatch{{Revision: "2", Wire: wire, Digest: digest(wire), Parent: chainText(chain), Next: chainText(next)}}}
	b, e := json.Marshal(struct {
		wireSnapshot
		storedLog
	}{w, log})
	if e != nil {
		t.Fatal(e)
	}
	return CompactStorageRequest{Identity: task.Identity(), ThroughRevision: 2, ExpectedGeneration: 1}, b
}
func TestMaintenanceBuildVerifiesChainAndCancellation(t *testing.T) {
	q, b := maintenanceImageFixture(t)
	r, chain, e := buildMaintenanceImage(context.Background(), q, b)
	if e != nil || r.Revision != 2 || !fingerprint(chain) {
		t.Fatal(r.Revision, e)
	}
	if _, e = encodeMaintenanceRecord(r); e != nil {
		t.Fatal(e)
	}
	for _, want := range []durable.RunRecord{
		{
			Format: durable.RecordFormat, RunID: "shape-nil", Revision: 1,
			Scopes: map[string]durable.ScopeRecord{}, Attempts: map[string]durable.AttemptRecord{},
			Streams: nil, Messages: nil, Managed: nil,
		},
		{
			Format: durable.RecordFormat, RunID: "shape-empty", Revision: 2,
			Scopes: map[string]durable.ScopeRecord{
				"root": {Nodes: map[string]durable.NodeRecord{
					"node": {Outputs: []durable.ValueRecord{
						{Name: "nil-data", Data: nil},
						{Name: "empty-data", Data: []byte{}},
					}},
				}},
			},
			Attempts: map[string]durable.AttemptRecord{},
			Streams:  map[string]durable.StreamRecord{}, Messages: map[string]durable.MessageAttemptRecord{}, Managed: map[string]durable.ManagedRecord{},
		},
	} {
		wire, err := encodeMaintenanceRecord(want)
		if err != nil {
			t.Fatalf("encode shape %q: %v", want.RunID, err)
		}
		var envelope struct {
			Version int               `json:"version"`
			Record  durable.RunRecord `json:"record"`
		}
		if err = json.Unmarshal(wire, &envelope); err != nil || envelope.Version != 1 || !reflect.DeepEqual(envelope.Record, want) {
			t.Fatalf("snapshot envelope changed shape %q: got %#v, want %#v (err=%v)", want.RunID, envelope, want, err)
		}
		got, err := recordio.DecodeSnapshot(wire)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("snapshot changed shape %q: got %#v, want %#v (err=%v)", want.RunID, got, want, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, e = buildMaintenanceImage(ctx, q, b); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	var obj map[string]any
	_ = json.Unmarshal(b, &obj)
	obj["head_chain"] = strings.Repeat("f", 64)
	bad, _ := json.Marshal(obj)
	if _, _, e = buildMaintenanceImage(context.Background(), q, bad); e == nil {
		t.Fatal("bad chain accepted")
	}
}
func TestMaintenanceSQLBoundaryContract(t *testing.T) {
	for _, guard := range []string{
		"IF t<r.updated_at OR t<r.maintenance_watermark OR t<r.receipt_observed_through THEN",
		"IF t<first_sample OR t<r.updated_at OR t<r.maintenance_watermark OR t<r.receipt_observed_through THEN",
	} {
		if strings.Count(maintenanceSQL, guard) != 1 {
			t.Fatal("maintenance clock guard missing or ambiguous", guard)
		}
	}
	for _, s := range []string{"CHECK(version=6)", "'maintain','retain','replay'", "maintenance_receipts", "maintenance_guards", "build_pins", "storage_snapshots", "active_snapshot", "RETURN storage_image(r,r.revision,true)"} {
		if !strings.Contains(schemaSQL, s) {
			t.Fatal("missing schema contract", s)
		}
	}
	for _, s := range []string{"SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp", "x.permission='maintain'", "pg_advisory_xact_lock", "FOR UPDATE", "p.expires_at<=t", "r.representation_generation<>p.generation", "r.revision<p.source_revision", "maintenance_watermark", "INSERT INTO pulse_durable_distributed.maintenance_receipts", "worker token forbidden", "jsonb_object_keys(j)", "j->'version' IS DISTINCT FROM '1'::jsonb", "j->'record'->>'run_id'", "j->'record'->>'revision'", "j->'record'->>'format'", "j->'record'->>'definition'", "j->'record'->>'inputs'", "j->'record'->>'root_scope'", "jsonb_object_keys(j->'record')"} {
		if !strings.Contains(maintenanceSQL, s) {
			t.Fatal("missing publication contract", s)
		}
	}
	for _, s := range []string{"DELETE FROM", "UPDATE pulse_durable_distributed.batches", "INSERT INTO pulse_durable_distributed.events", "SET revision=", "FROM pulse_durable_distributed.leases"} {
		if strings.Contains(maintenanceSQL, s) {
			t.Fatal("maintenance crossed boundary", s)
		}
	}
}

// This test is intentionally not executed by offline validation. It installs no
// extra grants: a normal worker must be denied even with valid worker ownership.
func TestPostgresMaintenanceWorkerDenied(t *testing.T) {
	c, _, ns := integration(t)
	task := taskFixture(t, ns, "maintenance-denied")
	ctx := context.Background()
	if _, e := c.Submit(ctx, task); e != nil {
		t.Fatal(e)
	}
	owner := claimOne(t, c, task, "maintenance-denied", time.Minute)
	if e := c.InitializeOwned(ctx, d.InitializeRequest{Token: owner.Token, Record: record(task, 1), Mutation: mut("maintenance-denied-init")}); e != nil {
		t.Fatal(e)
	}
	if _, e := c.InspectStorage(ctx, task.Identity()); e == nil {
		t.Fatal("worker acquired maintenance authority")
	}
}
