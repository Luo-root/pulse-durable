package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	d "github.com/Luo-root/pulse-durable/distributed"
)

func TestStorageCallRejectsUnknownFunction(t *testing.T) {
	for _, name := range []string{"", "begin", "maintenance", "storage_lock", "storage_inspect); SELECT 1--"} {
		a := map[string]any{}
		if _, err := (*Coordinator)(nil).storageCall(context.Background(), name, a, true); !errors.Is(err, d.ErrInvalid) {
			t.Fatalf("unknown function %q: %v", name, err)
		}
		if len(a) != 0 {
			t.Fatal("unknown function mutated request")
		}
	}
}

func TestStorageEntrypointDDL(t *testing.T) {
	if strings.Contains(maintenanceSQL, ".maintenance(") || strings.Contains(maintenanceSQL, "op text") {
		t.Fatal("legacy maintenance dispatcher installed")
	}
	for _, name := range []string{"storage_inspect", "storage_compact_begin", "storage_compact_publish", "storage_compact_abort", "storage_compact_find"} {
		if !strings.Contains(maintenanceSQL, "CREATE FUNCTION pulse_durable_distributed."+name+"(a jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER") {
			t.Fatal("missing independent entrypoint", name)
		}
	}
}

func TestPostgresStorageEntrypointACLAndAbortLifecycle(t *testing.T) {
	c, admin, ns := integration(t)
	ctx := context.Background()
	var legacy bool
	if err := admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_proc WHERE pronamespace='pulse_durable_distributed'::regnamespace AND proname='maintenance')`).Scan(&legacy); err != nil || legacy {
		t.Fatal("legacy maintenance function exists", legacy, err)
	}
	for _, name := range []string{"storage_inspect", "storage_compact_begin", "storage_compact_publish", "storage_compact_abort", "storage_compact_find", "storage_lock", "storage_clock", "storage_state", "storage_image"} {
		var granted, public bool
		if err := c.pool.QueryRow(ctx, `SELECT has_function_privilege(session_user,p.oid,'EXECUTE'), EXISTS(SELECT 1 FROM aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a WHERE a.grantee=0) FROM pg_proc p WHERE p.pronamespace='pulse_durable_distributed'::regnamespace AND p.proname=$1`, name).Scan(&granted, &public); err != nil {
			t.Fatal(name, err)
		}
		// integration 默认只授予 query/worker/submit/cancel；物理合成
		// 必须在后面显式授予 maintain 后才可执行。
		entry := false
		if granted != entry || public {
			t.Fatalf("%s granted=%v public=%v", name, granted, public)
		}
	}
	if _, err := c.pool.Exec(ctx, `UPDATE pulse_durable_distributed.runs SET maintenance_watermark=clock_timestamp()`); err == nil {
		t.Fatal("direct DML allowed")
	}
	task := taskFixture(t, ns, "abort-lifecycle")
	if _, err := c.Submit(ctx, task); err != nil {
		t.Fatal(err)
	}
	owner := claimOne(t, c, task, "abort-owner", time.Minute)
	if err := c.InitializeOwned(ctx, d.InitializeRequest{Token: owner.Token, Record: record(task, 1), Mutation: mut("abort-init")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.InspectStorage(ctx, task.Identity()); err == nil {
		t.Fatal("worker permission authorized maintenance")
	}
	var role string
	if err := c.pool.QueryRow(ctx, "SELECT session_user").Scan(&role); err != nil {
		t.Fatal(err)
	}
	setTestPermission(t, admin, ctx, role, ns, "maintain", true)
	q := CompactStorageRequest{Identity: task.Identity(), ExpectedGeneration: 1, ThroughRevision: 1, Mutation: d.Mutation{ID: "abort-build"}}
	q.Mutation.Digest = CompactStorageDigest(q)
	a, err := maintenanceArgs(q)
	if err != nil {
		t.Fatal(err)
	}
	a["worker_id"] = "forbidden"
	if _, err = c.storageCall(ctx, "storage_compact_begin", a, true); !errors.Is(err, d.ErrInvalid) {
		t.Fatal("worker token accepted", err)
	}
	delete(a, "worker_id")
	if _, err = c.storageCall(ctx, "storage_compact_begin", a, true); err != nil {
		t.Fatal(err)
	}
	if found, err := c.FindMaintenance(ctx, q.Identity, q.Mutation); err != nil || found.Status != "building" || found.Result != nil {
		t.Fatal(found, err)
	}
	b, err := c.storageCall(ctx, "storage_compact_abort", a, true)
	var closed MaintenanceResult
	if err != nil || json.Unmarshal(b, &closed) != nil || closed.Status != "closed" || closed.Result != nil {
		t.Fatal(closed, err)
	}
	if found, err := c.FindMaintenance(ctx, q.Identity, q.Mutation); err != nil || found.Status != "closed" || found.Result != nil {
		t.Fatal(found, err)
	}
	if _, err = c.storageCall(ctx, "storage_compact_begin", a, true); !errors.Is(err, d.ErrRevisionConflict) {
		t.Fatal("permanent guard restarted", err)
	}
}
