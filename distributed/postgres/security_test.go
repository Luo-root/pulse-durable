package postgres

import (
	"context"
	"testing"
)

func TestTemporaryTablesCannotShadowDefinerAuthority(t *testing.T) {
	c, admin, ns := integration(t)
	ctx := context.Background()
	other := "not-authorized-" + ns
	task := taskFixture(t, other, "private")
	if _, e := admin.Exec(ctx, `INSERT INTO pulse_durable_distributed.runs(namespace,run_id,task,digest,route) VALUES($1,$2,$3,$4,$5)`, other, task.Identity().RunID, task.Bytes(), task.SubmissionDigest(), task.Route()); e != nil {
		t.Fatal(scrub(e, false))
	}
	conn, e := c.pool.Acquire(ctx)
	if e != nil {
		t.Fatal(scrub(e, false))
	}
	defer conn.Release()
	if _, e = conn.Exec(ctx, `CREATE TEMP TABLE access(role_name name,namespace text,permission text)`); e != nil {
		t.Fatal(scrub(e, false))
	}
	if _, e = conn.Exec(ctx, `INSERT INTO pg_temp.access VALUES(session_user,$1,'query'),(session_user,$1,'cancel')`, other); e != nil {
		t.Fatal(scrub(e, false))
	}
	var b []byte
	if e = conn.QueryRow(ctx, `SELECT pulse_durable_distributed.api('get',jsonb_build_object('namespace',$1::text,'run_id',$2::text))`, other, task.Identity().RunID).Scan(&b); e == nil {
		t.Fatal("temporary access granted unauthorized cross-namespace read")
	}
	if e = conn.QueryRow(ctx, `SELECT pulse_durable_distributed.api('cancel',jsonb_build_object('namespace',$1::text,'run_id',$2::text,'mutation_id','bad','mutation_digest',repeat('a',64)))`, other, task.Identity().RunID).Scan(&b); e == nil {
		t.Fatal("temporary access granted unauthorized cross-namespace cancel")
	}
	var status string
	if e = admin.QueryRow(ctx, `SELECT status FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2`, other, task.Identity().RunID).Scan(&status); e != nil || status != "queued" {
		t.Fatalf("unauthorized cancellation changed authority: status=%s err=%v", status, e)
	}
	// Temporary business tables must not divert authorized queries/writes.
	if _, e = conn.Exec(ctx, `CREATE TEMP TABLE runs(namespace text,run_id text); CREATE TEMP TABLE leases(namespace text,lease_id text); CREATE TEMP TABLE events(namespace text,run_id text)`); e != nil {
		t.Fatal(scrub(e, false))
	}
	own := taskFixture(t, ns, "shadow")
	if _, e = conn.Exec(ctx, `SELECT pulse_durable_distributed.api('submit',jsonb_build_object('namespace',$1::text,'run_id',$2::text,'task',encode($3::bytea,'base64'),'digest',$4::text))`, ns, own.Identity().RunID, own.Bytes(), own.SubmissionDigest()); e != nil {
		t.Fatalf("authorized submit diverted to temp tables: %v", scrub(e, false))
	}
	var n int
	if e = admin.QueryRow(ctx, `SELECT count(*) FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2`, ns, own.Identity().RunID).Scan(&n); e != nil || n != 1 {
		t.Fatalf("authorized write missed authority: count=%d err=%v", n, e)
	}
	var tempCount int
	if e = conn.QueryRow(ctx, `SELECT count(*) FROM pg_temp.runs`).Scan(&tempCount); e != nil || tempCount != 0 {
		t.Fatalf("temporary runs mutated: count=%d err=%v", tempCount, e)
	}
}
