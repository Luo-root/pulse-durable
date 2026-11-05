package postgres

import (
	"context"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresRetentionCandidatePreview(t *testing.T) {
	dsn := os.Getenv("PULSE_DISTRIBUTED_TEST_DSN")
	if dsn == "" || os.Getenv("PULSE_DISTRIBUTED_TEST_ALLOW_DESTRUCTIVE") != "yes" {
		t.Skip("dedicated DSN and PULSE_DISTRIBUTED_TEST_ALLOW_DESTRUCTIVE=yes required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	admin, err := connect(ctx, dsn)
	if err != nil {
		t.Fatal("admin connection", err)
	}
	defer admin.Close()

	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)
	role := "pulse_retention_test_" + suffix
	ns := "retention-preview-" + suffix
	if _, err = admin.Exec(ctx, "CREATE ROLE "+role+" LOGIN PASSWORD 'retention-test-only'"); err != nil {
		t.Fatal("create retain role", scrub(err, false))
	}
	if err = Migrate(ctx, dsn, Grant{Role: role, Namespace: ns, Permissions: []string{"retain"}}); err != nil {
		t.Fatal("migrate new retention layout", err)
	}
	var schemaAllowed, functionAllowed, apiAllowed, pinAllowed bool
	if err = admin.QueryRow(ctx, `SELECT has_schema_privilege($1,'pulse_durable_distributed','USAGE'),has_function_privilege($1,'pulse_durable_distributed.retention_candidate_preview(jsonb)','EXECUTE'),has_function_privilege($1,'pulse_durable_distributed.api(text,jsonb)','EXECUTE'),has_function_privilege($1,'pulse_durable_distributed.pin_create(jsonb)','EXECUTE')`, role).Scan(&schemaAllowed, &functionAllowed, &apiAllowed, &pinAllowed); err != nil {
		t.Fatal("verify retain ACL", scrub(err, false))
	}
	if !schemaAllowed || !functionAllowed || apiAllowed || pinAllowed {
		t.Fatalf("retain ACL schema=%v preview=%v api=%v pin=%v", schemaAllowed, functionAllowed, apiAllowed, pinAllowed)
	}

	now := time.Now().UTC().Truncate(time.Microsecond)
	hex64 := strings.Repeat("a", 64)
	if _, err = admin.Exec(ctx, `INSERT INTO pulse_durable_distributed.runs(namespace,run_id,task,digest,route,status,revision,record,snapshot_digest,head_chain,created_at,updated_at,finished_at)
VALUES($1,$2,$3,$4,'{}'::jsonb,'succeeded',1,$5,$6,$7,$8,$8,$8)`, ns, "run-retention-1", []byte(`{"inputs":"retention"}`), hex64, []byte(`{"record":true}`), hex64, hex64, now); err != nil {
		t.Fatal("insert terminal run", scrub(err, false))
	}
	if _, err = admin.Exec(ctx, `INSERT INTO pulse_durable_distributed.batches(namespace,run_id,revision,wire,digest,parent_chain,next_chain) VALUES($1,$2,1,$3,$4,$5,$6)`, ns, "run-retention-1", []byte{0x7b}, hex64, hex64, hex64); err != nil {
		t.Fatal("insert batch", scrub(err, false))
	}
	if _, err = admin.Exec(ctx, `INSERT INTO pulse_durable_distributed.maintenance_guards(namespace,run_id,mutation_id,digest,operation) VALUES($1,$2,$3,$4,'compact_storage')`, ns, "run-retention-1", "maintenance-1", hex64); err != nil {
		t.Fatal("insert maintenance guard", scrub(err, false))
	}
	if _, err = admin.Exec(ctx, `INSERT INTO pulse_durable_distributed.build_pins(namespace,run_id,mutation_id,holder,generation,source_revision,source_chain,through_revision,through_chain,expires_at) VALUES($1,$2,$3,$4,1,1,$5,1,$5,$6)`, ns, "run-retention-1", "maintenance-1", role, hex64, now.Add(time.Hour)); err != nil {
		t.Fatal("insert build pin", scrub(err, false))
	}
	if _, err = admin.Exec(ctx, `INSERT INTO pulse_durable_distributed.mutation_guards(namespace,run_id,mutation_id,digest,operation,epoch,revision,occurred_at) VALUES($1,$2,$3,$4,'initialize',0,1,$5)`, ns, "run-retention-1", "mutation-1", hex64, now); err != nil {
		t.Fatal("insert mutation guard", scrub(err, false))
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("parse DSN", err)
	}
	u := url.URL{Scheme: "postgres", Host: net.JoinHostPort(cfg.ConnConfig.Host, strconv.Itoa(int(cfg.ConnConfig.Port))), Path: "/" + cfg.ConnConfig.Database, User: url.UserPassword(role, "retention-test-only")}
	q := u.Query()
	q.Set("sslmode", "disable")
	u.RawQuery = q.Encode()
	c, err := New(ctx, u.String())
	if err != nil {
		t.Fatal("retain coordinator", err)
	}
	defer c.Close()

	policy, err := c.GetRetentionPolicy(ctx, ns)
	if err != nil {
		t.Fatal("get retention policy", err)
	}
	policyVersion, err := strconv.ParseUint(policy.PolicyVersion, 10, 64)
	if err != nil {
		t.Fatal("parse policy version", err)
	}
	var beforePolicies, beforePreviews int
	if err = admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM pulse_durable_distributed.retention_policies WHERE namespace=$1)::int,(SELECT count(*) FROM pulse_durable_distributed.retention_previews WHERE namespace=$1)::int`, ns).Scan(&beforePolicies, &beforePreviews); err != nil {
		t.Fatal("count retention metadata before preview", scrub(err, false))
	}
	preview, err := c.PreviewRetentionCandidates(ctx, "dry-retention-1", ns, policyVersion, policy.Policy, d.RetentionCursor{}, 10, now.Add(-time.Minute), now.Add(time.Minute))
	if err != nil {
		t.Fatal("preview retention candidates", err)
	}
	if preview.Namespace != ns || preview.PolicyVersion != policyVersion || preview.Cursor.Set || preview.NextCursor.Set {
		t.Fatalf("unexpected preview binding/cursor: %+v", preview)
	}
	var afterPolicies, afterPreviews int
	if err = admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM pulse_durable_distributed.retention_policies WHERE namespace=$1)::int,(SELECT count(*) FROM pulse_durable_distributed.retention_previews WHERE namespace=$1)::int`, ns).Scan(&afterPolicies, &afterPreviews); err != nil {
		t.Fatal("count retention metadata after preview", scrub(err, false))
	}
	if beforePolicies != afterPolicies || beforePreviews != afterPreviews {
		t.Fatalf("candidate preview mutated retention metadata: before=%d/%d after=%d/%d", beforePolicies, beforePreviews, afterPolicies, afterPreviews)
	}
	if len(preview.Summary.Candidates) != 1 {
		t.Fatalf("candidate count=%d", len(preview.Summary.Candidates))
	}
	candidate := preview.Summary.Candidates[0]
	if candidate.Namespace != ns || candidate.Kind != "run" || candidate.ID != "run-retention-1" || candidate.Revision != 1 || candidate.Rows != 1 || candidate.Bytes != 1 {
		t.Fatalf("unexpected candidate: %+v", candidate)
	}
	if preview.Summary.LogicalRows != 1 || preview.Summary.LogicalBytes != 1 {
		t.Fatalf("unexpected summary rows=%d bytes=%d", preview.Summary.LogicalRows, preview.Summary.LogicalBytes)
	}
	for _, reason := range []d.RetentionProtectedReason{d.RetentionProtectedHistoryRetained, d.RetentionProtectedTombstone, d.RetentionProtectedBuildPin, d.RetentionProtectedGuard} {
		if !containsRetentionReason(candidate.Protected, reason) {
			t.Errorf("missing protection reason %q: %v", reason, candidate.Protected)
		}
	}

	if _, err = c.pool.Exec(ctx, `SELECT pulse_durable_distributed.api('list',$1::jsonb)`, `{"namespace":"`+ns+`","limit":1,"status":""}`); err == nil {
		t.Fatal("retain-only role unexpectedly called query API")
	}
	if _, err = c.pool.Exec(ctx, `SELECT pulse_durable_distributed.api('claim',$1::jsonb)`, `{"namespace":"`+ns+`","limit":1}`); err == nil {
		t.Fatal("retain-only role unexpectedly called worker API")
	}
}

func containsRetentionReason(reasons []d.RetentionProtectedReason, want d.RetentionProtectedReason) bool {
	for _, reason := range reasons {
		if reason == want {
			return true
		}
	}
	return false
}
