package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/jackc/pgx/v5/pgxpool"
)

func uint64ptr(v uint64) *uint64 { return &v }

// Request bounds are actual database observations, never the test machine clock.
func receiptQuery(t *testing.T, admin *pgxpool.Pool, i d.Identity, m d.Mutation, op string, epoch *int64, revision *uint64) d.MutationQuery {
	t.Helper()
	q := d.MutationQuery{Identity: i, Mutation: m, Operation: op, ExpectedEpoch: epoch, ExpectedRevision: revision}
	if err := admin.QueryRow(context.Background(), `SELECT created_at,clock_timestamp() FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2`, i.Namespace, i.RunID).Scan(&q.RequestedWindow.From, &q.RequestedWindow.Through); err != nil {
		t.Fatal(scrub(err, false))
	}
	return q
}

func TestReceiptLayoutAndDecoder(t *testing.T) {
	for _, fragment := range []string{"CHECK(version=6)", "mutation_guards", "mutation_receipts", "receipt_coverage_floor", "receipt_observed_through", "receipt missing inside coverage"} {
		if !strings.Contains(schemaSQL, fragment) {
			t.Fatal("missing", fragment)
		}
	}
	if strings.Contains(schemaSQL, "FROM events WHERE") || strings.Contains(schemaSQL, "CREATE UNIQUE INDEX mutation_guard ON") {
		t.Fatal("receipt identity still depends on events")
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	zero := int64(0)
	q := d.MutationQuery{Identity: d.Identity{Namespace: "n", RunID: "r"}, Mutation: mut("m"), Operation: "cancel", ExpectedEpoch: &zero, ExpectedRevision: uint64ptr(0), RequestedWindow: d.MutationWindow{From: now, Through: now.Add(time.Hour)}}
	wire := map[string]any{"state": "found", "coverage_from": now, "coverage_through": now.Add(time.Hour), "receipt": map[string]any{"operation": "cancel", "epoch": 0, "revision": "0", "mutation_id": "m", "mutation_digest": q.Mutation.Digest, "occurred_at": now}}
	b, _ := json.Marshal(wire)
	if r, e := decodeMutationResult(b, q); e != nil || r.Receipt.Epoch != 0 {
		t.Fatal(r, e)
	}
	one := int64(1)
	q.ExpectedEpoch = &one
	if _, e := decodeMutationResult(b, q); !errors.Is(e, d.ErrInvalid) {
		t.Fatal("accepted wrong known epoch", e)
	}
	q.ExpectedEpoch = nil
	q.ExpectedRevision = nil
	if _, e := decodeMutationResult(b, q); e != nil {
		t.Fatal("unknown epoch/revision rejected", e)
	}
}

func TestReceiptRejectsMalformedWire(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	zero := int64(0)
	q := d.MutationQuery{Identity: d.Identity{Namespace: "n", RunID: "r"}, Mutation: mut("m"), Operation: "cancel", RequestedWindow: d.MutationWindow{From: now, Through: now.Add(time.Hour)}}
	wire := `{"state":"found","coverage_from":"2026-01-01T00:00:00Z","coverage_through":"2026-01-01T01:00:00Z","receipt":{"operation":"cancel","epoch":0,"revision":"0","mutation_id":"m","mutation_digest":"` + q.Mutation.Digest + `","occurred_at":"2026-01-01T00:00:00Z"}}`
	bad := map[string]string{
		"missing epoch":        strings.Replace(wire, `"epoch":0,`, "", 1),
		"null epoch":           strings.Replace(wire, `"epoch":0`, `"epoch":null`, 1),
		"fraction epoch":       strings.Replace(wire, `"epoch":0`, `"epoch":0.0`, 1),
		"string epoch":         strings.Replace(wire, `"epoch":0`, `"epoch":"0"`, 1),
		"duplicate epoch":      strings.Replace(wire, `"epoch":0`, `"epoch":0,"epoch":0`, 1),
		"case alias":           strings.Replace(wire, `"epoch":0`, `"Epoch":0`, 1),
		"extra receipt field":  strings.Replace(wire, `"epoch":0`, `"epoch":0,"extra":0`, 1),
		"extra top field":      strings.Replace(wire, `"state":"found"`, `"state":"found","extra":0`, 1),
		"duplicate top field":  strings.Replace(wire, `"state":"found"`, `"state":"found","state":"found"`, 1),
		"missing receipt":      strings.Replace(wire, `,"receipt":`+strings.Split(wire, `,"receipt":`)[1], "}", 1),
		"trailing":             wire + `{}`,
		"surrogate":            strings.Replace(wire, `"m"`, `"\ud800"`, 1),
		"low surrogate":        strings.Replace(wire, `"m"`, `"\udc00"`, 1),
		"invalid UTF8":         strings.Replace(wire, `"m"`, "\""+string([]byte{0xff})+"\"", 1),
		"absent with receipt":  strings.Replace(wire, `"found"`, `"absent_within_window"`, 1),
		"expired with receipt": strings.Replace(wire, `"found"`, `"expired"`, 1),
	}
	for _, known := range []bool{false, true} {
		if known {
			q.ExpectedEpoch = &zero
			q.ExpectedRevision = uint64ptr(0)
		}
		if _, err := decodeMutationResult([]byte(wire), q); err != nil {
			t.Fatal("valid zero receipt rejected", err)
		}
		for name, input := range bad {
			t.Run(name+map[bool]string{false: "/unknown", true: "/exact-zero"}[known], func(t *testing.T) {
				if _, err := decodeMutationResult([]byte(input), q); !errors.Is(err, d.ErrInvalid) {
					t.Fatal("malformed receipt accepted", err)
				}
			})
		}
	}
	for _, key := range []string{"state", "coverage_from", "coverage_through"} {
		var obj map[string]json.RawMessage
		_ = json.Unmarshal([]byte(wire), &obj)
		delete(obj, key)
		b, _ := json.Marshal(obj)
		if _, err := decodeMutationResult(b, q); !errors.Is(err, d.ErrInvalid) {
			t.Fatal("missing top field accepted", key)
		}
	}
	for _, key := range []string{"operation", "epoch", "revision", "mutation_id", "mutation_digest", "occurred_at"} {
		for _, null := range []bool{false, true} {
			var obj map[string]json.RawMessage
			var receipt map[string]json.RawMessage
			_ = json.Unmarshal([]byte(wire), &obj)
			_ = json.Unmarshal(obj["receipt"], &receipt)
			if null {
				receipt[key] = json.RawMessage("null")
			} else {
				delete(receipt, key)
			}
			obj["receipt"], _ = json.Marshal(receipt)
			b, _ := json.Marshal(obj)
			if _, err := decodeMutationResult(b, q); !errors.Is(err, d.ErrInvalid) {
				t.Fatal("missing/null receipt field accepted", key, null)
			}
		}
	}
	for _, state := range []d.MutationQueryState{d.AbsentWithinWindow, d.Expired} {
		query := q
		if state == d.Expired {
			query.RequestedWindow.From = now.Add(-time.Hour)
		}
		obj := map[string]any{"state": state, "coverage_from": now, "coverage_through": now.Add(time.Hour), "receipt": nil}
		b, _ := json.Marshal(obj)
		if _, err := decodeMutationResult(b, query); err != nil {
			t.Fatal("valid null receipt rejected", state, err)
		}
		delete(obj, "receipt")
		b, _ = json.Marshal(obj)
		if _, err := decodeMutationResult(b, query); !errors.Is(err, d.ErrInvalid) {
			t.Fatal("missing null receipt accepted", state)
		}
	}
	if !receiptUnicode([]byte(`"\ud83d\ude00"`)) {
		t.Fatal("valid surrogate pair rejected")
	}
}

func TestPostgresIndependentReceiptsAndCoverage(t *testing.T) {
	c, admin, ns := integration(t)
	ctx := context.Background()
	task := taskFixture(t, ns, "receipt-independent")
	if _, e := c.Submit(ctx, task); e != nil {
		t.Fatal(e)
	}
	claim := claimOne(t, c, task, "receipt", time.Minute)
	m := mut("initialize-receipt")
	if e := c.InitializeOwned(ctx, d.InitializeRequest{Token: claim.Token, Record: record(task, 1), Mutation: m}); e != nil {
		t.Fatal(e)
	}
	q := receiptQuery(t, admin, task.Identity(), m, "initialize", &claim.Token.Epoch, uint64ptr(1))
	if _, e := admin.Exec(ctx, `DELETE FROM pulse_durable_distributed.events WHERE namespace=$1`, ns); e != nil {
		t.Fatal(e)
	}
	if r, e := c.FindMutation(ctx, q); e != nil || r.State != d.Found {
		t.Fatal(r, e)
	}
	if e := c.Release(ctx, claim.Token, m); !errors.Is(e, d.ErrInvalid) {
		t.Fatal("reused mid after events deletion", e)
	}
	for _, bad := range []d.MutationQuery{func() d.MutationQuery { v := q; v.Mutation.Digest = digest([]byte("other")); return v }(), func() d.MutationQuery { v := q; v.Operation = "apply"; return v }()} {
		if _, e := c.FindMutation(ctx, bad); !errors.Is(e, d.ErrRevisionConflict) {
			t.Fatal("identity conflict accepted", e)
		}
	}
	other := q
	other.Identity.Namespace = ns + "-denied"
	if _, e := c.FindMutation(ctx, other); e == nil {
		t.Fatal("cross namespace query accepted")
	}
	missing := q
	missing.Mutation = mut("absent")
	if r, e := c.FindMutation(ctx, missing); e != nil || r.State != d.AbsentWithinWindow {
		t.Fatal(r, e)
	}
	missing.Identity.RunID = "nonexistent"
	if _, e := c.FindMutation(ctx, missing); !errors.Is(e, d.ErrNotFound) {
		t.Fatal(e)
	}
	future := q
	future.RequestedWindow.Through = q.RequestedWindow.Through.Add(24 * time.Hour)
	if _, e := c.FindMutation(ctx, future); !errors.Is(e, d.ErrInvalid) {
		t.Fatal("future query accepted", e)
	}
	if _, e := admin.Exec(ctx, `UPDATE pulse_durable_distributed.runs SET receipt_coverage_floor=$3 WHERE namespace=$1 AND run_id=$2`, ns, task.Identity().RunID, q.RequestedWindow.Through); e != nil {
		t.Fatal(e)
	}
	if r, e := c.FindMutation(ctx, q); e != nil || r.State != d.Found {
		t.Fatal("retained receipt older than coverage", r, e)
	}
	if _, e := admin.Exec(ctx, `UPDATE pulse_durable_distributed.runs SET receipt_coverage_floor=created_at WHERE namespace=$1 AND run_id=$2`, ns, task.Identity().RunID); e != nil {
		t.Fatal(e)
	}
	if _, e := admin.Exec(ctx, `DELETE FROM pulse_durable_distributed.mutation_receipts WHERE namespace=$1 AND run_id=$2`, ns, task.Identity().RunID); e != nil {
		t.Fatal(e)
	}
	if _, e := c.FindMutation(ctx, q); !errors.Is(e, d.ErrInvalid) {
		t.Fatal("missing receipt not corruption", e)
	}
	// Emulate only the future GC's atomic floor contract; no collector is implemented.
	if _, e := admin.Exec(ctx, `UPDATE pulse_durable_distributed.runs SET receipt_coverage_floor=$3 WHERE namespace=$1 AND run_id=$2`, ns, task.Identity().RunID, q.RequestedWindow.Through); e != nil {
		t.Fatal(e)
	}
	if r, e := c.FindMutation(ctx, q); e != nil || r.State != d.Expired {
		t.Fatal(r, e)
	}
	missing = q
	missing.Mutation = mut("absent-boundary")
	missing.RequestedWindow.From = q.RequestedWindow.Through
	if r, e := c.FindMutation(ctx, missing); e != nil || r.State != d.AbsentWithinWindow {
		t.Fatal(r, e)
	}
	cancel := mut("cancel-unknown")
	if _, e := c.Cancel(ctx, task.Identity(), cancel); e != nil {
		t.Fatal(e)
	}
	cq := receiptQuery(t, admin, task.Identity(), cancel, "cancel", nil, nil)
	if r, e := c.FindMutation(ctx, cq); e != nil || r.State != d.Found || r.Receipt.Epoch != claim.Token.Epoch || r.Receipt.Revision != 1 {
		t.Fatal(r, e)
	}
	if _, e := admin.Exec(ctx, `UPDATE pulse_durable_distributed.runs SET receipt_observed_through=clock_timestamp()+interval '1 hour' WHERE namespace=$1 AND run_id=$2`, ns, task.Identity().RunID); e != nil {
		t.Fatal(e)
	}
	if _, e := c.FindMutation(ctx, cq); !errors.Is(e, d.ErrClockRegression) {
		t.Fatal("read regression accepted", e)
	}
}
