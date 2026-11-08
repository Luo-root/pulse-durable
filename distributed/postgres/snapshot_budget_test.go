package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/Luo-root/pulse-durable/recordio"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestSnapshotBudgetCapabilityIsCheckedBeforeConnection(t *testing.T) {
	_, supported := any(durable.ChangeBatch{}).(snapshotSizer)
	c, err := New(context.Background(), "")
	if c != nil {
		c.Close()
		t.Fatal("empty DSN accepted")
	}
	if !supported && !errors.Is(err, ErrSnapshotBudgetUnsupported) {
		t.Fatalf("missing capability did not fail before connection: %v", err)
	}
	if supported && errors.Is(err, ErrSnapshotBudgetUnsupported) {
		t.Fatal("compatible root rejected")
	}
}

func TestSnapshotBudgetScopeShapeMatchesCurrentWire(t *testing.T) {
	// 预算端与 wire2/snapshot2 使用同一份精确形状：删去兼容标记后，
	// 多出来的 bindings 和其他未知字段仍必须被对象字段校验拒绝。
	const prefix = "  WHEN 'scope' THEN '"
	_, rest, ok := strings.Cut(snapshotBudgetSQL, prefix)
	if !ok {
		t.Fatal("scope budget shape missing")
	}
	shape, _, ok := strings.Cut(rest, "'::jsonb")
	if !ok {
		t.Fatal("scope budget shape is not a fixed JSON object")
	}
	var fields map[string]string
	if err := json.Unmarshal([]byte(shape), &fields); err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"graph": "text", "path": "text", "inputs": "text", "nodes": "nodemap?"}
	if len(fields) != len(want) {
		t.Fatalf("unexpected scope budget fields: %v", fields)
	}
	for name, spec := range want {
		if fields[name] != spec {
			t.Fatalf("scope budget field %q=%q, want %q", name, fields[name], spec)
		}
	}
	for _, guard := range []string{
		"(SELECT count(*) FROM jsonb_object_keys(v))<>(SELECT count(*) FROM jsonb_object_keys(fields))",
		"snapshot_value_bytes(v->source,spec,wire)",
	} {
		if !strings.Contains(snapshotBudgetSQL, guard) {
			t.Fatalf("scope budget no longer rejects extra or missing fields: %s", guard)
		}
	}
}

func TestSnapshotBudgetAckFailsClosed(t *testing.T) {
	chain := [32]byte{1, 2, 3}
	ack := snapshotBudgetAck{Version: snapshotBudgetVersion, Bytes: "123", Revision: "10", Chain: chainText(chain)}
	valid, err := json.Marshal(ack)
	if err != nil {
		t.Fatal(err)
	}
	if err = validateSnapshotBudgetAck(valid, 123, 10, chain); err != nil {
		t.Fatal(err)
	}
	for _, wire := range []string{
		`{}`, `null`, string(valid) + `{}`,
		strings.Replace(string(valid), `"snapshot_budget_version":2`, `"snapshot_budget_version":null`, 1),
		strings.Replace(string(valid), `"snapshot_budget_version":2`, `"snapshot_budget_version":1`, 1),
		strings.Replace(string(valid), `"snapshot_bytes":"123"`, `"snapshot_bytes":"124"`, 1),
		strings.Replace(string(valid), `"snapshot_bytes":"123"`, `"snapshot_bytes":"0123"`, 1),
		strings.Replace(string(valid), `"revision":"10"`, `"revision":"9"`, 1),
		strings.Replace(string(valid), `"revision":"10"`, `"revision":"10","revision":"10"`, 1),
		strings.Replace(string(valid), `"revision":"10"`, `"Revision":"10"`, 1),
		strings.Replace(string(valid), `"revision":"10"`, `"revision":"10","future":true`, 1),
	} {
		if err = validateSnapshotBudgetAck([]byte(wire), 123, 10, chain); !errors.Is(err, d.ErrUnknown) {
			t.Fatalf("bad acknowledgement accepted: %s %v", wire, err)
		}
	}
}

func TestSnapshotLimitIsTypedAndSanitized(t *testing.T) {
	err := scrub(&pgconn.PgError{Code: "PD008", Message: "private row", Detail: "private payload"}, true)
	if !errors.Is(err, ErrSnapshotLimit) || !errors.Is(err, durable.ErrLimitExceeded) || errors.Is(err, d.ErrInvalid) || strings.Contains(err.Error(), "private") {
		t.Fatalf("snapshot limit classification: %v", err)
	}
}

func TestSnapshotCandidateUsesExactRootDelta(t *testing.T) {
	if err := requireSnapshotSizer(); err != nil {
		t.Skip("published root has no exact snapshot sizing capability")
	}
	r := record(taskFixture(t, "tenant", "candidate"), 9)
	before, err := recordio.EncodeSnapshot(r)
	if err != nil {
		t.Fatal(err)
	}
	batch := nextBatch(t, r)
	got, err := snapshotCandidate(batch, r, int64(len(before)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = batch.ApplyTo(&r); err != nil {
		t.Fatal(err)
	}
	after, err := recordio.EncodeSnapshot(r)
	if err != nil {
		t.Fatal(err)
	}
	if got != int64(len(after)) || len(after) != len(before)+1 {
		t.Fatalf("9 to 10 delta: got=%d before=%d after=%d", got, len(before), len(after))
	}
	for _, kind := range []string{"valid", "missing", "wrong", "false", "version"} {
		t.Run(kind, func(t *testing.T) {
			obj := map[string]any{"snapshot_budget_version": snapshotBudgetVersion, "snapshot_bytes": strconv.Itoa(len(after)), "snapshot_ledger_bytes": strconv.Itoa(len(after)), "snapshot_ledger_valid": true, "snapshot_ledger": snapshotLedgerFixture(t, after)}
			switch kind {
			case "missing":
				delete(obj, "snapshot_bytes")
			case "wrong":
				obj["snapshot_ledger_bytes"] = "1"
			case "false":
				obj["snapshot_ledger_valid"] = false
			case "version":
				obj["snapshot_budget_version"] = 1
			}
			b, _ := json.Marshal(obj)
			err := validateLoadedSnapshotBudget(b, after)
			if kind == "valid" && err != nil {
				t.Fatal(err)
			}
			if kind != "valid" && !errors.Is(err, ErrInvalidRecordBudget) {
				t.Fatalf("load budget accepted: %v", err)
			}
		})
	}
}
