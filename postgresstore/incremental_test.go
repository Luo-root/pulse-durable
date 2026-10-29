package postgresstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strconv"
	"strings"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
)

func initialRecord(id string) durable.RunRecord {
	d := strings.Repeat("0", 64)
	return durable.RunRecord{Format: durable.RecordFormat, RunID: id, Revision: 1, Version: "v1", Definition: d, Inputs: d, RootScope: "scope", Scopes: map[string]durable.ScopeRecord{"scope": {Graph: "g", Inputs: d, Nodes: map[string]durable.NodeRecord{}}}, Attempts: map[string]durable.AttemptRecord{}}
}
func wireBatch(t testing.TB, r durable.RunRecord, ops []any) durable.ChangeBatch {
	t.Helper()
	wire, err := json.Marshal(map[string]any{"version": 1, "header": map[string]string{"batch_format": "1", "logical_format": "2", "run_id": r.RunID, "business_version": r.Version, "definition_digest": r.Definition, "inputs_digest": r.Inputs, "root_scope": r.RootScope, "expected_revision": strconv.FormatUint(r.Revision, 10), "revision": strconv.FormatUint(r.Revision+1, 10)}, "ops": ops})
	if err != nil {
		t.Fatal(err)
	}
	b, err := durable.UnmarshalChangeBatch(wire)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func nodeBatch(t testing.TB, r durable.RunRecord, blob []byte) durable.ChangeBatch {
	sum := sha256.Sum256(blob)
	n := durable.NodeRecord{NodeID: "node", Attempt: "attempt", Reason: "completed", Outputs: []durable.ValueRecord{{Name: "blob", TypeID: "bytes", Version: "v1", State: "ready", Data: blob, Digest: hex.EncodeToString(sum[:])}}}
	var expected any
	if old, ok := r.Scopes["scope"].Nodes["node"]; ok {
		expected = old
	}
	fullNode := func(v durable.NodeRecord) any {
		outputs := make([]any, len(v.Outputs))
		for i, o := range v.Outputs {
			var data any
			if o.Data != nil {
				nums := make([]int, len(o.Data))
				for j, v := range o.Data {
					nums[j] = int(v)
				}
				data = nums
			}
			outputs[i] = map[string]any{"name": o.Name, "type_id": o.TypeID, "version": o.Version, "state": o.State, "digest": o.Digest, "data": data, "stream": o.Stream}
		}
		return map[string]any{"node_id": v.NodeID, "attempt": v.Attempt, "reason": v.Reason, "outputs": outputs}
	}
	if old, ok := expected.(durable.NodeRecord); ok {
		expected = fullNode(old)
	}
	return wireBatch(t, r, []any{map[string]any{"kind": "node", "key": "", "scope": "scope", "name": "node", "expected": expected, "value": fullNode(n)}})
}
func attemptBatch(t testing.TB, r durable.RunRecord) durable.ChangeBatch {
	return wireBatch(t, r, []any{map[string]any{"kind": "attempt", "key": fmt.Sprint("a-", r.Revision), "expected": nil, "value": durable.AttemptRecord{Parent: r.RootScope, NodeID: "node", Index: 1, Limit: 1, State: durable.AttemptActive}}})
}
func TestTypedNodeBatch(t *testing.T) {
	r := initialRecord("node")
	b := nodeBatch(t, r, []byte{0, 255})
	if _, err := b.ApplyTo(&r); err != nil {
		t.Fatal(err)
	}
	if string(r.Scopes["scope"].Nodes["node"].Outputs[0].Data) != string([]byte{0, 255}) {
		t.Fatal("blob changed")
	}
}

func TestBudgetAndInvalidatedSession(t *testing.T) {
	x := &session{runID: "budget"}
	r := initialRecord("budget")
	x.cache = &r
	x.batchCount = MaxChainBatches
	if _, err := x.Apply(context.Background(), attemptBatch(t, r)); !errors.Is(err, ErrBudget) {
		t.Fatalf("budget: %v", err)
	}
	if r.Revision != 1 {
		t.Fatal("budget changed projection")
	}
	x.invalidate()
	if _, err := x.Load(context.Background()); !errors.Is(err, errUncertain) {
		t.Fatal(err)
	}
	if err := x.Initialize(context.Background(), r); !errors.Is(err, errUncertain) {
		t.Fatal(err)
	}
	if _, err := x.Apply(context.Background(), attemptBatch(t, r)); !errors.Is(err, errUncertain) {
		t.Fatal(err)
	}
}

func TestSnapshotExactEmpty(t *testing.T) {
	for _, empty := range []bool{false, true} {
		r := initialRecord("empty")
		if empty {
			r.Streams = map[string]durable.StreamRecord{}
			r.Messages = map[string]durable.MessageAttemptRecord{}
			r.Managed = map[string]durable.ManagedRecord{}
		}
		data, err := encodeSnapshot(r)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeSnapshot(data)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(r, got) {
			t.Fatal("snapshot normalized nil/empty")
		}
		copy := durable.CloneRecord(got)
		copy.Scopes["scope"] = durable.ScopeRecord{}
		if reflect.DeepEqual(copy, got) {
			t.Fatal("clone aliases snapshot")
		}
	}
}
func TestPGIncrementalIntegrity(t *testing.T) {
	dsn := os.Getenv("PULSE_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("opt-in isolated database required; not database acceptance")
	}
	ctx := context.Background()
	resetPGSchema(t, dsn)
	defer resetPGSchema(t, dsn)
	for _, mode := range []string{"head-chain", "gap", "digest", "noncanonical", "precondition", "insert-failure", "commit-failure"} {
		t.Run(mode, func(t *testing.T) {
			s, err := New(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			a, err := s.Acquire(ctx, mode)
			if err != nil {
				t.Fatal(err)
			}
			r := initialRecord(mode)
			if err = a.Initialize(ctx, r); err != nil {
				t.Fatal(err)
			}
			b := attemptBatch(t, r)
			if mode == "insert-failure" || mode == "commit-failure" {
				// A trigger faults the immutable batch insert after private projection mutation.
				_, err = s.pool.Exec(ctx, `CREATE FUNCTION pulse_durable.fail_batch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected'; END $$; CREATE TRIGGER fail_batch BEFORE INSERT ON pulse_durable.batches FOR EACH ROW EXECUTE FUNCTION pulse_durable.fail_batch()`)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "commit-failure" {
					if _, err = s.pool.Exec(ctx, `DROP TRIGGER fail_batch ON pulse_durable.batches; CREATE CONSTRAINT TRIGGER fail_batch AFTER INSERT ON pulse_durable.batches DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION pulse_durable.fail_batch()`); err != nil {
						t.Fatal(err)
					}
				}
				if _, err = a.Apply(ctx, b); err == nil {
					t.Fatal("fault accepted")
				}
				if _, err = a.Load(ctx); !errors.Is(err, errUncertain) {
					t.Fatalf("cache continued after fault: %v", err)
				}
				if _, err = s.pool.Exec(ctx, `DROP TRIGGER fail_batch ON pulse_durable.batches; DROP FUNCTION pulse_durable.fail_batch()`); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err = a.Apply(ctx, b); err != nil {
					t.Fatal(err)
				}
				switch mode {
				case "head-chain":
					if _, err = s.pool.Exec(ctx, `UPDATE pulse_durable.heads SET chain=decode(repeat('00',32),'hex') WHERE run_id=$1`, mode); err != nil {
						t.Fatal(err)
					}
					current := durable.CloneRecord(r)
					if _, err = b.ApplyTo(&current); err != nil {
						t.Fatal(err)
					}
					if _, applyErr := a.Apply(ctx, attemptBatch(t, current)); !errors.Is(applyErr, durable.ErrConflict) {
						t.Fatalf("head chain CAS: %v", applyErr)
					}
				case "gap":
					_, err = s.pool.Exec(ctx, `DELETE FROM pulse_durable.batches WHERE run_id=$1`, mode)
				case "digest":
					_, err = s.pool.Exec(ctx, `UPDATE pulse_durable.batches SET digest=decode(repeat('00',32),'hex') WHERE run_id=$1`, mode)
				case "noncanonical", "precondition":
					wire, e := durable.MarshalChangeBatch(b)
					if e != nil {
						t.Fatal(e)
					}
					if mode == "noncanonical" {
						wire = append([]byte(" "), wire...)
					} else {
						var envelope map[string]any
						if e = json.Unmarshal(wire, &envelope); e != nil {
							t.Fatal(e)
						}
						op := envelope["ops"].([]any)[0].(map[string]any)
						op["expected"] = op["value"]
						wire, e = json.Marshal(envelope)
						if e != nil {
							t.Fatal(e)
						}
						bad, e := durable.UnmarshalChangeBatch(wire)
						if e != nil {
							t.Fatal(e)
						}
						wire, e = durable.MarshalChangeBatch(bad)
						if e != nil {
							t.Fatal(e)
						}
					}
					sum := sha256.Sum256(wire)
					base, e := encodeSnapshot(r)
					if e != nil {
						t.Fatal(e)
					}
					seed := durable.SeedChain(sha256.Sum256(base))
					next, e := durable.NextChain(seed, sum, 2)
					if e != nil {
						t.Fatal(e)
					}
					_, err = s.pool.Exec(ctx, `UPDATE pulse_durable.batches SET wire=$2,digest=$3,chain=$4 WHERE run_id=$1`, mode, wire, sum[:], next[:])
					if err == nil {
						_, err = s.pool.Exec(ctx, `UPDATE pulse_durable.heads SET chain=$2 WHERE run_id=$1`, mode, next[:])
					}
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if err = a.Close(); err != nil {
				t.Fatal(err)
			}
			a, err = s.Acquire(ctx, mode)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			got, err := a.Load(ctx)
			if mode == "insert-failure" || mode == "commit-failure" {
				if err != nil || !reflect.DeepEqual(got, r) {
					t.Fatalf("partial failed batch: rev=%d err=%v", got.Revision, err)
				}
			} else if err == nil {
				t.Fatal("corruption accepted")
			}
		})
	}
}
func TestPGLegacyUntouched(t *testing.T) {
	dsn := os.Getenv("PULSE_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("opt-in isolated database required")
	}
	ctx := context.Background()
	resetPGSchema(t, dsn)
	defer resetPGSchema(t, dsn)
	s, err := New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.pool.Exec(ctx, `DROP SCHEMA pulse_durable CASCADE; CREATE SCHEMA pulse_durable; CREATE TABLE pulse_durable.schema_meta(version integer NOT NULL); INSERT INTO pulse_durable.schema_meta VALUES(1); CREATE TABLE pulse_durable.runs(run_id text PRIMARY KEY,revision text NOT NULL,record bytea NOT NULL); INSERT INTO pulse_durable.runs VALUES('old','3',decode('00ff','hex'))`)
	if err != nil {
		t.Fatal(err)
	}
	if other, e := New(ctx, dsn); e == nil {
		other.Close()
		t.Fatal("legacy schema accepted")
	}
	var blob []byte
	var version, count int
	if err = s.pool.QueryRow(ctx, `SELECT record FROM pulse_durable.runs WHERE run_id='old'`).Scan(&blob); err != nil {
		t.Fatal(err)
	}
	if err = s.pool.QueryRow(ctx, `SELECT version FROM pulse_durable.schema_meta`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err = s.pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables WHERE table_schema='pulse_durable'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if string(blob) != string([]byte{0, 255}) || version != 1 || count != 2 {
		t.Fatal("legacy data/layout changed")
	}
	s.Close()
}

// Validation deliberately accepts column order permutations. Every production
// INSERT must therefore bind names rather than depend on ordinal_position.
func TestPGReorderedColumns(t *testing.T) {
	dsn := os.Getenv("PULSE_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("opt-in isolated database required")
	}
	ctx := context.Background()
	resetPGSchema(t, dsn)
	defer resetPGSchema(t, dsn)
	seed, err := New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	_, err = seed.pool.Exec(ctx, `DROP TABLE pulse_durable.snapshots,pulse_durable.heads,pulse_durable.batches;
 CREATE TABLE pulse_durable.snapshots(record bytea NOT NULL,revision text NOT NULL,run_id text NOT NULL,chain bytea NOT NULL,digest bytea NOT NULL,PRIMARY KEY(run_id,revision));
 CREATE TABLE pulse_durable.heads(generation text NOT NULL,revision text NOT NULL,base_revision text NOT NULL,chain bytea NOT NULL,run_id text PRIMARY KEY NOT NULL);
 CREATE TABLE pulse_durable.batches(wire bytea NOT NULL,parent_revision text NOT NULL,chain bytea NOT NULL,run_id text NOT NULL,digest bytea NOT NULL,revision text NOT NULL,parent_chain bytea NOT NULL,PRIMARY KEY(run_id,revision));`)
	closeErr := seed.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	s, err := New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a, err := s.Acquire(ctx, "reordered")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	initial := initialRecord("reordered")
	if err = a.Initialize(ctx, initial); err != nil {
		t.Fatal(err)
	}
	batch := attemptBatch(t, initial)
	ack, err := a.Apply(ctx, batch)
	if err != nil || ack.Header != batch.Header() {
		t.Fatalf("Apply ack=%+v err=%v", ack, err)
	}
	expected := durable.CloneRecord(initial)
	if _, err = batch.ApplyTo(&expected); err != nil {
		t.Fatal(err)
	}
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	a, err = reopened.Acquire(ctx, initial.RunID)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	got, err := a.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatal("reordered layout changed reopened projection")
	}
	if got.Revision != 2 || got.Attempts["a-1"].State != durable.AttemptActive {
		t.Fatal("committed attempt missing")
	}
	if err = a.Close(); err != nil {
		t.Fatal(err)
	}
	state, err := reopened.CompactStorage(ctx, durable.CompactStorageRequest{RunID: initial.RunID, ThroughRevision: 2, ExpectedGeneration: 1})
	if err != nil || state.Generation != 2 || state.BaseRevision != 2 {
		t.Fatalf("reordered synthesis: %+v %v", state, err)
	}
	image, err := reopened.LoadRevision(ctx, initial.RunID, 2)
	if err != nil || !reflect.DeepEqual(image.Record, expected) || image.Chain != state.HeadChain {
		t.Fatalf("reordered synthesized load: %v", err)
	}
	old, err := reopened.LoadRevision(ctx, initial.RunID, 1)
	if err != nil || !reflect.DeepEqual(old.Record, initial) {
		t.Fatalf("reordered historical load: %v", err)
	}
}

func TestPGExitChild(t *testing.T) {
	if os.Getenv("PULSE_PG_EXIT_CHILD") != "1" {
		t.Skip("parent process only")
	}
	ctx := context.Background()
	s, err := New(ctx, postgresDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	a, err := s.Acquire(ctx, "exit")
	if err != nil {
		t.Fatal(err)
	}
	r := initialRecord("exit")
	if err = a.Initialize(ctx, r); err != nil {
		t.Fatal(err)
	}
	if _, err = a.Apply(ctx, attemptBatch(t, r)); err != nil {
		t.Fatal(err)
	}
	os.Exit(0) // Deliberately do not Close: PostgreSQL must release the session lock.
}
func TestPGExitRecovery(t *testing.T) {
	dsn := os.Getenv("PULSE_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("opt-in isolated database required")
	}
	resetPGSchema(t, dsn)
	defer resetPGSchema(t, dsn)
	cmd := exec.Command(os.Args[0], "-test.run=^TestPGExitChild$")
	cmd.Env = append(os.Environ(), "PULSE_PG_EXIT_CHILD=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("exit child: %v (%s)", err, out)
	}
	s, err := New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	a, err := s.Acquire(context.Background(), "exit")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	r, err := a.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if r.Revision != 2 || r.Attempts["a-1"].State != durable.AttemptActive {
		t.Fatal("committed batch lost after process exit")
	}
}
