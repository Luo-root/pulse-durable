package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/Luo-root/pulse-durable/recordio"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 夹具只构造公开 wire；真正的校验、应用和计数都调用生产入口。
func snapshotTestPayload(v any) any {
	var walk func(reflect.Value) any
	walk = func(v reflect.Value) any {
		if !v.IsValid() {
			return nil
		}
		switch v.Kind() {
		case reflect.Interface, reflect.Pointer:
			if v.IsNil() {
				return nil
			}
			return walk(v.Elem())
		case reflect.Struct:
			out := map[string]any{}
			for i := 0; i < v.NumField(); i++ {
				name := strings.Split(v.Type().Field(i).Tag.Get("json"), ",")[0]
				if name == "" {
					name = v.Type().Field(i).Name
				}
				out[name] = walk(v.Field(i))
			}
			return out
		case reflect.Slice:
			if v.IsNil() {
				return nil
			}
			out := make([]any, v.Len())
			for i := range out {
				out[i] = walk(v.Index(i))
			}
			return out
		case reflect.Map:
			if v.IsNil() {
				return nil
			}
			out := map[string]any{}
			it := v.MapRange()
			for it.Next() {
				out[it.Key().String()] = walk(it.Value())
			}
			return out
		case reflect.Uint64:
			return strconv.FormatUint(v.Uint(), 10)
		default:
			return v.Interface()
		}
	}
	return walk(reflect.ValueOf(v))
}

func snapshotTestOp(kind, key, scope, name, sequence string, state bool, before, after any) map[string]any {
	op := map[string]any{"kind": kind, "key": key, "expected": snapshotTestPayload(before), "value": snapshotTestPayload(after)}
	if scope != "" {
		op["scope"] = scope
	}
	if name != "" {
		op["name"] = name
	}
	if sequence != "" {
		op["sequence"] = sequence
	}
	if state {
		op["state"] = true
	}
	return op
}

func snapshotTestBatch(t *testing.T, r durable.RunRecord, ops ...map[string]any) durable.ChangeBatch {
	t.Helper()
	if len(ops) == 0 {
		return nextBatch(t, r)
	}
	sortKey := func(op map[string]any) string {
		return fmt.Sprintf("%s\x00%s\x00%s\x00%s\x00%s\x00%v", op["kind"], op["key"], op["scope"], op["name"], op["sequence"], op["state"])
	}
	// 同类复合键按公开格式排列；Marshal 后再走一次规范化。
	for i := 1; i < len(ops); i++ {
		for j := i; j > 0 && sortKey(ops[j]) < sortKey(ops[j-1]); j-- {
			ops[j], ops[j-1] = ops[j-1], ops[j]
		}
	}
	wire, err := json.Marshal(map[string]any{"version": 1, "header": map[string]string{"batch_format": "1", "logical_format": "2", "run_id": r.RunID, "business_version": r.Version, "definition_digest": r.Definition, "inputs_digest": r.Inputs, "root_scope": r.RootScope, "expected_revision": strconv.FormatUint(r.Revision, 10), "revision": strconv.FormatUint(r.Revision+1, 10)}, "ops": ops})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := durable.UnmarshalChangeBatch(wire)
	if err != nil {
		t.Fatalf("fixture wire: %v", err)
	}
	return batch
}

type snapshotTestStreamHeader struct {
	Stream                     durable.StreamDescriptorRecord
	LastSequence, AckedThrough uint64
	Cursor                     *durable.ValueRecord
}
type snapshotTestManagedHeader struct{ Scope, Parent, NodeID string }

type snapshotTestRun struct {
	c     *Coordinator
	admin *pgxpool.Pool
	token d.Token
	r     durable.RunRecord
	chain [32]byte
}

func newSnapshotTestRun(t *testing.T, c *Coordinator, admin *pgxpool.Pool, ns, id string) *snapshotTestRun {
	t.Helper()
	ctx := context.Background()
	task := taskFixture(t, ns, id)
	if _, err := c.Submit(ctx, task); err != nil {
		t.Fatal(err)
	}
	claim := claimOne(t, c, task, "snapshot-"+id, 10*time.Minute)
	r := record(task, 1)
	if err := c.InitializeOwned(ctx, d.InitializeRequest{Token: claim.Token, Record: r, Mutation: mut("snapshot-init-" + id)}); err != nil {
		t.Fatal(err)
	}
	wire, err := recordio.EncodeSnapshot(r)
	if err != nil {
		t.Fatal(err)
	}
	h := &snapshotTestRun{c: c, admin: admin, token: claim.Token, r: r, chain: durable.SeedChain(sha256.Sum256(wire))}
	h.check(t)
	return h
}
func (h *snapshotTestRun) check(t *testing.T) {
	t.Helper()
	encoded, err := recordio.EncodeSnapshot(h.r)
	if err != nil {
		t.Fatal(err)
	}
	var size, ledger string
	var valid bool
	var version int
	err = h.admin.QueryRow(context.Background(), `SELECT r.snapshot_bytes::text,r.snapshot_budget_version,s->>'snapshot_ledger_bytes',(s->>'snapshot_ledger_valid')::boolean FROM pulse_durable_distributed.runs r CROSS JOIN LATERAL pulse_durable_distributed.snapshot_budget_summary(r) s WHERE namespace=$1 AND run_id=$2`, h.token.Identity.Namespace, h.r.RunID).Scan(&size, &version, &ledger, &valid)
	if err != nil {
		t.Fatal(err)
	}
	if size != strconv.Itoa(len(encoded)) || ledger != size || !valid || version != 1 {
		t.Fatalf("revision %d SQL=%s ledger=%s valid=%v version=%d EncodeSnapshot=%d", h.r.Revision, size, ledger, valid, version, len(encoded))
	}
	h.c.projectionMu.Lock()
	p := h.c.projections[h.token]
	h.c.projectionMu.Unlock()
	if p != nil && p.loaded && p.snapshotBytes != int64(len(encoded)) {
		t.Fatalf("revision %d Go=%d EncodeSnapshot=%d", h.r.Revision, p.snapshotBytes, len(encoded))
	}
}
func (h *snapshotTestRun) apply(t *testing.T, batch durable.ChangeBatch) {
	t.Helper()
	ctx := context.Background()
	dg, err := d.DeltaMutationDigest(h.token, batch)
	if err != nil {
		t.Fatal(err)
	}
	before, err := recordio.EncodeSnapshot(h.r)
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := snapshotCandidate(batch, h.r, int64(len(before)))
	if err != nil {
		t.Fatal(err)
	}
	ack, err := h.c.ApplyOwned(ctx, h.token, d.Mutation{ID: fmt.Sprintf("snapshot-%d", batch.Header().Revision), Digest: dg}, batch)
	if err != nil {
		t.Fatal(err)
	}
	if ack.Header != batch.Header() {
		t.Fatal("ack header mismatch")
	}
	if _, err = batch.ApplyTo(&h.r); err != nil {
		t.Fatal(err)
	}
	after, err := recordio.EncodeSnapshot(h.r)
	if err != nil || candidate != int64(len(after)) {
		t.Fatalf("delta=%d snapshot=%d err=%v", candidate, len(after), err)
	}
	wire, err := durable.MarshalChangeBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	h.chain, err = durable.NextChain(h.chain, sha256.Sum256(wire), h.r.Revision)
	if err != nil {
		t.Fatal(err)
	}
	h.check(t)
}
func (h *snapshotTestRun) rawApply(t *testing.T, wire []byte) error {
	t.Helper()
	next, err := durable.NextChain(h.chain, sha256.Sum256(wire), h.r.Revision+1)
	if err != nil {
		t.Fatal(err)
	}
	a := owned(h.token)
	mutate(a, mut(fmt.Sprintf("raw-snapshot-%d", h.r.Revision+1)))
	a["expected"] = strconv.FormatUint(h.r.Revision, 10)
	a["wire"] = wire
	a["batch_digest"] = digest(wire)
	a["parent_chain"] = chainText(h.chain)
	a["next_chain"] = chainText(next)
	// 此字段不应参与数据库计数。
	a["record_bytes"] = 0
	_, err = h.c.call(context.Background(), "apply", a, true)
	return err
}
func (h *snapshotTestRun) publication(t *testing.T) string {
	t.Helper()
	var state string
	err := h.admin.QueryRow(context.Background(), `SELECT jsonb_build_object('run',to_jsonb(r),'objects',(SELECT jsonb_agg(to_jsonb(o) ORDER BY kind,key1,key2,key3) FROM pulse_durable_distributed.snapshot_objects o WHERE namespace=$1 AND run_id=$2),'containers',(SELECT jsonb_agg(to_jsonb(c) ORDER BY kind,key1) FROM pulse_durable_distributed.snapshot_containers c WHERE namespace=$1 AND run_id=$2),'batches',(SELECT count(*) FROM pulse_durable_distributed.batches WHERE namespace=$1 AND run_id=$2),'guards',(SELECT count(*) FROM pulse_durable_distributed.mutation_guards WHERE namespace=$1 AND run_id=$2),'receipts',(SELECT count(*) FROM pulse_durable_distributed.mutation_receipts WHERE namespace=$1 AND run_id=$2),'events',(SELECT count(*) FROM pulse_durable_distributed.events WHERE namespace=$1 AND run_id=$2))::text FROM pulse_durable_distributed.runs r WHERE namespace=$1 AND run_id=$2`, h.token.Identity.Namespace, h.r.RunID).Scan(&state)
	if err != nil {
		t.Fatal(err)
	}
	return state
}
func (h *snapshotTestRun) reload(t *testing.T) {
	t.Helper()
	h.c.DropOwnedProjection(h.token)
	got, err := h.c.LoadOwned(context.Background(), h.token)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, h.r) {
		t.Fatalf("reload differs at revision %d", h.r.Revision)
	}
	h.check(t)
}

func TestPostgresSnapshotObjectsContainersAndWire(t *testing.T) {
	c, admin, ns := integration(t)
	h := newSnapshotTestRun(t, c, admin, ns, "snapshot-objects")
	key := "key/<>&\"\\\n\x01\u2028\u2029"
	child := "child" + key
	v := durable.ValueRecord{Name: "value" + key, TypeID: "bytes", Version: "v1", State: "ready", Data: []byte{0, 1, 127, 128, 254, 255}, Digest: "digest" + key}
	id := durable.StreamIDRecord{Scope: child, NodeID: key, Key: key, Group: key, Generation: 9}
	desc := durable.StreamDescriptorRecord{ID: id, Graph: "graph" + key, Path: key, Attempt: key, TypeID: "bytes", Version: "v1", SourceID: key, Resumable: true, Capacity: 2, Terminal: "", Stopped: false}
	stream := snapshotTestStreamHeader{Stream: desc, LastSequence: 9, Cursor: &v}
	managed := snapshotTestManagedHeader{Scope: child, Parent: key, NodeID: key}
	attempt := durable.AttemptRecord{Parent: child, NodeID: key, Index: 1, Limit: 2, State: durable.AttemptActive}
	node := durable.NodeRecord{NodeID: key, Attempt: key, Reason: "completed", Outputs: []durable.ValueRecord{v}}
	pending := durable.StreamMessageRecord{Sequence: 9, Value: v}
	message := durable.MessageAttemptRecord{Scope: child, Parent: key, NodeID: key, Input: id, Sequence: 9, Index: 1, Limit: 2, State: durable.AttemptActive}
	valueEmpty := v
	valueEmpty.Name = "empty"
	valueEmpty.Data = []byte{}
	valueNil := v
	valueNil.Name = "nil"
	valueNil.Data = nil
	h.apply(t, snapshotTestBatch(t, h.r,
		snapshotTestOp("scope", child, "", "", "", false, nil, durable.ScopeRecord{Graph: "child", Path: key, Inputs: h.r.Inputs}),
		snapshotTestOp("node", "", child, key, "", false, nil, node), snapshotTestOp("attempt", key, "", "", "", false, nil, attempt),
		snapshotTestOp("stream", key, "", "", "", false, nil, stream), snapshotTestOp("pending", "", key, "", "9", false, nil, pending),
		snapshotTestOp("managed", key, "", "", "", false, nil, managed), snapshotTestOp("managed_entry", "", key, valueEmpty.Name, "", false, nil, valueEmpty), snapshotTestOp("managed_entry", "", key, valueNil.Name, "", true, nil, valueNil),
		snapshotTestOp("message", key, "", "", "", false, nil, message)))
	// 在 pending/managed_entry 尚未删除时核对完整 Load 账本。
	h.reload(t)
	// 替换缩小、同父多子项和两个 managed 列表独立计数。
	small := v
	small.Data = []byte{1}
	small.Digest = "x"
	small.Stream = &desc
	nextNode := node
	nextNode.Outputs = []durable.ValueRecord{small}
	nextPending := pending
	nextPending.Value = small
	other := small
	other.Name = "other"
	h.apply(t, snapshotTestBatch(t, h.r, snapshotTestOp("node", "", child, key, "", false, node, nextNode), snapshotTestOp("pending", "", key, "", "9", false, pending, nextPending), snapshotTestOp("managed", key, "", "", "", false, managed, managed), snapshotTestOp("managed_entry", "", key, valueEmpty.Name, "", false, valueEmpty, smallNamed(small, valueEmpty.Name)), snapshotTestOp("managed_entry", "", key, other.Name, "", true, nil, other)))
	h.reload(t)
	// pending 删除最后一项留 []；managed 最后一项删除变 null。
	h.apply(t, snapshotTestBatch(t, h.r, snapshotTestOp("pending", "", key, "", "9", false, nextPending, nil), snapshotTestOp("managed", key, "", "", "", false, managed, managed), snapshotTestOp("managed_entry", "", key, valueEmpty.Name, "", false, smallNamed(small, valueEmpty.Name), nil), snapshotTestOp("managed_entry", "", key, valueNil.Name, "", true, valueNil, nil), snapshotTestOp("managed_entry", "", key, other.Name, "", true, other, nil)))
	if h.r.Streams[key].Pending == nil || h.r.Managed[key].State != nil || h.r.Managed[key].Values != nil {
		t.Fatal("nil/empty fixture did not reach required state")
	}
	for h.r.Revision < 11 {
		h.apply(t, nextBatch(t, h.r))
	}
	h.reload(t)
	// Load 必须核对账本；失败不能自动修复。
	before := h.publication(t)
	if _, err := admin.Exec(context.Background(), `UPDATE pulse_durable_distributed.snapshot_objects SET bytes=bytes+1 WHERE namespace=$1 AND run_id=$2 AND kind='node'`, ns, h.r.RunID); err != nil {
		t.Fatal(err)
	}
	corrupt := h.publication(t)
	c.DropOwnedProjection(h.token)
	if _, err := c.LoadOwned(context.Background(), h.token); !errors.Is(err, ErrInvalidRecordBudget) || !errors.Is(err, d.ErrInvalidRecord) {
		t.Fatalf("corrupt ledger accepted: %v", err)
	}
	if h.publication(t) != corrupt || corrupt == before {
		t.Fatal("load repaired or changed corrupt ledger")
	}
}
func smallNamed(v durable.ValueRecord, name string) durable.ValueRecord { v.Name = name; return v }

func snapshotDataNode(name string, n int) durable.NodeRecord {
	return durable.NodeRecord{NodeID: name, Attempt: "a", Reason: "completed", Outputs: []durable.ValueRecord{{Name: "out", TypeID: "bytes", Version: "v1", State: "ready", Digest: "d", Data: make([]byte, n)}}}
}
func snapshotNodeBatch(t *testing.T, r durable.RunRecord, n durable.NodeRecord, before any) durable.ChangeBatch {
	return snapshotTestBatch(t, r, snapshotTestOp("node", "", r.RootScope, n.NodeID, "", false, before, n))
}

func TestPostgresSnapshotOriginalCapacitySequence(t *testing.T) {
	c, admin, ns := integration(t)
	h := newSnapshotTestRun(t, c, admin, ns, "snapshot-capacity")
	accepted := 0
	rejected := false
	for i := 0; i < 20; i++ {
		node := snapshotDataNode(fmt.Sprintf("n%02d", i), 768<<10)
		batch := snapshotNodeBatch(t, h.r, node, nil)
		before, _ := recordio.EncodeSnapshot(h.r)
		proposal := durable.CloneRecord(h.r)
		if _, err := batch.ApplyTo(&proposal); err != nil {
			t.Fatal(err)
		}
		encoded, err := recordio.EncodeSnapshot(proposal)
		if err != nil {
			t.Fatal(err)
		}
		if len(encoded) <= MaxRecordBytes {
			h.apply(t, batch)
			accepted++
			continue
		}
		state := h.publication(t)
		dg, err := d.DeltaMutationDigest(h.token, batch)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = c.ApplyOwned(context.Background(), h.token, d.Mutation{ID: "capacity-over", Digest: dg}, batch); !errors.Is(err, ErrSnapshotLimit) || !errors.Is(err, durable.ErrLimitExceeded) {
			t.Fatalf("Go over budget: %v", err)
		}
		if h.publication(t) != state {
			t.Fatal("Go preflight published state")
		}
		wire, err := durable.MarshalChangeBatch(batch)
		if err != nil {
			t.Fatal(err)
		}
		if err = h.rawApply(t, wire); !errors.Is(err, ErrSnapshotLimit) {
			t.Fatalf("SQL over budget: %v", err)
		}
		if h.publication(t) != state {
			t.Fatal("SQL budget refusal published batch/head/ledger/guard/receipt/event")
		}
		t.Logf("first rejected node=%d before=%d candidate=%d", i+1, len(before), len(encoded))
		rejected = true
		break
	}
	if !rejected || accepted != 15 {
		t.Fatalf("capacity sequence accepted=%d rejected=%v", accepted, rejected)
	}
	h.reload(t)
}

func TestPostgresSnapshotExactBoundaryAndOneByte(t *testing.T) {
	c, admin, ns := integration(t)
	h := newSnapshotTestRun(t, c, admin, ns, "snapshot-boundary")
	for i := 0; i < 15; i++ {
		h.apply(t, snapshotNodeBatch(t, h.r, snapshotDataNode(fmt.Sprintf("n%02d", i), 768<<10), nil))
	}
	node := snapshotDataNode("boundary", 0)
	proposal := durable.CloneRecord(h.r)
	batch := snapshotNodeBatch(t, h.r, node, nil)
	if _, err := batch.ApplyTo(&proposal); err != nil {
		t.Fatal(err)
	}
	encoded, err := recordio.EncodeSnapshot(proposal)
	if err != nil {
		t.Fatal(err)
	}
	gap := MaxRecordBytes - len(encoded)
	if gap < 0 {
		t.Fatal("fixture already over boundary")
	}
	node.Reason += strings.Repeat("x", gap%4)
	node.Outputs[0].Data = make([]byte, (gap/4)*3)
	h.apply(t, snapshotNodeBatch(t, h.r, node, nil))
	encoded, err = recordio.EncodeSnapshot(h.r)
	if err != nil || len(encoded) != MaxRecordBytes {
		t.Fatalf("exact boundary=%d err=%v", len(encoded), err)
	}
	larger := node
	larger.Reason += "x"
	batch = snapshotNodeBatch(t, h.r, larger, node)
	state := h.publication(t)
	dg, err := d.DeltaMutationDigest(h.token, batch)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = c.ApplyOwned(context.Background(), h.token, d.Mutation{ID: "plus-one", Digest: dg}, batch); !errors.Is(err, ErrSnapshotLimit) {
		t.Fatal("Go +1", err)
	}
	wire, err := durable.MarshalChangeBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	if err = h.rawApply(t, wire); !errors.Is(err, ErrSnapshotLimit) {
		t.Fatal("SQL +1", err)
	}
	if h.publication(t) != state {
		t.Fatal("+1 refusal changed publication")
	}
	// 缩小后可以继续提交，拒绝不污染本地投影。
	smaller := node
	smaller.Outputs = append([]durable.ValueRecord(nil), node.Outputs...)
	smaller.Outputs[0].Data = []byte{1}
	h.apply(t, snapshotNodeBatch(t, h.r, smaller, node))
	h.reload(t)
}

func TestPostgresSnapshotRejectsUnknownMalformedDuplicateAndOldClient(t *testing.T) {
	c, admin, ns := integration(t)
	ctx := context.Background()
	var role string
	if err := c.pool.QueryRow(ctx, "SELECT session_user").Scan(&role); err != nil {
		t.Fatal(err)
	}
	// 该用例直接调用 storage_inspect 验证请求格式；先显式授予
	// maintain，确保断言落在请求校验而不是入口 ACL。
	setTestPermission(t, admin, ctx, role, ns, "maintain", true)
	h := newSnapshotTestRun(t, c, admin, ns, "snapshot-reject")
	batch := snapshotNodeBatch(t, h.r, snapshotDataNode("node", 1), nil)
	wire, err := durable.MarshalChangeBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"unknown_kind", "unknown_field", "wrong_type", "duplicate_op", "duplicate_json", "old_client", "storage_missing_budget"} {
		t.Run(kind, func(t *testing.T) {
			var obj map[string]any
			dec := json.NewDecoder(strings.NewReader(string(wire)))
			dec.UseNumber()
			if err := dec.Decode(&obj); err != nil {
				t.Fatal(err)
			}
			op := obj["ops"].([]any)[0].(map[string]any)
			switch kind {
			case "unknown_kind":
				op["kind"] = "future"
			case "unknown_field":
				op["future"] = true
			case "wrong_type":
				op["value"].(map[string]any)["outputs"] = false
			case "duplicate_op":
				obj["ops"] = []any{op, op}
			}
			mutated, err := json.Marshal(obj)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "duplicate_json" {
				mutated = []byte(strings.Replace(string(mutated), `"version":1`, `"version":1,"version":1`, 1))
			}
			state := h.publication(t)
			if kind == "old_client" || kind == "storage_missing_budget" {
				// 直接调用现行 SQL 入口，验证缺少预算协议信封时拒绝且不发布。
				a, _ := json.Marshal(args(h.token.Identity))
				var response []byte
				if kind == "storage_missing_budget" {
					err = c.pool.QueryRow(context.Background(), "SELECT pulse_durable_distributed.storage_inspect($1::jsonb)", a).Scan(&response)
				} else {
					err = c.pool.QueryRow(context.Background(), "SELECT pulse_durable_distributed.api($1,$2::jsonb)", "get", a).Scan(&response)
				}
				err = scrub(err, false)
			} else {
				err = h.rawApply(t, mutated)
			}
			if !errors.Is(err, d.ErrInvalid) {
				t.Fatalf("malformed request accepted: %v", err)
			}
			if h.publication(t) != state {
				t.Fatal("rejection published state")
			}
		})
	}
	h.reload(t)
}

func TestPostgresSnapshotCompactionKeepsCurrentLedger(t *testing.T) {
	c, admin, ns := integration(t)
	ctx := context.Background()
	var role string
	if err := c.pool.QueryRow(ctx, "SELECT session_user").Scan(&role); err != nil {
		t.Fatal(err)
	}
	setTestPermission(t, admin, ctx, role, ns, "maintain", true)
	h := newSnapshotTestRun(t, c, admin, ns, "snapshot-compaction")
	h.apply(t, snapshotNodeBatch(t, h.r, snapshotDataNode("prefix", 100), nil))
	h.apply(t, snapshotNodeBatch(t, h.r, snapshotDataNode("suffix", 1000), nil))
	state := func() string {
		t.Helper()
		var out string
		if err := admin.QueryRow(ctx, `SELECT jsonb_build_object('bytes',snapshot_bytes,'fixed',snapshot_fixed_bytes,'version',snapshot_budget_version,'revision',revision::text,'head',head_chain,'objects',(SELECT jsonb_agg(to_jsonb(o) ORDER BY kind,key1,key2,key3) FROM pulse_durable_distributed.snapshot_objects o WHERE namespace=$1 AND run_id=$2),'containers',(SELECT jsonb_agg(to_jsonb(c) ORDER BY kind,key1) FROM pulse_durable_distributed.snapshot_containers c WHERE namespace=$1 AND run_id=$2))::text FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2`, ns, h.r.RunID).Scan(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	before := state()
	q := CompactStorageRequest{Identity: h.token.Identity, ExpectedGeneration: 1, ThroughRevision: 2, Mutation: d.Mutation{ID: "snapshot-compact-prefix"}}
	q.Mutation.Digest = CompactStorageDigest(q)
	if _, err := c.CompactStorage(ctx, q); err != nil {
		t.Fatal(err)
	}
	if state() != before {
		t.Fatal("compaction below head reset current ledger or business head")
	}
	h.reload(t)
	h.apply(t, snapshotNodeBatch(t, h.r, snapshotDataNode("after-compact", 10), nil))
	h.reload(t)
}

func TestPostgresSnapshotOldExpectedSizeIsNotAuthority(t *testing.T) {
	c, admin, ns := integration(t)
	h := newSnapshotTestRun(t, c, admin, ns, "snapshot-expected")
	old := snapshotDataNode("node", 4096)
	h.apply(t, snapshotNodeBatch(t, h.r, old, nil))
	next := snapshotDataNode("node", 1)
	batch := snapshotNodeBatch(t, h.r, next, old)
	proposal := durable.CloneRecord(h.r)
	if _, err := batch.ApplyTo(&proposal); err != nil {
		t.Fatal(err)
	}
	expected, err := recordio.EncodeSnapshot(proposal)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := durable.MarshalChangeBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	var obj map[string]json.RawMessage
	if err = json.Unmarshal(wire, &obj); err != nil {
		t.Fatal(err)
	}
	var ops []map[string]json.RawMessage
	if err = json.Unmarshal(obj["ops"], &ops); err != nil {
		t.Fatal(err)
	}
	fake := old
	fake.Outputs[0].Data = nil
	ops[0]["expected"], err = json.Marshal(snapshotTestPayload(fake))
	if err != nil {
		t.Fatal(err)
	}
	obj["ops"], err = json.Marshal(ops)
	if err != nil {
		t.Fatal(err)
	}
	wire, err = json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	// 只验证预算，不另写业务旧值判定；事务回滚避免制造不可重放的测试历史。
	tx, err := admin.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(context.Background(), `SELECT 1 FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2 FOR UPDATE`, ns, h.r.RunID); err != nil {
		t.Fatal(err)
	}
	var got int64
	err = tx.QueryRow(context.Background(), `SELECT pulse_durable_distributed.snapshot_budget_apply(r,$3::jsonb) FROM pulse_durable_distributed.runs r WHERE namespace=$1 AND run_id=$2`, ns, h.r.RunID, wire).Scan(&got)
	if err != nil {
		t.Fatal(err)
	}
	if got != int64(len(expected)) {
		t.Fatalf("incoming expected controlled old debit: SQL=%d expected=%d", got, len(expected))
	}
}
