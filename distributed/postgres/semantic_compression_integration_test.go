package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/Luo-root/pulse-durable/recordio"
	"github.com/jackc/pgx/v5/pgxpool"
)

// 本文件验收 distributed PostgreSQL 的专用语义压缩入口。
//
// 夹具只负责构造「已经完成首次证据物化」的权威状态：业务 Attempt 历史仍由
// 生产 ApplyOwned 写入，随后用管理员连接把闭合的语义证据组件安装为当前
// active snapshot。distributed 后端本身还没有物化入口，因此这里显式模拟
// 那一步的结果，而不是绕过压缩入口自己删对象。
//
// 压缩本身只允许通过 CompressSemanticHistory 触发：SQL 必须在 run 锁内
// 重放权威 image、重算候选、精确 CAS 删除，并把批次、账本、永久 guard 和
// 回执放在同一个事务里发布。

// semanticTestSummaryDigest 复刻根包摘要事实的 NUL 分隔口径。
// 它只用于构造夹具输入，不参与产品判定。
func semanticTestSummaryDigest(domain string, values ...any) string {
	b := []byte(domain)
	for _, value := range values {
		b = append(b, 0)
		b = append(b, []byte(fmt.Sprint(value))...)
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// semanticTestDomainDigest 复刻根包 {domain, values} 的 JSON 摘要口径。
// revision 必须保持 JSON 数字，不能退化成字符串。
func semanticTestDomainDigest(domain string, values ...any) string {
	b, err := json.Marshal(struct {
		Domain string `json:"domain"`
		Values []any  `json:"values"`
	}{domain, values})
	if err != nil {
		panic(err)
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func semanticTestRecordDigest(r durable.RunRecord) string {
	b, err := recordio.EncodeSnapshot(r)
	if err != nil {
		panic(err)
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// newSemanticRetryRun 构造带 Retry 失败前缀与闭合语义证据的权威 Run。
// 返回的 record 是当前 head 的完整逻辑记录；head 是它的链头。
func newSemanticRetryRun(t *testing.T, c *Coordinator, admin *pgxpool.Pool, ns, id string) (durable.RunRecord, string) {
	t.Helper()
	h := newSnapshotTestRun(t, c, admin, ns, id)

	// 持久身份必须是 16 字节 hex；恢复校验会拒绝随手拼的字符串。
	parentID := strings.Repeat("c", 32)
	firstID := strings.Repeat("a", 32)
	secondID := strings.Repeat("b", 32)
	attempts := []durable.AttemptRecord{
		{Parent: h.r.RootScope, NodeID: "node", Index: 1, Limit: 1, State: durable.AttemptFailed},
		{Parent: parentID, NodeID: "node", Boundary: "retry", Index: 1, Limit: 3, State: durable.AttemptFailed},
		{Parent: parentID, NodeID: "node", Boundary: "retry", Index: 2, Limit: 3, State: durable.AttemptFailed},
	}
	h.apply(t, snapshotTestBatch(t, h.r,
		snapshotTestOp("attempt", parentID, "", "", "", false, nil, attempts[0]),
		snapshotTestOp("attempt", firstID, "", "", "", false, nil, attempts[1]),
		snapshotTestOp("attempt", secondID, "", "", "", false, nil, attempts[2]),
	))

	record := durable.CloneRecord(h.r)
	// SourceDigest 绑定去掉语义组件后的业务事实投影。
	source := durable.CloneRecord(record)
	source.HistorySummaries, source.CommitIndex, source.Provenance = nil, nil, nil
	sourceDigest := semanticTestRecordDigest(source)

	lastDigest := semanticTestSummaryDigest("retry", record.RunID, record.Revision,
		parentID, "node", "retry", 2, 3, string(durable.AttemptFailed))
	// wire 口径的 uint64 是十进制字符串；applied_revision=0 表示尚未应用的候选。
	payload, err := json.Marshal(map[string]any{
		"applied_revision":   "0",
		"last_pruned_id":     secondID,
		"parent":             parentID,
		"node_id":            "node",
		"boundary":           "retry",
		"through_index":      2,
		"limit":              3,
		"last_pruned_digest": lastDigest,
	})
	if err != nil {
		t.Fatal(err)
	}
	payloadHash := sha256.Sum256(payload)
	record.HistorySummaries = []durable.HistorySummary{{
		Kind: durable.RetryPrefixSummaryKind, ID: "retry",
		SourceRevision: record.Revision, SourceDigest: sourceDigest,
		Payload: payload, Digest: hex.EncodeToString(payloadHash[:]),
	}}
	record.CommitIndex = make([]durable.CommitIndexEntry, record.Revision)
	for i := range record.CommitIndex {
		record.CommitIndex[i] = durable.CommitIndexEntry{
			Revision: uint64(i + 1), ParentRevision: uint64(i),
			BatchDigest: strings.Repeat(fmt.Sprintf("%x", i+1), 64),
		}
	}
	provenance := durable.Provenance{
		OriginRunID: record.RunID, OriginRevision: 1,
		OriginRecordDigest: sourceDigest, CompressionID: "semantic-compression-test",
	}
	provenance.Digest = semanticTestDomainDigest("provenance/v1",
		provenance.OriginRunID, provenance.OriginRevision, provenance.OriginRecordDigest, provenance.CompressionID)
	record.Provenance = &provenance
	if err := durable.ValidateSemanticEvidence(record); err != nil {
		t.Fatalf("fixture evidence is not closed: %v", err)
	}

	head := chainText(h.chain)
	installSemanticSnapshot(t, admin, ns, record, head)
	return record, head
}

// installSemanticSnapshot 把闭合证据记录安装为当前 active snapshot。
// 它同时按真实快照字节重算大小账本，避免夹具留下自相矛盾的权威状态。
func installSemanticSnapshot(t *testing.T, admin *pgxpool.Pool, ns string, record durable.RunRecord, head string) {
	t.Helper()
	ctx := context.Background()
	wire, err := recordio.EncodeSnapshot(record)
	if err != nil {
		t.Fatal(err)
	}
	var topBytes int64
	if err := admin.QueryRow(ctx, `SELECT coalesce(sum(snapshot_container_bytes(is_nil,entry_count,entry_bytes)) FILTER(WHERE kind IN ('scopes','attempts','streams','messages','managed')),0) FROM pulse_durable_distributed.snapshot_containers WHERE namespace=$1 AND run_id=$2`, ns, record.RunID).Scan(&topBytes); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `INSERT INTO pulse_durable_distributed.storage_snapshots(namespace,run_id,generation,revision,chain,digest,record) VALUES($1,$2,2,$3,$4,$5,$6)`,
		ns, record.RunID, record.Revision, head, digest(wire), wire); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, `UPDATE pulse_durable_distributed.runs SET record=$3,snapshot_digest=$4,snapshot_bytes=$5,snapshot_fixed_bytes=$6,revision=$7,head_chain=$8,active_snapshot=2,representation_generation=2,active_log_bytes=0,active_log_count=0 WHERE namespace=$1 AND run_id=$2`,
		ns, record.RunID, wire, digest(wire), int64(len(wire)), int64(len(wire))-topBytes, record.Revision, head); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresSemanticCompressionPublishesAtomicReceipt(t *testing.T) {
	c, admin, ns := integration(t)
	ctx := context.Background()
	var role string
	if err := c.pool.QueryRow(ctx, "SELECT session_user").Scan(&role); err != nil {
		t.Fatal(err)
	}
	// 语义压缩是维护权限，不是 owner token 权限。
	if _, err := c.CompressSemanticHistory(ctx, d.SemanticCompressionRequest{
		Identity: d.Identity{Namespace: ns, RunID: "absent"}, ExpectedRevision: 1,
		ExpectedHeadChain: strings.Repeat("a", 64), Mutation: mut("denied"),
		MaxObjects: 100, MaxBytes: 1 << 20,
	}); err == nil {
		t.Fatal("worker permission authorized semantic compression")
	}
	setTestPermission(t, admin, ctx, role, ns, "maintain", true)

	record, head := newSemanticRetryRun(t, c, admin, ns, "semantic-retry")
	q := d.SemanticCompressionRequest{
		Identity: d.Identity{Namespace: ns, RunID: record.RunID}, ExpectedRevision: record.Revision,
		ExpectedHeadChain: head, Mutation: mut("semantic-retry-commit"),
		MaxObjects: 100, MaxBytes: 1 << 20,
	}

	receipt, err := c.CompressSemanticHistory(ctx, q)
	if err != nil {
		t.Fatalf("semantic compression failed: %v", err)
	}
	if err := receipt.Validate(q); err != nil {
		t.Fatalf("receipt is not bound to request: %+v %v", receipt, err)
	}
	if receipt.DeletedObjects != 2 {
		t.Fatalf("deleted objects=%d want 2", receipt.DeletedObjects)
	}

	// 响应丢失后用同一 mutation 重试必须返回逐字相同的永久回执。
	again, err := c.CompressSemanticHistory(ctx, q)
	if err != nil || !reflect.DeepEqual(receipt, again) {
		t.Fatalf("retry did not return the permanent receipt: %+v vs %+v err=%v", receipt, again, err)
	}

	var revision, headChain, floor string
	var attemptRows, batchRows, guardRows, receiptRows, eventRows int
	var ledgerValid bool
	var snapshotBytes int64
	if err := admin.QueryRow(ctx, `SELECT r.revision::text,r.head_chain,r.history_floor::text,r.snapshot_bytes,
		(SELECT count(*) FROM pulse_durable_distributed.snapshot_objects o WHERE o.namespace=$1 AND o.run_id=$2 AND o.kind='attempt'),
		(SELECT count(*) FROM pulse_durable_distributed.batches b WHERE b.namespace=$1 AND b.run_id=$2),
		(SELECT count(*) FROM pulse_durable_distributed.mutation_guards g WHERE g.namespace=$1 AND g.run_id=$2 AND g.mutation_id=$3 AND g.operation='semantic_compress'),
		(SELECT count(*) FROM pulse_durable_distributed.mutation_receipts m WHERE m.namespace=$1 AND m.run_id=$2 AND m.mutation_id=$3),
		(SELECT count(*) FROM pulse_durable_distributed.events e WHERE e.namespace=$1 AND e.run_id=$2 AND e.operation='semantic_compress'),
		(s->>'snapshot_ledger_valid')::boolean
		FROM pulse_durable_distributed.runs r CROSS JOIN LATERAL pulse_durable_distributed.snapshot_budget_summary(r) s
		WHERE r.namespace=$1 AND r.run_id=$2`,
		ns, record.RunID, q.Mutation.ID).Scan(&revision, &headChain, &floor, &snapshotBytes,
		&attemptRows, &batchRows, &guardRows, &receiptRows, &eventRows, &ledgerValid); err != nil {
		t.Fatal(err)
	}
	if revision != fmt.Sprint(record.Revision+1) || headChain != receipt.AppliedHeadChain {
		t.Fatalf("business head not published once: revision=%s chain=%s", revision, headChain)
	}
	// 语义压缩只消费摘要候选，绝不推进 history_floor（那是 GC/retention 的职责）。
	if floor != "1" {
		t.Fatalf("semantic compression advanced history_floor=%s", floor)
	}
	if attemptRows != 1 || batchRows != 2 || guardRows != 1 || receiptRows != 1 || eventRows != 1 {
		t.Fatalf("atomic publication mismatch attempts=%d batches=%d guards=%d receipts=%d events=%d",
			attemptRows, batchRows, guardRows, receiptRows, eventRows)
	}
	if !ledgerValid {
		t.Fatal("snapshot ledger is inconsistent after semantic compression")
	}

	// 冷恢复必须读回压缩后的记录：只保留父 Attempt，摘要已应用，索引追加一条。
	c.DropOwnedProjection(d.Token{Identity: q.Identity})
	var image []byte
	if err := admin.QueryRow(ctx, `SELECT pulse_durable_distributed.storage_image(r,r.revision,true) FROM pulse_durable_distributed.runs r WHERE namespace=$1 AND run_id=$2`, ns, record.RunID).Scan(&image); err != nil {
		t.Fatal(err)
	}
	reloaded, reloadedChain, err := buildMaintenanceImage(ctx, CompactStorageRequest{Identity: q.Identity, ThroughRevision: record.Revision + 1}, image)
	if err != nil {
		t.Fatalf("cold recovery after compression failed: %v", err)
	}
	if reloadedChain != receipt.AppliedHeadChain || reloaded.Revision != record.Revision+1 {
		t.Fatalf("cold recovery head mismatch chain=%s revision=%d", reloadedChain, reloaded.Revision)
	}
	if len(reloaded.Attempts) != 1 || len(reloaded.CommitIndex) != int(record.Revision)+1 {
		t.Fatalf("cold recovery projection attempts=%d index=%d", len(reloaded.Attempts), len(reloaded.CommitIndex))
	}
	encoded, err := recordio.EncodeSnapshot(reloaded)
	if err != nil || int64(len(encoded)) != snapshotBytes {
		t.Fatalf("snapshot ledger bytes=%d actual=%d err=%v", snapshotBytes, len(encoded), err)
	}
	if err := durable.ValidateSemanticEvidence(reloaded); err != nil {
		t.Fatalf("compressed record lost evidence closure: %v", err)
	}

	// 既有回执查询入口必须能解析这次维护变更。
	query := receiptQuery(t, admin, q.Identity, q.Mutation, "semantic_compress", nil, uint64ptr(record.Revision+1))
	found, err := c.FindMutation(ctx, query)
	if err != nil || found.State != d.Found || found.Receipt == nil || found.Receipt.Revision != record.Revision+1 {
		t.Fatalf("mutation receipt query state=%v err=%v", found.State, err)
	}
}

func TestPostgresSemanticCompressionRejectsStaleHeadAndReusedMutation(t *testing.T) {
	c, admin, ns := integration(t)
	ctx := context.Background()
	var role string
	if err := c.pool.QueryRow(ctx, "SELECT session_user").Scan(&role); err != nil {
		t.Fatal(err)
	}
	setTestPermission(t, admin, ctx, role, ns, "maintain", true)
	record, head := newSemanticRetryRun(t, c, admin, ns, "semantic-conflict")

	stale := d.SemanticCompressionRequest{
		Identity: d.Identity{Namespace: ns, RunID: record.RunID}, ExpectedRevision: record.Revision,
		ExpectedHeadChain: strings.Repeat("f", 64), Mutation: mut("semantic-stale"),
		MaxObjects: 100, MaxBytes: 1 << 20,
	}
	if _, err := c.CompressSemanticHistory(ctx, stale); !errors.Is(err, d.ErrRevisionConflict) {
		t.Fatalf("stale head accepted: %v", err)
	}

	good := stale
	good.ExpectedHeadChain = head
	good.Mutation = mut("semantic-once")
	receipt, err := c.CompressSemanticHistory(ctx, good)
	if err != nil {
		t.Fatal(err)
	}

	// 同一 mutation ID 换摘要必须冲突，不能覆盖已提交回执。
	reused := good
	reused.Mutation = d.Mutation{ID: good.Mutation.ID, Digest: digest([]byte("different-plan"))}
	if _, err := c.CompressSemanticHistory(ctx, reused); !errors.Is(err, d.ErrRevisionConflict) {
		t.Fatalf("mutation digest reuse accepted: %v", err)
	}
	// 已提交后 head 已推进；旧 expected revision 的新 mutation 必须冲突且零发布。
	next := good
	next.Mutation = mut("semantic-after")
	if _, err := c.CompressSemanticHistory(ctx, next); !errors.Is(err, d.ErrRevisionConflict) {
		t.Fatalf("post-commit replay accepted: %v", err)
	}
	var batches, receipts int
	if err := admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM pulse_durable_distributed.batches WHERE namespace=$1 AND run_id=$2),(SELECT count(*) FROM pulse_durable_distributed.mutation_receipts WHERE namespace=$1 AND run_id=$2 AND mutation_id=$3)`,
		ns, record.RunID, good.Mutation.ID).Scan(&batches, &receipts); err != nil {
		t.Fatal(err)
	}
	if batches != 2 || receipts != 1 {
		t.Fatalf("rejected requests published state batches=%d receipts=%d", batches, receipts)
	}
	if receipt.AppliedRevision != record.Revision+1 {
		t.Fatalf("unexpected applied revision %d", receipt.AppliedRevision)
	}
}

// semanticPhaseArgs 构造一次语义压缩阶段请求，供故障注入用例精确控制时序。
func semanticPhaseArgs(q d.SemanticCompressionRequest, phase string) map[string]any {
	a := args(q.Identity)
	a["phase"] = phase
	a["expected_revision"] = strconv.FormatUint(q.ExpectedRevision, 10)
	a["expected_head_chain"] = q.ExpectedHeadChain
	a["max_objects"] = strconv.FormatUint(q.MaxObjects, 10)
	a["max_bytes"] = strconv.FormatUint(q.MaxBytes, 10)
	mutate(a, q.Mutation)
	return a
}

// TestPostgresSemanticCompressionRejectsConcurrentOwnerWrite 验证 prepare 与
// commit 之间发生业务写入时，锁后复验必须拒绝且零发布：调用方上传的 image
// 摘要与批次都不能覆盖新的权威 head。
func TestPostgresSemanticCompressionRejectsConcurrentOwnerWrite(t *testing.T) {
	c, admin, ns := integration(t)
	ctx := context.Background()
	var role string
	if err := c.pool.QueryRow(ctx, "SELECT session_user").Scan(&role); err != nil {
		t.Fatal(err)
	}
	setTestPermission(t, admin, ctx, role, ns, "maintain", true)
	record, head := newSemanticRetryRun(t, c, admin, ns, "semantic-race")
	q := d.SemanticCompressionRequest{
		Identity: d.Identity{Namespace: ns, RunID: record.RunID}, ExpectedRevision: record.Revision,
		ExpectedHeadChain: head, Mutation: mut("semantic-race-commit"), MaxObjects: 100, MaxBytes: 1 << 20,
	}
	preparedRaw, err := c.semanticCompressionCall(ctx, semanticPhaseArgs(q, "prepare"), true)
	if err != nil {
		t.Fatal(err)
	}
	prepared, _, err := decodeSemanticCompressionResult(preparedRaw, q)
	if err != nil {
		t.Fatal(err)
	}
	image, chain, err := buildMaintenanceImage(ctx, CompactStorageRequest{Identity: q.Identity, ThroughRevision: q.ExpectedRevision}, prepared.Image)
	if err != nil || chain != head {
		t.Fatalf("prepare image chain=%s err=%v", chain, err)
	}
	plan, err := durable.PlanHistoryCompression(image, durable.HistoryCompressionRequest{RunID: q.Identity.RunID, ThroughRevision: q.ExpectedRevision, MaxObjects: q.MaxObjects, MaxBytes: q.MaxBytes})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := durable.BuildHistoryCompressionBatch(image, plan)
	if err != nil {
		t.Fatal(err)
	}
	proposed := durable.CloneRecord(image)
	if _, err := batch.ApplyTo(&proposed); err != nil {
		t.Fatal(err)
	}
	wire, err := durable.MarshalChangeBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	recordWire, err := recordio.EncodeSnapshot(proposed)
	if err != nil {
		t.Fatal(err)
	}

	// prepare 之后由真实 owner 推进业务 head；压缩提案随之失效。
	var leaseID, workerID, bootID string
	var epoch int64
	if err := admin.QueryRow(ctx, `SELECT lease_id,worker_id,boot_id,epoch FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2`, ns, record.RunID).Scan(&leaseID, &workerID, &bootID, &epoch); err != nil {
		t.Fatal(err)
	}
	owner := d.Token{Identity: q.Identity, Worker: d.Worker{ID: workerID, BootID: bootID}, LeaseID: leaseID, Epoch: epoch}
	live, err := c.LoadOwned(ctx, owner)
	if err != nil {
		t.Fatal(err)
	}
	next := semanticOrdinaryBatch(t, live)
	nextDigest, err := d.DeltaMutationDigest(owner, next)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.ApplyOwned(ctx, owner, d.Mutation{ID: "semantic-race-owner", Digest: nextDigest}, next); err != nil {
		t.Fatal(err)
	}

	commit := semanticPhaseArgs(q, "commit")
	commit["source_image_digest"] = prepared.SourceImageDigest
	commit["plan_digest"] = plan.PlanDigest
	commit["batch"] = base64.StdEncoding.EncodeToString(wire)
	commit["batch_digest"] = digest(wire)
	commit["record"] = base64.StdEncoding.EncodeToString(recordWire)
	commit["record_digest"] = digest(recordWire)
	if _, err := c.semanticCompressionCall(ctx, commit, true); !errors.Is(err, d.ErrRevisionConflict) {
		t.Fatalf("stale proposal after owner write accepted: %v", err)
	}
	var batches, guards, receipts int
	if err := admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM pulse_durable_distributed.batches WHERE namespace=$1 AND run_id=$2),(SELECT count(*) FROM pulse_durable_distributed.mutation_guards WHERE namespace=$1 AND run_id=$2 AND mutation_id=$3),(SELECT count(*) FROM pulse_durable_distributed.mutation_receipts WHERE namespace=$1 AND run_id=$2 AND mutation_id=$3)`,
		ns, record.RunID, q.Mutation.ID).Scan(&batches, &guards, &receipts); err != nil {
		t.Fatal(err)
	}
	// 只应保留 owner 那一条业务批次；压缩不得留下 guard/receipt/批次。
	if batches != 2 || guards != 0 || receipts != 0 {
		t.Fatalf("rejected compression published state batches=%d guards=%d receipts=%d", batches, guards, receipts)
	}
}

// TestPostgresSemanticCompressionRejectsForgedDeletionSet 验证调用方即使
// 重算 batch 摘要，也不能扩大删除集合：SQL 必须按权威记录重算候选并拒绝。
func TestPostgresSemanticCompressionRejectsForgedDeletionSet(t *testing.T) {
	c, admin, ns := integration(t)
	ctx := context.Background()
	var role string
	if err := c.pool.QueryRow(ctx, "SELECT session_user").Scan(&role); err != nil {
		t.Fatal(err)
	}
	setTestPermission(t, admin, ctx, role, ns, "maintain", true)
	record, head := newSemanticRetryRun(t, c, admin, ns, "semantic-forge")
	q := d.SemanticCompressionRequest{
		Identity: d.Identity{Namespace: ns, RunID: record.RunID}, ExpectedRevision: record.Revision,
		ExpectedHeadChain: head, Mutation: mut("semantic-forge-commit"), MaxObjects: 100, MaxBytes: 1 << 20,
	}
	preparedRaw, err := c.semanticCompressionCall(ctx, semanticPhaseArgs(q, "prepare"), true)
	if err != nil {
		t.Fatal(err)
	}
	prepared, _, err := decodeSemanticCompressionResult(preparedRaw, q)
	if err != nil {
		t.Fatal(err)
	}
	image, _, err := buildMaintenanceImage(ctx, CompactStorageRequest{Identity: q.Identity, ThroughRevision: q.ExpectedRevision}, prepared.Image)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := durable.PlanHistoryCompression(image, durable.HistoryCompressionRequest{RunID: q.Identity.RunID, ThroughRevision: q.ExpectedRevision, MaxObjects: q.MaxObjects, MaxBytes: q.MaxBytes})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := durable.BuildHistoryCompressionBatch(image, plan)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := durable.MarshalChangeBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	// 篡改 wire：额外删除受保护的父 Attempt，并重算摘要冒充合法提案。
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(wire, &envelope); err != nil {
		t.Fatal(err)
	}
	var ops []map[string]json.RawMessage
	if err := json.Unmarshal(envelope["ops"], &ops); err != nil {
		t.Fatal(err)
	}
	parentID := strings.Repeat("c", 32)
	parent := image.Attempts[parentID]
	expected, err := json.Marshal(snapshotTestPayload(parent))
	if err != nil {
		t.Fatal(err)
	}
	extra := map[string]json.RawMessage{
		"kind":     json.RawMessage(`"attempt"`),
		"key":      json.RawMessage(`"` + parentID + `"`),
		"expected": expected,
		"value":    json.RawMessage(`null`),
	}
	ops = append(ops, extra)
	forgedOps, err := json.Marshal(ops)
	if err != nil {
		t.Fatal(err)
	}
	envelope["ops"] = forgedOps
	forgedWire, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	proposed := durable.CloneRecord(image)
	if _, err := batch.ApplyTo(&proposed); err != nil {
		t.Fatal(err)
	}
	recordWire, err := recordio.EncodeSnapshot(proposed)
	if err != nil {
		t.Fatal(err)
	}
	commit := semanticPhaseArgs(q, "commit")
	commit["source_image_digest"] = prepared.SourceImageDigest
	commit["plan_digest"] = plan.PlanDigest
	commit["batch"] = base64.StdEncoding.EncodeToString(forgedWire)
	commit["batch_digest"] = digest(forgedWire)
	commit["record"] = base64.StdEncoding.EncodeToString(recordWire)
	commit["record_digest"] = digest(recordWire)
	if _, err := c.semanticCompressionCall(ctx, commit, true); err == nil {
		t.Fatal("forged deletion set accepted")
	}
	var batches, receipts int
	if err := admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM pulse_durable_distributed.batches WHERE namespace=$1 AND run_id=$2),(SELECT count(*) FROM pulse_durable_distributed.mutation_receipts WHERE namespace=$1 AND run_id=$2 AND mutation_id=$3)`,
		ns, record.RunID, q.Mutation.ID).Scan(&batches, &receipts); err != nil {
		t.Fatal(err)
	}
	if batches != 1 || receipts != 0 {
		t.Fatalf("forged request published state batches=%d receipts=%d", batches, receipts)
	}
}

// semanticOrdinaryBatch 为已带语义证据的记录构造一次普通业务提交。
// 证据记录要求每批恰好追加一条 CommitIndex，且其 BatchDigest 绑定
// 「去掉 commit_index 操作后的 canonical wire」；这里按同一口径构造，
// 不绕过根包的语义转移校验。
func semanticOrdinaryBatch(t *testing.T, r durable.RunRecord) durable.ChangeBatch {
	t.Helper()
	base := snapshotTestBatch(t, r, snapshotTestOp("attempt", strings.Repeat("d", 32), "", "", "", false, nil,
		durable.AttemptRecord{Parent: r.RootScope, NodeID: "other", Index: 1, Limit: 1, State: durable.AttemptActive}))
	contentWire, err := durable.MarshalChangeBatch(base)
	if err != nil {
		t.Fatal(err)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(contentWire, &envelope); err != nil {
		t.Fatal(err)
	}
	var ops []json.RawMessage
	if err := json.Unmarshal(envelope["ops"], &ops); err != nil {
		t.Fatal(err)
	}
	applied := r.Revision + 1
	entry, err := json.Marshal(map[string]any{
		"kind": "commit_index", "key": strconv.FormatUint(applied, 10),
		"expected": nil,
		"value": map[string]any{
			"revision":        strconv.FormatUint(applied, 10),
			"parent_revision": strconv.FormatUint(r.Revision, 10),
			"batch_digest":    digest(contentWire),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// "attempt" < "commit_index"，追加后仍满足 canonical 排序。
	ops = append(ops, entry)
	allOps, err := json.Marshal(ops)
	if err != nil {
		t.Fatal(err)
	}
	envelope["ops"] = allOps
	full, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := durable.UnmarshalChangeBatch(full)
	if err != nil {
		t.Fatalf("ordinary evidence batch: %v", err)
	}
	return batch
}

// TestPostgresSemanticCompressionKeepsLedgerAcrossLaterBatches 验证压缩之后
// 继续普通业务提交时，证据账本仍逐批精确。runs.record 是基础快照、不随
// 普通 apply 更新，因此索引长度必须取自权威 revision，而不是基础快照数组。
func TestPostgresSemanticCompressionKeepsLedgerAcrossLaterBatches(t *testing.T) {
	c, admin, ns := integration(t)
	ctx := context.Background()
	var role string
	if err := c.pool.QueryRow(ctx, "SELECT session_user").Scan(&role); err != nil {
		t.Fatal(err)
	}
	setTestPermission(t, admin, ctx, role, ns, "maintain", true)
	record, head := newSemanticRetryRun(t, c, admin, ns, "semantic-ledger")
	q := d.SemanticCompressionRequest{
		Identity: d.Identity{Namespace: ns, RunID: record.RunID}, ExpectedRevision: record.Revision,
		ExpectedHeadChain: head, Mutation: mut("semantic-ledger-commit"), MaxObjects: 100, MaxBytes: 1 << 20,
	}
	if _, err := c.CompressSemanticHistory(ctx, q); err != nil {
		t.Fatal(err)
	}
	var leaseID, workerID, bootID string
	var epoch int64
	if err := admin.QueryRow(ctx, `SELECT lease_id,worker_id,boot_id,epoch FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2`, ns, record.RunID).Scan(&leaseID, &workerID, &bootID, &epoch); err != nil {
		t.Fatal(err)
	}
	owner := d.Token{Identity: q.Identity, Worker: d.Worker{ID: workerID, BootID: bootID}, LeaseID: leaseID, Epoch: epoch}
	// 连续多批：每批都新增一条 CommitIndex，基础快照始终停在压缩那一步。
	for i := 0; i < 3; i++ {
		c.DropOwnedProjection(owner)
		live, err := c.LoadOwned(ctx, owner)
		if err != nil {
			t.Fatalf("reload %d: %v", i, err)
		}
		batch := semanticOrdinaryBatchN(t, live, i)
		dg, err := d.DeltaMutationDigest(owner, batch)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.ApplyOwned(ctx, owner, d.Mutation{ID: fmt.Sprintf("semantic-ledger-%d", i), Digest: dg}, batch); err != nil {
			t.Fatalf("apply %d: %v", i, err)
		}
		var snapshotBytes int64
		var ledgerValid bool
		var revision string
		if err := admin.QueryRow(ctx, `SELECT r.snapshot_bytes,r.revision::text,(s->>'snapshot_ledger_valid')::boolean FROM pulse_durable_distributed.runs r CROSS JOIN LATERAL pulse_durable_distributed.snapshot_budget_summary(r) s WHERE r.namespace=$1 AND r.run_id=$2`, ns, record.RunID).Scan(&snapshotBytes, &revision, &ledgerValid); err != nil {
			t.Fatal(err)
		}
		if !ledgerValid {
			t.Fatalf("ledger invalid after batch %d", i)
		}
		var image []byte
		if err := admin.QueryRow(ctx, `SELECT pulse_durable_distributed.storage_image(r,r.revision,true) FROM pulse_durable_distributed.runs r WHERE namespace=$1 AND run_id=$2`, ns, record.RunID).Scan(&image); err != nil {
			t.Fatal(err)
		}
		want, _, err := buildMaintenanceImage(ctx, CompactStorageRequest{Identity: q.Identity, ThroughRevision: live.Revision + 1}, image)
		if err != nil {
			t.Fatalf("replay %d: %v", i, err)
		}
		encoded, err := recordio.EncodeSnapshot(want)
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(encoded)) != snapshotBytes {
			t.Fatalf("batch %d ledger bytes=%d actual=%d", i, snapshotBytes, len(encoded))
		}
		if len(want.CommitIndex) != int(want.Revision) {
			t.Fatalf("batch %d commit index len=%d revision=%d", i, len(want.CommitIndex), want.Revision)
		}
	}
}

// semanticOrdinaryBatchN 构造第 n 次普通证据提交，使用互不相同的 Attempt 身份。
func semanticOrdinaryBatchN(t *testing.T, r durable.RunRecord, n int) durable.ChangeBatch {
	t.Helper()
	// 身份必须与夹具已用的 a/b/c/d 前缀不冲突。
	id := strings.Repeat(fmt.Sprintf("%x", 1+n), 32)
	base := snapshotTestBatch(t, r, snapshotTestOp("attempt", id, "", "", "", false, nil,
		durable.AttemptRecord{Parent: r.RootScope, NodeID: fmt.Sprintf("extra-%d", n), Index: 1, Limit: 1, State: durable.AttemptActive}))
	contentWire, err := durable.MarshalChangeBatch(base)
	if err != nil {
		t.Fatal(err)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(contentWire, &envelope); err != nil {
		t.Fatal(err)
	}
	var ops []json.RawMessage
	if err := json.Unmarshal(envelope["ops"], &ops); err != nil {
		t.Fatal(err)
	}
	applied := r.Revision + 1
	entry, err := json.Marshal(map[string]any{
		"kind": "commit_index", "key": strconv.FormatUint(applied, 10),
		"expected": nil,
		"value": map[string]any{
			"revision":        strconv.FormatUint(applied, 10),
			"parent_revision": strconv.FormatUint(r.Revision, 10),
			"batch_digest":    digest(contentWire),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ops = append(ops, entry)
	allOps, err := json.Marshal(ops)
	if err != nil {
		t.Fatal(err)
	}
	envelope["ops"] = allOps
	full, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := durable.UnmarshalChangeBatch(full)
	if err != nil {
		t.Fatalf("ordinary evidence batch %d: %v", n, err)
	}
	return batch
}

// TestPostgresSemanticCompressionRejectsExtraBusinessOperation 验证 maintain
// 凭据不能借压缩批次夹带业务修改：即使删除集合与候选完全一致、CAS 前置条件
// 正确，任何非压缩语义的操作都必须被拒绝且零发布。
func TestPostgresSemanticCompressionRejectsExtraBusinessOperation(t *testing.T) {
	c, admin, ns := integration(t)
	ctx := context.Background()
	var role string
	if err := c.pool.QueryRow(ctx, "SELECT session_user").Scan(&role); err != nil {
		t.Fatal(err)
	}
	setTestPermission(t, admin, ctx, role, ns, "maintain", true)
	record, head := newSemanticRetryRun(t, c, admin, ns, "semantic-extra-op")
	q := d.SemanticCompressionRequest{
		Identity: d.Identity{Namespace: ns, RunID: record.RunID}, ExpectedRevision: record.Revision,
		ExpectedHeadChain: head, Mutation: mut("semantic-extra-commit"), MaxObjects: 100, MaxBytes: 1 << 20,
	}
	preparedRaw, err := c.semanticCompressionCall(ctx, semanticPhaseArgs(q, "prepare"), true)
	if err != nil {
		t.Fatal(err)
	}
	prepared, _, err := decodeSemanticCompressionResult(preparedRaw, q)
	if err != nil {
		t.Fatal(err)
	}
	image, _, err := buildMaintenanceImage(ctx, CompactStorageRequest{Identity: q.Identity, ThroughRevision: q.ExpectedRevision}, prepared.Image)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := durable.PlanHistoryCompression(image, durable.HistoryCompressionRequest{RunID: q.Identity.RunID, ThroughRevision: q.ExpectedRevision, MaxObjects: q.MaxObjects, MaxBytes: q.MaxBytes})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := durable.BuildHistoryCompressionBatch(image, plan)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := durable.MarshalChangeBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(wire, &envelope); err != nil {
		t.Fatal(err)
	}
	var ops []json.RawMessage
	if err := json.Unmarshal(envelope["ops"], &ops); err != nil {
		t.Fatal(err)
	}
	// 夹带一个合法 CAS 的新 Scope 创建：删除集合不变，但这不是压缩语义。
	scopeValue, err := json.Marshal(snapshotTestPayload(durable.ScopeRecord{Graph: "injected", Path: "p", Inputs: image.Inputs}))
	if err != nil {
		t.Fatal(err)
	}
	extra, err := json.Marshal(map[string]any{
		"kind": "scope", "key": strings.Repeat("e", 32), "expected": nil, "value": json.RawMessage(scopeValue),
	})
	if err != nil {
		t.Fatal(err)
	}
	// "commit_index" < "scope"，追加到末尾仍保持 canonical 排序。
	ops = append(ops, extra)
	allOps, err := json.Marshal(ops)
	if err != nil {
		t.Fatal(err)
	}
	envelope["ops"] = allOps
	forgedWire, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	proposed := durable.CloneRecord(image)
	if _, err := batch.ApplyTo(&proposed); err != nil {
		t.Fatal(err)
	}
	recordWire, err := recordio.EncodeSnapshot(proposed)
	if err != nil {
		t.Fatal(err)
	}
	commit := semanticPhaseArgs(q, "commit")
	commit["source_image_digest"] = prepared.SourceImageDigest
	commit["plan_digest"] = plan.PlanDigest
	commit["batch"] = base64.StdEncoding.EncodeToString(forgedWire)
	commit["batch_digest"] = digest(forgedWire)
	commit["record"] = base64.StdEncoding.EncodeToString(recordWire)
	commit["record_digest"] = digest(recordWire)
	if _, err := c.semanticCompressionCall(ctx, commit, true); !errors.Is(err, d.ErrInvalid) {
		t.Fatalf("extra business operation accepted: %v", err)
	}
	var batches, guards, receipts, scopes int
	if err := admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM pulse_durable_distributed.batches WHERE namespace=$1 AND run_id=$2),(SELECT count(*) FROM pulse_durable_distributed.mutation_guards WHERE namespace=$1 AND run_id=$2 AND mutation_id=$3),(SELECT count(*) FROM pulse_durable_distributed.mutation_receipts WHERE namespace=$1 AND run_id=$2 AND mutation_id=$3),(SELECT count(*) FROM pulse_durable_distributed.snapshot_objects o WHERE o.namespace=$1 AND o.run_id=$2 AND o.kind='scope')`,
		ns, record.RunID, q.Mutation.ID).Scan(&batches, &guards, &receipts, &scopes); err != nil {
		t.Fatal(err)
	}
	if batches != 1 || guards != 0 || receipts != 0 || scopes != 1 {
		t.Fatalf("injected operation published state batches=%d guards=%d receipts=%d scopes=%d", batches, guards, receipts, scopes)
	}
}

// TestPostgresSemanticCompressionRejectsSummaryKindSwap 复现独立复核实证过的
// 攻击：删除集合与候选完全一致、CAS 前置条件正确、record proposal 自洽，但把
// retry_prefix 摘要换成结构合法的 message_prefix。该篡改会让发布后的 Run 无法
// 冷恢复，因此必须在发布前拒绝且零发布。
func TestPostgresSemanticCompressionRejectsSummaryKindSwap(t *testing.T) {
	c, admin, ns := integration(t)
	ctx := context.Background()
	var role string
	if err := c.pool.QueryRow(ctx, "SELECT session_user").Scan(&role); err != nil {
		t.Fatal(err)
	}
	setTestPermission(t, admin, ctx, role, ns, "maintain", true)
	record, head := newSemanticRetryRun(t, c, admin, ns, "semantic-kind-swap")
	q := d.SemanticCompressionRequest{
		Identity: d.Identity{Namespace: ns, RunID: record.RunID}, ExpectedRevision: record.Revision,
		ExpectedHeadChain: head, Mutation: mut("semantic-swap-commit"), MaxObjects: 100, MaxBytes: 1 << 20,
	}
	preparedRaw, err := c.semanticCompressionCall(ctx, semanticPhaseArgs(q, "prepare"), true)
	if err != nil {
		t.Fatal(err)
	}
	prepared, _, err := decodeSemanticCompressionResult(preparedRaw, q)
	if err != nil {
		t.Fatal(err)
	}
	image, _, err := buildMaintenanceImage(ctx, CompactStorageRequest{Identity: q.Identity, ThroughRevision: q.ExpectedRevision}, prepared.Image)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := durable.PlanHistoryCompression(image, durable.HistoryCompressionRequest{RunID: q.Identity.RunID, ThroughRevision: q.ExpectedRevision, MaxObjects: q.MaxObjects, MaxBytes: q.MaxBytes})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := durable.BuildHistoryCompressionBatch(image, plan)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := durable.MarshalChangeBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(wire, &envelope); err != nil {
		t.Fatal(err)
	}
	var ops []map[string]json.RawMessage
	if err := json.Unmarshal(envelope["ops"], &ops); err != nil {
		t.Fatal(err)
	}
	// 结构合法但语义错误的 message_prefix payload：摘要摘要自洽即可通过结构校验。
	fakePayload := []byte(`{"applied_revision":"3","processing_scope":"root","processing_attempt":"x","node_id":"n","input":{"scope":"root","node_id":"n","key":"k","group":"g","generation":"1"},"through_sequence":"1","budget_digest":"` + strings.Repeat("a", 64) + `","managed_digest":"` + strings.Repeat("b", 64) + `","confirmation_ids":["` + strings.Repeat("c", 32) + `"]}`)
	fakeHash := sha256.Sum256(fakePayload)
	fakeDigest := hex.EncodeToString(fakeHash[:])
	wireBytes := make([]int, len(fakePayload))
	for i, b := range fakePayload {
		wireBytes[i] = int(b)
	}
	payloadWire, err := json.Marshal(wireBytes)
	if err != nil {
		t.Fatal(err)
	}
	swapped := false
	for i := range ops {
		var kind string
		if err := json.Unmarshal(ops[i]["kind"], &kind); err != nil || kind != "history_summary" {
			continue
		}
		var value map[string]json.RawMessage
		if err := json.Unmarshal(ops[i]["value"], &value); err != nil {
			t.Fatal(err)
		}
		value["kind"] = json.RawMessage(`"message_prefix"`)
		value["payload"] = payloadWire
		value["digest"] = json.RawMessage(`"` + fakeDigest + `"`)
		nv, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		ops[i]["value"] = nv
		swapped = true
	}
	if !swapped {
		t.Fatal("no history_summary operation found in the legitimate batch")
	}
	forgedOps, err := json.Marshal(ops)
	if err != nil {
		t.Fatal(err)
	}
	envelope["ops"] = forgedOps
	forgedWire, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	// 构造与篡改批次自洽的 record proposal，确保拒绝来自语义约束而非提案不一致。
	proposed := durable.CloneRecord(image)
	if _, err := batch.ApplyTo(&proposed); err != nil {
		t.Fatal(err)
	}
	for i := range proposed.HistorySummaries {
		if proposed.HistorySummaries[i].Kind == durable.RetryPrefixSummaryKind {
			proposed.HistorySummaries[i].Kind = durable.MessagePrefixSummaryKind
			proposed.HistorySummaries[i].Payload = append([]byte(nil), fakePayload...)
			proposed.HistorySummaries[i].Digest = fakeDigest
		}
	}
	recordWire, err := recordio.EncodeSnapshot(proposed)
	if err != nil {
		t.Fatal(err)
	}
	commit := semanticPhaseArgs(q, "commit")
	commit["source_image_digest"] = prepared.SourceImageDigest
	commit["plan_digest"] = plan.PlanDigest
	commit["batch"] = base64.StdEncoding.EncodeToString(forgedWire)
	commit["batch_digest"] = digest(forgedWire)
	commit["record"] = base64.StdEncoding.EncodeToString(recordWire)
	commit["record_digest"] = digest(recordWire)
	if _, err := c.semanticCompressionCall(ctx, commit, true); !errors.Is(err, d.ErrInvalid) {
		t.Fatalf("summary kind swap accepted: %v", err)
	}
	var batches, guards, receipts int
	if err := admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM pulse_durable_distributed.batches WHERE namespace=$1 AND run_id=$2),(SELECT count(*) FROM pulse_durable_distributed.mutation_guards WHERE namespace=$1 AND run_id=$2 AND mutation_id=$3),(SELECT count(*) FROM pulse_durable_distributed.mutation_receipts WHERE namespace=$1 AND run_id=$2 AND mutation_id=$3)`,
		ns, record.RunID, q.Mutation.ID).Scan(&batches, &guards, &receipts); err != nil {
		t.Fatal(err)
	}
	if batches != 1 || guards != 0 || receipts != 0 {
		t.Fatalf("kind swap published state batches=%d guards=%d receipts=%d", batches, guards, receipts)
	}
	// 权威 head 未变，Run 仍可正常冷恢复。
	var img []byte
	if err := admin.QueryRow(ctx, `SELECT pulse_durable_distributed.storage_image(r,r.revision,true) FROM pulse_durable_distributed.runs r WHERE namespace=$1 AND run_id=$2`, ns, record.RunID).Scan(&img); err != nil {
		t.Fatal(err)
	}
	if _, _, err := buildMaintenanceImage(ctx, CompactStorageRequest{Identity: q.Identity, ThroughRevision: record.Revision}, img); err != nil {
		t.Fatalf("run became unrecoverable after rejected swap: %v", err)
	}
}
