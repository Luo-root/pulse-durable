// 本文件只验证历史 GC 公共协议的形状、完整绑定和边界，不取得删除授权，
// 不访问数据库。先建立有效控制，再逐字段变异，区分摘要失配与非法范围。
package distributed

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func historyGCFixture(t *testing.T) (HistoryGCRequest, HistoryGCPlan, HistoryGCExecute, HistoryGCReceipt) {
	t.Helper()
	q := HistoryGCRequest{Identity: Identity{Namespace: "gc-ns", RunID: "gc-run"}, PlanID: "gc-plan", PolicyVersion: 7, PolicyDigest: strings.Repeat("a", 64), MaxRows: 4, MaxBytes: 100}
	p := HistoryGCPlan{Request: q, RequestDigest: q.Digest(), HeadRevision: 12, HeadChain: strings.Repeat("b", 64), SnapshotGeneration: 2, SnapshotRevision: 10, SnapshotChain: strings.Repeat("c", 64), SnapshotDigest: strings.Repeat("d", 64), BeforeFloor: 1, AfterFloor: 10, FirstRevision: 2, ThroughRevision: 5, PrefixDigest: strings.Repeat("e", 64), Rows: 4, Bytes: 40, PreparedAt: time.Date(2026, 11, 5, 12, 0, 0, 123456000, time.UTC)}
	p.PlanDigest = p.Digest()
	e := HistoryGCExecute{Identity: q.Identity, PlanID: q.PlanID, PlanDigest: p.PlanDigest}
	r := HistoryGCReceipt{Identity: q.Identity, PlanID: q.PlanID, PlanDigest: p.PlanDigest, Rows: p.Rows, Bytes: p.Bytes, HistoryFloor: p.AfterFloor, OccurredAt: p.PreparedAt.Add(time.Microsecond)}
	r.ReceiptDigest = r.Digest()
	for name, err := range map[string]error{"request": q.Validate(), "plan": p.Validate(q), "execute": e.Validate(), "receipt": r.Validate(p), "prepared": (HistoryGCResult{State: HistoryGCPrepared, Plan: &p}).Validate(e), "executed": (HistoryGCResult{State: HistoryGCExecuted, Plan: &p, Receipt: &r}).Validate(e)} {
		if err != nil {
			t.Fatalf("valid %s control: %v", name, err)
		}
	}
	return q, p, e, r
}
func historyGCRequireInvalid(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("want ErrInvalid, got %v", err)
	}
}

func TestHistoryGCRequestBindingAndBounds(t *testing.T) {
	q, p, _, _ := historyGCFixture(t)
	mutations := map[string]func(*HistoryGCRequest){
		"namespace":     func(x *HistoryGCRequest) { x.Identity.Namespace = "other" },
		"run":           func(x *HistoryGCRequest) { x.Identity.RunID = "other" },
		"plan":          func(x *HistoryGCRequest) { x.PlanID = "other" },
		"version":       func(x *HistoryGCRequest) { x.PolicyVersion++ },
		"policy digest": func(x *HistoryGCRequest) { x.PolicyDigest = strings.Repeat("f", 64) },
		"rows budget":   func(x *HistoryGCRequest) { x.MaxRows++ },
		"bytes budget":  func(x *HistoryGCRequest) { x.MaxBytes++ },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			x := q
			mutate(&x)
			if x.Validate() != nil {
				t.Fatal("binding control must remain valid")
			}
			if x.Digest() == q.Digest() {
				t.Fatal("digest did not bind field")
			}
			historyGCRequireInvalid(t, p.Validate(x))
		})
	}
	for name, mutate := range map[string]func(*HistoryGCRequest){
		"zero version": func(x *HistoryGCRequest) { x.PolicyVersion = 0 }, "bad digest": func(x *HistoryGCRequest) { x.PolicyDigest = "bad" },
		"zero rows": func(x *HistoryGCRequest) { x.MaxRows = 0 }, "too many rows": func(x *HistoryGCRequest) { x.MaxRows = HistoryGCMaxRows + 1 },
		"zero bytes": func(x *HistoryGCRequest) { x.MaxBytes = 0 }, "too many bytes": func(x *HistoryGCRequest) { x.MaxBytes = HistoryGCMaxBytes + 1 },
	} {
		t.Run(name, func(t *testing.T) { x := q; mutate(&x); historyGCRequireInvalid(t, x.Validate()) })
	}
	for _, bad := range []string{"", "bad\x00text", string([]byte{0xff}), strings.Repeat("x", MaxTextBytes+1)} {
		for _, field := range []string{"namespace", "run", "plan"} {
			t.Run(field+"/"+hex.EncodeToString([]byte(bad[:min(len(bad), 8)])), func(t *testing.T) {
				x := q
				switch field {
				case "namespace":
					x.Identity.Namespace = bad
				case "run":
					x.Identity.RunID = bad
				case "plan":
					x.PlanID = bad
				}
				historyGCRequireInvalid(t, x.Validate())
				historyGCRequireInvalid(t, (HistoryGCExecute{Identity: x.Identity, PlanID: x.PlanID, PlanDigest: p.PlanDigest}).Validate())
			})
		}
	}
	q.MaxRows = HistoryGCMaxRows
	q.MaxBytes = HistoryGCMaxBytes
	if err := q.Validate(); err != nil {
		t.Fatal("inclusive budgets", err)
	}
}

func TestHistoryGCPlanDigestBindsEveryObservation(t *testing.T) {
	q, p, _, _ := historyGCFixture(t)
	mutations := map[string]func(*HistoryGCPlan){
		"request digest": func(x *HistoryGCPlan) { x.RequestDigest = strings.Repeat("f", 64) },
		"head revision":  func(x *HistoryGCPlan) { x.HeadRevision++ }, "head chain": func(x *HistoryGCPlan) { x.HeadChain = strings.Repeat("f", 64) },
		"generation": func(x *HistoryGCPlan) { x.SnapshotGeneration++ }, "snapshot revision": func(x *HistoryGCPlan) { x.SnapshotRevision++ },
		"snapshot chain": func(x *HistoryGCPlan) { x.SnapshotChain = strings.Repeat("f", 64) }, "snapshot digest": func(x *HistoryGCPlan) { x.SnapshotDigest = strings.Repeat("f", 64) },
		"before floor": func(x *HistoryGCPlan) { x.BeforeFloor++ }, "after floor": func(x *HistoryGCPlan) { x.AfterFloor++ },
		"first": func(x *HistoryGCPlan) { x.FirstRevision++ }, "through": func(x *HistoryGCPlan) { x.ThroughRevision++ },
		"prefix": func(x *HistoryGCPlan) { x.PrefixDigest = strings.Repeat("f", 64) }, "rows": func(x *HistoryGCPlan) { x.Rows++ }, "bytes": func(x *HistoryGCPlan) { x.Bytes++ },
		"prepared time": func(x *HistoryGCPlan) { x.PreparedAt = x.PreparedAt.Add(time.Microsecond) },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			x := p
			mutate(&x)
			if x.Digest() == p.Digest() {
				t.Fatal("digest did not bind observation")
			}
			historyGCRequireInvalid(t, x.Validate(q))
		})
	}
	x := p
	x.PlanDigest = strings.Repeat("f", 64)
	if x.Digest() != p.Digest() {
		t.Fatal("plan digest must exclude itself")
	}
	historyGCRequireInvalid(t, x.Validate(q))
}

func TestHistoryGCPlanRejectsResignedInvalidRanges(t *testing.T) {
	q, p, _, _ := historyGCFixture(t)
	mutations := map[string]func(*HistoryGCPlan){
		"generation one": func(x *HistoryGCPlan) { x.SnapshotGeneration = 1 }, "generation overflow": func(x *HistoryGCPlan) { x.SnapshotGeneration = uint64(math.MaxInt64) + 1 },
		"snapshot one": func(x *HistoryGCPlan) { x.SnapshotRevision = 1 }, "head below snapshot": func(x *HistoryGCPlan) { x.HeadRevision = x.SnapshotRevision - 1 },
		"zero floor": func(x *HistoryGCPlan) { x.BeforeFloor = 0 }, "floor regression": func(x *HistoryGCPlan) { x.BeforeFloor = x.SnapshotRevision + 1 }, "after not snapshot": func(x *HistoryGCPlan) { x.AfterFloor-- },
		"first includes base": func(x *HistoryGCPlan) { x.FirstRevision = 1 }, "reversed range": func(x *HistoryGCPlan) { x.ThroughRevision = x.FirstRevision - 1 },
		"through snapshot": func(x *HistoryGCPlan) {
			x.ThroughRevision = x.SnapshotRevision
			x.Rows = x.ThroughRevision - x.FirstRevision + 1
		},
		"zero rows": func(x *HistoryGCPlan) { x.Rows = 0 }, "range count": func(x *HistoryGCPlan) { x.Rows-- },
		"row budget":      func(x *HistoryGCPlan) { x.Request.MaxRows = 3; x.RequestDigest = x.Request.Digest() },
		"wire below rows": func(x *HistoryGCPlan) { x.Bytes = x.Rows - 1 }, "byte budget": func(x *HistoryGCPlan) { x.Bytes = q.MaxBytes + 1 },
		"zero time": func(x *HistoryGCPlan) { x.PreparedAt = time.Time{} }, "submicro time": func(x *HistoryGCPlan) { x.PreparedAt = x.PreparedAt.Add(time.Nanosecond) },
		"year zero": func(x *HistoryGCPlan) { x.PreparedAt = time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC) }, "year 10000": func(x *HistoryGCPlan) { x.PreparedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) },
		"bad head chain": func(x *HistoryGCPlan) { x.HeadChain = "" }, "bad snapshot chain": func(x *HistoryGCPlan) { x.SnapshotChain = "" }, "bad snapshot digest": func(x *HistoryGCPlan) { x.SnapshotDigest = "" }, "bad prefix": func(x *HistoryGCPlan) { x.PrefixDigest = "" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			x := p
			mutate(&x)
			x.PlanDigest = x.Digest()
			historyGCRequireInvalid(t, x.Validate(x.Request))
		})
	}
	// floor/head 相等和单行单字节范围均合法，不能因边界差一错误被拒绝。
	x := p
	x.BeforeFloor = x.SnapshotRevision
	x.HeadRevision = x.SnapshotRevision
	x.FirstRevision = x.SnapshotRevision - 1
	x.ThroughRevision = x.FirstRevision
	x.Rows = 1
	x.Bytes = 1
	x.PlanDigest = x.Digest()
	if err := x.Validate(q); err != nil {
		t.Fatal("inclusive control", err)
	}
}

func TestHistoryGCReceiptAndResultBindings(t *testing.T) {
	_, p, e, r := historyGCFixture(t)
	mutations := map[string]func(*HistoryGCReceipt){
		"namespace": func(x *HistoryGCReceipt) { x.Identity.Namespace = "other" }, "run": func(x *HistoryGCReceipt) { x.Identity.RunID = "other" }, "plan ID": func(x *HistoryGCReceipt) { x.PlanID = "other" }, "plan digest": func(x *HistoryGCReceipt) { x.PlanDigest = strings.Repeat("f", 64) },
		"rows": func(x *HistoryGCReceipt) { x.Rows++ }, "bytes": func(x *HistoryGCReceipt) { x.Bytes++ }, "floor": func(x *HistoryGCReceipt) { x.HistoryFloor++ },
		"later time": func(x *HistoryGCReceipt) { x.OccurredAt = x.OccurredAt.Add(time.Microsecond) },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			x := r
			mutate(&x)
			if x.Digest() == r.Digest() {
				t.Fatal("receipt digest did not bind field")
			}
			historyGCRequireInvalid(t, x.Validate(p))
			x.ReceiptDigest = x.Digest()
			if name == "later time" {
				if err := x.Validate(p); err != nil {
					t.Fatal(err)
				}
			} else {
				historyGCRequireInvalid(t, x.Validate(p))
			}
		})
	}
	for _, at := range []time.Time{{}, p.PreparedAt.Add(-time.Microsecond), r.OccurredAt.Add(time.Nanosecond), time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)} {
		x := r
		x.OccurredAt = at
		x.ReceiptDigest = x.Digest()
		historyGCRequireInvalid(t, x.Validate(p))
	}
	x := r
	x.OccurredAt = p.PreparedAt
	x.ReceiptDigest = x.Digest()
	if err := x.Validate(p); err != nil {
		t.Fatal("equal receipt time", err)
	}
	x = r
	x.ReceiptDigest = "bad"
	historyGCRequireInvalid(t, x.Validate(p))
	for name, result := range map[string]HistoryGCResult{
		"zero": {}, "unknown": {State: "authorized", Plan: &p}, "absent plan": {State: HistoryGCAbsent, Plan: &p}, "absent receipt": {State: HistoryGCAbsent, Receipt: &r},
		"prepared nil": {State: HistoryGCPrepared}, "prepared receipt": {State: HistoryGCPrepared, Plan: &p, Receipt: &r}, "executed nil plan": {State: HistoryGCExecuted, Receipt: &r}, "executed nil receipt": {State: HistoryGCExecuted, Plan: &p},
	} {
		t.Run(name, func(t *testing.T) { historyGCRequireInvalid(t, result.Validate(e)) })
	}
	if err := (HistoryGCResult{State: HistoryGCAbsent}).Validate(e); err != nil {
		t.Fatal("absent observation", err)
	}
	// absent 观察仍要求合法查询身份，不能转换为执行授权。
	bad := e
	bad.PlanDigest = ""
	historyGCRequireInvalid(t, (HistoryGCResult{State: HistoryGCAbsent}).Validate(bad))
	for name, mutate := range map[string]func(*HistoryGCExecute){"namespace": func(x *HistoryGCExecute) { x.Identity.Namespace = "other" }, "run": func(x *HistoryGCExecute) { x.Identity.RunID = "other" }, "plan": func(x *HistoryGCExecute) { x.PlanID = "other" }, "digest": func(x *HistoryGCExecute) { x.PlanDigest = strings.Repeat("f", 64) }} {
		t.Run("result/"+name, func(t *testing.T) {
			x := e
			mutate(&x)
			if err := x.Validate(); err != nil {
				t.Fatal(err)
			}
			historyGCRequireInvalid(t, (HistoryGCResult{State: HistoryGCPrepared, Plan: &p}).Validate(x))
			historyGCRequireInvalid(t, (HistoryGCResult{State: HistoryGCExecuted, Plan: &p, Receipt: &r}).Validate(x))
		})
	}
	badPlan := p
	badPlan.Bytes++
	historyGCRequireInvalid(t, r.Validate(badPlan))
	historyGCRequireInvalid(t, (HistoryGCResult{State: HistoryGCPrepared, Plan: &badPlan}).Validate(e))
	badReceipt := r
	badReceipt.Bytes++
	historyGCRequireInvalid(t, (HistoryGCResult{State: HistoryGCExecuted, Plan: &p, Receipt: &badReceipt}).Validate(e))
}

func TestHistoryGCQueryRecoversUnknownPreparation(t *testing.T) {
	q, p, _, r := historyGCFixture(t)
	query := HistoryGCQuery{Identity: q.Identity, PlanID: q.PlanID, RequestDigest: q.Digest()}
	if err := query.Validate(); err != nil {
		t.Fatal(err)
	}
	// 准备响应丢失的调用方仅知道请求摘要，不需要已丢失的计划摘要。
	for _, result := range []HistoryGCResult{{State: HistoryGCPrepared, Plan: &p}, {State: HistoryGCExecuted, Plan: &p, Receipt: &r}, {State: HistoryGCAbsent}} {
		if err := result.ValidateQuery(query); err != nil {
			t.Fatal("valid query control", err)
		}
	}
	for name, change := range map[string]func(*HistoryGCQuery){
		"namespace": func(x *HistoryGCQuery) { x.Identity.Namespace = "other" }, "run": func(x *HistoryGCQuery) { x.Identity.RunID = "other" }, "plan": func(x *HistoryGCQuery) { x.PlanID = "other" }, "request digest": func(x *HistoryGCQuery) { x.RequestDigest = strings.Repeat("f", 64) },
	} {
		t.Run("query/"+name, func(t *testing.T) {
			x := query
			change(&x)
			if x.Validate() != nil {
				t.Fatal("mutation must remain well formed")
			}
			for _, result := range []HistoryGCResult{{State: HistoryGCPrepared, Plan: &p}, {State: HistoryGCExecuted, Plan: &p, Receipt: &r}} {
				historyGCRequireInvalid(t, result.ValidateQuery(x))
			}
		})
	}
	for _, bad := range []string{"", "bad\x00text", string([]byte{0xff})} {
		x := query
		x.Identity.Namespace = bad
		historyGCRequireInvalid(t, x.Validate())
		x = query
		x.Identity.RunID = bad
		historyGCRequireInvalid(t, x.Validate())
		x = query
		x.PlanID = bad
		historyGCRequireInvalid(t, x.Validate())
		x = query
		x.RequestDigest = bad
		historyGCRequireInvalid(t, x.Validate())
		historyGCRequireInvalid(t, (HistoryGCResult{State: HistoryGCAbsent}).ValidateQuery(x))
	}
	for _, result := range []HistoryGCResult{{}, {State: "bad"}, {State: HistoryGCPrepared}, {State: HistoryGCExecuted, Plan: &p}, {State: HistoryGCAbsent, Plan: &p}, {State: HistoryGCAbsent, Receipt: &r}} {
		historyGCRequireInvalid(t, result.ValidateQuery(query))
	}
}

func TestHistoryGCDigestEscapingAndUint64Precision(t *testing.T) {
	q, p, _, _ := historyGCFixture(t)
	// 独立构造已知传输表示，防止误用分隔符拼接或对象编码。
	q.Identity.Namespace = "ns\"\\\n<>&\u2028"
	q.Identity.RunID = "run,:[\"]"
	q.PlanID = "plan\t/"
	if err := q.Validate(); err != nil {
		t.Fatal("escaped text control", err)
	}
	fields := []string{"pulse-durable/history-gc/request/v1", q.Identity.Namespace, q.Identity.RunID, q.PlanID, "7", q.PolicyDigest, "4", "100"}
	wire, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(wire)
	if q.Digest() != hex.EncodeToString(sum[:]) {
		t.Fatal("request digest does not hash JSON string array")
	}
	a := q
	b := q
	a.Identity.Namespace = "a:b"
	a.Identity.RunID = "c"
	b.Identity.Namespace = "a"
	b.Identity.RunID = "b:c"
	if a.Digest() == b.Digest() {
		t.Fatal("identity delimiter collision")
	}
	for _, n := range []uint64{1 << 53, 1<<53 + 1, math.MaxUint64 - 1, math.MaxUint64} {
		x := q
		x.PolicyVersion = n
		if err := x.Validate(); err != nil {
			t.Fatal("uint64 version", err)
		}
		y := x
		y.PolicyVersion = n - 1
		if x.Digest() == y.Digest() {
			t.Fatalf("policy version loses uint64 precision: %d", n)
		}
		plan := p
		plan.Request = x
		plan.RequestDigest = x.Digest()
		plan.HeadRevision = n
		plan.SnapshotRevision = n
		plan.BeforeFloor = 1
		plan.AfterFloor = n
		plan.FirstRevision = n - 2
		plan.ThroughRevision = n - 1
		plan.Rows = 2
		plan.PlanDigest = plan.Digest()
		if err := plan.Validate(x); err != nil {
			t.Fatalf("large revision control %d: %v", n, err)
		}
		adjacent := plan
		adjacent.HeadRevision--
		if adjacent.Digest() == plan.Digest() {
			t.Fatal("head precision lost")
		}
		receipt := HistoryGCReceipt{Identity: x.Identity, PlanID: x.PlanID, PlanDigest: plan.PlanDigest, Rows: plan.Rows, Bytes: plan.Bytes, HistoryFloor: n, OccurredAt: plan.PreparedAt}
		receipt.ReceiptDigest = receipt.Digest()
		if err := receipt.Validate(plan); err != nil {
			t.Fatal(err)
		}
		other := receipt
		other.HistoryFloor--
		if other.Digest() == receipt.Digest() {
			t.Fatal("floor precision lost")
		}
	}
	// 不同时区的同一时刻必须产生相同的微秒摘要。
	zone := time.FixedZone("offset", 8*60*60)
	other := p
	other.PreparedAt = p.PreparedAt.In(zone)
	if other.Digest() != p.Digest() {
		t.Fatal("plan digest depends on timezone")
	}
}
