// 本文件验证 GC 传输边界，不连接数据库；有效控制与恶意 wire 变异独立构造。
package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	gotoken "go/token"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/jackc/pgx/v5/pgconn"
)

func gcAdapterFixture(t *testing.T) (d.HistoryGCPlan, d.HistoryGCReceipt) {
	t.Helper()
	q := d.HistoryGCRequest{Identity: d.Identity{Namespace: "ns", RunID: "run"}, PlanID: "plan", PolicyVersion: 18446744073709551615, PolicyDigest: strings.Repeat("a", 64), MaxRows: 4, MaxBytes: 100}
	p := d.HistoryGCPlan{Request: q, RequestDigest: q.Digest(), HeadRevision: 9007199254740993, HeadChain: strings.Repeat("b", 64), SnapshotGeneration: 2, SnapshotRevision: 10, SnapshotChain: strings.Repeat("c", 64), SnapshotDigest: strings.Repeat("d", 64), BeforeFloor: 1, AfterFloor: 10, FirstRevision: 2, ThroughRevision: 5, PrefixDigest: strings.Repeat("e", 64), Rows: 4, Bytes: 40, PreparedAt: time.Date(2026, 11, 5, 1, 0, 0, 123456000, time.UTC)}
	p.PlanDigest = p.Digest()
	r := d.HistoryGCReceipt{Identity: q.Identity, PlanID: q.PlanID, PlanDigest: p.PlanDigest, Rows: p.Rows, Bytes: p.Bytes, HistoryFloor: p.AfterFloor, OccurredAt: p.PreparedAt.Add(time.Microsecond)}
	r.ReceiptDigest = r.Digest()
	if p.Validate(q) != nil || r.Validate(p) != nil {
		t.Fatal("invalid fixture")
	}
	return p, r
}

// 从公共对象产生独立 wire：UseNumber 保留十进制原文，再递归转成字符串，
// 不使用适配器内部 wire 结构体构造期望，防止测试复制同一字段映射错误。
func gcAdapterWire(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var tree any
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err = dec.Decode(&tree); err != nil {
		t.Fatal(err)
	}
	var convert func(any) any
	convert = func(v any) any {
		switch x := v.(type) {
		case json.Number:
			return string(x)
		case map[string]any:
			for k, v := range x {
				x[k] = convert(v)
			}
		case []any:
			for i, v := range x {
				x[i] = convert(v)
			}
		}
		return v
	}
	b, err = json.Marshal(convert(tree))
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func gcDecodePlan(b []byte) (d.HistoryGCPlan, error) {
	var w historyGCWirePlan
	if strictPinJSON(b, &w) != nil {
		return d.HistoryGCPlan{}, d.ErrUnknown
	}
	return w.decode()
}

func TestHistoryGCAdapterStrictDecoder(t *testing.T) {
	p, r := gcAdapterFixture(t)
	b := gcAdapterWire(t, p)
	got, err := gcDecodePlan(b)
	if err != nil || !reflect.DeepEqual(got, p) {
		t.Fatalf("valid plan control: %+v %v", got, err)
	}
	mutations := map[string]func(string) string{
		"missing":          func(s string) string { return strings.Replace(s, `"rows":"4",`, "", 1) },
		"duplicate":        func(s string) string { return strings.Replace(s, `"rows":"4"`, `"rows":"4","rows":"4"`, 1) },
		"unknown":          func(s string) string { return strings.Replace(s, `"rows":"4"`, `"extra":true,"rows":"4"`, 1) },
		"alias":            func(s string) string { return strings.Replace(s, `"rows":`, `"Rows":`, 1) },
		"numeric":          func(s string) string { return strings.Replace(s, `"rows":"4"`, `"rows":4`, 1) },
		"null":             func(s string) string { return strings.Replace(s, `"rows":"4"`, `"rows":null`, 1) },
		"nested missing":   func(s string) string { return strings.Replace(s, `"namespace":"ns",`, "", 1) },
		"nested duplicate": func(s string) string { return strings.Replace(s, `"run_id":"run"`, `"run_id":"run","run_id":"run"`, 1) },
		"time":             func(s string) string { return strings.Replace(s, p.PreparedAt.Format(time.RFC3339Nano), "not-time", 1) },
		"submicro": func(s string) string {
			return strings.Replace(s, p.PreparedAt.Format(time.RFC3339Nano), p.PreparedAt.Add(time.Nanosecond).Format(time.RFC3339Nano), 1)
		},
		"digest":         func(s string) string { return strings.Replace(s, p.PlanDigest, strings.Repeat("f", 64), 1) },
		"request digest": func(s string) string { return strings.Replace(s, p.RequestDigest, strings.Repeat("f", 64), 1) },
		"trailing":       func(s string) string { return s + ` {}` },
	}
	for _, n := range []string{"04", "+4", "-4", "4.0", "4e0", " 4", "0", "18446744073709551616"} {
		n := n
		mutations["integer/"+n] = func(s string) string { return strings.Replace(s, `"rows":"4"`, `"rows":"`+n+`"`, 1) }
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			bad := mutate(string(b))
			if bad == string(b) {
				t.Fatal("mutation did not apply")
			}
			if _, err := gcDecodePlan([]byte(bad)); !errors.Is(err, d.ErrUnknown) {
				t.Fatalf("malicious wire accepted: %v", err)
			}
		})
	}
	executed := d.HistoryGCResult{State: d.HistoryGCExecuted, Plan: &p, Receipt: &r}
	wire := gcAdapterWire(t, executed)
	out, err := decodeHistoryGCResult(wire)
	e := d.HistoryGCExecute{Identity: p.Request.Identity, PlanID: p.Request.PlanID, PlanDigest: p.PlanDigest}
	if err != nil || out.Validate(e) != nil || !reflect.DeepEqual(out, executed) {
		t.Fatalf("valid executed control: %+v %v", out, err)
	}
	for name, mutate := range map[string]func(string) string{
		"receipt digest": func(s string) string { return strings.Replace(s, r.ReceiptDigest, strings.Repeat("f", 64), 1) },
		"receipt rows": func(s string) string {
			return strings.Replace(s, `"receipt":{"bytes":"40"`, `"receipt":{"bytes":"41"`, 1)
		},
		"receipt missing": func(s string) string { return strings.Replace(s, `"receipt_digest":"`+r.ReceiptDigest+`",`, "", 1) },
	} {
		t.Run(name, func(t *testing.T) {
			bad := mutate(string(wire))
			if bad == string(wire) {
				t.Fatal("mutation did not apply")
			}
			if _, err := decodeHistoryGCResult([]byte(bad)); !errors.Is(err, d.ErrUnknown) {
				t.Fatalf("receipt accepted: %v", err)
			}
		})
	}
	for _, change := range []func(*d.HistoryGCExecute){func(x *d.HistoryGCExecute) { x.Identity.Namespace = "other" }, func(x *d.HistoryGCExecute) { x.PlanID = "other" }, func(x *d.HistoryGCExecute) { x.PlanDigest = strings.Repeat("f", 64) }} {
		x := e
		change(&x)
		if out.Validate(x) == nil {
			t.Fatal("execute input not bound")
		}
	}
	for _, state := range []d.HistoryGCState{d.HistoryGCPrepared, d.HistoryGCAbsent, "bad"} {
		x := out
		x.State = state
		if x.Validate(e) == nil {
			t.Fatalf("illegal state accepted %s", state)
		}
	}
}

func TestHistoryGCAdapterWhitelistAndNil(t *testing.T) {
	// AST 检查完整函数内的 SQL 常量，避免仅证明不存在连接时提前拒绝。
	file, err := parser.ParseFile(gotoken.NewFileSet(), "history_gc.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var sqls []string
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "historyGCCall" {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			lit, ok := n.(*ast.BasicLit)
			if ok && lit.Kind == gotoken.STRING {
				s, err := strconv.Unquote(lit.Value)
				if err == nil && strings.HasPrefix(s, "SELECT ") {
					sqls = append(sqls, s)
				}
			}
			return true
		})
	}
	want := []string{"SELECT pulse_durable_distributed.history_gc_prepare($1::jsonb)", "SELECT pulse_durable_distributed.history_gc_execute($1::jsonb)", "SELECT pulse_durable_distributed.history_gc_find($1::jsonb)"}
	if !reflect.DeepEqual(sqls, want) {
		t.Fatalf("SQL whitelist: %v", sqls)
	}
	var c *Coordinator
	a := map[string]any{"namespace": "ns", "sentinel": "unchanged"}
	before := gcAdapterWire(t, a)
	for _, op := range []string{"prepare", "execute", "find", "unknown", "prepare; DROP TABLE runs"} {
		if _, err := c.historyGCCall(context.Background(), op, a, true); !errors.Is(err, d.ErrInvalid) {
			t.Fatalf("nil/unknown %q: %v", op, err)
		}
	}
	if !bytes.Equal(before, gcAdapterWire(t, a)) {
		t.Fatal("request map modified")
	}
	p, _ := gcAdapterFixture(t)
	if _, err := c.PrepareHistoryGC(context.Background(), p.Request); !errors.Is(err, d.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := c.ExecuteHistoryGC(context.Background(), d.HistoryGCExecute{Identity: p.Request.Identity, PlanID: p.Request.PlanID, PlanDigest: p.PlanDigest}); !errors.Is(err, d.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := c.FindHistoryGC(context.Background(), d.HistoryGCQuery{Identity: p.Request.Identity, PlanID: p.Request.PlanID, RequestDigest: p.RequestDigest}); !errors.Is(err, d.ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := c.historyGCCall(nil, "find", a, false); !errors.Is(err, d.ErrInvalid) {
		t.Fatal(err)
	}
}

func TestHistoryGCAdapterErrorSemantics(t *testing.T) {
	const secret = "SECRET-password-raw-SQL"
	for code, want := range map[string]error{"HG001": d.ErrInvalid, "HG002": d.ErrConflict, "HG003": d.ErrPinned, "HG004": d.ErrHistoryExpired, "HG005": d.ErrUnknown, "HG006": d.ErrNotFound, "PD007": d.ErrClockRegression} {
		for _, write := range []bool{false, true} {
			err := historyGCError(&pgconn.PgError{Code: code, Message: secret, Detail: secret}, write)
			if !errors.Is(err, want) || strings.Contains(err.Error(), secret) {
				t.Fatalf("%s write=%v: %v", code, write, err)
			}
		}
	}
	if historyGCError(nil, true) != nil {
		t.Fatal("nil error changed")
	}
	lost := errors.New(secret)
	if !errors.Is(historyGCError(lost, true), d.ErrUnknown) {
		t.Fatal("lost write response is not unknown")
	}
	if err := historyGCError(lost, false); err == nil || errors.Is(err, d.ErrUnknown) || strings.Contains(err.Error(), secret) {
		t.Fatalf("read error: %v", err)
	}
	if err := historyGCError(&pgconn.PgError{Code: "XX000", Message: secret}, true); err == nil || strings.Contains(err.Error(), secret) {
		t.Fatalf("unmapped SQL error leaked: %v", err)
	}
	if !errors.Is(historyGCError(context.Canceled, false), context.Canceled) {
		t.Fatal("read cancellation lost")
	}
}
