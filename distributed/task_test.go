package distributed

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)

func fixture(t *testing.T) (Assembly, pulse.Inputs) {
	t.Helper()
	g, err := pulse.New("portable", "finite inputs")
	if err != nil {
		t.Fatal(err)
	}
	k := pulse.NewKey[[]byte]("value")
	if err = g.DeclareInput(pulse.NewPort(k)); err != nil {
		t.Fatal(err)
	}
	if err = g.Add(pulse.NewNode("consume", "no preparation execution", pulse.Requires(k), nil, func(*pulse.RunCtx) error { panic("BUSINESS MUST NOT RUN") })); err != nil {
		t.Fatal(err)
	}
	keys := pulse.NewRegistry()
	if err = pulse.RegisterKey(keys, k); err != nil {
		t.Fatal(err)
	}
	var in pulse.Inputs
	if err = pulse.Seed(&in, k, []byte(nil)); err != nil {
		t.Fatal(err)
	}
	return Assembly{g, durable.NewCodecs(), keys}, in
}
func taskFixture(t *testing.T) Task {
	t.Helper()
	a, in := fixture(t)
	task, err := NewTask(context.Background(), Identity{"tenant", "run"}, "test", "v1", a.Graph, in, a.Codecs)
	if err != nil {
		t.Fatal(err)
	}
	return task
}
func registerFixture(t *testing.T) (*Registry, Route) {
	t.Helper()
	r := &Registry{}
	route, err := r.Register("test", "v1", func() (Assembly, error) { a, _ := fixture(t); return a, nil })
	if err != nil {
		t.Fatal(err)
	}
	return r, route
}
func TestCanonicalImmutableAndDuplicate(t *testing.T) {
	task := taskFixture(t)
	other := taskFixture(t)
	if !bytes.Equal(task.Bytes(), other.Bytes()) {
		t.Fatal("not deterministic")
	}
	wire := task.Bytes()
	wire[0] = 'x'
	p := task.Payload()
	p[0] = 'x'
	m := task.CodecManifest()
	m[0].TypeID = "changed"
	if _, err := ParseTask(task.Bytes()); err != nil {
		t.Fatal("accessor mutated task", err)
	}
	if dup, err := CompareSubmission(task, other); !dup || err != nil {
		t.Fatal(dup, err)
	}
	d := task.data()
	d.Route.BusinessVersion = "v2"
	b, _ := json.Marshal(d)
	conflict, err := ParseTask(b)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = CompareSubmission(task, conflict); !errors.Is(err, ErrSubmitConflict) {
		t.Fatal(err)
	}
	if _, err = CompareSubmission(Task{}, task); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

// 新封装的版本只在顶层出现；业务 Route 固定为四个字符串字段。
// 同时对照 Prepare 的原始字节，避免只改封装却悄悄改变输入编码。
func TestTaskEnvelopeKeepsVersionsOutsideRoute(t *testing.T) {
	a, in := fixture(t)
	prepared, err := durable.Prepare(context.Background(), a.Graph, in, a.Codecs)
	if err != nil {
		t.Fatal(err)
	}
	task := taskFixture(t)
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(task.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if Protocol != 2 || PayloadFormat != 1 || len(envelope) != 8 || string(envelope["protocol"]) != "2" || string(envelope["payload_format"]) != "1" {
		t.Fatal("unexpected envelope versions or fields")
	}
	var fields map[string]string
	if err := json.Unmarshal(envelope["route"], &fields); err != nil {
		t.Fatal(err)
	}
	route := task.Route()
	want := map[string]string{"task_type": "test", "business_version": "v1", "definition": prepared.Definition, "manifest_digest": route.ManifestDigest}
	if len(fields) != len(want) {
		t.Fatal("route contains non-business fields")
	}
	for name, value := range want {
		if fields[name] != value {
			t.Fatalf("route field %s differs", name)
		}
	}
	if !bytes.Equal(task.Payload(), prepared.Payload) || task.InputsFingerprint() != prepared.Inputs {
		t.Fatal("envelope migration changed canonical input bytes")
	}
}

// 这是旧协议的冻结字节：版本仍在 route 内，不能随着当前 DTO 重新编码。
// 空清单和 [] 输入的摘要均有效，用于确保拒绝原因不依赖损坏的载荷。
const legacyTaskWireV1 = `{"identity":{"namespace":"tenant","run_id":"run"},"route":{"task_type":"test","business_version":"v1","definition":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","manifest_digest":"4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945","protocol":1,"record_format":2,"payload_format":1},"codec_manifest":[],"inputs":"4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945","payload":"W10=","payload_digest":"4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"}`

func TestTaskEnvelopeRejectsVersionsBeforeAssembly(t *testing.T) {
	task := taskFixture(t)
	r := &Registry{}
	calls := 0
	if _, err := r.Register("test", "v1", func() (Assembly, error) {
		calls++
		a, _ := fixture(t)
		return a, nil
	}); err != nil {
		t.Fatal(err)
	}
	calls = 0
	wires := map[string][]byte{
		"legacy frozen task":     []byte(legacyTaskWireV1),
		"missing protocol":       bytes.Replace(task.Bytes(), []byte(`"protocol":2,`), nil, 1),
		"missing payload format": bytes.Replace(task.Bytes(), []byte(`"payload_format":1,`), nil, 1),
		// 即使顶层版本正确，旧的 route 字段也必须单独拒绝。
		"route record format":  bytes.Replace(task.Bytes(), []byte(`"route":{`), []byte(`"route":{"record_format":2,`), 1),
		"route protocol":       bytes.Replace(task.Bytes(), []byte(`"route":{`), []byte(`"route":{"protocol":2,`), 1),
		"route payload format": bytes.Replace(task.Bytes(), []byte(`"route":{`), []byte(`"route":{"payload_format":1,`), 1),
	}
	for name, mutate := range map[string]func(*descriptor){
		"old protocol":          func(d *descriptor) { d.Protocol = 1 },
		"future protocol":       func(d *descriptor) { d.Protocol = 3 },
		"zero payload format":   func(d *descriptor) { d.PayloadFormat = 0 },
		"future payload format": func(d *descriptor) { d.PayloadFormat = 2 },
	} {
		d := task.data()
		mutate(&d)
		wire, err := json.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		wires[name] = wire
	}
	for name, wire := range wires {
		t.Run(name, func(t *testing.T) {
			before := append([]byte(nil), wire...)
			if parsed, err := ParseTask(wire); !errors.Is(err, ErrInvalid) || parsed != (Task{}) {
				t.Fatalf("invalid envelope accepted: %v", err)
			}
			// 伪造私有 Task 以覆盖从存储恢复损坏任务时的 Resolve 入口；
			// 该入口必须再次验证字节，不能因路由已注册而调用 Factory。
			if _, _, err := r.Resolve(context.Background(), Task{wire: string(wire), digest: hash(wire)}); !errors.Is(err, ErrInvalid) {
				t.Fatalf("resolve accepted invalid envelope: %v", err)
			}
			if calls != 0 || !bytes.Equal(wire, before) {
				t.Fatal("rejection assembled business or changed input bytes")
			}
		})
	}
}

func TestCanonicalCorruptionAndLimits(t *testing.T) {
	task := taskFixture(t)
	for _, wire := range [][]byte{append(task.Bytes(), ' '), append([]byte(" "), task.Bytes()...), append(task.Bytes(), task.Bytes()...), bytes.Replace(task.Bytes(), []byte(`"identity":`), []byte(`"unknown":0,"identity":`), 1), bytes.Replace(task.Bytes(), []byte(`"run_id":"run"`), []byte(`"run_id":"run","run_id":"run"`), 1), bytes.Repeat([]byte("x"), MaxEnvelopeBytes+1)} {
		if _, err := ParseTask(wire); err == nil {
			t.Fatal("accepted non-canonical bytes")
		}
	}
	for name, mutate := range map[string]func(*descriptor){
		"payload digest":  func(d *descriptor) { d.Payload[0] ^= 1 },
		"manifest digest": func(d *descriptor) { d.Manifest[0].Version = "2" },
		"protocol":        func(d *descriptor) { d.Protocol++ },
		"payload format":  func(d *descriptor) { d.PayloadFormat++ },
		"long name":       func(d *descriptor) { d.Identity.RunID = strings.Repeat("x", MaxTextBytes+1) },
		"utf8":            func(d *descriptor) { d.Identity.Namespace = "\xff" },
		"payload size":    func(d *descriptor) { d.Payload = make([]byte, MaxPayloadBytes+1); d.PayloadDigest = hash(d.Payload) },
		"manifest size":   func(d *descriptor) { d.Manifest = make([]durable.CodecBinding, MaxManifestEntries+1) },
	} {
		t.Run(name, func(t *testing.T) {
			d := task.data()
			mutate(&d)
			b, _ := json.Marshal(d)
			if name == "utf8" { // encoding/json 会替换非法 UTF-8：提交时须在 JSON 编码前拒绝。
				a, in := fixture(t)
				if _, err := NewTask(context.Background(), d.Identity, "test", "v1", a.Graph, in, a.Codecs); err == nil {
					t.Fatal("invalid UTF-8 accepted")
				}
				return
			}
			if _, err := ParseTask(b); err == nil {
				t.Fatal("accepted invalid descriptor")
			}
		})
	}
	d := task.data()
	d.Identity.Namespace = strings.Repeat("x", MaxTextBytes)
	d.Identity.RunID = strings.Repeat("y", MaxTextBytes)
	b, _ := json.Marshal(d)
	if _, err := ParseTask(b); err != nil {
		t.Fatal("boundary rejected", err)
	}
}
func TestRegistryExactRoutesAndBadPayload(t *testing.T) {
	r, route := registerFixture(t)
	task := taskFixture(t)
	if route != task.Route() {
		t.Fatal("capability mismatch")
	}
	if _, err := r.Register("test", "v1", func() (Assembly, error) { a, _ := fixture(t); return a, nil }); !errors.Is(err, ErrDuplicate) {
		t.Fatal(err)
	}
	if _, err := r.Register("test", "v2", func() (Assembly, error) { a, _ := fixture(t); return a, nil }); err != nil {
		t.Fatal(err)
	}
	// 能力集合与任务使用同一完整业务路由；返回的切片不能修改注册表。
	caps := r.Capabilities()
	wantCaps := map[Route]bool{route: true}
	v2 := route
	v2.BusinessVersion = "v2"
	wantCaps[v2] = true
	if len(caps) != len(wantCaps) {
		t.Fatal("capability set differs from registered routes")
	}
	for _, capability := range caps {
		if !wantCaps[capability] {
			t.Fatal("unexpected capability")
		}
		delete(wantCaps, capability)
	}
	caps[0] = Route{}
	a, in, err := r.Resolve(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := durable.Prepare(context.Background(), a.Graph, in, a.Codecs)
	if err != nil || !bytes.Equal(prepared.Payload, task.Payload()) {
		t.Fatal("typed nil round trip", err)
	}
	for name, mutate := range map[string]func(*descriptor){
		"task type":  func(d *descriptor) { d.Route.TaskType = "absent" },
		"business":   func(d *descriptor) { d.Route.BusinessVersion = "absent" },
		"definition": func(d *descriptor) { d.Route.Definition = hash([]byte("other")) },
		"codec": func(d *descriptor) {
			d.Manifest[0].Version = "2"
			m, _ := json.Marshal(d.Manifest)
			d.Route.ManifestDigest = hash(m)
		},
		"fingerprint": func(d *descriptor) { d.Inputs = hash([]byte("other")) },
		"payload": func(d *descriptor) {
			d.Payload = []byte(`[{"secret":"DO NOT LEAK"}]`)
			d.PayloadDigest = hash(d.Payload)
		},
	} {
		t.Run(name, func(t *testing.T) {
			d := task.data()
			mutate(&d)
			b, _ := json.Marshal(d)
			bad, err := ParseTask(b)
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = r.Resolve(context.Background(), bad)
			if err == nil || strings.Contains(err.Error(), "SECRET") || strings.Contains(err.Error(), "DO NOT LEAK") {
				t.Fatal("missing rejection or leaked error", err)
			}
		})
	}
}
func TestFactoryFailureSanitized(t *testing.T) {
	for _, f := range []Factory{nil, func() (Assembly, error) { panic("secret") }, func() (Assembly, error) { panic(nil) }, func() (Assembly, error) { return Assembly{}, errors.New("secret") }} {
		if _, err := (&Registry{}).Register("test", "v1", f); err != ErrInvalid {
			t.Fatal(err)
		}
	}
	r := &Registry{}
	calls := 0
	_, err := r.Register("test", "v1", func() (Assembly, error) {
		calls++
		a, _ := fixture(t)
		if calls > 1 {
			a.Graph, _ = pulse.New("changed", "changed")
		}
		return a, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = r.Resolve(context.Background(), taskFixture(t)); err != ErrMismatch {
		t.Fatal("factory drift accepted", err)
	}
}
func TestPullCapacity(t *testing.T) {
	for _, tc := range []struct {
		p            PullPolicy
		active, want int
		bad          bool
	}{
		{PullPolicy{Capacity: 8, BatchMax: 3}, 0, 3, false}, {PullPolicy{Capacity: 8, BatchMax: 3}, 7, 1, false}, {PullPolicy{Capacity: 8, BatchMax: 3}, 8, 0, false},
		{PullPolicy{Capacity: 8, BatchMax: 3, Paused: true}, 0, 0, false}, {PullPolicy{Capacity: 8, BatchMax: 3, Draining: true}, 2, 0, false},
		{PullPolicy{}, 0, 0, true}, {PullPolicy{Capacity: 1, BatchMax: 1}, 2, 0, true}, {PullPolicy{Capacity: 1, BatchMax: 1}, -1, 0, true}, {PullPolicy{Capacity: 1, BatchMax: 1025}, 0, 0, true},
	} {
		n, err := tc.p.Limit(tc.active)
		if n != tc.want || (err != nil) != tc.bad {
			t.Fatal(tc, n, err)
		}
	}
}

// 全新的真实操作系统进程解码 base64 线格式参数，独立组装
// 类型化键与编解码器，并重新计算出完全相同的实际输入。不涉及数据库
// 或工作进程；这不是跨机器或持久化验收。
func TestFreshProcessDecode(t *testing.T) {
	if os.Getenv("PULSE_DISTRIBUTED_DECODE_CHILD") == "1" {
		var wire []byte
		if err := json.Unmarshal([]byte(os.Getenv("PULSE_DISTRIBUTED_WIRE")), &wire); err != nil {
			t.Fatal(err)
		}
		task, err := ParseTask(wire)
		if err != nil {
			t.Fatal(err)
		}
		r, _ := registerFixture(t)
		if _, _, err = r.Resolve(context.Background(), task); err != nil {
			t.Fatal(err)
		}
		return
	}
	wire, _ := json.Marshal(taskFixture(t).Bytes())
	cmd := exec.Command(os.Args[0], "-test.run=^TestFreshProcessDecode$")
	cmd.Env = append(os.Environ(), "PULSE_DISTRIBUTED_DECODE_CHILD=1", "PULSE_DISTRIBUTED_WIRE="+string(wire))
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fresh process decode: %v: %s", err, output)
	}
}
