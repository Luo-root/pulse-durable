package durable

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Luo-root/pulse"
)

// 固定脱敏样例由 Durable b4805d5 + Pulse 1f67045d6c42 生成。
// 不由当前编码器动态制造“旧记录”；description 也属于旧定义指纹。
const legacyFormatOne = `{
  "format": 1,
  "run_id": "legacy-format-one",
  "revision": 3,
  "version": "fixture/1",
  "definition": "353c64e7cb227c50143a57036bf485f94d710d9512f9ca026791aeda82eb31dc",
  "inputs": "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945",
  "root_scope": "c2ca14f87b4791abd5df80676ca63060",
  "scopes": {
    "c2ca14f87b4791abd5df80676ca63060": {
      "graph": "legacy-format-one",
      "path": "",
      "inputs": "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945",
      "nodes": {
        "source": {
          "node_id": "source",
          "attempt": "599f9e4683c2b75a4e9a864bd36af9c3",
          "reason": "completed",
          "outputs": [{"name":"answer","type_id":"go/int","version":"1","state":"ready","digest":"73475cb40a568e8da8a045ced110137e159f890ac4da883b6b17dc651b3a8049","data":"NDI="}]
        }
      }
    }
  },
  "attempts": {
    "599f9e4683c2b75a4e9a864bd36af9c3": {"parent":"c2ca14f87b4791abd5df80676ca63060","node_id":"source","boundary":"","index":1,"limit":1,"state":"confirmed"}
  }
}`

func TestResumeFixedFormatOneRecord(t *testing.T) {
	var record RunRecord
	if err := json.Unmarshal([]byte(legacyFormatOne), &record); err != nil {
		t.Fatal(err)
	}
	store := &MemoryStore{records: map[string]RunRecord{record.RunID: record}}
	g, err := pulse.New("legacy-format-one", "旧格式完整节点恢复")
	if err != nil {
		t.Fatal(err)
	}
	answer := pulse.NewKey[int]("answer")
	if err := g.DeclareOutput(pulse.NewPort(answer)); err != nil {
		t.Fatal(err)
	}
	testAdd(t, g, pulse.NewNode("source", "生成原始格式fixture", nil, pulse.Provides(answer), func(*pulse.RunCtx) error { t.Error("confirmed legacy source reran"); return nil }))
	e, err := testRuntime(t, store, NewCodecs()).Resume(context.Background(), g, pulse.Inputs{}, RunOptions{RunID: record.RunID, Version: record.Version})
	if err != nil {
		t.Fatal(err)
	}
	if value, state, err := pulse.ReadOutput(e, answer); err != nil || value != 42 || state != pulse.SlotStateReady {
		t.Fatalf("legacy value=%d state=%s err=%v", value, state, err)
	}
}

func TestFormatOneRejectsIncrementalFields(t *testing.T) {
	p := newStreamProtocol(t)
	p.s.record.Format = 1
	if err := p.s.validateLoaded(); err == nil {
		t.Fatal("format 1 accepted incremental stream fields")
	}
}
