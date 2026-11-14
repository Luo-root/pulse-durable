// 本文件验收指定位置派生回放的只读计划契约。
// 计划必须绑定来源已提交位置、划清保留/重算集合，并始终不可执行。
package durable

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

// replayPlanFixture 构造一个带完整语义证据、根图含三个已确认节点的来源记录。
// downstream 通过流来源引用 upstream 的成果：只读片不凭记录推断依赖后代，
// 因此 downstream 仍是候选保留；真实依赖后代由执行片持图定义重算。
func replayPlanFixture(t *testing.T) RunRecord {
	t.Helper()
	record, _ := format3Fixture(t)
	record.RunID = "replay-source"
	record.Revision = 2
	record.RootScope = "root"
	upstreamStream := &StreamDescriptorRecord{
		ID:      StreamIDRecord{Scope: "root", NodeID: "upstream", Key: "out", Group: "default", Generation: 1},
		Attempt: strings.Repeat("a", 32), Graph: "graph", Path: "", TypeID: "bytes", Version: "v1",
	}
	record.Scopes = map[string]ScopeRecord{
		"root": {Graph: "graph", Inputs: record.Inputs, Nodes: map[string]NodeRecord{
			"upstream": {NodeID: "upstream", Attempt: strings.Repeat("a", 32), Reason: "completed",
				Outputs: []ValueRecord{{Name: "out", TypeID: "bytes", Version: "v1", State: "ready", Stream: upstreamStream}}},
			"downstream": {NodeID: "downstream", Attempt: strings.Repeat("b", 32), Reason: "completed",
				Outputs: []ValueRecord{{Name: "in", TypeID: "bytes", Version: "v1", State: "ready", Stream: upstreamStream}}},
			"sibling": {NodeID: "sibling", Attempt: strings.Repeat("c", 32), Reason: "completed",
				Outputs: []ValueRecord{{Name: "own", TypeID: "bytes", Version: "v1", State: "ready", Digest: strings.Repeat("d", 64), Data: []byte{1}}}},
		}},
	}
	record.Attempts = map[string]AttemptRecord{}
	record.CommitIndex = []CommitIndexEntry{
		{Revision: 1, ParentRevision: 0, BatchDigest: strings.Repeat("1", 64)},
		{Revision: 2, ParentRevision: 1, BatchDigest: strings.Repeat("2", 64)},
	}
	source := CloneRecord(record)
	source.HistorySummaries, source.CommitIndex, source.Provenance = nil, nil, nil
	payload := []byte(`{"schema":"history-evidence/v1","source":"committed-chain"}`)
	record.HistorySummaries = []HistorySummary{{Kind: historyEvidenceSummaryKind, ID: "origin",
		SourceRevision: 1, SourceDigest: recordDigest(source), Payload: payload, Digest: digest(payload)}}
	p := Provenance{OriginRunID: record.RunID, OriginRevision: 1,
		OriginRecordDigest: recordDigest(source), CompressionID: "replay-plan-fixture"}
	p.Digest = provenanceDigest(p.OriginRunID, p.OriginRevision, p.OriginRecordDigest, p.CompressionID)
	record.Provenance = &p
	if err := ValidateSemanticEvidence(record); err != nil {
		t.Fatalf("fixture evidence: %v", err)
	}
	return record
}

func TestPlanDerivedReplayIsPureAndBound(t *testing.T) {
	record := replayPlanFixture(t)
	before := CloneRecord(record)
	request := ReplayRequest{SourceRunID: record.RunID, ThroughRevision: record.Revision,
		TargetNodeID: "upstream", MaxObjects: 100, MaxBytes: 1 << 20}
	plan, err := PlanDerivedReplay(record, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Validate(request); err != nil {
		t.Fatal(err)
	}
	if plan.Executable {
		t.Fatal("replay plan must not be executable before the execution contract lands")
	}
	// 只读片只确定目标必须重算；其余成果是候选保留，不是复用授权。
	// 真实依赖后代由执行片持图定义重算，因此这里不得凭记录臆断 downstream。
	if !reflect.DeepEqual(plan.RecomputeNodes, []string{"upstream"}) {
		t.Fatalf("recompute set=%v", plan.RecomputeNodes)
	}
	if !reflect.DeepEqual(plan.RetainedNodes, []string{"downstream", "sibling"}) {
		t.Fatalf("candidate retained set=%v", plan.RetainedNodes)
	}
	if plan.Rows != 1 || plan.Cut.ThroughRevision != 2 || plan.Cut.ParentRevision != 1 {
		t.Fatalf("cut/rows mismatch: %+v", plan.Cut)
	}
	if plan.Cut.BatchDigest != record.CommitIndex[1].BatchDigest {
		t.Fatal("cut is not bound to the source commit index entry")
	}
	if plan.PlanDigest != plan.Digest() || plan.RequestDigest != request.Digest() {
		t.Fatal("plan digests are not reproducible")
	}
	if !reflect.DeepEqual(record, before) {
		t.Fatal("planning mutated the source record")
	}
}

func TestPlanDerivedReplayRejectsInvalidSourcesAndTargets(t *testing.T) {
	record := replayPlanFixture(t)
	base := ReplayRequest{SourceRunID: record.RunID, ThroughRevision: record.Revision,
		TargetNodeID: "upstream", MaxObjects: 100, MaxBytes: 1 << 20}
	for name, mutate := range map[string]func(*RunRecord, *ReplayRequest){
		"wrong run":      func(r *RunRecord, q *ReplayRequest) { q.SourceRunID = "other" },
		"stale position": func(r *RunRecord, q *ReplayRequest) { q.ThroughRevision = 1 },
		"empty target":   func(r *RunRecord, q *ReplayRequest) { q.TargetNodeID = "" },
		"unknown target": func(r *RunRecord, q *ReplayRequest) { q.TargetNodeID = "ghost" },
		"no evidence": func(r *RunRecord, q *ReplayRequest) {
			r.HistorySummaries, r.CommitIndex, r.Provenance = nil, nil, nil
		},
		"zero budget": func(r *RunRecord, q *ReplayRequest) { q.MaxObjects = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			r, q := CloneRecord(record), base
			mutate(&r, &q)
			if _, err := PlanDerivedReplay(r, q); err == nil {
				t.Fatal("invalid replay source/target accepted")
			}
		})
	}
	// 目标未确认时没有可切断的成果，必须冲突而不是静默整图重跑。
	unconfirmed := CloneRecord(record)
	root := unconfirmed.Scopes[unconfirmed.RootScope]
	delete(root.Nodes, "upstream")
	unconfirmed.Scopes[unconfirmed.RootScope] = root
	if _, err := PlanDerivedReplay(unconfirmed, base); !errors.Is(err, ErrConflict) {
		t.Fatalf("unconfirmed target accepted: %v", err)
	}
}

func TestReplayPlanValidateRejectsForgedSets(t *testing.T) {
	record := replayPlanFixture(t)
	request := ReplayRequest{SourceRunID: record.RunID, ThroughRevision: record.Revision,
		TargetNodeID: "upstream", MaxObjects: 100, MaxBytes: 1 << 20}
	plan, err := PlanDerivedReplay(record, request)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*ReplayPlan){
		"executable": func(p *ReplayPlan) { p.Executable = true },
		"target dropped": func(p *ReplayPlan) {
			p.RecomputeNodes = []string{"downstream"}
			p.RetainedNodes = []string{"sibling", "upstream"}
			p.Rows = 1
		},
		"overlap": func(p *ReplayPlan) { p.RetainedNodes = append(p.RetainedNodes, "upstream") },
		"unsorted": func(p *ReplayPlan) {
			p.RetainedNodes = []string{"sibling", "downstream"}
		},
		"duplicate": func(p *ReplayPlan) {
			p.RetainedNodes = []string{"downstream", "downstream", "sibling"}
		},
		"cut parent": func(p *ReplayPlan) { p.Cut.ParentRevision = 5 },
		"cut digest": func(p *ReplayPlan) { p.Cut.BatchDigest = strings.Repeat("f", 64) },
		"rows":       func(p *ReplayPlan) { p.Rows = 99 },
	} {
		t.Run(name, func(t *testing.T) {
			forged := plan
			forged.RetainedNodes = append([]string(nil), plan.RetainedNodes...)
			forged.RecomputeNodes = append([]string(nil), plan.RecomputeNodes...)
			mutate(&forged)
			// 重签摘要也不能让被篡改的集合合法：Validate 必须独立拒绝。
			forged.PlanDigest = forged.Digest()
			if err := forged.Validate(request); err == nil {
				t.Fatal("forged replay plan accepted after re-signing")
			}
		})
	}
}
