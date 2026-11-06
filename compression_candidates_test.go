// 本文件用事实摘要与对抗变异验证候选生成；不把候选存在视为可执行压缩。
package durable

import (
	"errors"
	"strings"
	"testing"
)

func TestCompressionCandidatesRejectUnknownAndProtectPrefixes(t *testing.T) {
	record := compressionPlanRecord(t)
	payload := raw(retryPrefixSummaryPayload{LastPrunedID: "a2", Parent: "parent", NodeID: "node", Boundary: "retry", ThroughIndex: 2, Limit: 3, LastPrunedDigest: summaryDigest("retry", record.RunID, record.Revision, "parent", "node", "retry", 2, 3, string(AttemptFailed))})
	record.HistorySummaries[0].Payload = payload
	record.HistorySummaries[0].Digest = digest(payload)
	rows, err := candidateObjectsFromSummary(record, record.HistorySummaries[0])
	if err != nil || len(rows) != 2 {
		t.Fatalf("retry candidates=%v err=%v", rows, err)
	}
	unknown := HistorySummary{Kind: "future_kind", ID: "future", SourceRevision: record.Revision, SourceDigest: compressionSourceDigest(record), Payload: []byte(`{}`), Digest: digest([]byte(`{}`))}
	if _, err := candidateObjectsFromSummary(record, unknown); !errors.Is(err, ErrUnsupportedCapability) {
		t.Fatalf("unknown summary kind accepted: %v", err)
	}
}

func TestCompressionRetryCandidatesRejectForgedFacts(t *testing.T) {
	for _, mode := range []string{"digest", "active", "gap", "duplicate", "limit", "scope-reference", "managed-reference", "oversized-through", "source", "applied"} {
		t.Run(mode, func(t *testing.T) {
			r := compressionPlanRecord(t)
			summary := &r.HistorySummaries[0]
			var payload retryPrefixSummaryPayload
			if err := unmarshalRecord(summary.Payload, &payload); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "digest":
				payload.LastPrunedDigest = strings.Repeat("f", 64)
			case "active":
				a := r.Attempts["a1"]
				a.State = AttemptActive
				r.Attempts["a1"] = a
			case "gap":
				delete(r.Attempts, "a1")
			case "duplicate":
				r.Attempts["copy"] = r.Attempts["a1"]
			case "limit":
				a := r.Attempts["a2"]
				a.Limit++
				r.Attempts["a2"] = a
			case "scope-reference":
				r.Scopes["a1"] = ScopeRecord{}
			case "managed-reference":
				r.Managed = map[string]ManagedRecord{"m": {State: []ValueRecord{{Stream: &StreamDescriptorRecord{Attempt: "a1"}}}}}
			case "oversized-through":
				payload.ThroughIndex = int(^uint(0) >> 1)
				payload.Limit = payload.ThroughIndex
			case "applied":
				payload.AppliedRevision = r.Revision
			case "source":
				summary.SourceDigest = strings.Repeat("c", 64)
			}
			summary.Payload = raw(payload)
			summary.Digest = digest(summary.Payload)
			if mode != "source" {
				summary.SourceDigest = compressionSourceDigest(r)
			}
			if _, err := candidateObjectsFromSummary(r, *summary); err == nil {
				t.Fatal("forged or referenced retry prefix accepted")
			}
		})
	}
}

func compressionMessageRecord(t *testing.T) RunRecord {
	t.Helper()
	r, _ := format3Fixture(t)
	input := StreamIDRecord{Scope: "root", NodeID: "source", Key: "items", Group: "g", Generation: 2}
	r.Streams = map[string]StreamRecord{streamKey(input): {Stream: StreamDescriptorRecord{ID: input}, LastSequence: 3, AckedThrough: 2, Pending: []StreamMessageRecord{{Sequence: 3}}}}
	oldInput := input
	oldInput.Generation = 1
	r.Messages = map[string]MessageAttemptRecord{
		"m1-failed": {Scope: "root", Parent: "proc", NodeID: "consumer", Input: oldInput, Sequence: 1, Index: 1, Limit: 2, State: AttemptFailed},
		"m1":        {Scope: "root", Parent: "proc", NodeID: "consumer", Input: input, Sequence: 1, Index: 2, Limit: 2, State: AttemptConfirmed},
		"m2":        {Scope: "root", Parent: "proc", NodeID: "consumer", Input: input, Sequence: 2, Index: 1, Limit: 2, State: AttemptConfirmed},
		"future":    {Scope: "root", Parent: "proc", NodeID: "consumer", Input: input, Sequence: 3, Index: 1, Limit: 2, State: AttemptActive},
	}
	r.Managed = map[string]ManagedRecord{managedKey("root", "proc", "consumer"): {Scope: "root", Parent: "proc", NodeID: "consumer"}}
	p := messagePrefixSummaryPayload{ProcessingScope: "root", ProcessingAttempt: "proc", NodeID: "consumer", Input: input, ThroughSequence: 2}
	index := indexCompressionCandidates(r)
	_, budget, err := messagePrefixForCompression(r, p, index)
	if err != nil {
		t.Fatal(err)
	}
	p.BudgetDigest = budget
	p.ManagedDigest = compressionManagedDigest(r.Managed[managedKey("root", "proc", "consumer")])
	p.ConfirmationIDs = index.confirmations[managedKey("root", "proc", "consumer")]
	payload := raw(p)
	r.HistorySummaries = []HistorySummary{{Kind: MessagePrefixSummaryKind, ID: "messages", SourceRevision: r.Revision, SourceDigest: compressionSourceDigest(r), Payload: payload, Digest: digest(payload)}}
	return r
}

func TestCompressionMessagePrefixAcknowledgedRetriesAndGeneration(t *testing.T) {
	r := compressionMessageRecord(t)
	request := HistoryCompressionRequest{RunID: r.RunID, ThroughRevision: r.Revision, MaxObjects: 100, MaxBytes: 1 << 20}
	plan, err := PlanHistoryCompression(r, request)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Executable || plan.Rows != 3 {
		t.Fatalf("expected acknowledged retry group only: %+v", plan)
	}
	for _, ref := range plan.DeleteObjects {
		if ref.ID == "future" {
			t.Fatal("unsettled future message selected")
		}
	}
	if err := plan.Validate(request); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"ack", "pending", "active", "gap", "duplicate", "missing-managed", "forged-budget"} {
		t.Run(mode, func(t *testing.T) {
			changed := CloneRecord(r)
			summary := &changed.HistorySummaries[0]
			var p messagePrefixSummaryPayload
			if err := unmarshalRecord(summary.Payload, &p); err != nil {
				t.Fatal(err)
			}
			stream := changed.Streams[streamKey(p.Input)]
			switch mode {
			case "ack":
				stream.AckedThrough = 1
			case "pending":
				stream.Pending = append(stream.Pending, StreamMessageRecord{Sequence: 1})
			case "active":
				m := changed.Messages["m1"]
				m.State = AttemptActive
				changed.Messages["m1"] = m
			case "gap":
				delete(changed.Messages, "m1-failed")
			case "duplicate":
				changed.Messages["copy"] = changed.Messages["m1"]
			case "missing-managed":
				changed.Managed = nil
			case "forged-budget":
				p.BudgetDigest = strings.Repeat("f", 64)
				summary.Payload = raw(p)
				summary.Digest = digest(summary.Payload)
			}
			changed.Streams[streamKey(p.Input)] = stream
			summary.SourceDigest = compressionSourceDigest(changed)
			if _, err := candidateObjectsFromSummary(changed, *summary); err == nil {
				t.Fatal("invalid message prefix accepted")
			}
		})
	}
}

func TestCompressionPlanCollectsCandidateObjectsWithoutEnablingExecution(t *testing.T) {
	record := compressionPlanRecord(t)
	request := HistoryCompressionRequest{RunID: record.RunID, ThroughRevision: record.Revision, MaxObjects: 100, MaxBytes: 1 << 20}
	plan, err := PlanHistoryCompression(record, request)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Executable || len(plan.DeleteObjects) != 2 || plan.Rows != 2 {
		t.Fatalf("unexpected compression plan: %+v", plan)
	}
	plan.Executable = true
	plan.PlanDigest = plan.Digest()
	if err := plan.Validate(request); err == nil {
		t.Fatal("caller enabled an unimplemented execution contract")
	}
}
