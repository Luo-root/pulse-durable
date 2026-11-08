package durable

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Luo-root/pulse-durable/internal/snapshotfmt"
)

func compressionPlanRecord(t *testing.T) RunRecord {
	t.Helper()
	record, _ := format3Fixture(t)
	record.HistorySummaries = nil
	firstID, secondID := strings.Repeat("a", 32), strings.Repeat("b", 32)
	record.Attempts = map[string]AttemptRecord{
		firstID:  {Parent: "parent", NodeID: "node", Boundary: "retry", Index: 1, Limit: 3, State: AttemptFailed},
		secondID: {Parent: "parent", NodeID: "node", Boundary: "retry", Index: 2, Limit: 3, State: AttemptFailed},
	}
	through := 2
	last := record.Attempts[secondID]
	payload := raw(retryPrefixSummaryPayload{LastPrunedID: secondID, Parent: "parent", NodeID: "node", Boundary: "retry", ThroughIndex: through, Limit: 3, LastPrunedDigest: summaryDigest("retry", record.RunID, record.Revision, "parent", "node", "retry", last.Index, last.Limit, string(last.State))})
	summary := HistorySummary{Kind: RetryPrefixSummaryKind, ID: "retry", SourceRevision: record.Revision, SourceDigest: compressionSourceDigest(record), Payload: payload, Digest: digest(payload)}
	record.HistorySummaries = []HistorySummary{summary}
	return record
}

func TestHistoryCompressionPlanIsPureAndBound(t *testing.T) {
	record := compressionPlanRecord(t)
	before := CloneRecord(record)
	request := HistoryCompressionRequest{RunID: record.RunID, ThroughRevision: record.Revision, MaxObjects: 100, MaxBytes: 1 << 20}
	plan, err := PlanHistoryCompression(record, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Validate(request); err != nil {
		t.Fatal(err)
	}
	if plan.Executable || plan.Rows != 2 || len(plan.DeleteObjects) != 2 {
		t.Fatalf("plan unexpectedly executable: %+v", plan)
	}
	if len(plan.SummaryDigests) != len(record.HistorySummaries) || len(plan.ProtectedObjects) == 0 {
		t.Fatalf("plan lost evidence or protections: %+v", plan)
	}
	if recordDigest(record) != recordDigest(before) {
		t.Fatal("planning mutated the input record")
	}
	if plan.PlanDigest != plan.Digest() || plan.RequestDigest != request.Digest() {
		t.Fatal("plan digest is not reproducible")
	}
	for _, protected := range plan.ProtectedObjects {
		for _, deleted := range plan.DeleteObjects {
			if protected == deleted {
				t.Fatal("plan protection and deletion sets overlap")
			}
		}
	}
}

func TestHistoryCompressionExactBytesAndCanonicalProtection(t *testing.T) {
	r := compressionPlanRecord(t)
	q := HistoryCompressionRequest{RunID: r.RunID, ThroughRevision: r.Revision, MaxObjects: 100, MaxBytes: 1 << 20}
	p, err := PlanHistoryCompression(r, q)
	if err != nil {
		t.Fatal(err)
	}
	source, err := snapshotfmt.Encode(r)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := json.Marshal(p)
	if err != nil || p.Bytes != uint64(len(source)+len(wire)) {
		t.Fatalf("budget=%d source=%d plan=%d error=%v", p.Bytes, len(source), len(wire), err)
	}
	q.MaxBytes = uint64(len(source))
	if _, err := PlanHistoryCompression(r, q); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("ignored plan overhead: %v", err)
	}
	for _, mode := range []string{"overlap", "duplicate", "order", "executable"} {
		t.Run(mode, func(t *testing.T) {
			changed := p
			changed.ProtectedObjects = append([]CompressionObjectRef(nil), p.ProtectedObjects...)
			switch mode {
			case "overlap":
				changed.ProtectedObjects = append(changed.ProtectedObjects, p.DeleteObjects[0])
				sortCompressionObjects(changed.ProtectedObjects)
			case "duplicate":
				changed.ProtectedObjects = append(changed.ProtectedObjects, changed.ProtectedObjects[0])
				sortCompressionObjects(changed.ProtectedObjects)
			case "order":
				changed.ProtectedObjects[0], changed.ProtectedObjects[1] = changed.ProtectedObjects[1], changed.ProtectedObjects[0]
			case "executable":
				changed.Executable = true
			}
			changed.PlanDigest = changed.Digest()
			if err := changed.Validate(p.Request); err == nil {
				t.Fatal("noncanonical or unsafe plan accepted")
			}
		})
	}
	nilShape, emptyShape := r, CloneRecord(r)
	emptyShape.Messages = map[string]MessageAttemptRecord{}
	if recordDigest(nilShape) == recordDigest(emptyShape) {
		t.Fatal("head digest normalized nil and empty objects")
	}
	one := HistoryCompressionPlan{DeleteObjects: []CompressionObjectRef{{Kind: "attempt", ID: "a,b"}, {Kind: "message", ID: "c"}}}
	two := HistoryCompressionPlan{DeleteObjects: []CompressionObjectRef{{Kind: "attempt", ID: "a"}, {Kind: "message", ID: "b,c"}}}
	if one.Digest() == two.Digest() {
		t.Fatal("structured keys collided across separator boundaries")
	}
}

func TestHistoryCompressionRejectsOverlappingSummaryGroups(t *testing.T) {
	r := compressionPlanRecord(t)
	duplicate := copyHistorySummary(r.HistorySummaries[0])
	duplicate.ID = "same-group-different-id"
	r.HistorySummaries = append(r.HistorySummaries, duplicate)
	q := HistoryCompressionRequest{RunID: r.RunID, ThroughRevision: r.Revision, MaxObjects: 100, MaxBytes: 1 << 20}
	if _, err := PlanHistoryCompression(r, q); !errors.Is(err, ErrConflict) {
		t.Fatalf("overlapping business group accepted: %v", err)
	}
}

func BenchmarkHistoryCompressionIndependentRetryGroups(b *testing.B) {
	for _, groups := range []int{128, 256, 512, 1024} {
		b.Run(fmt.Sprint(groups), func(b *testing.B) {
			r := RunRecord{RunID: "scale", Revision: 1, Attempts: make(map[string]AttemptRecord),
				CommitIndex: []CommitIndexEntry{{Revision: 1, BatchDigest: strings.Repeat("b", 64)}},
				Provenance:  &Provenance{OriginRunID: "scale", OriginRevision: 1, OriginRecordDigest: strings.Repeat("c", 64), CompressionID: "scale"}}
			r.Provenance.Digest = provenanceDigest("scale", 1, r.Provenance.OriginRecordDigest, "scale")
			for i := 0; i < groups; i++ {
				id := fmt.Sprintf("attempt-%d", i)
				parent := fmt.Sprintf("parent-%d", i)
				r.Attempts[id] = AttemptRecord{Parent: parent, NodeID: "node", Boundary: "retry", Index: 1, Limit: 2, State: AttemptFailed}
				payload := raw(retryPrefixSummaryPayload{LastPrunedID: id, Parent: parent, NodeID: "node", Boundary: "retry", ThroughIndex: 1, Limit: 2,
					LastPrunedDigest: summaryDigest("retry", "scale", uint64(1), parent, "node", "retry", 1, 2, string(AttemptFailed))})
				r.HistorySummaries = append(r.HistorySummaries, HistorySummary{Kind: RetryPrefixSummaryKind, ID: id, SourceRevision: 1, Payload: payload, Digest: digest(payload)})
			}
			source := compressionSourceDigest(r)
			for i := range r.HistorySummaries {
				r.HistorySummaries[i].SourceDigest = source
			}
			request := HistoryCompressionRequest{RunID: "scale", ThroughRevision: 1, MaxObjects: HistoryCompressionMaxObjects, MaxBytes: HistoryCompressionMaxBytes}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := PlanHistoryCompression(r, request); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestHistoryCompressionPlanRejectsLegacyAndBudget(t *testing.T) {
	record := compressionPlanRecord(t)
	legacy := record
	legacy.HistorySummaries = nil
	if _, err := PlanHistoryCompression(legacy, HistoryCompressionRequest{RunID: record.RunID, ThroughRevision: record.Revision, MaxObjects: 10, MaxBytes: 1 << 20}); err == nil {
		t.Fatalf("incomplete evidence record accepted")
	}
	request := HistoryCompressionRequest{RunID: record.RunID, ThroughRevision: record.Revision, MaxObjects: 1, MaxBytes: 1 << 20}
	if _, err := PlanHistoryCompression(record, request); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("protected object budget not enforced: %v", err)
	}
	if _, err := PlanHistoryCompression(record, HistoryCompressionRequest{RunID: record.RunID, ThroughRevision: record.Revision + 1, MaxObjects: 10, MaxBytes: 1 << 20}); !errors.Is(err, ErrConflict) {
		t.Fatalf("future through revision accepted: %v", err)
	}
	if _, err := PlanHistoryCompression(record, HistoryCompressionRequest{RunID: strings.Repeat("x", 0), ThroughRevision: 1, MaxObjects: 10, MaxBytes: 1 << 20}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid request accepted: %v", err)
	}
}
