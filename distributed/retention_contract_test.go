package distributed

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func retentionContractPolicy() RetentionPolicy {
	d := time.Hour
	return RetentionPolicy{Enabled: true, ReceiptTTL: &d}
}
func retentionContractQuery() RetentionDryRun {
	return RetentionDryRun{Namespace: "ns", PolicyVersion: 2, Policy: retentionContractPolicy(), ObservedFrom: time.Unix(10, 0).UTC(), ObservedThrough: time.Unix(20, 0).UTC(), Limit: 10}
}
func retentionContractCandidates() []RetentionCandidate {
	return []RetentionCandidate{{Namespace: "ns", Kind: "run", ID: "b", Revision: 1, Rows: 2, Bytes: 20}, {Namespace: "ns", Kind: "run", ID: "a", Revision: 1, Rows: 1, Bytes: 10, Protected: []RetentionProtectedReason{RetentionProtectedPin}}}
}
func TestRetentionContractStableSortAndDuplicate(t *testing.T) {
	s := RetentionCandidateSummary{Candidates: retentionContractCandidates(), LogicalRows: 3, LogicalBytes: 30}
	if err := s.validate(); err != nil {
		t.Fatal(err)
	}
	s.Candidates[0], s.Candidates[1] = s.Candidates[1], s.Candidates[0]
	if err := s.validate(); err != nil {
		t.Fatal(err)
	}
	s.Candidates = append(s.Candidates, s.Candidates[0])
	if !errors.Is(s.validate(), ErrInvalid) {
		t.Fatal("duplicate accepted")
	}
}
func TestRetentionContractBindingAndReceipt(t *testing.T) {
	q := retentionContractQuery()
	pd, _ := retentionPolicyDigest(q.Policy)
	r := RetentionDryRunResult{DryRunID: "dry", Namespace: q.Namespace, PolicyVersion: q.PolicyVersion, Policy: q.Policy, PolicyDigest: pd, ObservedFrom: q.ObservedFrom, ObservedThrough: q.ObservedThrough, Summary: RetentionCandidateSummary{Candidates: retentionContractCandidates(), LogicalRows: 3, LogicalBytes: 30}, ReceiptDigest: strings.Repeat("a", 64)}
	if err := r.Validate(q); err != nil {
		t.Fatal(err)
	}
	q.PolicyVersion++
	if !errors.Is(r.Validate(q), ErrInvalid) {
		t.Fatal("version not bound")
	}
	e := RetentionExecute{DryRunID: "dry", Namespace: "ns", PolicyVersion: 2, PolicyDigest: pd, MaxRuns: 2, MaxRows: 3, MaxBytes: 30}
	if err := e.Validate(); err != nil {
		t.Fatal(err)
	}
	out := RetentionExecuteResult{DryRunID: "dry", Namespace: "ns", PolicyVersion: 2, Cursor: e.Cursor, ReceiptDigest: strings.Repeat("b", 64), PlanDigest: strings.Repeat("c", 64), Runs: 1, Rows: 1, Bytes: 10}
	if err := out.Validate(e); err != nil {
		t.Fatal(err)
	}
}
func TestRetentionContractCursorAndBudgets(t *testing.T) {
	q := retentionContractQuery()
	q.Cursor = RetentionCursor{Set: true}
	if !errors.Is(q.Validate(), ErrInvalid) {
		t.Fatal("empty set cursor accepted")
	}
	q = retentionContractQuery()
	q.Limit = maxRetentionCandidates + 1
	if !errors.Is(q.Validate(), ErrInvalid) {
		t.Fatal("unbounded limit accepted")
	}
	e := RetentionExecute{DryRunID: "dry", Namespace: "ns", PolicyVersion: 1, PolicyDigest: strings.Repeat("a", 64), MaxRuns: maxRetentionRuns + 1, MaxRows: 1, MaxBytes: 1}
	if !errors.Is(e.Validate(), ErrInvalid) {
		t.Fatal("unbounded runs accepted")
	}
}
func TestRetentionContractCloneIsolation(t *testing.T) {
	q := retentionContractQuery()
	pd, _ := retentionPolicyDigest(q.Policy)
	r := RetentionDryRunResult{DryRunID: "dry", Namespace: "ns", PolicyVersion: 2, Policy: q.Policy, PolicyDigest: pd, ObservedFrom: q.ObservedFrom, ObservedThrough: q.ObservedThrough, Summary: RetentionCandidateSummary{Candidates: retentionContractCandidates(), Protected: map[string][]RetentionProtectedReason{"x": {RetentionProtectedGuard}}, LogicalRows: 3, LogicalBytes: 30}, ReceiptDigest: strings.Repeat("a", 64)}
	c := r.Clone()
	c.Summary.Candidates[0].ID = "changed"
	c.Summary.Protected["x"][0] = RetentionProtectedPin
	if r.Summary.Candidates[0].ID == "changed" || r.Summary.Protected["x"][0] == RetentionProtectedPin {
		t.Fatal("clone aliases result")
	}
	receipt := RetentionReceipt{DryRunID: "dry", PlanDigest: strings.Repeat("a", 64), Namespace: "ns", PolicyVersion: 1, PolicyDigest: strings.Repeat("b", 64), Candidates: retentionContractCandidates(), Guards: []string{"permanent"}}
	if err := receipt.Validate(); err != nil {
		t.Fatal(err)
	}
}
