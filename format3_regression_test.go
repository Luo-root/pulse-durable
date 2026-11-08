package durable

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func format3Fixture(t *testing.T) (RunRecord, ChangeBatch) {
	t.Helper()
	base := RunRecord{
		RunID:      "semantic-run",
		Revision:   1,
		Version:    "v1",
		Definition: strings.Repeat("a", 64),
		Inputs:     strings.Repeat("b", 64),
		RootScope:  "root",
		Scopes: map[string]ScopeRecord{
			"root": {Graph: "graph", Inputs: strings.Repeat("b", 64), Nodes: map[string]NodeRecord{}},
		},
		Attempts:    map[string]AttemptRecord{},
		CommitIndex: []CommitIndexEntry{{Revision: 1, ParentRevision: 0, BatchDigest: strings.Repeat("0", 64)}},
		Provenance:  &Provenance{OriginRunID: "semantic-run", OriginRevision: 1, OriginRecordDigest: strings.Repeat("0", 64), CompressionID: "semantic-compression-v1"},
	}
	base.Provenance.Digest = provenanceDigest(base.Provenance.OriginRunID, base.Provenance.OriginRevision, base.Provenance.OriginRecordDigest, base.Provenance.CompressionID)
	payload := raw(retryPrefixSummaryPayload{LastPrunedID: strings.Repeat("b", 32), Parent: strings.Repeat("c", 32), NodeID: "node", Boundary: "retry", ThroughIndex: 2, Limit: 3, LastPrunedDigest: strings.Repeat("d", 64)})
	oldPayload := raw(retryPrefixSummaryPayload{LastPrunedID: strings.Repeat("a", 32), Parent: strings.Repeat("c", 32), NodeID: "node", Boundary: "retry", ThroughIndex: 1, Limit: 3, LastPrunedDigest: strings.Repeat("e", 64)})
	base.HistorySummaries = []HistorySummary{{Kind: RetryPrefixSummaryKind, ID: "root/node/retry", SourceRevision: 1, SourceDigest: strings.Repeat("1", 64), Payload: oldPayload, Digest: digest(oldPayload)}}
	base.CommitIndex = []CommitIndexEntry{{Revision: 1, ParentRevision: 0, BatchDigest: strings.Repeat("0", 64)}}
	summary := HistorySummary{
		Kind:           RetryPrefixSummaryKind,
		ID:             "root/node/retry",
		SourceRevision: 1,
		SourceDigest:   strings.Repeat("c", 64),
		Payload:        payload,
		Digest:         digest(payload),
	}
	entry1 := CommitIndexEntry{Revision: 2, ParentRevision: 1, BatchDigest: strings.Repeat("d", 64)}
	provenance := Provenance{
		OriginRunID:        "semantic-run",
		OriginRevision:     1,
		OriginRecordDigest: strings.Repeat("f", 64),
		CompressionID:      "semantic-compression-v1",
	}
	provenance.Digest = provenanceDigest(provenance.OriginRunID, provenance.OriginRevision, provenance.OriginRecordDigest, provenance.CompressionID)

	tx := newRecordTransaction(base.Revision)
	oldSummary := base.HistorySummaries[0]
	if err := tx.putHistorySummary(summary.key(), &oldSummary, &summary); err != nil {
		t.Fatal(err)
	}
	if err := tx.putCommitIndex(entry1.Revision, nil, &entry1); err != nil {
		t.Fatal(err)
	}
	oldProvenance := *base.Provenance
	if err := tx.putProvenance(&oldProvenance, &provenance); err != nil {
		t.Fatal(err)
	}
	batch := ChangeBatch{header: changeHeaderFor(base, tx), tx: tx}
	digestValue, err := semanticBatchContentDigest(batch)
	if err != nil {
		t.Fatal(err)
	}
	entry1.BatchDigest = digestValue
	tx.commitIndex[entry1.Revision] = putCommitIndex{absent: true, value: entry1}
	return base, ChangeBatch{header: changeHeaderFor(base, tx), tx: tx}
}

func TestFormat3OrdinaryTransactionAddsCommitIndex(t *testing.T) {
	base, _ := format3Fixture(t)
	tx := newRecordTransaction(base.Revision)
	value := AttemptRecord{Parent: base.RootScope, NodeID: "ordinary", Index: 1, Limit: 1, State: AttemptActive}
	if err := tx.putAttempt("ordinary", nil, value); err != nil {
		t.Fatal(err)
	}
	if err := tx.ensureSemanticCommitIndex(base); err != nil {
		t.Fatal(err)
	}
	batch := ChangeBatch{header: changeHeaderFor(base, tx), tx: tx}
	proposal := CloneRecord(base)
	if _, err := batch.ApplyTo(&proposal); err != nil {
		t.Fatal(err)
	}
	if proposal.Revision != 2 || len(proposal.CommitIndex) != 2 {
		t.Fatalf("ordinary Format3 transaction did not append index: revision=%d indexes=%d", proposal.Revision, len(proposal.CommitIndex))
	}
	contentDigest, err := semanticBatchContentDigest(batch)
	if err != nil {
		t.Fatal(err)
	}
	if proposal.CommitIndex[1].BatchDigest != contentDigest {
		t.Fatalf("ordinary transaction digest=%s want=%s", proposal.CommitIndex[1].BatchDigest, contentDigest)
	}
}

func TestFormat3ApplyAndRecoveryEvidence(t *testing.T) {
	base, batch := format3Fixture(t)
	before := CloneRecord(base)
	ack, err := batch.ApplyTo(&base)
	if err != nil {
		t.Fatal(err)
	}
	if !hasCompleteSemanticEvidence(base) || base.Revision != 2 {
		t.Fatalf("format3 upgrade did not publish: ack=%+v record=%+v", ack, base)
	}
	if err := validateSemanticEvidence(base); err != nil {
		t.Fatal(err)
	}
	if len(base.HistorySummaries) != 1 || len(base.CommitIndex) != 2 || base.Provenance == nil {
		t.Fatalf("semantic evidence incomplete: %+v", base)
	}
	if !reflect.DeepEqual(before.Scopes, base.Scopes) || !reflect.DeepEqual(before.Attempts, base.Attempts) {
		t.Fatal("semantic batch changed unrelated execution state")
	}

	base.HistorySummaries[0].Payload[0] = 'X'
	stored := batch.tx.summaries[historySummaryKey{kind: RetryPrefixSummaryKind, id: "root/node/retry"}].value.Payload
	if !bytes.Equal(stored, raw(retryPrefixSummaryPayload{LastPrunedID: strings.Repeat("b", 32), Parent: strings.Repeat("c", 32), NodeID: "node", Boundary: "retry", ThroughIndex: 2, Limit: 3, LastPrunedDigest: strings.Repeat("d", 64)})) {
		t.Fatal("summary transaction retained an alias")
	}
}

func TestFormat3WireCanonicalRoundTrip(t *testing.T) {
	base, batch := format3Fixture(t)
	wire, err := MarshalChangeBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{`"version":2`, `"kind":"history_summary"`, `"kind":"commit_index"`, `"kind":"provenance"`} {
		if !bytes.Contains(wire, []byte(marker)) {
			t.Fatalf("format3 wire lacks %s: %s", marker, wire)
		}
	}
	decoded, err := UnmarshalChangeBatch(wire)
	if err != nil {
		t.Fatal(err)
	}
	again, err := MarshalChangeBatch(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(wire, again) {
		t.Fatalf("format3 wire is not canonical:\n%s\n%s", wire, again)
	}
	digest1, err := ChangeBatchDigest(batch)
	if err != nil {
		t.Fatal(err)
	}
	digest2, err := ChangeBatchDigest(decoded)
	if err != nil || digest1 != digest2 {
		t.Fatalf("format3 digest changed: %v", err)
	}
	want := CloneRecord(base)
	got := CloneRecord(base)
	if _, err := batch.ApplyTo(&want); err != nil {
		t.Fatal(err)
	}
	if _, err := decoded.ApplyTo(&got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(want, got) {
		t.Fatal("decoded format3 batch changed a different projection")
	}
}

func TestFormat3PreconditionsAndStrictRecordValidation(t *testing.T) {
	base, batch := format3Fixture(t)
	changed := CloneRecord(base)
	changed.Provenance = nil
	if err := validateSemanticEvidence(changed); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("partial evidence component accepted: %v", err)
	}

	legacy := CloneRecord(base)
	legacy.HistorySummaries = nil
	if err := validateSemanticEvidence(legacy); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("missing summaries were not rejected: %v", err)
	}

	proposal := CloneRecord(base)
	bad := batch
	bad.tx = batch.tx.copy()
	key := historySummaryKey{kind: RetryPrefixSummaryKind, id: "root/node/retry"}
	op := bad.tx.summaries[key]
	op.absent = false
	op.expected = HistorySummary{
		Kind:           RetryPrefixSummaryKind,
		ID:             "root/node/retry",
		SourceRevision: 1,
		SourceDigest:   strings.Repeat("0", 64),
		Payload:        []byte("old"),
		Digest:         digest([]byte("old")),
	}
	bad.tx.summaries[key] = op
	if _, err := bad.ApplyTo(&proposal); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed summary precondition accepted: %v", err)
	}
	if !reflect.DeepEqual(proposal, base) {
		t.Fatal("failed semantic CAS changed the record")
	}
}

func TestEvidenceWireRejectsLegacyVersion(t *testing.T) {
	base, batch := format3Fixture(t)
	wire, err := MarshalChangeBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	legacyWire := bytes.Replace(wire, []byte(`"version":2`), []byte(`"version":1`), 1)
	if _, err := UnmarshalChangeBatch(legacyWire); err == nil {
		t.Fatal("legacy wire version accepted")
	}

	ordinary := ChangeBatch{
		header: ChangeHeader{RunID: base.RunID, BusinessVersion: base.Version,
			DefinitionDigest: base.Definition, InputsDigest: base.Inputs,
			RootScope: base.RootScope, ExpectedRevision: 1, Revision: 2},
		tx: newRecordTransaction(1),
	}
	if _, err := MarshalChangeBatch(ordinary); err != nil {
		t.Fatalf("ordinary wire without semantic evidence rejected: %v", err)
	}
}
