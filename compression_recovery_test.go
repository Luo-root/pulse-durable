// 本文件验证摘要的恢复预算，不通过假夹具冒称删除事务已实现。
// 普通协议先产生真实 Retry/消息事实，再显式安装“删除已原子提交”的恢复夹具。
package durable

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/Luo-root/pulse"
)

func semanticRecoveryFixture(t *testing.T, record RunRecord, summary HistorySummary) RunRecord {
	t.Helper()
	record.Format = SemanticRecordFormat
	record.HistorySummaries = []HistorySummary{summary}
	record.CommitIndex = make([]CommitIndexEntry, record.Revision)
	for i := range record.CommitIndex {
		record.CommitIndex[i] = CommitIndexEntry{Revision: uint64(i + 1), ParentRevision: uint64(i), BatchDigest: strings.Repeat("a", 64)}
	}
	record.Provenance = &Provenance{OriginRunID: record.RunID, OriginRevision: summary.SourceRevision, OriginRecordDigest: summary.SourceDigest, CompressionID: "explicit-recovery-fixture"}
	record.Provenance.Digest = provenanceDigest(record.Provenance.OriginRunID, record.Provenance.OriginRevision, record.Provenance.OriginRecordDigest, record.Provenance.CompressionID)
	if err := ValidateSemanticEvidence(record); err != nil {
		t.Fatal(err)
	}
	return record
}

func retryRecoveryFixture(t *testing.T, limit int, retainActive bool) (streamProtocol, pulse.CheckpointAttemptRequest, string) {
	t.Helper()
	p := newStreamProtocol(t)
	request := pulse.CheckpointAttemptRequest{Site: p.site, Parent: p.processor.Scope, NodeID: "processor", Boundary: "aspect/0", Limit: limit}
	var ids []string
	for i := 0; i < 2; i++ {
		a, err := p.s.openAttempt(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, a.Scope)
		if err := p.s.failAttempt(t.Context(), a.Scope); err != nil {
			t.Fatal(err)
		}
	}
	active := ""
	if retainActive {
		a, err := p.s.openAttempt(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		active = a.Scope
	}
	r := CloneRecord(p.s.record)
	sourceRevision := r.Revision
	sourceDigest := recordDigest(r)
	last := r.Attempts[ids[1]]
	payload := retryPrefixSummaryPayload{AppliedRevision: r.Revision + 1, LastPrunedID: ids[1], Parent: request.Parent, NodeID: request.NodeID, Boundary: request.Boundary, ThroughIndex: 2, Limit: limit,
		LastPrunedDigest: summaryDigest("retry", r.RunID, sourceRevision, request.Parent, request.NodeID, request.Boundary, last.Index, last.Limit, string(last.State))}
	for _, id := range ids {
		delete(r.Attempts, id)
	}
	r.Revision++
	encoded := raw(payload)
	summary := HistorySummary{Kind: RetryPrefixSummaryKind, ID: "retry", SourceRevision: sourceRevision, SourceDigest: sourceDigest, Payload: encoded, Digest: digest(encoded)}
	r = semanticRecoveryFixture(t, r, summary)
	replaceMemoryFixture(p.s.session, r)
	p.s.record, p.s.summaryIndex = r, nil
	return p, request, active
}

func TestCompressionRecoveryRetryKeepsBudget(t *testing.T) {
	for _, retain := range []bool{false, true} {
		t.Run(fmt.Sprint(retain), func(t *testing.T) {
			p, request, active := retryRecoveryFixture(t, 3, retain)
			if err := p.s.validateLoaded(); err != nil {
				t.Fatal(err)
			}
			before := p.s.record.Revision
			a, err := p.s.openAttempt(t.Context(), request)
			if err != nil || a.Index != 3 || a.Exhausted || retain && a.Scope != active {
				t.Fatalf("retry budget reset or live suffix lost: %+v %v", a, err)
			}
			if retain && p.s.record.Revision != before || !retain && p.s.record.Revision != before+1 {
				t.Fatal("wrong resumed/new commit count")
			}
			if err := p.s.failAttempt(t.Context(), a.Scope); err != nil {
				t.Fatal(err)
			}
			before = p.s.record.Revision
			exhausted, err := p.s.openAttempt(t.Context(), request)
			if err != nil || !exhausted.Exhausted || exhausted.Index != 3 || p.s.record.Revision != before {
				t.Fatalf("compressed retry acquired new budget: %+v %v", exhausted, err)
			}
			if err := p.s.validateLoaded(); err != nil {
				t.Fatal(err)
			}
		})
	}
	p, request, _ := retryRecoveryFixture(t, 2, false)
	before := p.s.record.Revision
	a, err := p.s.openAttempt(t.Context(), request)
	if err != nil || !a.Exhausted || a.Index != 2 || a.Scope == "" || p.s.record.Revision != before {
		t.Fatalf("fully pruned exhausted budget reopened: %+v %v", a, err)
	}
	request.Limit = 3
	if _, err := p.s.openAttempt(t.Context(), request); !errors.Is(err, ErrConflict) {
		t.Fatalf("compressed limit drift accepted: %v", err)
	}
}

func TestCompressionRecoveryRetryRejectsGapAndReappearance(t *testing.T) {
	for _, mode := range []string{"gap", "limit", "reappearance", "applied-position", "last-id"} {
		t.Run(mode, func(t *testing.T) {
			p, _, active := retryRecoveryFixture(t, 4, true)
			r := CloneRecord(p.s.record)
			var payload retryPrefixSummaryPayload
			if err := unmarshalRecord(r.HistorySummaries[0].Payload, &payload); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "gap":
				a := r.Attempts[active]
				a.Index = 4
				r.Attempts[active] = a
			case "limit":
				a := r.Attempts[active]
				a.Limit++
				r.Attempts[active] = a
			case "reappearance":
				r.Attempts[payload.LastPrunedID] = AttemptRecord{Parent: payload.Parent, NodeID: payload.NodeID, Boundary: payload.Boundary, Index: 2, Limit: payload.Limit, State: AttemptFailed}
			case "applied-position":
				payload.AppliedRevision++
			case "last-id":
				payload.LastPrunedID = "not-an-id"
			}
			r.HistorySummaries[0].Payload = raw(payload)
			r.HistorySummaries[0].Digest = digest(r.HistorySummaries[0].Payload)
			state := &runState{record: r, def: p.s.def}
			if err := state.validateLoaded(); err == nil {
				t.Fatal("invalid compressed Retry evidence accepted")
			}
		})
	}
}

func TestCompressionRecoveryRejectsOrdinaryAppliedSummaryEdits(t *testing.T) {
	for _, mode := range []string{"delete", "replace", "insert-applied"} {
		t.Run(mode, func(t *testing.T) {
			p, _, _ := retryRecoveryFixture(t, 3, false)
			before := CloneRecord(p.s.record)
			tx := newRecordTransaction(before.Revision)
			old := before.HistorySummaries[0]
			var err error
			switch mode {
			case "delete":
				err = tx.putHistorySummary(old.key(), &old, nil)
			case "replace":
				value := copyHistorySummary(old)
				var payload retryPrefixSummaryPayload
				if err := unmarshalRecord(value.Payload, &payload); err != nil {
					t.Fatal(err)
				}
				payload.AppliedRevision = 0
				value.Payload = raw(payload)
				value.Digest = digest(value.Payload)
				err = tx.putHistorySummary(old.key(), &old, &value)
			case "insert-applied":
				value := copyHistorySummary(old)
				value.ID = "forged-second"
				err = tx.putHistorySummary(value.key(), nil, &value)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := p.s.commitRecordTransaction(t.Context(), tx); err == nil {
				t.Fatal("ordinary transaction modified applied high-water evidence")
			}
			if !reflect.DeepEqual(p.s.record, before) {
				t.Fatal("rejected summary mutation changed runtime")
			}
		})
	}
}

func TestCompressionRecoveryManagedWitnessAndOldMessageReject(t *testing.T) {
	p := newStreamProtocol(t)
	if err := p.append(1); err != nil {
		t.Fatal(err)
	}
	a, err := p.s.openMessage(t.Context(), p.messageRequest(1, 2))
	if err != nil {
		t.Fatal(err)
	}
	if err := p.s.commitMessage(t.Context(), pulse.CheckpointMessageCommit{Site: p.site, Scope: p.s.record.RootScope, Attempt: a.Attempt.Scope,
		Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: p.input.Stream.ID, Sequence: 1}}, State: []pulse.CheckpointState{{Name: "sum", Type: reflect.TypeFor[int](), Value: 1}}}); err != nil {
		t.Fatal(err)
	}
	r := CloneRecord(p.s.record)
	sourceRevision, sourceDigest := r.Revision, recordDigest(r)
	key := managedKey(r.RootScope, p.processor.Scope, "processor")
	payload := messagePrefixSummaryPayload{AppliedRevision: r.Revision + 1, ProcessingScope: r.RootScope, ProcessingAttempt: p.processor.Scope, NodeID: "processor", Input: streamID(p.input.Stream.ID), ThroughSequence: 1,
		BudgetDigest: strings.Repeat("b", 64), ManagedDigest: compressionManagedDigest(r.Managed[key]), ConfirmationIDs: []string{a.Attempt.Scope}}
	// 同一 Managed 在另一输入流中已有旧确认；它不能证明压缩后发生过更新。
	secondInput := streamID(p.input.Stream.ID)
	secondInput.Group = "second"
	secondStream := r.Streams[streamKey(streamID(p.input.Stream.ID))]
	secondStream.Stream.ID = secondInput
	r.Streams[streamKey(secondInput)] = secondStream
	oldID, err := randomID()
	if err != nil {
		t.Fatal(err)
	}
	r.Messages[oldID] = MessageAttemptRecord{Scope: r.RootScope, Parent: p.processor.Scope, NodeID: "processor", Input: secondInput, Sequence: 1, Index: 1, Limit: 2, State: AttemptConfirmed}
	payload.ConfirmationIDs = append(payload.ConfirmationIDs, oldID)
	sort.Strings(payload.ConfirmationIDs)
	delete(r.Messages, a.Attempt.Scope)
	r.Revision++
	encoded := raw(payload)
	r = semanticRecoveryFixture(t, r, HistorySummary{Kind: MessagePrefixSummaryKind, ID: "messages", SourceRevision: sourceRevision, SourceDigest: sourceDigest, Payload: encoded, Digest: digest(encoded)})
	replaceMemoryFixture(p.s.session, r)
	p.s.record, p.s.summaryIndex, p.s.messageIndexReady = r, nil, false
	if err := p.s.validateLoaded(); err != nil {
		t.Fatal(err)
	}
	if _, err := p.s.openMessage(t.Context(), p.messageRequest(1, 2)); err == nil {
		t.Fatal("pruned acknowledged message reopened")
	}
	if err := p.s.failMessage(t.Context(), a.Attempt.Scope); err == nil {
		t.Fatal("pruned message could fail again")
	}
	if err := p.s.commitMessage(t.Context(), pulse.CheckpointMessageCommit{Site: p.site, Scope: r.RootScope,
		Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: p.input.Stream.ID, Sequence: 1}}}); err == nil {
		t.Fatal("pruned message could be acknowledged again")
	}
	changed := CloneRecord(r)
	managed := changed.Managed[key]
	managed.State[0].Data = []byte("2")
	managed.State[0].Digest = digest(managed.State[0].Data)
	changed.Managed[key] = managed
	if err := (&runState{record: changed, def: p.s.def}).validateLoaded(); err == nil {
		t.Fatal("managed state changed without a retained confirmation")
	}
	if err := p.append(2); err != nil {
		t.Fatal(err)
	}
	m, err := p.s.openMessage(t.Context(), p.messageRequest(2, 2))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.State) != 1 || m.State[0].Value != 1 {
		t.Fatal("compressed Managed State lost")
	}
	if err := p.s.commitMessage(t.Context(), pulse.CheckpointMessageCommit{Site: p.site, Scope: r.RootScope, Attempt: m.Attempt.Scope,
		Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: p.input.Stream.ID, Sequence: 2}}, State: []pulse.CheckpointState{{Name: "sum", Type: reflect.TypeFor[int](), Value: 3}}}); err != nil {
		t.Fatal(err)
	}
	if err := (&runState{record: CloneRecord(p.s.record), def: p.s.def}).validateLoaded(); err != nil {
		t.Fatalf("valid later state rejected: %v", err)
	}
}
