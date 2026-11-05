// 本文件验证 Format3 普通提交的原子性与增量成本。
// 使用真实类型化批次和独立快照编码作判据，不连接后端或开启压缩删除。
package durable

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/Luo-root/pulse-durable/internal/snapshotfmt"
)

func TestFormat3SnapshotSizeDeltaMatchesFullEncoding(t *testing.T) {
	for _, payload := range [][]byte{nil, {}, {0, 255, 1}, bytes.Repeat([]byte{0xff}, 1024)} {
		for _, mode := range []string{"ordinary", "insert-summary", "replace-summary", "remove-summary", "provenance"} {
			t.Run(fmt.Sprintf("%s/%d/%t", mode, len(payload), payload == nil), func(t *testing.T) {
				before, _ := format3Fixture(t)
				tx := newRecordTransaction(before.Revision)
				summary := HistorySummary{Kind: "future", ID: "键<>&", SourceRevision: 1, SourceDigest: strings.Repeat("a", 64), Payload: payload, Digest: digest(payload)}
				var err error
				switch mode {
				case "ordinary":
					err = tx.putAttempt("ordinary", nil, AttemptRecord{Parent: "root", NodeID: "ordinary", Index: 1, Limit: 1, State: AttemptActive})
				case "insert-summary":
					err = tx.putHistorySummary(summary.key(), nil, &summary)
				case "replace-summary":
					old := before.HistorySummaries[0]
					summary.Kind, summary.ID = old.Kind, old.ID
					err = tx.putHistorySummary(old.key(), &old, &summary)
				case "remove-summary":
					before.HistorySummaries = append(before.HistorySummaries, summary)
					err = tx.putHistorySummary(summary.key(), &summary, nil)
				case "provenance":
					value := *before.Provenance
					value.CompressionID = "来源<>&"
					value.Digest = provenanceDigest(value.OriginRunID, value.OriginRevision, value.OriginRecordDigest, value.CompressionID)
					err = tx.putProvenance(before.Provenance, &value)
				}
				if err != nil {
					t.Fatal(err)
				}
				if err := tx.ensureSemanticCommitIndex(before); err != nil {
					t.Fatal(err)
				}
				batch := ChangeBatch{header: changeHeaderFor(before, tx), tx: tx}
				old, err := snapshotfmt.Encode(before)
				if err != nil {
					t.Fatal(err)
				}
				untouched := CloneRecord(before)
				delta, err := batch.SnapshotSizeDelta(before)
				if err != nil || !reflect.DeepEqual(before, untouched) {
					t.Fatalf("size query mutated source or failed: %v", err)
				}
				after := CloneRecord(before)
				if _, err := batch.ApplyTo(&after); err != nil {
					t.Fatal(err)
				}
				encoded, err := snapshotfmt.Encode(after)
				if err != nil || delta != int64(len(encoded)-len(old)) {
					t.Fatalf("size delta=%d want=%d err=%v", delta, len(encoded)-len(old), err)
				}
			})
		}
	}
}

func TestFormat3RejectsInvalidTransitionsWithoutMutation(t *testing.T) {
	for _, mode := range []string{"missing-index", "rewrite-index", "remove-index", "wrong-digest", "remove-summary", "future-summary", "remove-provenance", "legacy-upgrade", "invalid-metadata"} {
		t.Run(mode, func(t *testing.T) {
			before, batch := format3Fixture(t)
			tx := batch.tx.copy()
			batch.tx = tx
			switch mode {
			case "missing-index":
				tx.commitIndex = nil
			case "rewrite-index":
				old := before.CommitIndex[0]
				value := old
				value.BatchDigest = strings.Repeat("e", 64)
				if err := tx.putCommitIndex(old.Revision, &old, &value); err != nil {
					t.Fatal(err)
				}
			case "remove-index":
				old := before.CommitIndex[0]
				if err := tx.putCommitIndex(old.Revision, &old, nil); err != nil {
					t.Fatal(err)
				}
			case "wrong-digest":
				op := tx.commitIndex[2]
				op.value.BatchDigest = strings.Repeat("e", 64)
				tx.commitIndex[2] = op
			case "remove-summary":
				tx.summaries = nil
				old := before.HistorySummaries[0]
				if err := tx.putHistorySummary(old.key(), &old, nil); err != nil {
					t.Fatal(err)
				}
			case "future-summary":
				for key, op := range tx.summaries {
					op.value.SourceRevision = 3
					tx.summaries[key] = op
				}
			case "remove-provenance":
				tx.provenance = nil
				if err := tx.putProvenance(before.Provenance, nil); err != nil {
					t.Fatal(err)
				}
			case "legacy-upgrade":
				before.Format = RecordFormat
				before.HistorySummaries, before.CommitIndex, before.Provenance = nil, nil, nil
			case "invalid-metadata":
				if err := tx.putAttempt("bad", nil, AttemptRecord{NodeID: string([]byte{0xff})}); err != nil {
					t.Fatal(err)
				}
			}
			// 非 digest 场景重新绑定内容，确保命中的是目标不变量而非偶然摘要不符。
			if mode != "wrong-digest" && mode != "invalid-metadata" {
				value, err := semanticBatchContentDigest(batch)
				if err != nil {
					t.Fatal(err)
				}
				if op, ok := tx.commitIndex[2]; ok {
					op.value.BatchDigest = value
					tx.commitIndex[2] = op
				}
			}
			untouched := CloneRecord(before)
			if _, err := batch.SnapshotSizeDelta(before); err == nil {
				t.Fatal("invalid size query accepted")
			}
			if _, err := batch.ApplyTo(&before); err == nil {
				t.Fatal("invalid transition accepted")
			}
			if !reflect.DeepEqual(before, untouched) {
				t.Fatal("rejected transition changed source")
			}
		})
	}
}

func TestFormat3ClonePreservesNilAndEmptyEvidence(t *testing.T) {
	for _, payload := range [][]byte{nil, {}, {0xff, 1}} {
		r, _ := format3Fixture(t)
		r.HistorySummaries[0].Payload = payload
		cloned := CloneRecord(r)
		if !reflect.DeepEqual(r, cloned) {
			t.Fatal("clone normalized evidence shape")
		}
		if len(payload) > 0 {
			cloned.HistorySummaries[0].Payload[0] = 0
			if r.HistorySummaries[0].Payload[0] != 0xff {
				t.Fatal("clone aliased summary payload")
			}
		}
	}
	r := RunRecord{HistorySummaries: []HistorySummary{}, CommitIndex: []CommitIndexEntry{}}
	if !reflect.DeepEqual(r, CloneRecord(r)) {
		t.Fatal("clone normalized empty evidence containers")
	}
}

func TestFormat3RuntimeCommitAndNoop(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			r, _ := format3Fixture(t)
			session := &transactionSession{}
			s := &runState{record: r, session: session}
			session.base = &s.record
			before := CloneRecord(r)
			if err := s.commitRecordTransaction(t.Context(), newRecordTransaction(r.Revision)); err != nil {
				t.Fatal(err)
			}
			if session.calls != 0 || !reflect.DeepEqual(s.record, before) {
				t.Fatal("empty transaction consumed a commit or revision")
			}
			failure := errors.New("uncertain commit")
			session.after = func(saved RunRecord) error {
				if len(saved.CommitIndex) != 2 || !reflect.DeepEqual(s.record, before) {
					t.Fatal("index absent from store proposal or published before acknowledgement")
				}
				saved.CommitIndex[0].BatchDigest = "store alias"
				saved.HistorySummaries[0].Payload[0] = 'X'
				if fail {
					return failure
				}
				return nil
			}
			tx := newRecordTransaction(r.Revision)
			if err := tx.putAttempt("ordinary", nil, AttemptRecord{Parent: "root", NodeID: "ordinary", Index: 1, Limit: 1, State: AttemptActive}); err != nil {
				t.Fatal(err)
			}
			err := s.commitRecordTransaction(t.Context(), tx)
			if fail {
				if !errors.Is(err, failure) || s.stopped == nil || !reflect.DeepEqual(s.record, before) {
					t.Fatalf("failed store call published evidence: %v", err)
				}
			} else if err != nil || s.record.Revision != 2 || len(s.record.CommitIndex) != 2 || s.record.CommitIndex[0] != before.CommitIndex[0] || string(s.record.HistorySummaries[0].Payload) != string(before.HistorySummaries[0].Payload) {
				t.Fatalf("acknowledged commit lost or aliased evidence: %v", err)
			}
		})
	}
}

func TestFormat3MemoryLoadAndHistoryEvidence(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprint(corrupt), func(t *testing.T) {
			r, batch := format3Fixture(t)
			store := NewMemoryStore()
			ss, err := store.Acquire(t.Context(), r.RunID)
			if err != nil {
				t.Fatal(err)
			}
			defer ss.Close()
			// 显式注入逻辑格式夹具，仅测试读取与普通 Apply，绝不宣称首次物化已实现。
			if corrupt {
				r.CommitIndex[0].BatchDigest = "bad"
			}
			replaceMemoryFixture(ss, r)
			got, loadErr := ss.Load(t.Context())
			image, historyErr := store.LoadRevision(t.Context(), r.RunID, r.Revision)
			if corrupt {
				if !errors.Is(loadErr, ErrInvalidRecord) || !errors.Is(historyErr, ErrInvalidRecord) {
					t.Fatalf("invalid evidence accepted: load=%v history=%v", loadErr, historyErr)
				}
				return
			}
			if loadErr != nil || historyErr != nil || !reflect.DeepEqual(r, got) || !reflect.DeepEqual(r, image.Record) {
				t.Fatalf("semantic reads disagree: load=%v history=%v", loadErr, historyErr)
			}
			if _, err := ss.Apply(t.Context(), batch); err != nil {
				t.Fatal(err)
			}
			if _, err := batch.ApplyTo(&r); err != nil {
				t.Fatal(err)
			}
			image, err = store.LoadRevision(t.Context(), r.RunID, r.Revision)
			if err != nil || !reflect.DeepEqual(image.Record, r) {
				t.Fatalf("semantic batch replay changed projection: %v", err)
			}
		})
	}
}

func BenchmarkFormat3SnapshotSizeDeltaUnrelatedHistory(b *testing.B) {
	for _, count := range []int{0, 1024, 16384} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			payload := []byte("evidence")
			r := RunRecord{Format: SemanticRecordFormat, RunID: "cost", Revision: 1, RootScope: "root",
				HistorySummaries: []HistorySummary{{Kind: "future", ID: "id", SourceRevision: 1, SourceDigest: strings.Repeat("a", 64), Payload: payload, Digest: digest(payload)}},
				CommitIndex:      []CommitIndexEntry{{Revision: 1, BatchDigest: strings.Repeat("b", 64)}},
				Provenance:       &Provenance{OriginRunID: "cost", OriginRevision: 1, OriginRecordDigest: strings.Repeat("c", 64), CompressionID: "id"},
				Messages:         make(map[string]MessageAttemptRecord, count)}
			r.Provenance.Digest = provenanceDigest(r.Provenance.OriginRunID, 1, r.Provenance.OriginRecordDigest, "id")
			for i := 0; i < count; i++ {
				r.Messages[fmt.Sprint(i)] = MessageAttemptRecord{Index: 1, Limit: 2}
			}
			tx := newRecordTransaction(1)
			if err := tx.putAttempt("new", nil, AttemptRecord{Index: 1, Limit: 1, State: AttemptActive}); err != nil {
				b.Fatal(err)
			}
			if err := tx.ensureSemanticCommitIndex(r); err != nil {
				b.Fatal(err)
			}
			batch := ChangeBatch{header: changeHeaderFor(r, tx), tx: tx}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := batch.SnapshotSizeDelta(r); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
