// 本文件验证独立 PostgreSQL 的可选语义证据读取与损坏拒绝。
// 测试显式注入已合成快照，不提供或冒称首次物化的生产入口。
package postgresstore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
)

func semanticSnapshotFixture(t *testing.T, r durable.RunRecord) durable.RunRecord {
	t.Helper()
	sum := func(data []byte) string { h := sha256.Sum256(data); return hex.EncodeToString(h[:]) }
	payload := []byte{0xff, 0, 1}
	r.HistorySummaries = []durable.HistorySummary{{Kind: "future", ID: "evidence", SourceRevision: r.Revision, SourceDigest: sum([]byte("source")), Payload: payload, Digest: sum(payload)}}
	for revision := uint64(1); revision <= r.Revision; revision++ {
		r.CommitIndex = append(r.CommitIndex, durable.CommitIndexEntry{Revision: revision, ParentRevision: revision - 1, BatchDigest: sum([]byte("fixture"))})
	}
	p := &durable.Provenance{OriginRunID: r.RunID, OriginRevision: r.Revision, OriginRecordDigest: sum([]byte("origin")), CompressionID: "explicit-test-fixture"}
	canonical, err := json.Marshal(struct {
		Domain string `json:"domain"`
		Values []any  `json:"values"`
	}{"provenance/v1", []any{p.OriginRunID, p.OriginRevision, p.OriginRecordDigest, p.CompressionID}})
	if err != nil {
		t.Fatal(err)
	}
	p.Digest = sum(canonical)
	r.Provenance = p
	if err := durable.ValidateSemanticEvidence(r); err != nil {
		t.Fatal(err)
	}
	return r
}

func TestFormat3SnapshotDecodeAndCorruption(t *testing.T) {
	r := initialRecord("semantic")
	r.Revision = 2
	r = semanticSnapshotFixture(t, r)
	data, err := encodeSnapshot(r)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeSnapshot(data)
	if err != nil || !reflect.DeepEqual(got, r) {
		t.Fatalf("semantic snapshot changed: %v", err)
	}
	r.HistorySummaries[0].Payload[0] ^= 1
	data, err = encodeSnapshot(r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeSnapshot(data); err == nil {
		t.Fatal("corrupt semantic payload accepted")
	}
}

func TestPGFormat3SnapshotLoadHistoryAndCorruption(t *testing.T) {
	dsn := os.Getenv("PULSE_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("opt-in isolated database required; not database acceptance")
	}
	for _, mode := range []string{"valid", "summary-digest", "index-gap", "partial-evidence"} {
		t.Run(mode, func(t *testing.T) {
			resetPGSchema(t, dsn)
			defer resetPGSchema(t, dsn)
			s, err := New(t.Context(), dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			r := initialRecord("semantic")
			ss, err := s.Acquire(t.Context(), r.RunID)
			if err != nil {
				t.Fatal(err)
			}
			if err := ss.Initialize(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			if _, err := ss.Apply(t.Context(), attemptBatch(t, r)); err != nil {
				t.Fatal(err)
			}
			if err := ss.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := s.CompactStorage(t.Context(), durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: 2, ExpectedGeneration: 1}); err != nil {
				t.Fatal(err)
			}
			image, err := s.LoadRevision(t.Context(), r.RunID, 2)
			if err != nil {
				t.Fatal(err)
			}
			r = semanticSnapshotFixture(t, image.Record)
			switch mode {
			case "summary-digest":
				r.HistorySummaries[0].Payload[0] ^= 1
			case "index-gap":
				r.CommitIndex = r.CommitIndex[1:]
			case "partial-evidence":
				r.Provenance = nil
			}
			data, err := encodeSnapshot(r)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(data)
			if _, err := s.pool.Exec(t.Context(), `UPDATE pulse_durable.snapshots SET record=$1,digest=$2 WHERE run_id=$3 AND revision='2'`, data, digest[:], r.RunID); err != nil {
				t.Fatal(err)
			}
			ss, err = s.Acquire(t.Context(), r.RunID)
			if err != nil {
				t.Fatal(err)
			}
			got, loadErr := ss.Load(t.Context())
			if err := ss.Close(); err != nil {
				t.Fatal(err)
			}
			history, historyErr := s.LoadRevision(t.Context(), r.RunID, 2)
			_, compactErr := s.CompactStorage(t.Context(), durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: 2, ExpectedGeneration: 2})
			if mode != "valid" {
				for _, err := range []error{loadErr, historyErr, compactErr} {
					if !errors.Is(err, durable.ErrInvalidRecord) || errors.Is(err, durable.ErrHistoryExpired) {
						t.Fatalf("corrupt semantic evidence accepted or expired: %v", err)
					}
				}
				return
			}
			if loadErr != nil || historyErr != nil || compactErr != nil || !reflect.DeepEqual(got, r) || !reflect.DeepEqual(history.Record, r) {
				t.Fatalf("semantic projection changed: load=%v history=%v compact=%v", loadErr, historyErr, compactErr)
			}
			got.HistorySummaries[0].Payload[0] = 0
			ss, err = s.Acquire(t.Context(), r.RunID)
			if err != nil {
				t.Fatal(err)
			}
			defer ss.Close()
			loaded, err := ss.Load(t.Context())
			if err != nil || loaded.HistorySummaries[0].Payload[0] != 0xff {
				t.Fatalf("Load retained caller payload alias: %v", err)
			}
		})
	}
}
