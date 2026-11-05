// 本文件验证 SQLite 读取边界的 Format3 证据闭合。
// 格式3仅通过明确的测试夹具注入；不是公开首次物化或语义压缩执行验收。
package sqlitestore

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
)

func semanticSnapshotFixture(t *testing.T, r durable.RunRecord) durable.RunRecord {
	t.Helper()
	sum := func(data []byte) string { h := sha256.Sum256(data); return hex.EncodeToString(h[:]) }
	r.Format = durable.SemanticRecordFormat
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

func TestFormat3SnapshotLoadHistoryAndCorruption(t *testing.T) {
	for _, mode := range []string{"valid", "summary-digest", "index-gap", "legacy-evidence"} {
		t.Run(mode, func(t *testing.T) {
			s := openStore(t, filepath.Join(t.TempDir(), "semantic.sqlite"))
			r, batches := runtimeBatches(t, "semantic")
			ss := acquire(t, s, r.RunID)
			if err := ss.Initialize(ctx, r); err != nil {
				t.Fatal(err)
			}
			if _, err := ss.Apply(ctx, batches[0]); err != nil {
				t.Fatal(err)
			}
			if err := ss.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := s.CompactStorage(ctx, durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: 2, ExpectedGeneration: 1}); err != nil {
				t.Fatal(err)
			}
			image, err := s.LoadRevision(ctx, r.RunID, 2)
			if err != nil {
				t.Fatal(err)
			}
			r = semanticSnapshotFixture(t, image.Record)
			switch mode {
			case "summary-digest":
				r.HistorySummaries[0].Payload[0] ^= 1
			case "index-gap":
				r.CommitIndex = r.CommitIndex[1:]
			case "legacy-evidence":
				r.Format = durable.RecordFormat
			}
			data, err := recordio.EncodeSnapshot(r)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(data)
			if _, err := s.db.Exec(`UPDATE durable_snapshots SET record=?,digest=? WHERE run_id=? AND revision='2'`, data, digest[:], r.RunID); err != nil {
				t.Fatal(err)
			}
			ss = acquire(t, s, r.RunID)
			got, loadErr := ss.Load(ctx)
			if err := ss.Close(); err != nil {
				t.Fatal(err)
			}
			history, historyErr := s.LoadRevision(ctx, r.RunID, 2)
			_, compactErr := s.CompactStorage(ctx, durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: 2, ExpectedGeneration: 2})
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
			ss = acquire(t, s, r.RunID)
			loaded, err := ss.Load(ctx)
			if err != nil || loaded.HistorySummaries[0].Payload[0] != 0xff {
				t.Fatalf("Load retained caller payload alias: %v", err)
			}
		})
	}
}
