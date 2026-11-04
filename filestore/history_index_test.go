package filestore

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
)

func historyFixture(t *testing.T, revisions int) (*Store, manifest, map[uint64]durable.RevisionImage, map[uint64]batchRef) {
	t.Helper()
	st := newTestStore(t)
	ss := acquireTestSession(t, st, "indexed")
	r := incrementalSeed("indexed")
	if err := ss.Initialize(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	inner := ss.(*session)
	images := map[uint64]durable.RevisionImage{1: {Record: durable.CloneRecord(r), Chain: inner.head.HeadChain}}
	refs := map[uint64]batchRef{1: originBatch(inner.head)}
	for r.Revision < uint64(revisions) {
		b := revisionBatch(t, r)
		if _, err := ss.Apply(t.Context(), b); err != nil {
			t.Fatal(err)
		}
		if _, err := b.ApplyTo(&r); err != nil {
			t.Fatal(err)
		}
		images[r.Revision] = durable.RevisionImage{Record: durable.CloneRecord(r), Chain: inner.head.HeadChain}
		refs[r.Revision] = headBatch(inner.head)
	}
	h := inner.head
	if err := ss.Close(); err != nil {
		t.Fatal(err)
	}
	return st, h, images, refs
}
func compactThrough(t *testing.T, st *Store, through, generation uint64) durable.StorageState {
	t.Helper()
	state, err := st.CompactStorage(t.Context(), durable.CompactStorageRequest{RunID: "indexed", ThroughRevision: through, ExpectedGeneration: generation})
	if err != nil {
		t.Fatal(err)
	}
	return state
}
func requireImage(t *testing.T, st *Store, revision uint64, want durable.RevisionImage) {
	t.Helper()
	got, err := st.LoadRevision(t.Context(), "indexed", revision)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("revision %d: %v, image matches=%v", revision, err, reflect.DeepEqual(got, want))
	}
}
func requireInvalidHistory(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, durable.ErrInvalidRecord) || errors.Is(err, durable.ErrHistoryExpired) {
		t.Fatalf("expected corrupt retained path, got %v", err)
	}
}
func storeFiles(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	files := make(map[string][]byte)
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".lock") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		files[entry.Name()] = data
	}
	return files
}

func TestSameRevisionHeadBaseReferencesMustMatch(t *testing.T) {
	for _, field := range []string{"chain", "id", "index_digest"} {
		t.Run(field, func(t *testing.T) {
			st, _, _, _ := historyFixture(t, 3)
			state := compactThrough(t, st, 3, 1)
			path := filepath.Join(st.dir, fileBase("indexed")+".head")
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var h manifest
			if err = json.Unmarshal(data, &h); err != nil {
				t.Fatal(err)
			}
			switch field {
			case "chain":
				h.HeadChain[0] ^= 1
			case "id":
				h.HeadID, err = newID()
				if err != nil {
					t.Fatal(err)
				}
			case "index_digest":
				h.HeadIndexDigest[0] ^= 1
			}
			data, err = json.Marshal(h)
			if err != nil {
				t.Fatal(err)
			}
			if err = os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			before := storeFiles(t, st.dir)
			_, err = st.LoadRevision(t.Context(), "indexed", state.HeadRevision)
			requireInvalidHistory(t, err)
			_, err = st.CompactStorage(t.Context(), durable.CompactStorageRequest{RunID: "indexed", ThroughRevision: state.BaseRevision, ExpectedGeneration: state.Generation})
			requireInvalidHistory(t, err)
			if !reflect.DeepEqual(storeFiles(t, st.dir), before) {
				t.Fatal("invalid head/base references changed storage")
			}
		})
	}
}

func TestRevisionOneAndNoopIgnoreLaterCorruption(t *testing.T) {
	st, h, images, refs := historyFixture(t, 8)
	path := filepath.Join(st.dir, fileBase("indexed")+".batch-"+refs[2].ID)
	if err := os.WriteFile(path, []byte("corrupt revision 2"), 0600); err != nil {
		t.Fatal(err)
	}
	requireImage(t, st, 1, images[1])
	before := storeFiles(t, st.dir)
	state := compactThrough(t, st, 1, h.Generation)
	if state.Generation != h.Generation || state.BaseRevision != 1 || !reflect.DeepEqual(storeFiles(t, st.dir), before) {
		t.Fatal("same-base synthesis changed storage")
	}
	_, err := st.LoadRevision(t.Context(), "indexed", 2)
	requireInvalidHistory(t, err)
	_, err = st.CompactStorage(t.Context(), durable.CompactStorageRequest{RunID: "indexed", ThroughRevision: 2, ExpectedGeneration: h.Generation})
	requireInvalidHistory(t, err)
	_, err = st.AuditHistory(t.Context(), durable.AuditHistoryRequest{RunID: "indexed"})
	requireInvalidHistory(t, err)
}

func TestArchivedCorruptionIsExplicitlyAudited(t *testing.T) {
	st, _, images, refs := historyFixture(t, 12)
	compactThrough(t, st, 6, 1)
	state := compactThrough(t, st, 8, 2)
	path := filepath.Join(st.dir, fileBase("indexed")+".batch-"+refs[2].ID)
	if err := os.WriteFile(path, []byte("corrupt archived batch"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, rev := range []uint64{1, 6, 7, 8, 9, 12} {
		requireImage(t, st, rev, images[rev])
	}
	before := storeFiles(t, st.dir)
	after := compactThrough(t, st, 8, state.Generation)
	if after != state || !reflect.DeepEqual(storeFiles(t, st.dir), before) {
		t.Fatal("no-op wrote a new representation")
	}
	compactThrough(t, st, 10, state.Generation)
	_, err := st.LoadRevision(t.Context(), "indexed", 2)
	requireInvalidHistory(t, err)
	_, err = st.AuditHistory(t.Context(), durable.AuditHistoryRequest{RunID: "indexed"})
	requireInvalidHistory(t, err)
}

func TestRoutingDoesNotDecodeUnneededSuffix(t *testing.T) {
	st, _, images, refs := historyFixture(t, 17)
	path := filepath.Join(st.dir, fileBase("indexed")+".batch-"+refs[17].ID)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// 保留经过 head 认证的路由头，只破坏目标之后的 payload。
	data[batchIndexSize] ^= 1
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	requireImage(t, st, 2, images[2])
	state := compactThrough(t, st, 9, 1)
	if state.HeadRevision != 17 || state.HeadChain != images[17].Chain {
		t.Fatal("suffix head changed")
	}
	requireImage(t, st, 9, images[9])
	compactThrough(t, st, 9, state.Generation)
	_, err = st.LoadRevision(t.Context(), "indexed", 17)
	requireInvalidHistory(t, err)
	_, err = st.AuditHistory(t.Context(), durable.AuditHistoryRequest{RunID: "indexed"})
	requireInvalidHistory(t, err)
}

func TestIndexedHistoryAcrossManyCompactionsAndAppend(t *testing.T) {
	st, _, images, _ := historyFixture(t, 64)
	generation := uint64(1)
	for _, through := range []uint64{3, 6, 9, 14, 18, 23, 29, 33, 40, 45, 49, 55, 61} {
		state := compactThrough(t, st, through, generation)
		generation = state.Generation
	}
	reopened, err := New(st.dir)
	if err != nil {
		t.Fatal(err)
	}
	for rev := uint64(1); rev <= 64; rev++ {
		requireImage(t, reopened, rev, images[rev])
	}
	ss := acquireTestSession(t, reopened, "indexed")
	r, err := ss.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	b := revisionBatch(t, r)
	if _, err = ss.Apply(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	if _, err = b.ApplyTo(&r); err != nil {
		t.Fatal(err)
	}
	images[65] = durable.RevisionImage{Record: durable.CloneRecord(r), Chain: ss.(*session).head.HeadChain}
	ss.Close()
	for rev := uint64(1); rev <= 65; rev++ {
		requireImage(t, reopened, rev, images[rev])
	}
	result, err := reopened.AuditHistory(t.Context(), durable.AuditHistoryRequest{RunID: "indexed"})
	if err != nil || result.Snapshots != generation || result.Batches != 64 || result.State.HeadChain != images[65].Chain || result.BatchBytes == 0 {
		t.Fatalf("audit %+v: %v", result, err)
	}
	isolated, err := reopened.LoadRevision(t.Context(), "indexed", 65)
	if err != nil {
		t.Fatal(err)
	}
	delete(isolated.Record.Attempts, "attempt-1")
	requireImage(t, reopened, 65, images[65])
}

func TestNeededHistoryPathsRejectDamage(t *testing.T) {
	for _, mode := range []string{"missing-batch", "payload", "routing-header", "trailing-bytes", "snapshot", "missing-snapshot"} {
		t.Run(mode, func(t *testing.T) {
			st, h, _, refs := historyFixture(t, 3)
			path := filepath.Join(st.dir, fileBase("indexed")+".batch-"+refs[2].ID)
			if mode == "snapshot" || mode == "missing-snapshot" {
				path = filepath.Join(st.dir, fileBase("indexed")+".snapshot-"+h.OriginID)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "missing-batch", "missing-snapshot":
				err = os.Rename(path, path+".unreferenced")
			case "payload":
				data[batchIndexSize] ^= 1
			case "trailing-bytes":
				data = append(data, 'x')
			default:
				data[0] ^= 1
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode != "missing-batch" && mode != "missing-snapshot" {
				if err = os.WriteFile(path, data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			_, err = st.LoadRevision(t.Context(), "indexed", 2)
			requireInvalidHistory(t, err)
			_, err = st.CompactStorage(t.Context(), durable.CompactStorageRequest{RunID: "indexed", ThroughRevision: 2, ExpectedGeneration: 1})
			requireInvalidHistory(t, err)
			_, err = st.AuditHistory(t.Context(), durable.AuditHistoryRequest{RunID: "indexed"})
			requireInvalidHistory(t, err)
		})
	}
}

func TestHistoryRejectsAuthenticatedInvalidBatchSemantics(t *testing.T) {
	for _, mode := range []string{"identity", "precondition"} {
		t.Run(mode, func(t *testing.T) {
			st, h, _, refs := historyFixture(t, 2)
			inner := &session{dir: st.dir, runID: "indexed", path: filepath.Join(st.dir, fileBase("indexed")+".head")}
			b, index, err := inner.readBatch(refs[2], originBatch(h))
			if err != nil {
				t.Fatal(err)
			}
			wire, err := durable.MarshalChangeBatch(b)
			if err != nil {
				t.Fatal(err)
			}
			var batch map[string]any
			if err = json.Unmarshal(wire, &batch); err != nil {
				t.Fatal(err)
			}
			if mode == "identity" {
				batch["header"].(map[string]any)["run_id"] = "another-run"
			} else {
				op := batch["ops"].([]any)[0].(map[string]any)
				op["expected"] = op["value"]
			}
			wire, err = json.Marshal(batch)
			if err != nil {
				t.Fatal(err)
			}
			b, err = durable.UnmarshalChangeBatch(wire)
			if err != nil {
				t.Fatal(err)
			}
			wire, err = durable.MarshalChangeBatch(b)
			if err != nil {
				t.Fatal(err)
			}
			index.WireDigest, index.WireSize = sha256.Sum256(wire), uint64(len(wire))
			data := encodeBatch(h.HeadID, index, wire)
			h.HeadIndexDigest = sha256.Sum256(data[:batchIndexSize])
			h.HeadChain, err = durable.NextChain(index.Parent.Chain, index.WireDigest, 2)
			if err != nil {
				t.Fatal(err)
			}
			h.HistoryBytes, h.BatchBytes = uint64(len(data)), uint64(len(data))
			if err = os.WriteFile(inner.batchPath(h.HeadID), data, 0600); err != nil {
				t.Fatal(err)
			}
			head, _ := json.Marshal(h)
			if err = os.WriteFile(inner.path, head, 0600); err != nil {
				t.Fatal(err)
			}
			_, err = st.LoadRevision(t.Context(), "indexed", 2)
			requireInvalidHistory(t, err)
			_, err = st.CompactStorage(t.Context(), durable.CompactStorageRequest{RunID: "indexed", ThroughRevision: 2, ExpectedGeneration: 1})
			requireInvalidHistory(t, err)
			_, err = st.AuditHistory(t.Context(), durable.AuditHistoryRequest{RunID: "indexed"})
			requireInvalidHistory(t, err)
		})
	}
}

func TestSnapshotIndexIsAuthoritativeAndNoopValidatesBase(t *testing.T) {
	for _, mode := range []string{"index-digest", "missing-index", "snapshot-digest"} {
		t.Run(mode, func(t *testing.T) {
			st, _, images, _ := historyFixture(t, 10)
			compactThrough(t, st, 3, 1)
			compactThrough(t, st, 6, 2)
			state := compactThrough(t, st, 9, 3)
			ss := acquireTestSession(t, st, "indexed")
			inner := ss.(*session)
			h, err := inner.readHead()
			if err != nil {
				t.Fatal(err)
			}
			ss.Close()
			path := inner.snapshotIndexPath(h.SnapshotIndexID)
			if mode == "snapshot-digest" {
				path = inner.snapshotPath(h.SnapshotID)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "missing-index" {
				err = os.Rename(path, path+".orphan")
			} else {
				data[0] ^= 1
				err = os.WriteFile(path, data, 0600)
			}
			if err != nil {
				t.Fatal(err)
			}
			requireImage(t, st, 1, images[1])
			_, err = st.CompactStorage(t.Context(), durable.CompactStorageRequest{RunID: "indexed", ThroughRevision: 9, ExpectedGeneration: state.Generation})
			requireInvalidHistory(t, err)
			_, err = st.AuditHistory(t.Context(), durable.AuditHistoryRequest{RunID: "indexed"})
			requireInvalidHistory(t, err)
		})
	}
}

func TestOldManifestRefusesAllOperationsWithoutMutation(t *testing.T) {
	for _, format := range []int{1, 2} {
		t.Run(string(rune('0'+format)), func(t *testing.T) {
			st, h, _, _ := historyFixture(t, 3)
			h.Format = format
			data, _ := json.Marshal(h)
			if err := os.WriteFile(filepath.Join(st.dir, fileBase("indexed")+".head"), data, 0600); err != nil {
				t.Fatal(err)
			}
			createTestTemporary(t, st, "indexed")
			before := storeFiles(t, st.dir)
			ss := acquireTestSession(t, st, "indexed")
			_, err := ss.Load(t.Context())
			requireInvalidHistory(t, err)
			requireInvalidHistory(t, ss.Initialize(t.Context(), incrementalSeed("indexed")))
			ss.Close()
			_, err = st.InspectStorage(t.Context(), "indexed")
			requireInvalidHistory(t, err)
			_, err = st.LoadRevision(t.Context(), "indexed", 1)
			requireInvalidHistory(t, err)
			_, err = st.CompactStorage(t.Context(), durable.CompactStorageRequest{RunID: "indexed", ThroughRevision: 2, ExpectedGeneration: 1})
			requireInvalidHistory(t, err)
			_, err = st.AuditHistory(t.Context(), durable.AuditHistoryRequest{RunID: "indexed"})
			requireInvalidHistory(t, err)
			if !reflect.DeepEqual(before, storeFiles(t, st.dir)) {
				t.Fatal("old layout changed")
			}
		})
	}
}

func TestApplyAddsOnlyOneBatchAndSmallHead(t *testing.T) {
	st, _, _, _ := historyFixture(t, 17)
	ss := acquireTestSession(t, st, "indexed")
	r, err := ss.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	before := storeFiles(t, st.dir)
	if _, err = ss.Apply(t.Context(), revisionBatch(t, r)); err != nil {
		t.Fatal(err)
	}
	after := storeFiles(t, st.dir)
	headName := fileBase("indexed") + ".head"
	if len(after) != len(before)+1 || len(after[headName]) > 4096 {
		t.Fatalf("file count %d -> %d, head bytes %d", len(before), len(after), len(after[headName]))
	}
	for path, data := range before {
		if path != headName && !bytes.Equal(after[path], data) {
			t.Fatalf("Apply rewrote %s", path)
		}
	}
}
