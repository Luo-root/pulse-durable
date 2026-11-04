package sqlitestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
)

func TestMaintenanceSynthesisHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runs.sqlite")
	s := openStore(t, path)
	r, batches := runtimeBatches(t, "history")
	ss := acquire(t, s, r.RunID)
	if err := ss.Initialize(ctx, r); err != nil {
		t.Fatal(err)
	}
	images := []durable.RunRecord{durable.CloneRecord(r)}
	for _, b := range batches[:len(batches)-1] {
		if _, err := ss.Apply(ctx, b); err != nil {
			t.Fatal(err)
		}
		if _, err := b.ApplyTo(&r); err != nil {
			t.Fatal(err)
		}
		images = append(images, durable.CloneRecord(r))
	}
	before, err := s.InspectStorage(ctx, r.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.CompactStorage(ctx, durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: 2, ExpectedGeneration: 1}); !errors.Is(err, durable.ErrBusy) {
		t.Fatalf("active session: %v", err)
	}
	if err = ss.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := s.CompactStorage(ctx, durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: 2, ExpectedGeneration: 1})
	if err != nil {
		t.Fatal(err)
	}
	if after.Generation != 2 || after.BaseRevision != 2 || after.HeadRevision != before.HeadRevision || after.HeadChain != before.HeadChain {
		t.Fatalf("changed business head: before=%+v after=%+v", before, after)
	}
	for i, want := range images {
		got, e := s.LoadRevision(ctx, r.RunID, uint64(i+1))
		if e != nil || !reflect.DeepEqual(got.Record, want) {
			t.Fatalf("history %d: %v\ngot=%#v\nwant=%#v", i+1, e, got.Record, want)
		}
	}
	if _, err = s.CompactStorage(ctx, durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: 2, ExpectedGeneration: 1}); !errors.Is(err, durable.ErrConflict) {
		t.Fatalf("stale generation: %v", err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, path)
	ss = acquire(t, s, r.RunID)
	got, err := ss.Load(ctx)
	if err != nil || !reflect.DeepEqual(got, r) {
		t.Fatalf("reopen: %v", err)
	}
	if _, err = ss.Apply(ctx, batches[len(batches)-1]); err != nil {
		t.Fatal(err)
	}
	if _, err = batches[len(batches)-1].ApplyTo(&r); err != nil {
		t.Fatal(err)
	}
	ss.Close()
	after, err = s.CompactStorage(ctx, durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: r.Revision, ExpectedGeneration: 2})
	if err != nil {
		t.Fatal(err)
	}
	if after.Generation != 3 || after.HeadRevision != r.Revision {
		t.Fatal(after)
	}
	exact, err := s.LoadRevision(ctx, r.RunID, r.Revision)
	if err != nil || !reflect.DeepEqual(exact.Record, r) || exact.Chain != after.HeadChain {
		t.Fatalf("second synthesis: %v", err)
	}
	old, err := s.LoadRevision(ctx, r.RunID, 1)
	if err != nil || !reflect.DeepEqual(old.Record, images[0]) {
		t.Fatalf("old history after multiple synthesis: %v", err)
	}
	ss = acquire(t, s, r.RunID)
	got, err = ss.Load(ctx)
	if err != nil || !reflect.DeepEqual(got, r) {
		t.Fatalf("head-base reopen: %v", err)
	}
}

// snapshotBytes 捕获物理不可变锚点，而非仅捕获解码值。
func snapshotBytes(t *testing.T, s *Store, id, revision string) (record, digest, chain []byte) {
	t.Helper()
	if err := s.db.QueryRow(`SELECT record,digest,chain FROM durable_snapshots WHERE run_id=? AND revision=?`, id, revision).Scan(&record, &digest, &chain); err != nil {
		t.Fatal(err)
	}
	return
}

func TestMaintenanceSameBasePreservesImmutableSnapshot(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "immutable.sqlite"))
	r, batches := runtimeBatches(t, "immutable")
	ss := acquire(t, s, r.RunID)
	if err := ss.Initialize(ctx, r); err != nil {
		t.Fatal(err)
	}
	ss.Close()
	// 合法的非规范字节确保重新编码会明显改变
	// 原始修订 1 种子，即使解码后的记录完全相同。
	data, _, _ := snapshotBytes(t, s, r.RunID, "1")
	data = append([]byte(" \n"), data...)
	digest := sha256.Sum256(data)
	seed := durable.SeedChain(digest)
	if _, err := s.db.Exec(`UPDATE durable_snapshots SET record=?,digest=?,chain=? WHERE run_id=? AND revision='1'`, data, digest[:], seed[:], r.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE durable_heads SET chain=? WHERE run_id=?`, seed[:], r.RunID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE durable_manifests SET base_chain=? WHERE run_id=?`, seed[:], r.RunID); err != nil {
		t.Fatal(err)
	}
	ss = acquire(t, s, r.RunID)
	wants := []durable.RunRecord{durable.CloneRecord(r)}
	for _, batch := range batches {
		if _, err := ss.Apply(ctx, batch); err != nil {
			t.Fatal(err)
		}
		if _, err := batch.ApplyTo(&r); err != nil {
			t.Fatal(err)
		}
		wants = append(wants, durable.CloneRecord(r))
	}
	ss.Close()
	state, err := s.InspectStorage(ctx, r.RunID)
	if err != nil {
		t.Fatal(err)
	}
	headRevision, headChain := state.HeadRevision, state.HeadChain
	for _, base := range []struct {
		n uint64
		s string
	}{{1, "1"}, {2, "2"}} {
		if base.n != state.BaseRevision {
			state, err = s.CompactStorage(ctx, durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: base.n, ExpectedGeneration: state.Generation})
			if err != nil {
				t.Fatal(err)
			}
			// 合成快照也接受非规范的合法 JSON；其
			// 链保持原始批次锚点，而非快照摘要。
			data, _, _ = snapshotBytes(t, s, r.RunID, base.s)
			data = append([]byte(" \n"), data...)
			digest = sha256.Sum256(data)
			if _, err = s.db.Exec(`UPDATE durable_snapshots SET record=?,digest=? WHERE run_id=? AND revision=?`, data, digest[:], r.RunID, base.s); err != nil {
				t.Fatal(err)
			}
		}
		wantData, wantDigest, wantChain := snapshotBytes(t, s, r.RunID, base.s)
		for attempt := 0; attempt < 3; attempt++ {
			generation := state.Generation
			state, err = s.CompactStorage(ctx, durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: base.n, ExpectedGeneration: generation})
			if err != nil {
				t.Fatalf("base %d attempt %d: %v", base.n, attempt, err)
			}
			if state.Generation != generation+1 || state.BaseRevision != base.n || state.HeadRevision != headRevision || state.HeadChain != headChain || state.BaseChain != toChain(wantChain) {
				t.Fatalf("unexpected state: %+v", state)
			}
			gotData, gotDigest, gotChain := snapshotBytes(t, s, r.RunID, base.s)
			if !bytes.Equal(gotData, wantData) || !bytes.Equal(gotDigest, wantDigest) || !bytes.Equal(gotChain, wantChain) {
				t.Fatalf("base %d attempt %d replaced immutable snapshot", base.n, attempt)
			}
			for i, want := range wants {
				got, err := s.LoadRevision(ctx, r.RunID, uint64(i+1))
				if err != nil || !reflect.DeepEqual(got.Record, want) {
					t.Fatalf("base %d history %d: %v", base.n, i+1, err)
				}
			}
			if got, err := s.InspectStorage(ctx, r.RunID); err != nil || got != state {
				t.Fatalf("persisted state: %+v %v", got, err)
			}
		}
	}
}

func TestMaintenanceRejectsExistingNewBase(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "conflict.sqlite"))
	r, batches := runtimeBatches(t, "conflict")
	ss := acquire(t, s, r.RunID)
	if err := ss.Initialize(ctx, r); err != nil {
		t.Fatal(err)
	}
	for _, batch := range batches {
		if _, err := ss.Apply(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	ss.Close()
	before, err := s.InspectStorage(ctx, r.RunID)
	if err != nil {
		t.Fatal(err)
	}
	image, err := s.LoadRevision(ctx, r.RunID, 2)
	if err != nil {
		t.Fatal(err)
	}
	data, err := recordio.EncodeSnapshot(image.Record)
	if err != nil {
		t.Fatal(err)
	}
	data = append([]byte(" \n"), data...)
	digest := sha256.Sum256(data)
	if _, err = s.db.Exec(`INSERT INTO durable_snapshots (run_id,revision,record,digest,chain) VALUES (?,'2',?,?,?)`, r.RunID, data, digest[:], image.Chain[:]); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CompactStorage(ctx, durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: 2, ExpectedGeneration: before.Generation}); !errors.Is(err, durable.ErrConflict) {
		t.Fatalf("existing new base: %v", err)
	}
	gotData, gotDigest, gotChain := snapshotBytes(t, s, r.RunID, "2")
	if !bytes.Equal(data, gotData) || !bytes.Equal(digest[:], gotDigest) || !bytes.Equal(image.Chain[:], gotChain) {
		t.Fatal("conflicting snapshot changed")
	}
	if after, err := s.InspectStorage(ctx, r.RunID); err != nil || before != after {
		t.Fatalf("conflict published: %+v %v", after, err)
	}
}

func TestMaintenanceRejectsCorruptHeadSuffix(t *testing.T) {
	for _, mutation := range []string{"wire", "parent-chain", "missing-batch"} {
		for _, through := range []uint64{1, 2} {
			t.Run(mutation+"/"+string(rune('0'+through)), func(t *testing.T) {
				s := openStore(t, filepath.Join(t.TempDir(), "suffix.sqlite"))
				r, batches, _ := atomicMessageFixture(t)
				ss := acquire(t, s, r.RunID)
				if err := ss.Initialize(ctx, r); err != nil {
					t.Fatal(err)
				}
				for _, batch := range batches {
					if _, err := ss.Apply(ctx, batch); err != nil {
						t.Fatal(err)
					}
				}
				ss.Close()
				before, err := s.InspectStorage(ctx, r.RunID)
				if err != nil {
					t.Fatal(err)
				}
				if before.HeadRevision <= 3 {
					t.Fatal("fixture must leave a suffix after revision 3")
				}
				switch mutation {
				case "wire":
					_, err = s.db.Exec(`UPDATE durable_batches SET wire=x'7b' WHERE revision='3'`)
				case "parent-chain":
					_, err = s.db.Exec(`UPDATE durable_batches SET parent_chain=zeroblob(32) WHERE revision='3'`)
				case "missing-batch":
					_, err = s.db.Exec(`DELETE FROM durable_batches WHERE revision='3'`)
				}
				if err != nil {
					t.Fatal(err)
				}
				if _, err = s.CompactStorage(ctx, durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: through, ExpectedGeneration: before.Generation}); !errors.Is(err, durable.ErrInvalidRecord) {
					t.Fatalf("corrupt suffix accepted: %v", err)
				}
				if after, err := s.InspectStorage(ctx, r.RunID); err != nil || before != after {
					t.Fatalf("corruption published: %+v %v", after, err)
				}
				var count int
				if err = s.db.QueryRow(`SELECT count(*) FROM durable_snapshots WHERE run_id=?`, r.RunID).Scan(&count); err != nil || count != 1 {
					t.Fatalf("snapshot inserted on failed compaction: %d %v", count, err)
				}
			})
		}
	}
}

func TestMaintenanceCancellationAndCorruption(t *testing.T) {
	for _, mutation := range []string{"snapshot", "snapshot-chain", "batch-chain", "missing-manifest"} {
		t.Run(mutation, func(t *testing.T) {
			s := openStore(t, filepath.Join(t.TempDir(), "runs.sqlite"))
			r, b := runtimeBatches(t, "corrupt")
			ss := acquire(t, s, r.RunID)
			if err := ss.Initialize(ctx, r); err != nil {
				t.Fatal(err)
			}
			for _, batch := range b {
				if _, err := ss.Apply(ctx, batch); err != nil {
					t.Fatal(err)
				}
			}
			ss.Close()
			before, err := s.InspectStorage(ctx, r.RunID)
			if err != nil {
				t.Fatal(err)
			}
			c, cancel := context.WithCancel(ctx)
			cancel()
			if _, err = s.CompactStorage(c, durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: 2, ExpectedGeneration: 1}); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			after, err := s.InspectStorage(ctx, r.RunID)
			if err != nil || before != after {
				t.Fatalf("cancel published: %v", err)
			}
			switch mutation {
			case "snapshot":
				_, err = s.db.Exec(`UPDATE durable_snapshots SET record=x'7b'`)
			case "snapshot-chain":
				_, err = s.db.Exec(`UPDATE durable_snapshots SET chain=zeroblob(32)`)
			case "batch-chain":
				_, err = s.db.Exec(`UPDATE durable_batches SET chain=zeroblob(32) WHERE revision='2'`)
			case "missing-manifest":
				_, err = s.db.Exec(`DELETE FROM durable_manifests`)
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = s.LoadRevision(ctx, r.RunID, 2); !errors.Is(err, durable.ErrInvalidRecord) {
				t.Fatalf("corruption: %v", err)
			}
		})
	}
}
func TestRejectSchema2WithDataUnchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.sqlite")
	db, err := sql.Open("sqlite", filepath.ToSlash(path))
	if err != nil {
		t.Fatal(err)
	}
	ddl := `CREATE TABLE durable_snapshots (run_id TEXT PRIMARY KEY NOT NULL, record BLOB NOT NULL CHECK(typeof(record) = 'blob'), digest BLOB NOT NULL CHECK(length(digest) = 32), chain BLOB NOT NULL CHECK(length(chain) = 32)) WITHOUT ROWID`
	for _, statement := range []string{ddl, headSQL, batchSQL, `INSERT INTO durable_snapshots (run_id,record,digest,chain) VALUES ('old',x'00',zeroblob(32),zeroblob(32))`, `PRAGMA user_version=2`} {
		if _, err = db.Exec(statement); err != nil {
			db.Close()
			t.Fatal(err)
		}
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if opened, e := New(path); e == nil {
		opened.Close()
		t.Fatal("schema 2 adopted")
	}
	after, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("schema 2 changed: %v", err)
	}
}

func TestAtomicMessageSynthesisEveryRevision(t *testing.T) {
	r, batches, _ := atomicMessageFixture(t)
	s := openStore(t, filepath.Join(t.TempDir(), "atomic.sqlite"))
	ss := acquire(t, s, r.RunID)
	if err := ss.Initialize(ctx, r); err != nil {
		t.Fatal(err)
	}
	wants := []durable.RunRecord{durable.CloneRecord(r)}
	for _, batch := range batches {
		if _, err := ss.Apply(ctx, batch); err != nil {
			t.Fatal(err)
		}
		if _, err := batch.ApplyTo(&r); err != nil {
			t.Fatal(err)
		}
		wants = append(wants, durable.CloneRecord(r))
	}
	ss.Close()
	generation := uint64(1)
	for i := 1; i < len(wants); i++ {
		want := wants[i]
		state, err := s.CompactStorage(ctx, durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: want.Revision, ExpectedGeneration: generation})
		if err != nil {
			t.Fatalf("synthesize %d: %v", want.Revision, err)
		}
		generation = state.Generation
		got, err := s.LoadRevision(ctx, r.RunID, want.Revision)
		if err != nil || !reflect.DeepEqual(got.Record, want) {
			t.Fatalf("exact %d: %v", want.Revision, err)
		}
		ss = acquire(t, s, r.RunID)
		head, err := ss.Load(ctx)
		if err != nil || !reflect.DeepEqual(head, r) {
			t.Fatalf("suffix %d: %v", want.Revision, err)
		}
		ss.Close()
	}
}

func TestSynthesizedSnapshotLosslessAndStructure(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "runs.sqlite"))
	r, b := runtimeBatches(t, "lossless")
	ss := acquire(t, s, r.RunID)
	if err := ss.Initialize(ctx, r); err != nil {
		t.Fatal(err)
	}
	for _, batch := range b {
		if _, err := ss.Apply(ctx, batch); err != nil {
			t.Fatal(err)
		}
	}
	ss.Close()
	if _, err := s.CompactStorage(ctx, durable.CompactStorageRequest{RunID: r.RunID, ThroughRevision: 2, ExpectedGeneration: 1}); err != nil {
		t.Fatal(err)
	}
	image, err := s.LoadRevision(ctx, r.RunID, 2)
	if err != nil {
		t.Fatal(err)
	}
	// 重新编码一个无效但校验和正确的合成快照。不能仅因
	// 快照字节与其摘要一致就通过。
	delete(image.Record.Scopes, image.Record.RootScope)
	wire, err := recordio.EncodeSnapshot(image.Record)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(wire)
	if _, err = s.db.Exec(`UPDATE durable_snapshots SET record=?,digest=? WHERE revision='2'`, wire, sum[:]); err != nil {
		t.Fatal(err)
	}
	if _, err = s.LoadRevision(ctx, r.RunID, 2); !errors.Is(err, durable.ErrInvalidRecord) {
		t.Fatalf("invalid structure: %v", err)
	}
}
