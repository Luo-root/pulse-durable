package filestore

import (
	"context"
	"crypto/sha256"

	"encoding/json"
	"errors"
	"github.com/Luo-root/pulse-durable/recordio"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
)

func TestMaintenanceExactHistoryAndAppend(t *testing.T) {
	st := newTestStore(t)
	ctx := t.Context()
	ss := acquireTestSession(t, st, "history")
	r := incrementalSeed("history")
	if err := ss.Initialize(ctx, r); err != nil {
		t.Fatal(err)
	}
	images := map[uint64]durable.RevisionImage{}
	inner := ss.(*session)
	images[1] = durable.RevisionImage{Record: durable.CloneRecord(r), Chain: inner.head.HeadChain}
	var oldBatch string
	for i := 0; i < 4; i++ {
		b := revisionBatch(t, r)
		if _, err := ss.Apply(ctx, b); err != nil {
			t.Fatal(err)
		}
		if _, err := b.ApplyTo(&r); err != nil {
			t.Fatal(err)
		}
		images[r.Revision] = durable.RevisionImage{Record: durable.CloneRecord(r), Chain: inner.head.HeadChain}
		if r.Revision == 2 {
			oldBatch = inner.batchPath(inner.head.HeadID)
		}
	}
	if _, err := st.InspectStorage(ctx, "history"); !errors.Is(err, durable.ErrBusy) {
		t.Fatal(err)
	}
	if _, err := st.LoadRevision(ctx, "history", 1); !errors.Is(err, durable.ErrBusy) {
		t.Fatal(err)
	}
	if _, err := st.CompactStorage(ctx, durable.CompactStorageRequest{RunID: "history", ThroughRevision: 3, ExpectedGeneration: 1}); !errors.Is(err, durable.ErrBusy) {
		t.Fatal(err)
	}
	ss.Close()
	for _, through := range []uint64{3, 4, 5} {
		before, err := st.InspectStorage(ctx, "history")
		if err != nil {
			t.Fatal(err)
		}
		after, err := st.CompactStorage(ctx, durable.CompactStorageRequest{RunID: "history", ThroughRevision: through, ExpectedGeneration: before.Generation})
		if err != nil {
			t.Fatal(err)
		}
		if after.Generation != before.Generation+1 || after.BaseChain != images[through].Chain || after.HeadChain != images[5].Chain {
			t.Fatalf("state: %+v", after)
		}
		if _, err = st.CompactStorage(ctx, durable.CompactStorageRequest{RunID: "history", ThroughRevision: through, ExpectedGeneration: before.Generation}); !errors.Is(err, durable.ErrConflict) {
			t.Fatal(err)
		}
		reopened, err := New(st.dir)
		if err != nil {
			t.Fatal(err)
		}
		for rev, want := range images {
			got, err := reopened.LoadRevision(ctx, "history", rev)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("revision %d: %v", rev, err)
			}
		}
		ss = acquireTestSession(t, reopened, "history")
		got, err := ss.Load(ctx)
		if err != nil || !reflect.DeepEqual(got, r) {
			t.Fatalf("head: %v", err)
		}
		ss.Close()
	}
	ss = acquireTestSession(t, st, "history")
	b := revisionBatch(t, r)
	if _, err := ss.Apply(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, err := b.ApplyTo(&r); err != nil {
		t.Fatal(err)
	}
	newChain := ss.(*session).head.HeadChain
	ss.Close()
	got, err := st.LoadRevision(ctx, "history", 6)
	if err != nil || got.Chain != newChain || !reflect.DeepEqual(got.Record, r) {
		t.Fatalf("append: %v", err)
	}
	if err = os.WriteFile(oldBatch, []byte("corrupt retained history"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = st.LoadRevision(ctx, "history", 2); !errors.Is(err, durable.ErrInvalidRecord) || errors.Is(err, durable.ErrHistoryExpired) {
		t.Fatal(err)
	}
	if _, err = st.CompactStorage(ctx, durable.CompactStorageRequest{RunID: "history", ThroughRevision: 6, ExpectedGeneration: 4}); !errors.Is(err, durable.ErrInvalidRecord) {
		t.Fatal(err)
	}
	// 当前 Load 无须重放归档历史。
	ss = acquireTestSession(t, st, "history")
	if _, err = ss.Load(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestMaintenanceCancellationOverflowAndLegacy(t *testing.T) {
	st := newTestStore(t)
	ss := acquireTestSession(t, st, "limits")
	r := incrementalSeed("limits")
	if err := ss.Initialize(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	inner := ss.(*session)
	h := inner.head
	ss.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := st.CompactStorage(ctx, durable.CompactStorageRequest{RunID: "limits", ThroughRevision: 1, ExpectedGeneration: 1}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := st.CompactStorage(t.Context(), durable.CompactStorageRequest{RunID: "limits", ExpectedGeneration: 1}); !errors.Is(err, durable.ErrConflict) {
		t.Fatal(err)
	}
	h.Generation = ^uint64(0)
	data, _ := json.Marshal(h)
	if err := os.WriteFile(inner.path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CompactStorage(t.Context(), durable.CompactStorageRequest{RunID: "limits", ThroughRevision: 1, ExpectedGeneration: h.Generation}); !errors.Is(err, durable.ErrLimitExceeded) {
		t.Fatal(err)
	}
	h.Format = 1
	data, _ = json.Marshal(h)
	if err := os.WriteFile(inner.path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := st.InspectStorage(t.Context(), "limits"); err == nil {
		t.Fatal("legacy accepted")
	}
	after, err := os.ReadFile(inner.path)
	if err != nil || string(after) != string(data) {
		t.Fatal("legacy changed", err)
	}
}

// 在发布边界真实退出进程后，必须留下任一完整代际，
// 绝不能通过枚举目录采纳合成快照。
func TestMaintenanceProcessPublication(t *testing.T) {
	if dir := os.Getenv("PULSE_MAINTENANCE_CHILD_DIR"); dir != "" {
		st, err := New(dir)
		if err != nil {
			t.Fatal(err)
		}
		ss := acquireTestSession(t, st, "exit")
		inner := ss.(*session)
		if os.Getenv("PULSE_MAINTENANCE_STAGE") == "after" {
			inner.publicationSync = func(string) error { os.Exit(73); return nil }
		}
		h, err := inner.readHead()
		if err != nil {
			t.Fatal(err)
		}
		image, baseID, suffix, err := inner.history(t.Context(), h, 2)
		if err != nil {
			t.Fatal(err)
		}
		data, err := recordio.EncodeSnapshot(image.Record)
		if err != nil {
			t.Fatal(err)
		}
		id, err := newID()
		if err != nil {
			t.Fatal(err)
		}
		if err = inner.writeImmutable(t.Context(), inner.snapshotPath(id), data); err != nil {
			t.Fatal(err)
		}
		h.Generation++
		h.BaseRevision = 2
		h.BaseID = baseID
		h.ChainAnchor = image.Chain
		h.SnapshotID = id
		h.SnapshotDigest = sha256.Sum256(data)
		h.BatchBytes = suffix
		ctx := context.Context(t.Context())
		if os.Getenv("PULSE_MAINTENANCE_STAGE") == "before" {
			ctx = &exitContext{Context: ctx, target: 1}
		}
		if err = inner.publish(ctx, h); err != nil {
			t.Fatal(err)
		}
		t.Fatal("child did not exit")
	}
	for _, stage := range []string{"before", "after"} {
		t.Run(stage, func(t *testing.T) {
			st := newTestStore(t)
			ss := acquireTestSession(t, st, "exit")
			r := incrementalSeed("exit")
			if err := ss.Initialize(t.Context(), r); err != nil {
				t.Fatal(err)
			}
			if _, err := ss.Apply(t.Context(), revisionBatch(t, r)); err != nil {
				t.Fatal(err)
			}
			ss.Close()
			cmd := exec.Command(os.Args[0], "-test.run=^TestMaintenanceProcessPublication$")
			cmd.Env = append(os.Environ(), "PULSE_MAINTENANCE_CHILD_DIR="+st.dir, "PULSE_MAINTENANCE_STAGE="+stage)
			out, err := cmd.CombinedOutput()
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != 73 {
				t.Fatalf("exit %v: %s", err, out)
			}
			state, err := st.InspectStorage(t.Context(), "exit")
			if err != nil {
				t.Fatal(err)
			}
			want := uint64(1)
			if stage == "after" {
				want = 2
			}
			if state.Generation != want || state.BaseRevision != want {
				t.Fatalf("state %+v", state)
			}
			for _, rev := range []uint64{1, 2} {
				if _, err = st.LoadRevision(t.Context(), "exit", rev); err != nil {
					t.Fatal(err)
				}
			}
			entries, err := os.ReadDir(st.dir)
			if err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, e := range entries {
				if strings.Contains(e.Name(), "snapshot-") {
					count++
				}
			}
			if count != 2 {
				t.Fatalf("snapshot retained/orphan count %d", count)
			}
		})
	}
}

func TestPublicationSyncUnknownPoisonsSession(t *testing.T) {
	st := newTestStore(t)
	ss := acquireTestSession(t, st, "unknown")
	r := incrementalSeed("unknown")
	if err := ss.Initialize(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("directory sync failed")
	inner := ss.(*session)
	inner.publicationSync = func(string) error { return injected }
	_, err := ss.Apply(t.Context(), revisionBatch(t, r))
	var unknown *PublicationUnknownError
	if !errors.As(err, &unknown) || !errors.Is(err, injected) || !inner.poisoned {
		t.Fatal(err)
	}
	if _, err = ss.Load(t.Context()); err == nil {
		t.Fatal("uncertain session remained usable")
	}
	ss.Close()
	fresh := acquireTestSession(t, st, "unknown")
	got, err := fresh.Load(t.Context())
	if err != nil || got.Revision != 2 {
		t.Fatalf("published state: %v", err)
	}
}
