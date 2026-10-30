package filestore

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	pulse "github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)

func incrementalSeed(id string) durable.RunRecord {
	return durable.RunRecord{Format: durable.RecordFormat, RunID: id, Revision: 1, Version: "1", Definition: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Inputs: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", RootScope: "root", Scopes: map[string]durable.ScopeRecord{"root": {Graph: "g", Inputs: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", Nodes: map[string]durable.NodeRecord{}}}}
}
func TestIncrementalInitializationIsolationAndLegacy(t *testing.T) {
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ss, err := st.Acquire(ctx, "../run")
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()
	if _, err = ss.Load(ctx); !errors.Is(err, durable.ErrNotFound) {
		t.Fatal(err)
	}
	r := incrementalSeed("../run")
	if err = ss.Initialize(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err = ss.Initialize(ctx, r); !errors.Is(err, durable.ErrConflict) {
		t.Fatal(err)
	}
	delete(r.Scopes, "root")
	loaded, err := ss.Load(ctx)
	if err != nil || len(loaded.Scopes) != 1 {
		t.Fatalf("input isolation: %v", err)
	}
	delete(loaded.Scopes, "root")
	loaded, err = ss.Load(ctx)
	if err != nil || len(loaded.Scopes) != 1 {
		t.Fatalf("load isolation: %v", err)
	}
	if _, err = st.Acquire(ctx, "../run"); !errors.Is(err, durable.ErrBusy) {
		t.Fatal(err)
	}
	if err = ss.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = ss.Load(ctx); !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
	if err = ss.Initialize(ctx, incrementalSeed("../run")); !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
	if _, err = ss.Apply(ctx, durable.ChangeBatch{}); !errors.Is(err, os.ErrClosed) {
		t.Fatal(err)
	}
	legacy := []byte("{old-user-data}")
	path := filepath.Join(st.dir, fileBase("old")+".json")
	if err = os.WriteFile(path, legacy, 0600); err != nil {
		t.Fatal(err)
	}
	old, err := st.Acquire(ctx, "old")
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if _, err = old.Load(ctx); err == nil || errors.Is(err, durable.ErrNotFound) {
		t.Fatal("legacy accepted", err)
	}
	if err = old.Initialize(ctx, incrementalSeed("old")); err == nil {
		t.Fatal("legacy overwritten")
	}
	after, err := os.ReadFile(path)
	if err != nil || string(after) != string(legacy) {
		t.Fatal("legacy changed", err)
	}
}

type measuredStore struct {
	*Store
	sizes   []int64
	commits int
}
type measuredSession struct {
	durable.Session
	owner *measuredStore
}

func (s *measuredStore) Acquire(ctx context.Context, id string) (durable.Session, error) {
	ss, err := s.Store.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	return &measuredSession{ss, s}, nil
}
func (s *measuredSession) Apply(ctx context.Context, b durable.ChangeBatch) (durable.CommitAck, error) {
	ack, err := s.Session.Apply(ctx, b)
	if err != nil {
		return ack, err
	}
	inner := s.Session.(*session)
	info, err := os.Stat(inner.batchPath(inner.head.HeadID))
	if err != nil {
		return ack, err
	}
	s.owner.sizes = append(s.owner.sizes, info.Size())
	s.owner.commits++
	return ack, nil
}
func TestIncrementalRuntimeFiveHundredAndResume(t *testing.T) {
	st, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	measured := &measuredStore{Store: st}
	total, calls := 0, 0
	g, err := pulse.New("incremental-500", "")
	if err != nil {
		t.Fatal(err)
	}
	key := pulse.NewKey[pulse.Stream[int]]("numbers")
	if err = pulse.ProduceStream(g, "source", "", key, func(_ *pulse.RunCtx, w *pulse.StreamWriter[int]) error {
		for i := int(w.Position().Sequence) + 1; i <= 500; i++ {
			if err := w.SendAt(i, i); err != nil {
				return err
			}
		}
		return nil
	}, pulse.WithBuffer(1), pulse.WithStreamSource("integers", true)); err != nil {
		t.Fatal(err)
	}
	if err = pulse.ConsumeStream(g, "sum", "", key, func(rc *pulse.RunCtx, v int) error {
		old, _, err := pulse.GetMessageState[int](rc, "sum")
		if err != nil {
			return err
		}
		total = old + v
		calls++
		return pulse.SetMessageState(rc, "sum", total)
	}, pulse.WithMessageHandling(pulse.WithMessageRetry(2, 0))); err != nil {
		t.Fatal(err)
	}
	rt, err := durable.New(measured, durable.NewCodecs())
	if err != nil {
		t.Fatal(err)
	}
	opts := durable.RunOptions{RunID: "500", Version: "1"}
	if _, err = rt.Run(t.Context(), g, pulse.Inputs{}, opts); err != nil {
		t.Fatal(err)
	}
	if total != 125250 || calls != 500 || measured.commits < 1500 {
		t.Fatalf("sum=%d calls=%d commits=%d", total, calls, measured.commits)
	}
	// 每个不可变文件仅包含自身原子批次，而非累积历史。
	var first, last int64
	for i, n := range measured.sizes {
		if i < 100 {
			first += n
		}
		if i >= len(measured.sizes)-100 {
			last += n
		}
	}
	if last > first*3 {
		t.Fatalf("batch byte growth: first100=%d last100=%d", first, last)
	}
	t.Logf("actual immutable bytes first100=%d last100=%d commits=%d", first, last, measured.commits)
	reopened, err := New(st.dir)
	if err != nil {
		t.Fatal(err)
	}
	rt, err = durable.New(reopened, durable.NewCodecs())
	if err != nil {
		t.Fatal(err)
	}
	total = -1
	if _, err = rt.Resume(t.Context(), g, pulse.Inputs{}, opts); err != nil {
		t.Fatal(err)
	}
	if total != -1 || calls != 500 {
		t.Fatalf("confirmed work reran: total=%d calls=%d", total, calls)
	}
	ss, err := reopened.Acquire(t.Context(), "500")
	if err != nil {
		t.Fatal(err)
	}
	inner := ss.(*session)
	if _, err = ss.Load(t.Context()); err != nil {
		t.Fatal(err)
	}
	path := inner.batchPath(inner.head.HeadID)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// 缺少头所选择的批次必须失败；不能转而识别其他文件。
	if err = os.Rename(path, path+".missing"); err != nil {
		t.Fatal(err)
	}
	if _, err = ss.Load(t.Context()); err == nil {
		t.Fatal("missing batch accepted")
	}
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 1
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = ss.Load(t.Context()); err == nil {
		t.Fatal("corrupt batch accepted")
	}
	if err = ss.Close(); err != nil {
		t.Fatal(err)
	}
}
