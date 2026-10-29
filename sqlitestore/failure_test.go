package sqlitestore

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
)

func TestApplyFailureInvalidatesProjection(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "r.sqlite"))
	ss := acquire(t, s, "id")
	r, bs := runtimeBatches(t, "id")
	if e := ss.Initialize(ctx, r); e != nil {
		t.Fatal(e)
	}
	// 在类型化私有投影改变后、事务提交前强制失败。
	// 任何暂定投影都不得被观察或复用。
	if _, e := s.db.Exec(`CREATE TRIGGER fail_batch BEFORE INSERT ON durable_batches BEGIN SELECT RAISE(ABORT,'injected batch failure'); END`); e != nil {
		t.Fatal(e)
	}
	if _, e := ss.Apply(ctx, bs[0]); e == nil {
		t.Fatal("injection did not fail")
	}
	if _, e := ss.Load(ctx); !errors.Is(e, errUncertain) {
		t.Fatal("tentative cache escaped", e)
	}
	if _, e := ss.Apply(ctx, bs[0]); !errors.Is(e, errUncertain) {
		t.Fatal("continued poisoned session", e)
	}
	ss.Close()
	fresh := acquire(t, s, "id")
	got, e := fresh.Load(ctx)
	if e != nil || !reflect.DeepEqual(got, r) {
		t.Fatal("partial transaction published", e)
	}
	if _, e = s.db.Exec(`DROP TRIGGER fail_batch`); e != nil {
		t.Fatal(e)
	}
	if _, e = fresh.Apply(ctx, bs[0]); e != nil {
		t.Fatal(e)
	}
}
func TestHeadCASBindsChainNotOnlyRevision(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "r.sqlite"))
	ss := acquire(t, s, "id")
	r, bs := runtimeBatches(t, "id")
	if e := ss.Initialize(ctx, r); e != nil {
		t.Fatal(e)
	}
	if _, e := s.db.Exec(`UPDATE durable_heads SET chain=zeroblob(32) WHERE run_id='id'`); e != nil {
		t.Fatal(e)
	}
	if _, e := ss.Apply(ctx, bs[0]); !errors.Is(e, durable.ErrConflict) {
		t.Fatal(e)
	}
	if _, e := ss.Load(ctx); !errors.Is(e, errUncertain) {
		t.Fatal("stale cache retained", e)
	}
	var count int
	if e := s.db.QueryRow(`SELECT count(*) FROM durable_batches`).Scan(&count); e != nil || count != 0 {
		t.Fatal("batch inserted despite head mismatch", e)
	}
}
func TestChainBudgetStopsWithoutPublication(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "r.sqlite"))
	ss := acquire(t, s, "id")
	r, bs := runtimeBatches(t, "id")
	if e := ss.Initialize(ctx, r); e != nil {
		t.Fatal(e)
	}
	concrete := ss.(*session)
	concrete.batchCount = maxChainBatches
	if _, e := ss.Apply(ctx, bs[0]); e == nil {
		t.Fatal("batch budget ignored")
	}
	concrete.batchCount = 0
	concrete.batchBytes = maxChainBytes
	if _, e := ss.Apply(ctx, bs[0]); e == nil {
		t.Fatal("byte budget ignored")
	}
	concrete.batchBytes = 0
	got, e := ss.Load(ctx)
	if e != nil || !reflect.DeepEqual(got, r) {
		t.Fatal("budget mutated projection", e)
	}
	if _, e = ss.Apply(ctx, bs[0]); e != nil {
		t.Fatal(e)
	}
}
