package sqlitestore

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)

// BenchmarkSQLiteRuntimeRunResume 测量完整 Runtime -> RunRecord ->
// recordio -> SQLite 路径，包括重新打开及权威 Resume。这是
// 本地开销检查，不是速度承诺；任何持久化或输出错误均使测试失败。
func BenchmarkSQLiteRuntimeRunResume(b *testing.B) {
	path := filepath.Join(b.TempDir(), "runs.sqlite")
	g, err := pulse.New("sqlite-cost", "")
	if err != nil {
		b.Fatal(err)
	}
	one, result := pulse.NewKey[int]("one"), pulse.NewKey[int]("result")
	calls := 0
	if err := g.Add(pulse.NewNode("first", "", nil, pulse.Provides(one), func(rc *pulse.RunCtx) error { calls++; return pulse.Set(rc, one, 41) })); err != nil {
		b.Fatal(err)
	}
	if err := g.Add(pulse.NewNode("second", "", pulse.Requires(one), pulse.Provides(result), func(rc *pulse.RunCtx) error {
		calls++
		v, err := pulse.Get(rc, one)
		if err != nil {
			return err
		}
		return pulse.Set(rc, result, v+1)
	})); err != nil {
		b.Fatal(err)
	}
	if err := g.DeclareOutput(pulse.NewPort(result)); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		func() {
			s, err := New(path)
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			rt, err := durable.New(s, durable.NewCodecs())
			if err != nil {
				b.Fatal(err)
			}
			opts := durable.RunOptions{RunID: fmt.Sprintf("run-%d", i), Version: "cost/1"}
			if _, err := rt.Run(context.Background(), g, pulse.Inputs{}, opts); err != nil {
				b.Fatal(err)
			}
			if err := s.Close(); err != nil {
				b.Fatal(err)
			}
			reopened, err := New(path)
			if err != nil {
				b.Fatal(err)
			}
			defer reopened.Close()
			rt, err = durable.New(reopened, durable.NewCodecs())
			if err != nil {
				b.Fatal(err)
			}
			e, err := rt.Resume(context.Background(), g, pulse.Inputs{}, opts)
			if err != nil {
				b.Fatal(err)
			}
			v, state, err := pulse.ReadOutput(e, result)
			if err != nil || state != pulse.SlotStateReady || v != 42 {
				b.Fatalf("output=%d state=%s err=%v", v, state, err)
			}
			ss, err := reopened.Acquire(context.Background(), opts.RunID)
			if err != nil {
				b.Fatal(err)
			}
			r, err := ss.Load(context.Background())
			if err != nil {
				b.Fatal(err)
			}
			if err := ss.Close(); err != nil {
				b.Fatal(err)
			}
			if r.Revision == 0 || len(r.Scopes[r.RootScope].Nodes) != 2 {
				b.Fatal("incomplete RunRecord")
			}
			if err := reopened.Close(); err != nil {
				b.Fatal(err)
			}
		}()
	}
	b.StopTimer()
	if calls != 2*b.N {
		b.Fatalf("Resume replayed business: calls=%d want=%d", calls, 2*b.N)
	}
}
