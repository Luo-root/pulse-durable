package durable_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/filestore"
)

type runIDCountingStore struct {
	durable.Store
	calls atomic.Int32
}

func (s *runIDCountingStore) Acquire(ctx context.Context, id string) (durable.Session, error) {
	s.calls.Add(1)
	return s.Store.Acquire(ctx, id)
}

// 参数拒绝发生在 Store 之前，不能依赖后端的报错形状或误分类为可靠故障。
func TestRuntimeRunIDValidationPrecedesStore(t *testing.T) {
	for _, backend := range []string{"memory", "file"} {
		t.Run(backend, func(t *testing.T) {
			var store durable.Store = durable.NewMemoryStore()
			if backend == "file" {
				var err error
				store, err = filestore.New(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
			}
			counted := &runIDCountingStore{Store: store}
			rt, err := durable.New(counted, durable.NewCodecs())
			if err != nil {
				t.Fatal(err)
			}
			g, err := pulse.New("run-id-validation", "身份参数校验")
			if err != nil {
				t.Fatal(err)
			}
			var body atomic.Int32
			if err := g.Add(pulse.NewNode("work", "合法身份运行", nil, nil, func(*pulse.RunCtx) error {
				body.Add(1)
				return nil
			})); err != nil {
				t.Fatal(err)
			}
			for _, id := range []string{"invalid\xff", "\xfe", "truncated\xc3"} {
				for _, resume := range []bool{false, true} {
					opts := durable.RunOptions{RunID: id, Version: "v1"}
					var e *pulse.Execution
					if resume {
						e, err = rt.Resume(context.Background(), g, pulse.Inputs{}, opts)
					} else {
						e, err = rt.Run(context.Background(), g, pulse.Inputs{}, opts)
					}
					var reliable *pulse.CheckpointError
					if err == nil || e != nil || errors.As(err, &reliable) || counted.calls.Load() != 0 || body.Load() != 0 {
						t.Fatalf("invalid identity: Resume=%t e=%v err=%v acquire=%d body=%d", resume, e, err, counted.calls.Load(), body.Load())
					}
				}
			}
			// 有效 Unicode 不是非法字节的替代品；可以原样 Run/Resume，确认成果不重跑。
			opts := durable.RunOptions{RunID: "订单-世界-😀", Version: "v1"}
			if _, err := rt.Run(context.Background(), g, pulse.Inputs{}, opts); err != nil {
				t.Fatal(err)
			}
			if _, err := rt.Resume(context.Background(), g, pulse.Inputs{}, opts); err != nil {
				t.Fatal(err)
			}
			if counted.calls.Load() != 2 || body.Load() != 1 {
				t.Fatalf("valid identity acquire=%d body=%d, want 2/1", counted.calls.Load(), body.Load())
			}
		})
	}
}
