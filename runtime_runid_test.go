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

type runOptionsCountingStore struct {
	durable.Store
	calls atomic.Int32
}

func (s *runOptionsCountingStore) Acquire(ctx context.Context, id string) (durable.Session, error) {
	s.calls.Add(1)
	return s.Store.Acquire(ctx, id)
}

// 参数拒绝发生在 Store 之前，不能依赖后端的报错形状或误分类为可靠故障。
func TestRuntimeRunOptionsValidationPrecedesStore(t *testing.T) {
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
			counted := &runOptionsCountingStore{Store: store}
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
			for _, text := range []string{"", "invalid\xff", "\xfe", "truncated\xc3"} {
				for _, opts := range []durable.RunOptions{
					{RunID: text, Version: "v1"},
					{RunID: "version-validation", Version: text},
				} {
					for _, resume := range []bool{false, true} {
						var e *pulse.Execution
						if resume {
							e, err = rt.Resume(context.Background(), g, pulse.Inputs{}, opts)
						} else {
							e, err = rt.Run(context.Background(), g, pulse.Inputs{}, opts)
						}
						var reliable *pulse.CheckpointError
						if err == nil || e != nil || errors.As(err, &reliable) || counted.calls.Load() != 0 || body.Load() != 0 {
							t.Fatalf("invalid run ID=%q version=%q: Resume=%t e=%v err=%v acquire=%d body=%d", opts.RunID, opts.Version, resume, e, err, counted.calls.Load(), body.Load())
						}
					}
				}
			}
			// Version 是文本而非 SemVer；合法 Unicode 和控制字符可原样恢复，成果不重跑。
			versions := []string{"业务版本/α", "not semver", "\x00/版本/\ufffd"}
			for _, version := range versions {
				opts := durable.RunOptions{RunID: "订单-世界-😀/" + version, Version: version}
				if _, err := rt.Run(context.Background(), g, pulse.Inputs{}, opts); err != nil {
					t.Fatal(err)
				}
				if _, err := rt.Resume(context.Background(), g, pulse.Inputs{}, opts); err != nil {
					t.Fatal(err)
				}
			}
			if counted.calls.Load() != int32(2*len(versions)) || body.Load() != int32(len(versions)) {
				t.Fatalf("valid options acquire=%d body=%d, want %d/%d", counted.calls.Load(), body.Load(), 2*len(versions), len(versions))
			}
		})
	}
}
