package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	if raw := os.Getenv("WORKFLOW_BENCH_CHILD"); raw != "" {
		childMain(raw)
		os.Exit(0)
	}
	os.Exit(m.Run())
}
func localDir(t testing.TB) string {
	t.Helper()
	dir, err := os.MkdirTemp(".", ".verify-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	})
	return dir
}

func TestCorrectness(t *testing.T) {
	for _, engine := range []string{"durable", "go-workflows"} {
		for _, storage := range []string{"memory", "file"} {
			for _, n := range []int{1, 2, 10, 100} {
				for _, rounds := range []int{1, 256} {
					t.Run(fmt.Sprintf("%s/%s/%d/%d", engine, storage, n, rounds), func(t *testing.T) {
						c := defaults()
						c.Engine = engine
						c.Backend = storage
						c.Nodes = n
						c.Rounds = rounds
						a := &audit{calls: make([]int, n)}
						ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						defer cancel()
						s, err := prepare(ctx, c, localDir(t), a, 0)
						if err != nil {
							t.Fatal(err)
						}
						t.Cleanup(func() {
							if err := s.close(); err != nil {
								t.Error(err)
							}
						})
						value, err := s.run(ctx, false)
						if err != nil || value != expected(c) {
							t.Fatalf("got %d, %v; want %d", value, err, expected(c))
						}
						confirmed, metrics := s.inspect(ctx)
						if confirmed != n {
							t.Fatalf("confirmed %d, want %d: %v", confirmed, n, metrics)
						}
						a.mu.Lock()
						defer a.mu.Unlock()
						for id, calls := range a.calls {
							if calls != 1 {
								t.Errorf("step %d calls=%d", id, calls)
							}
						}
						if a.maxActive != 1 || a.maxActive > c.Parallelism {
							t.Errorf("chain max active=%d", a.maxActive)
						}
					})
				}
			}
		}
	}
}

func TestGrowthAndBudget(t *testing.T) {
	for _, engine := range []string{"durable", "go-workflows"} {
		for _, storage := range []string{"memory", "file"} {
			c := defaults()
			c.Engine = engine
			c.Backend = storage
			c.Mode = "growth"
			c.Warmup = false
			c.Dir = localDir(t)
			r := runCase(c)
			if r.Status != "ok" || r.ConfirmedSteps != c.Nodes {
				t.Fatalf("%s/%s: %+v", engine, storage, r)
			}
			if engine == "durable" && r.Storage["canonical_batch_bytes"].(uint64) == 0 {
				t.Fatal("successful Apply must record canonical batch bytes")
			}
			if engine == "durable" && r.Storage["initial_snapshot_json_bytes"].(uint64) == 0 {
				t.Fatal("successful Initialize must record the full snapshot bytes")
			}
			if engine == "go-workflows" && r.Storage["history_events"].(int) <= c.Nodes {
				t.Fatal("missing activity workflow history")
			}
		}
	}
	c := defaults()
	c.Mode = "growth"
	c.Warmup = false
	c.MaxWriteBytes = 4096
	c.Dir = localDir(t)
	r := runCase(c)
	if r.Status != "resource_limit" || r.ConfirmedSteps <= 0 || r.ConfirmedSteps >= c.Nodes || r.Storage["initial_snapshot_json_bytes"].(uint64)+r.Storage["canonical_batch_bytes"].(uint64) > c.MaxWriteBytes || r.NSPerOp != nil {
		t.Fatalf("budget did not preserve partial progress: %+v", r)
	}
	c.MaxWriteBytes = 1
	r = runCase(c)
	if r.Status != "resource_limit" || r.Storage["initialize_calls"] != uint64(1) || r.Storage["successful_initializations"] != uint64(0) {
		t.Fatalf("initial save limit: %+v", r)
	}
}

func TestRealProcessRecovery(t *testing.T) {
	for _, engine := range []string{"durable", "go-workflows"} {
		for _, n := range []int{10, 100} {
			for _, mode := range []string{"resume", "reopen"} {
				t.Run(fmt.Sprintf("%s/%s/%d", engine, mode, n), func(t *testing.T) {
					c := defaults()
					c.Engine = engine
					c.Backend = "file"
					c.Nodes = n
					c.Mode = mode
					c.Warmup = false
					c.Dir = localDir(t)
					r := runCase(c)
					if r.Status != "ok" || r.Checksum == nil || *r.Checksum != expected(c) || r.ConfirmedSteps != n {
						t.Fatalf("%+v", r)
					}
					if err := checkAudit(filepath.Join(r.Dir, "audit.txt"), n, n); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}

// Optional ten-step run only; no scale matrix runs unless -bench is requested.
func BenchmarkRun(b *testing.B) {
	for _, engine := range []string{"durable", "go-workflows"} {
		for _, storage := range []string{"memory", "file"} {
			b.Run(engine+"/"+storage, func(b *testing.B) {
				b.StopTimer()
				c := defaults()
				c.Engine = engine
				c.Backend = storage
				dir := localDir(b)
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					s, err := prepare(context.Background(), c, filepath.Join(dir, fmt.Sprint(i)), nil, 0)
					if err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
					value, err := s.run(context.Background(), false)
					b.StopTimer()
					closeErr := s.close()
					if err != nil || closeErr != nil || value != expected(c) {
						b.Fatalf("value=%d run=%v close=%v", value, err, closeErr)
					}
				}
			})
		}
	}
}

func TestRunMetricsAndValidation(t *testing.T) {
	c := defaults()
	c.Iterations = 2
	c.Dir = localDir(t)
	r := runCase(c)
	if r.Status != "ok" || r.Iterations != 2 || r.RequestedIterations != 2 || r.CompletedIterations != 2 || r.NSPerOp == nil || *r.NSPerOp <= 0 || r.BytesPerOp == nil || *r.BytesPerOp <= 0 {
		t.Fatalf("%+v", r)
	}
	c.Iterations = 1
	c.MinTime = 20 * time.Millisecond
	c.Nodes = 1
	r = runCase(c)
	if r.Status != "ok" || r.Iterations <= 1 || r.Iterations != r.CompletedIterations || r.RequestedIterations != 1 {
		t.Fatalf("min-time iterations: %+v", r)
	}
	c.Backend = "memory"
	c.Mode = "resume"
	if runCase(c).Status != "error" {
		t.Fatal("memory recovery must be rejected")
	}
	c = defaults()
	c.Timeout = time.Nanosecond
	c.Dir = localDir(t)
	if r = runCase(c); r.Status != "timeout" || r.Iterations != 0 || r.NSPerOp != nil {
		t.Fatalf("expired case: %+v", r)
	}
}
