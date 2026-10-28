package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
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

type capturedBatch struct {
	batch durable.ChangeBatch
	nodes int
	bytes uint64
}

type captureStore struct {
	durable.Store
	seed    durable.RunRecord
	batches []capturedBatch
}

type captureSession struct {
	durable.Session
	owner *captureStore
}

func (s *captureStore) Acquire(ctx context.Context, id string) (durable.Session, error) {
	base, err := s.Store.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	return &captureSession{Session: base, owner: s}, nil
}

func (s *captureSession) Initialize(ctx context.Context, r durable.RunRecord) error {
	if err := s.Session.Initialize(ctx, r); err != nil {
		return err
	}
	s.owner.seed = durable.CloneRecord(r)
	return nil
}

func (s *captureSession) Apply(ctx context.Context, b durable.ChangeBatch) (durable.CommitAck, error) {
	ack, err := s.Session.Apply(ctx, b)
	if err != nil {
		return durable.CommitAck{}, err
	}
	wire, err := durable.MarshalChangeBatch(b)
	if err != nil {
		return durable.CommitAck{}, err
	}
	copyBatch, err := durable.UnmarshalChangeBatch(wire)
	if err != nil {
		return durable.CommitAck{}, err
	}
	r, err := s.Session.Load(ctx)
	if err != nil {
		return durable.CommitAck{}, err
	}
	s.owner.batches = append(s.owner.batches, capturedBatch{batch: copyBatch, nodes: recordNodes(r), bytes: uint64(len(wire))})
	return ack, nil
}

func capturedRuntime(t *testing.T, nodes int) *captureStore {
	t.Helper()
	const id = "captured-runtime-replay-budget"
	store := &captureStore{Store: durable.NewMemoryStore()}
	runtime, err := durable.New(store, durable.NewCodecs())
	if err != nil {
		t.Fatal(err)
	}
	graph, err := pulse.New("chain", "one node per business step")
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]pulse.Key[uint64], nodes)
	for i := range keys {
		keys[i] = pulse.NewKey[uint64](fmt.Sprintf("v%d", i))
	}
	for i := range keys {
		requires := pulse.Requires[uint64]()
		if i > 0 {
			requires = pulse.Requires(keys[i-1])
		}
		n := pulse.NewNode(fmt.Sprintf("n%d", i), "compute", requires, pulse.Provides(keys[i]), func(rc *pulse.RunCtx) error {
			input := uint64(1)
			if i > 0 {
				v, err := pulse.Get(rc, keys[i-1])
				if err != nil {
					return err
				}
				input += v
			}
			return pulse.Set(rc, keys[i], compute(input, i, 1))
		})
		if err := graph.Add(n); err != nil {
			t.Fatal(err)
		}
	}
	if err := graph.DeclareOutput(pulse.NewPort(keys[nodes-1])); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Run(t.Context(), graph, pulse.Inputs{}, durable.RunOptions{RunID: id, Version: "compute/1"}); err != nil {
		t.Fatal(err)
	}
	return store
}

func TestCapturedRuntimeReplayBudget(t *testing.T) {
	captured := capturedRuntime(t, 10)
	if len(captured.batches) == 0 || captured.seed.Revision == 0 {
		t.Fatalf("runtime capture is empty: seed=%+v batches=%d", captured.seed, len(captured.batches))
	}
	var prefix, next int
	var budget uint64
	for i, b := range captured.batches {
		if b.nodes > 0 && b.nodes < 10 {
			prefix, next = b.nodes, i+1
			budget = uint64(lenJSON(t, captured.seed))
			for _, prior := range captured.batches[:next] {
				budget += prior.bytes
			}
			break
		}
	}
	if prefix == 0 || next >= len(captured.batches) {
		t.Fatalf("capture has no confirmed partial prefix: %+v", captured.batches)
	}
	base := durable.NewMemoryStore()
	meter := &meterStore{Store: base, budget: budget}
	session, err := meter.Acquire(t.Context(), captured.seed.RunID)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := session.Initialize(t.Context(), captured.seed); err != nil {
		t.Fatal(err)
	}
	for i, b := range captured.batches {
		_, err := session.Apply(t.Context(), b.batch)
		if i < next {
			if err != nil {
				t.Fatalf("replay batch %d: %v", i, err)
			}
			continue
		}
		if !errors.Is(err, errBudget) {
			t.Fatalf("next batch %d error=%v, want budget limit", i, err)
		}
		break
	}
	authority, err := session.Load(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got := recordNodes(authority); got != prefix {
		t.Fatalf("authority prefix=%d, want %d", got, prefix)
	}
	if meter.initialSnapshotBytes+meter.canonicalBatchBytes > budget {
		t.Fatalf("metered bytes=%d exceed budget=%d", meter.initialSnapshotBytes+meter.canonicalBatchBytes, budget)
	}
}

func lenJSON(t *testing.T, v any) int {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return len(data)
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
	c.MaxWriteBytes = 1
	c.Dir = localDir(t)
	r := runCase(c)
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
