// workflow 对比每个业务步骤分别使用一个 durable 节点和一个 go-workflows 活动的情况。
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

type config struct {
	Engine        string        `json:"engine"`
	Backend       string        `json:"backend"`
	Shape         string        `json:"shape"`
	Nodes         int           `json:"nodes"`
	Rounds        int           `json:"rounds"`
	Parallelism   int           `json:"parallelism"`
	Procs         int           `json:"procs"`
	Mode          string        `json:"mode"`
	Iterations    int           `json:"iterations"`
	Timeout       time.Duration `json:"timeout_ns"`
	MinTime       time.Duration `json:"min_time_ns"`
	Dir           string        `json:"dir"`
	Warmup        bool          `json:"warmup"`
	MaxWriteBytes uint64        `json:"max_write_bytes"`
}

type result struct {
	config
	Edges               int            `json:"edges"`
	CriticalPath        int            `json:"critical_path"`
	NSPerOp             *float64       `json:"ns_per_op"`
	BytesPerOp          *float64       `json:"bytes_per_op"`
	AllocsPerOp         *float64       `json:"allocs_per_op"`
	CPUPerOp            *float64       `json:"cpu_ns_per_op"`
	CPUNS               int64          `json:"cpu_ns"`
	SetupNS             int64          `json:"setup_ns"`
	WarmupNS            int64          `json:"warmup_ns"`
	LaunchToChildNS     int64          `json:"launch_to_child_main_ns,omitempty"`
	ElapsedNS           int64          `json:"elapsed_ns"`
	AllocatedBytes      uint64         `json:"allocated_bytes"`
	Allocations         uint64         `json:"allocations"`
	CompletedIterations int            `json:"completed_iterations"`
	RequestedIterations int            `json:"requested_iterations"`
	ConfirmedSteps      int            `json:"confirmed_steps"`
	Checksum            *uint64        `json:"checksum"`
	Status              string         `json:"status"`
	Error               string         `json:"error,omitempty"`
	Storage             map[string]any `json:"storage,omitempty"`
	Recovery            map[string]any `json:"recovery,omitempty"`
	Notes               []string       `json:"notes"`
}

var errBudget = errors.New("logical JSON write budget exceeded")

func defaults() config {
	return config{Engine: "durable", Backend: "memory", Shape: "chain", Nodes: 10, Rounds: 1,
		Parallelism: 4, Procs: 4, Mode: "run", Iterations: 1, Timeout: 60 * time.Second, Dir: ".", Warmup: true, MaxWriteBytes: 1 << 30}
}

func main() {
	if raw := os.Getenv("WORKFLOW_BENCH_CHILD"); raw != "" {
		childMain(raw)
		return
	}
	c := defaults()
	flag.StringVar(&c.Engine, "engine", c.Engine, "durable|go-workflows")
	flag.StringVar(&c.Backend, "backend", c.Backend, "memory|file")
	flag.StringVar(&c.Shape, "shape", c.Shape, "chain")
	flag.IntVar(&c.Nodes, "nodes", c.Nodes, "business steps")
	flag.IntVar(&c.Rounds, "rounds", c.Rounds, "compute rounds per step")
	flag.IntVar(&c.Parallelism, "parallelism", c.Parallelism, "maximum activity/node concurrency")
	flag.IntVar(&c.Procs, "procs", c.Procs, "GOMAXPROCS")
	flag.StringVar(&c.Mode, "mode", c.Mode, "run|growth|resume|reopen")
	flag.IntVar(&c.Iterations, "iterations", c.Iterations, "measured fresh runs")
	flag.DurationVar(&c.Timeout, "timeout", c.Timeout, "whole case deadline, including setup and warmup")
	flag.DurationVar(&c.MinTime, "min-time", 0, "minimum accumulated measured time")
	flag.StringVar(&c.Dir, "dir", c.Dir, "parent directory for isolated case artifacts")
	flag.BoolVar(&c.Warmup, "warmup", c.Warmup, "one isolated untimed warmup")
	flag.Uint64Var(&c.MaxWriteBytes, "max-write-bytes", c.MaxWriteBytes, "growth-mode Durable logical JSON budget")
	flag.Parse()
	r := runCase(c)
	if err := json.NewEncoder(os.Stdout).Encode(r); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func (c config) validate() error {
	if c.Engine != "durable" && c.Engine != "go-workflows" {
		return errors.New("unknown engine")
	}
	if c.Backend != "memory" && c.Backend != "file" {
		return errors.New("unknown backend")
	}
	if c.Shape != "chain" {
		return errors.New("only chain is implemented")
	}
	if c.Nodes < 1 || c.Rounds < 1 || c.Parallelism < 1 || c.Procs < 1 || c.Iterations < 1 || c.Timeout <= 0 || c.MinTime < 0 {
		return errors.New("sizes, concurrency, iterations and timeout must be positive")
	}
	if c.Mode != "run" && c.Mode != "growth" && c.Mode != "resume" && c.Mode != "reopen" {
		return errors.New("unknown mode")
	}
	if c.Mode == "resume" || c.Mode == "reopen" {
		if c.Backend != "file" || c.Iterations != 1 || c.MinTime != 0 || (c.Mode == "resume" && c.Nodes < 2) {
			return errors.New("resume/reopen require file, iterations=1, min-time=0; resume requires nodes>=2")
		}
	}
	return nil
}

func newResult(c config) result {
	return result{config: c, RequestedIterations: c.Iterations, Edges: max(0, c.Nodes-1), CriticalPath: c.Nodes, Status: "ok", Notes: []string{
		"Chain input is 1 at root and 1+previous output thereafter; checksum is terminal uint64 output.",
		"Graph/registry/backend/worker preparation is excluded and reported as setup_ns; submission through confirmed completion and result read is timed.",
		"bytes_per_op and allocs_per_op are process-wide Go cumulative allocation deltas, not peak/live memory; workers and GC may contribute.",
		"Each measured iteration uses a fresh backend; warmup uses a separate backend; samples run serially.",
		"run uses native stores without audit or Save instrumentation; growth includes instrumentation and is not a run latency sample.",
		"Use an external process deadline in addition to -timeout; synchronous encoding/storage may overrun context cancellation.",
		"Operation time uses Windows QueryPerformanceCounter or Go monotonic time on Unix; setup/warmup use Go time.Now.",
		"cpu_ns measures process user+kernel CPU during timed operations; Windows CPU accounting is coarse and can report zero for short cases.",
		"max-write-bytes is enforced only by Durable growth instrumentation; use growth results and an external budget for ordinary run and SQLite.",
	}}
}

func fail(r *result, err error) {
	if err == nil {
		return
	}
	r.Status = "error"
	if errors.Is(err, context.DeadlineExceeded) {
		r.Status = "timeout"
	}
	if errors.Is(err, errBudget) {
		r.Status = "resource_limit"
	}
	r.Error = err.Error()
}

func runCase(c config) (r result) {
	r = newResult(c)
	defer func() { r.Iterations = r.CompletedIterations }()
	if err := c.validate(); err != nil {
		fail(&r, err)
		return r
	}
	runtime.GOMAXPROCS(c.Procs)
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()
	if err := os.MkdirAll(c.Dir, 0700); err != nil {
		fail(&r, err)
		return r
	}
	dir, err := os.MkdirTemp(c.Dir, "workflow-")
	if err != nil {
		fail(&r, err)
		return r
	}
	r.Dir = dir
	if c.Mode == "resume" || c.Mode == "reopen" {
		return recoverCase(ctx, c, dir)
	}
	if c.Warmup {
		warmStart := time.Now()
		s, err := prepare(ctx, c, filepath.Join(dir, "warmup"), nil, 0)
		if err != nil {
			fail(&r, err)
			return r
		}
		_, err = s.run(ctx, false)
		r.WarmupNS = time.Since(warmStart).Nanoseconds()
		if err != nil {
			r.ConfirmedSteps, r.Storage = s.inspect(context.Background())
			r.Notes = append(r.Notes, "Stopped during untimed warmup; no measured iteration completed.")
		}
		closeErr := s.close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			fail(&r, err)
			return r
		}
	}
	for i := 0; i < c.Iterations || time.Duration(r.ElapsedNS) < c.MinTime; i++ {
		setup := time.Now()
		s, err := prepare(ctx, c, filepath.Join(dir, fmt.Sprintf("sample-%06d", i)), nil, 0)
		r.SetupNS += time.Since(setup).Nanoseconds()
		if err != nil {
			fail(&r, err)
			break
		}
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		cpuStart, cpuErr := processCPU()
		start := clockNS()
		value, err := s.run(ctx, false)
		r.ElapsedNS += clockNS() - start
		cpuEnd, cpuEndErr := processCPU()
		if cpuErr == nil && cpuEndErr == nil {
			r.CPUNS += cpuEnd - cpuStart
		} else {
			r.Notes = append(r.Notes, fmt.Sprintf("CPU sample unavailable: %v %v", cpuErr, cpuEndErr))
		}
		runtime.ReadMemStats(&after)
		r.AllocatedBytes += after.TotalAlloc - before.TotalAlloc
		r.Allocations += after.Mallocs - before.Mallocs
		r.ConfirmedSteps, r.Storage = s.inspect(context.Background())
		closeErr := s.close()
		if err == nil {
			err = closeErr
		}
		if err == nil && value != expected(c) {
			err = fmt.Errorf("checksum: got %d want %d", value, expected(c))
		}
		if err != nil {
			fail(&r, err)
			break
		}
		r.Checksum = &value
		r.CompletedIterations++
	}
	if r.CompletedIterations > 0 && r.Status == "ok" {
		n := float64(r.CompletedIterations)
		r.NSPerOp = number(float64(r.ElapsedNS) / n)
		r.BytesPerOp = number(float64(r.AllocatedBytes) / n)
		r.AllocsPerOp = number(float64(r.Allocations) / n)
		r.CPUPerOp = number(float64(r.CPUNS) / n)
	}
	return r
}

func number(v float64) *float64 { return &v }
func compute(input uint64, id, rounds int) uint64 {
	x := input ^ (uint64(id+1) * 0x9e3779b97f4a7c15)
	for r := 0; r < rounds; r++ {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		x *= 0x2545f4914f6cdd1d
	}
	return x
}
func expected(c config) uint64 {
	var x uint64
	for i := 0; i < c.Nodes; i++ {
		x = compute(1+x, i, c.Rounds)
	}
	return x
}

type runner interface {
	run(context.Context, bool) (uint64, error)
	inspect(context.Context) (int, map[string]any)
	close() error
}

func prepare(ctx context.Context, c config, dir string, a *audit, crash int) (runner, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	if c.Engine == "durable" {
		return prepareDurable(ctx, c, dir, a, crash)
	}
	return prepareWorkflows(ctx, c, dir, a, crash)
}

// audit 仅在正确性或恢复检查时启用，普通采样绝不启用。
type audit struct {
	mu                sync.Mutex
	calls             []int
	active, maxActive int
	path              string
}

func (a *audit) enter(id int) (func(), error) {
	if a == nil {
		return func() {}, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.path != "" {
		f, err := os.OpenFile(a.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return nil, err
		}
		_, err = fmt.Fprintln(f, id)
		if err == nil {
			err = f.Sync()
		}
		closeErr := f.Close()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			return nil, err
		}
	}
	a.calls[id]++
	a.active++
	if a.active > a.maxActive {
		a.maxActive = a.active
	}
	return func() { a.mu.Lock(); a.active--; a.mu.Unlock() }, nil
}
