package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const crashExit = 73

type childRequest struct {
	Config         config
	Dir, Phase     string
	LaunchedUnixNS int64
}

func recoverCase(ctx context.Context, c config, dir string) result {
	r := newResult(c)
	r.Dir = dir
	prefix := c.Nodes / 2
	phase := "crash"
	if c.Mode == "reopen" {
		phase = "seed"
		prefix = c.Nodes
	}
	seed, seedWall, code, err := launchChild(ctx, childRequest{Config: c, Dir: dir, Phase: phase})
	if phase == "crash" && code == crashExit {
		err = nil
	}
	if err != nil {
		if seed.Status != "ok" {
			return seed
		}
		fail(&seed, err)
		return seed
	}
	if phase == "crash" && code != crashExit {
		fail(&r, fmt.Errorf("crash child exited %d, want %d", code, crashExit))
		return r
	}
	if err = checkAudit(filepath.Join(dir, "audit.txt"), c.Nodes, prefix); err != nil {
		fail(&r, err)
		return r
	}
	if phase == "crash" {
		var marker struct {
			Confirmed int `json:"confirmed_steps"`
		}
		data, readErr := os.ReadFile(filepath.Join(dir, "confirmed.json"))
		if readErr != nil {
			fail(&r, readErr)
			return r
		}
		if err = json.Unmarshal(data, &marker); err != nil || marker.Confirmed != prefix {
			fail(&r, fmt.Errorf("confirmation marker: %v, prefix %d", err, marker.Confirmed))
			return r
		}
	}
	// 在未启动工作进程的父进程中独立加载持久化记录与历史。
	s, err := prepare(ctx, c, dir, nil, 0)
	if err != nil {
		fail(&r, err)
		return r
	}
	confirmed, storage := s.inspect(ctx)
	closeErr := s.close()
	if closeErr != nil {
		fail(&r, closeErr)
		return r
	}
	if confirmed != prefix {
		fail(&r, fmt.Errorf("persistent prefix %d, want %d: %v", confirmed, prefix, storage))
		return r
	}
	r, wall, _, err := launchChild(ctx, childRequest{Config: c, Dir: dir, Phase: c.Mode})
	if err != nil {
		if r.Status == "ok" {
			fail(&r, err)
		}
		return r
	}
	r.Mode = c.Mode
	r.Dir = dir
	r.Recovery = map[string]any{
		"confirmed_prefix": prefix, "seed_process_wall_ns": seedWall.Nanoseconds(),
		"restart_process_wall_ns": wall.Nanoseconds(), "restart_setup_ns": r.SetupNS,
		"remaining_operation_ns":         r.ElapsedNS,
		"launch_to_child_main_ns":        r.LaunchToChildNS,
		"inspection_cleanup_ipc_exit_ns": max(int64(0), wall.Nanoseconds()-r.LaunchToChildNS-r.SetupNS-r.ElapsedNS),
		"prefix_storage":                 storage, "audit": "fsynced business-entry IDs; each ID must appear exactly once across processes; excluded from normal run samples",
	}
	if r.Status == "ok" {
		if err = checkAudit(filepath.Join(dir, "audit.txt"), c.Nodes, c.Nodes); err != nil {
			fail(&r, err)
		}
	}
	if c.Mode == "reopen" {
		r.Notes = append(r.Notes, "Completed reopen: Durable rebuilds/validates/restores through Runtime.Resume; go-workflows reads the finished instance result with no worker started. These are different native operations.")
	} else {
		r.Notes = append(r.Notes, "Real os.Exit(73), then a new process; recovery timer includes worker start (go-workflows), durable restore/replay, remaining business and result read; excludes process startup and preparation.")
	}
	return r
}

func launchChild(ctx context.Context, req childRequest) (result, time.Duration, int, error) {
	r := newResult(req.Config)
	exe, err := os.Executable()
	if err != nil {
		return r, 0, -1, err
	}
	start := time.Now()
	req.LaunchedUnixNS = start.UnixNano()
	raw, err := json.Marshal(req)
	if err != nil {
		return r, 0, -1, err
	}
	cmd := exec.CommandContext(ctx, exe)
	cmd.Env = append(os.Environ(), "WORKFLOW_BENCH_CHILD="+string(raw))
	var out, diagnostics bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &diagnostics
	err = cmd.Run()
	wall := time.Since(start)
	code := -1
	if cmd.ProcessState != nil {
		code = cmd.ProcessState.ExitCode()
	}
	if ctx.Err() != nil {
		probe := req.Config
		probe.Mode = "resume"
		inspectionCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if s, e := prepare(inspectionCtx, probe, req.Dir, nil, 0); e == nil {
			r.ConfirmedSteps, r.Storage = s.inspect(inspectionCtx)
			_ = s.close()
		}
		r.Dir = req.Dir
		return r, wall, code, ctx.Err()
	}
	if err != nil {
		return r, wall, code, fmt.Errorf("%s child: %w; %s", req.Phase, err, diagnostics.String())
	}
	if err = json.Unmarshal(out.Bytes(), &r); err != nil {
		return r, wall, code, fmt.Errorf("%s child JSON: %w; stdout=%q stderr=%q", req.Phase, err, out.String(), diagnostics.String())
	}
	if r.Status != "ok" {
		return r, wall, code, fmt.Errorf("%s child: %s: %s", req.Phase, r.Status, r.Error)
	}
	return r, wall, code, nil
}

func childMain(raw string) {
	enteredUnixNS := time.Now().UnixNano()
	var req childRequest
	if err := json.Unmarshal([]byte(raw), &req); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	c := req.Config
	runtime.GOMAXPROCS(c.Procs)
	runtimeConfig := c
	if req.Phase == "seed" || req.Phase == "crash" {
		runtimeConfig.Mode = "run"
	}
	r := newResult(c)
	r.LaunchToChildNS = max(int64(0), enteredUnixNS-req.LaunchedUnixNS)
	r.Dir = req.Dir
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()
	a := &audit{calls: make([]int, c.Nodes), path: filepath.Join(req.Dir, "audit.txt")}
	crash := 0
	if req.Phase == "crash" {
		crash = c.Nodes / 2
	}
	setup := time.Now()
	s, err := prepare(ctx, runtimeConfig, req.Dir, a, crash)
	r.SetupNS = time.Since(setup).Nanoseconds()
	if err == nil {
		start := clockNS()
		value, runErr := s.run(ctx, req.Phase == "resume" || req.Phase == "reopen")
		r.ElapsedNS = clockNS() - start
		r.ConfirmedSteps, r.Storage = s.inspect(context.Background())
		err = runErr
		if closeErr := s.close(); err == nil {
			err = closeErr
		}
		if err == nil && value != expected(c) {
			err = fmt.Errorf("checksum %d, want %d", value, expected(c))
		}
		if err == nil {
			r.Checksum = &value
			r.CompletedIterations = 1
			r.NSPerOp = number(float64(r.ElapsedNS))
		}
	}
	fail(&r, err)
	r.Iterations = r.CompletedIterations
	r.Notes = append(r.Notes, "Recovery/reopen allocations are not sampled; audit fsync is enabled, so operation latency is not comparable to ordinary run.")
	if err = json.NewEncoder(os.Stdout).Encode(r); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func checkAudit(path string, nodes, prefix int) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	counts := make([]int, nodes)
	for _, word := range strings.Fields(string(data)) {
		id, err := strconv.Atoi(word)
		if err != nil || id < 0 || id >= nodes {
			return fmt.Errorf("invalid audit ID %q", word)
		}
		counts[id]++
	}
	for i, count := range counts {
		want := 0
		if i < prefix {
			want = 1
		}
		if count != want {
			return fmt.Errorf("business step %d called %d times, want %d", i, count, want)
		}
	}
	return nil
}
