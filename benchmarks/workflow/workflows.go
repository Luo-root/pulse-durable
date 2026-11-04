package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cschleiden/go-workflows/backend"
	"github.com/cschleiden/go-workflows/backend/history"
	"github.com/cschleiden/go-workflows/backend/sqlite"
	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/core"
	"github.com/cschleiden/go-workflows/registry"
	"github.com/cschleiden/go-workflows/worker"
	"github.com/cschleiden/go-workflows/workflow"
)

type workflowsRunner struct {
	c               config
	dir, path       string
	db              *sql.DB
	backend         backend.Backend
	worker          *worker.Worker
	workerCtx       context.Context
	cancel          context.CancelFunc
	started         bool
	instance        *workflow.Instance
	persistIdentity bool
}

func chainWorkflow(ctx workflow.Context, nodes, rounds int) (uint64, error) {
	var x uint64
	for i := 0; i < nodes; i++ {
		v, err := workflow.ExecuteActivity[uint64](ctx, workflow.ActivityOptions{RetryOptions: workflow.RetryOptions{MaxAttempts: 1}}, "compute", 1+x, i, rounds).Get(ctx)
		if err != nil {
			return 0, err
		}
		x = v
	}
	return x, nil
}

func prepareWorkflows(ctx context.Context, c config, dir string, a *audit, crash int) (_ runner, err error) {
	path, err := filepath.Abs(filepath.Join(dir, "workflow.sqlite"))
	if err != nil {
		return nil, err
	}
	s := &workflowsRunner{c: c, dir: dir, path: path, persistIdentity: a != nil}
	uriPath := filepath.ToSlash(path)
	if !strings.HasPrefix(uriPath, "/") {
		uriPath = "/" + uriPath // Windows 盘符属于 URI 路径段，而非主机部分。
	}
	dsn := (&url.URL{Scheme: "file", Path: uriPath, RawQuery: "_txlock=immediate"}).String()
	if c.Backend == "memory" {
		dsn = fmt.Sprintf("file:bench-%d?mode=memory&cache=shared&_txlock=immediate", time.Now().UnixNano())
	}
	s.db, err = sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = s.close()
		}
	}()
	s.db.SetMaxOpenConns(1)
	s.db.SetMaxIdleConns(1) // 保留具名、共享缓存的内存数据库。
	// 与原生构造函数一致。文件测量不得放宽 synchronous 设置。
	for _, q := range []string{"PRAGMA journal_mode=WAL", "PRAGMA busy_timeout=5000"} {
		if _, err = s.db.ExecContext(ctx, q); err != nil {
			return nil, err
		}
	}
	s.backend = sqlite.NewSqliteBackendWithDB(s.db, sqlite.WithApplyMigrations(true), sqlite.WithBackendOptions(
		backend.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
		backend.WithWorkerName("workflow-bench-single-host"),
		backend.WithMaxHistorySize(int64(max(10000, c.Nodes*8+100))),
	))
	if crash > 0 {
		s.backend = &crashBackend{Backend: s.backend, at: crash, marker: filepath.Join(dir, "confirmed.json")}
	}
	opts := worker.DefaultOptions
	opts.WorkflowPollingInterval = time.Millisecond
	opts.ActivityPollingInterval = time.Millisecond
	opts.WorkflowPollers = 1
	opts.ActivityPollers = 1
	opts.MaxParallelWorkflowTasks = c.Parallelism
	opts.MaxParallelActivityTasks = c.Parallelism
	opts.SingleWorkerMode = true // 锁定版本的 Worker.New 不使用此字段。
	s.worker = worker.New(s.backend, &opts)
	if err = s.worker.RegisterWorkflow(chainWorkflow, registry.WithName("chain")); err != nil {
		return nil, err
	}
	activity := func(ctx context.Context, input uint64, id, rounds int) (uint64, error) {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		leave, err := a.enter(id)
		if err != nil {
			return 0, err
		}
		defer leave()
		return compute(input, id, rounds), nil
	}
	if err = s.worker.RegisterActivity(activity, registry.WithName("compute")); err != nil {
		return nil, err
	}
	s.workerCtx, s.cancel = context.WithCancel(ctx)
	identity := filepath.Join(dir, "instance.json")
	if data, readErr := os.ReadFile(identity); readErr == nil {
		if err = json.Unmarshal(data, &s.instance); err != nil {
			return nil, err
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return nil, readErr
	}
	// 重启后的工作进程不得在不计时的准备阶段执行剩余业务。
	if c.Mode != "resume" && c.Mode != "reopen" && a == nil {
		if err = s.start(); err != nil {
			return nil, err
		}
	}
	return s, nil
}
func (s *workflowsRunner) start() error {
	if s.started {
		return nil
	}
	if err := s.worker.Start(s.workerCtx); err != nil {
		return err
	}
	s.started = true
	return nil
}
func (s *workflowsRunner) run(ctx context.Context, resume bool) (uint64, error) {
	cl := client.New(s.backend)
	if !resume {
		instance, err := cl.CreateWorkflowInstance(ctx, client.WorkflowInstanceOptions{InstanceID: runID}, "chain", s.c.Nodes, s.c.Rounds)
		if err != nil {
			return 0, err
		}
		s.instance = instance
		if s.persistIdentity {
			if err := writeJSON(filepath.Join(s.dir, "instance.json"), instance); err != nil {
				return 0, err
			}
		}
	} else if s.instance == nil {
		return 0, errors.New("missing persisted workflow instance identity")
	}
	if s.c.Mode != "reopen" {
		if err := s.start(); err != nil {
			return 0, err
		}
	}
	// 采用固定低延迟完成轮询；原生客户端的指数等待可达 1 秒。
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := s.backend.GetWorkflowInstanceState(ctx, s.instance)
		if err != nil {
			return 0, err
		}
		if state == core.WorkflowInstanceStateFinished {
			return client.GetWorkflowResult[uint64](ctx, cl, s.instance, s.c.Timeout)
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-ticker.C:
		}
	}
}
func (s *workflowsRunner) inspect(ctx context.Context) (int, map[string]any) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	m := map[string]any{
		"persistence":      "SQLite named shared-cache memory via native WithDB constructor; not a Go map",
		"poll_interval_ns": int64(time.Millisecond), "workflow_pollers": 1, "activity_pollers": 1,
		"max_parallel_workflow_tasks": s.c.Parallelism, "max_parallel_activity_tasks": s.c.Parallelism,
		"single_worker_mode": true, "single_worker_mode_effect": "unused by Worker.New in pinned v1.4.2",
		"worker_name": "workflow-bench-single-host", "sticky_timeout_ns": int64(30 * time.Second),
		"history_limit_events": max(10000, s.c.Nodes*8+100),
		"metric_scope":         "history event count and current db/WAL file lengths, not cumulative physical/logical bytes written",
		"polling_cost":         "1ms workflow/activity/client polling trades idle CPU and SQL traffic for latency; no exporter or logging",
	}
	if s.c.Backend == "file" {
		m["persistence"] = "SQLite file, native WithDB constructor, WAL requested, synchronous not overridden"
	}
	pragmas := map[string]any{}
	for _, name := range []string{"journal_mode", "synchronous", "busy_timeout", "cache_size", "page_size", "wal_autocheckpoint", "locking_mode", "temp_store"} {
		var value any
		if err := s.db.QueryRowContext(ctx, "PRAGMA "+name).Scan(&value); err != nil {
			pragmas[name] = err.Error()
		} else {
			pragmas[name] = value
		}
	}
	m["pragmas"] = pragmas
	m["max_open_connections"] = s.db.Stats().MaxOpenConnections
	if s.c.Backend == "file" {
		for label, path := range map[string]string{"db_bytes": s.path, "wal_bytes": s.path + "-wal"} {
			if info, err := os.Stat(path); err == nil {
				m[label] = info.Size()
			} else if errors.Is(err, os.ErrNotExist) {
				m[label] = int64(0)
			} else {
				m[label] = err.Error()
			}
		}
	}
	if s.instance == nil {
		return 0, m
	}
	h, err := s.backend.GetWorkflowInstanceHistory(ctx, s.instance, nil)
	if err != nil {
		m["inspection_error"] = err.Error()
		return 0, m
	}
	m["history_events"] = len(h)
	return completedActivities(h), m
}
func (s *workflowsRunner) close() error {
	if s.cancel != nil {
		s.cancel()
	}
	var err error
	if s.started {
		err = s.worker.WaitForCompletion()
	}
	if s.backend != nil {
		err = errors.Join(err, s.backend.Close())
	}
	if s.db != nil {
		err = errors.Join(err, s.db.Close())
		// 由上下文触发的事务回滚在 Close 后仍可能占用连接。
		deadline := time.Now().Add(5 * time.Second)
		for s.db.Stats().OpenConnections > 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if s.db.Stats().OpenConnections > 0 {
			err = errors.Join(err, fmt.Errorf("SQLite connections still open after close: %+v", s.db.Stats()))
		}
	}
	return err
}
func completedActivities(events []*history.Event) int {
	n := 0
	for _, e := range events {
		if e.Type == history.EventType_ActivityCompleted {
			n++
		}
	}
	return n
}

// 在确认事务直至退出期间暂停领取活动，避免
// 在崩溃时已领取下一个活动并遗留其两分钟租约。
type crashBackend struct {
	backend.Backend
	mu            sync.Mutex
	at, completed int
	marker        string
}

func (b *crashBackend) GetActivityTask(ctx context.Context, queues []workflow.Queue) (*backend.ActivityTask, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Backend.GetActivityTask(ctx, queues)
}
func (b *crashBackend) CompleteWorkflowTask(ctx context.Context, task *backend.WorkflowTask, state core.WorkflowInstanceState, executed, activities, timers []*history.Event, workflows []*history.WorkflowEvent) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.Backend.CompleteWorkflowTask(ctx, task, state, executed, activities, timers, workflows); err != nil {
		return err
	}
	b.completed += completedActivities(executed)
	if b.completed == b.at {
		h, err := b.Backend.GetWorkflowInstanceHistory(ctx, task.WorkflowInstance, nil)
		if err != nil {
			return err
		}
		if completedActivities(h) != b.at {
			return errors.New("committed activity prefix mismatch")
		}
		if err := writeJSON(b.marker, map[string]any{"confirmed_steps": b.at, "boundary": "successful CompleteWorkflowTask; ActivityCompleted present in history; next pickup fenced"}); err != nil {
			return err
		}
		os.Exit(crashExit)
	}
	return nil
}
