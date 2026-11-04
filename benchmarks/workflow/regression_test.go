package main

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestSQLiteFilePaths(t *testing.T) {
	for _, name := range []string{"hash#tag", "percent%23tag", "question?tag"} {
		t.Run(name, func(t *testing.T) {
			if runtime.GOOS == "windows" && name == "question?tag" {
				t.Skip("Windows does not allow ? in directory names; exercised on Unix")
			}
			c := defaults()
			c.Engine, c.Backend, c.Nodes, c.Iterations = "go-workflows", "file", 2, 2
			c.Timeout = 10 * time.Second
			c.Dir = filepath.Join(localDir(t), name)
			r := runCase(c) // 包含独立预热以及两个全新后端的测量。
			if r.Status != "ok" || r.Iterations != 2 || r.Checksum == nil || *r.Checksum != expected(c) {
				t.Fatalf("%+v", r)
			}
			var files []os.FileInfo
			for _, sample := range []string{"warmup", "sample-000000", "sample-000001"} {
				t.Run(sample, func(t *testing.T) {
					dir := filepath.Join(r.Dir, sample)
					path := filepath.Join(dir, "workflow.sqlite")
					info, err := os.Stat(path)
					if err != nil {
						t.Fatalf("database missing from requested path: %v", err)
					}
					for _, other := range files {
						if os.SameFile(info, other) {
							t.Fatal("fresh backends share a physical database")
						}
					}
					files = append(files, info)
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					probe := c
					probe.Mode, probe.Iterations = "reopen", 1 // 不启动工作进程，直接检查。
					s, err := prepareWorkflows(ctx, probe, dir, nil, 0)
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						if err := s.close(); err != nil {
							t.Error(err)
						}
					}()
					var sequence int
					var schema, actual string
					db := s.(*workflowsRunner).db
					if err := db.QueryRowContext(ctx, "PRAGMA database_list").Scan(&sequence, &schema, &actual); err != nil {
						t.Fatal(err)
					}
					actualInfo, err := os.Stat(actual)
					if err != nil || schema != "main" || !os.SameFile(info, actualInfo) || !filepath.IsAbs(actual) {
						t.Fatalf("SQLite database_list=%q, want requested file %q: %v", actual, path, err)
					}
					var instances int
					if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM instances WHERE id = ?", runID).Scan(&instances); err != nil || instances != 1 {
						t.Fatalf("fresh database contains %d instances for %q, want 1: %v", instances, runID, err)
					}
					t.Logf("database_list main=%q; instances(%s)=1; fresh physical file", actual, runID)
				})
			}
		})
	}
}

func TestResumeIgnoresGrowthBudget(t *testing.T) {
	for _, engine := range []string{"durable", "go-workflows"} {
		t.Run(engine, func(t *testing.T) {
			c := defaults()
			c.Engine, c.Backend, c.Mode, c.Warmup = engine, "file", "resume", false
			c.MaxWriteBytes, c.Dir = 1, localDir(t)
			r := runCase(c)
			if r.Status != "ok" || r.Iterations != 1 || r.Checksum == nil || *r.Checksum != expected(c) || r.ConfirmedSteps != c.Nodes {
				t.Fatalf("resume incorrectly affected by growth budget: %+v", r)
			}
			if r.Recovery["confirmed_prefix"] != c.Nodes/2 {
				t.Fatalf("missing real confirmed-prefix recovery: %+v", r.Recovery)
			}
			if err := checkAudit(filepath.Join(r.Dir, "audit.txt"), c.Nodes, c.Nodes); err != nil {
				t.Fatal(err)
			}
			t.Logf("max_write_bytes=1; real exit/restart; prefix=%d; confirmed=%d; checksum=%d", c.Nodes/2, r.ConfirmedSteps, *r.Checksum)
		})
	}
}
