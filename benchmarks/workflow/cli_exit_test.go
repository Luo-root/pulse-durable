package main

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"testing"
)

// 实际调用 main，避免仅检查 runCase 而漏掉进程退出码。
func TestBenchmarkCLIResultExitCode(t *testing.T) {
	if os.Getenv("WORKFLOW_BENCH_EXIT_TEST") == "yes" {
		var args []string
		if err := json.Unmarshal([]byte(os.Getenv("WORKFLOW_BENCH_EXIT_ARGS")), &args); err != nil {
			panic(err)
		}
		os.Args = append([]string{"workflow"}, args...)
		main()
		os.Exit(0)
	}
	for _, tc := range []struct {
		name, status string
		code         int
		args         []string
	}{
		{"invalid", "error", 1, []string{"-nodes", "0"}},
		{"budget", "resource_limit", 1, []string{"-mode", "growth", "-max-write-bytes", "1"}},
		{"timeout", "timeout", 1, []string{"-timeout", "1ns"}},
		{"success", "ok", 0, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"-nodes", "1", "-warmup=false", "-procs", "2", "-dir", t.TempDir()}
			args = append(args, tc.args...)
			encoded, err := json.Marshal(args)
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestBenchmarkCLIResultExitCode$")
			cmd.Env = append(os.Environ(), "WORKFLOW_BENCH_EXIT_TEST=yes", "WORKFLOW_BENCH_EXIT_ARGS="+string(encoded))
			out, err := cmd.Output()
			code := 0
			var exit *exec.ExitError
			if errors.As(err, &exit) {
				code = exit.ExitCode()
			} else if err != nil {
				t.Fatal(err)
			}
			var got result
			if json.Unmarshal(out, &got) != nil || got.Status != tc.status || code != tc.code {
				t.Fatalf("exit=%d want=%d json=%s", code, tc.code, out)
			}
		})
	}
}
