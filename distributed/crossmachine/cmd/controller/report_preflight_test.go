package main

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	cm "github.com/Luo-root/pulse-durable/distributed/crossmachine"
)

func TestReportPreflightPreventsDatabaseConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	connected := make(chan struct{}, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			connected <- struct{}{}
			_ = conn.Close()
		}
	}()
	t.Setenv("PULSE_DISTRIBUTED_TEST_ALLOW", "yes")
	t.Setenv("REPORT_PREFLIGHT_ADMIN_DSN", "postgres://test@"+listener.Addr().String()+"/preflight?sslmode=disable")
	t.Setenv("REPORT_PREFLIGHT_WORKER_DSN", "postgres://worker:test-only@"+listener.Addr().String()+"/preflight?sslmode=disable")
	root := t.TempDir()
	config := filepath.Join(root, "config.json")
	c := cm.Config{BuildSHA: "preflight", Namespace: "preflight", Version: "v1", Capacity: 2, Timeout: 100 * time.Millisecond, Scenario: "normal", Prepare: true, AdminDSNEnv: "REPORT_PREFLIGHT_ADMIN_DSN", WorkerDSNEnv: "REPORT_PREFLIGHT_WORKER_DSN", Hosts: []cm.Host{
		{Label: "windows", OS: "windows", Workers: 1, Command: cm.Command{Path: "unused"}},
		{Label: "linux", OS: "linux", Workers: 1, Command: cm.Command{Path: "unused"}},
	}}
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(config, b, 0600); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"existing", "missing-parent"} {
		t.Run(mode, func(t *testing.T) {
			output := filepath.Join(root, mode+".json")
			const evidence = "previous report must remain unchanged"
			if mode == "existing" {
				if err := os.WriteFile(output, []byte(evidence), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				output = filepath.Join(root, "missing", "report.json")
			}
			if code := runArgs([]string{"-config", config, "-output", output}); code != 2 {
				t.Fatalf("exit=%d", code)
			}
			select {
			case <-connected:
				t.Fatal("database connection occurred before report reservation")
			default:
			}
			if mode == "existing" {
				got, err := os.ReadFile(output)
				if err != nil || string(got) != evidence {
					t.Fatal("previous evidence changed", err)
				}
			}
		})
	}
}
