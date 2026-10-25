package main

import (
	"encoding/json"
	"flag"
	cm "github.com/Luo-root/pulse-durable/distributed/crossmachine"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestExplicitOverrides(t *testing.T) {
	for _, tc := range []struct {
		name    string
		args    []string
		sha     string
		timeout time.Duration
	}{
		{"json", nil, "json-sha", 7 * time.Minute},
		{"sha", []string{"-build-sha", "flag-sha"}, "flag-sha", 7 * time.Minute},
		{"timeout", []string{"-timeout", "9m"}, "json-sha", 9 * time.Minute},
		{"explicit-zero", []string{"-timeout", "0s"}, "json-sha", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := flag.NewFlagSet("test", flag.ContinueOnError)
			sha := fs.String("build-sha", "", "")
			timeout := fs.Duration("timeout", 3*time.Minute, "")
			if fs.Parse(tc.args) != nil {
				t.Fatal("parse")
			}
			c := cm.Config{BuildSHA: "json-sha", Timeout: 7 * time.Minute}
			applyOverrides(fs, &c, *sha, *timeout)
			if c.BuildSHA != tc.sha || c.Timeout != tc.timeout {
				t.Fatalf("%+v", c)
			}
		})
	}
}
func TestCLIJSONSHAWithoutOverride(t *testing.T) {
	// No database access: kill without a callback is rejected before OpenPostgres.
	root := t.TempDir()
	config := filepath.Join(root, "config.json")
	output := filepath.Join(root, "report.json")
	c := cm.Config{BuildSHA: "json-sha", Namespace: "unit", Version: "v1", Capacity: 2, Timeout: 7 * time.Minute, Scenario: "checkpoint-kill", KillHost: "vps", Hosts: []cm.Host{{Label: "windows", OS: "windows", Workers: 1, Command: cm.Command{Path: "unused"}}, {Label: "vps", OS: "linux", Workers: 1, Command: cm.Command{Path: "unused"}}}}
	b, _ := json.Marshal(c)
	if os.WriteFile(config, b, 0600) != nil {
		t.Fatal("write")
	}
	if code := runArgs([]string{"-config", config, "-output", output}); code != 1 {
		t.Fatalf("exit %d", code)
	}
	wire, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var report cm.Report
	if json.Unmarshal(wire, &report) != nil || report.BuildSHA != "json-sha" || report.Result != "NOT_RUN" {
		t.Fatalf("%s", wire)
	}
}
