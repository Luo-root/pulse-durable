// Controller 读取不含凭据的配置。外部终止模式向标准输出发送 JSON
// 请求，并从标准输入接收匹配的确认；父进程负责在确认前，
// 通过 PID 和启动标识核验远端进程确已终止。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	cm "github.com/Luo-root/pulse-durable/distributed/crossmachine"
)

func applyOverrides(fs *flag.FlagSet, c *cm.Config, sha string, timeout time.Duration) {
	explicitTimeout := false
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "build-sha":
			c.BuildSHA = sha
		case "timeout":
			explicitTimeout = true
			c.Timeout = timeout
		}
	})
	if c.Timeout == 0 && !explicitTimeout {
		c.Timeout = 3 * time.Minute
	}
}

func main()    { os.Exit(run()) }
func run() int { return runArgs(os.Args[1:]) }
func runArgs(args []string) int {
	fs := flag.NewFlagSet("controller", flag.ContinueOnError)
	config := fs.String("config", "", "credential-free JSON configuration path")
	output := fs.String("output", "crossmachine-report.json", "report path (workspace-relative by default)")
	sha := fs.String("build-sha", "", "tested harness build SHA supplied by parent")
	timeout := fs.Duration("timeout", 3*time.Minute, "whole experiment deadline")
	external := fs.Bool("external-kill", false, "stdout kill request / stdin death-confirmation handshake")
	if fs.Parse(args) != nil {
		return 2
	}
	f, err := os.Open(*config)
	if err != nil {
		fmt.Fprintln(os.Stderr, "configuration unavailable")
		return 2
	}
	defer f.Close()
	var c cm.Config
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if dec.Decode(&c) != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration")
		return 2
	}
	applyOverrides(fs, &c, *sha, *timeout)
	if c.Validate() != nil {
		fmt.Fprintln(os.Stderr, "invalid configuration")
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, c.Timeout)
	defer cancel()
	ctl := cm.Controller{}
	if *external {
		ctl.Kill = func(ctx context.Context, w cm.WorkerEvidence) error {
			request := struct {
				Kind string `json:"kind"`
				Host string `json:"host_label"`
				PID  int    `json:"pid"`
				Boot string `json:"boot_id"`
			}{"kill_request", w.Host, w.PID, w.Boot}
			if json.NewEncoder(os.Stdout).Encode(request) != nil {
				return fmt.Errorf("kill request failed")
			}
			type ack struct {
				Kind string `json:"kind"`
				Host string `json:"host_label"`
				PID  int    `json:"pid"`
				Boot string `json:"boot_id"`
			}
			done := make(chan bool, 1)
			go func() {
				var a ack
				e := json.NewDecoder(os.Stdin).Decode(&a)
				done <- e == nil && a.Kind == "kill_confirmed" && a.Host == w.Host && a.PID == w.PID && a.Boot == w.Boot
			}()
			select {
			case ok := <-done:
				if ok {
					return nil
				}
				return fmt.Errorf("kill confirmation mismatch")
			case <-ctx.Done():
				return fmt.Errorf("kill confirmation timed out")
			}
		}
	}
	var report cm.Report
	if c.Scenario == "checkpoint-kill" && ctl.Kill == nil {
		report = ctl.Run(ctx, c, nil)
	} else {
		db, e := cm.OpenPostgres(ctx, c)
		if e != nil {
			report = cm.Report{Result: "NOT_RUN", Reason: "database_preparation_or_open_failed", BuildSHA: c.BuildSHA, Namespace: c.Namespace, Scenario: c.Scenario}
		} else {
			report = ctl.Run(ctx, c, db)
			db.Close()
		}
	}
	out, e := os.OpenFile(*output, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		fmt.Fprintln(os.Stderr, "report output unavailable (existing evidence is not overwritten)")
		return 2
	}
	e = json.NewEncoder(out).Encode(report)
	closeErr := out.Close()
	if e != nil || closeErr != nil {
		fmt.Fprintln(os.Stderr, "report write failed")
		return 2
	}
	fmt.Fprintln(os.Stdout, report.Result)
	if report.Result != "PASS" {
		return 1
	}
	return 0
}
