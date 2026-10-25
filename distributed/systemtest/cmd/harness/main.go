// Command harness is a stdin-controlled worker for manual multi-host experiments.
// Example: set DSN/ALLOW, then pipe JSON lines "start", "release", "drain".
// Provision namespace permissions separately; this command does no DDL and does
// not connect to any host unless the operator explicitly supplies its DSN.
package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/Luo-root/pulse-durable/distributed/systemtest"
	"os"
	"time"
)

func main() {
	var c systemtest.Config
	flag.StringVar(&c.Namespace, "namespace", "", "pre-authorized isolated namespace")
	flag.StringVar(&c.ID, "worker-id", "", "unique worker identity")
	flag.StringVar(&c.Version, "version", "v1", "exact business version")
	flag.IntVar(&c.Capacity, "capacity", 2, "bounded in-flight reservations")
	flag.DurationVar(&c.Duration, "duration", 2*time.Minute, "hard run ceiling; extend explicitly for soak")
	flag.BoolVar(&c.HoldFinish, "hold-finish", false, "fault injection: hold before Finish")
	flag.Parse()
	if err := systemtest.Run(context.Background(), c, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "harness failed (credentials and backend details omitted)")
		os.Exit(1)
	}
}
