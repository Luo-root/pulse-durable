package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Luo-root/pulse-durable/distributed/soaktest"
)

func main() {
	if run() != nil {
		os.Exit(1)
	}
}
func run() error {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: soak run|prepare [flags]")
		return fmt.Errorf("command required")
	}
	mode := os.Args[1]
	f := flag.NewFlagSet(mode, flag.ContinueOnError)
	c := soaktest.Default()
	if mode == "worker" {
		config := f.String("config", "", "internal non-secret config")
		id := f.String("id", "", "worker ID")
		if e := f.Parse(os.Args[2:]); e != nil {
			return e
		}
		if json.Unmarshal([]byte(*config), &c) != nil {
			return fmt.Errorf("invalid worker configuration")
		}
		if e := soaktest.Child(ctx, c, *id, os.Stdin, os.Stdout); e != nil {
			fmt.Fprintln(os.Stderr, "worker failed")
			return e
		}
		return nil
	}
	if mode != "run" && mode != "prepare" {
		fmt.Fprintln(os.Stderr, "usage: soak run|prepare [flags]")
		return fmt.Errorf("unknown command")
	}
	f.StringVar(&c.Namespace, "namespace", "", "required fresh isolated namespace")
	f.IntVar(&c.Workers, "workers", c.Workers, "real OS worker process count")
	f.IntVar(&c.Capacity, "capacity", c.Capacity, "concurrent tasks per worker")
	f.DurationVar(&c.Warm, "warm", c.Warm, "warm phase per repeat")
	f.DurationVar(&c.Steady, "steady", c.Steady, "steady submission phase per repeat")
	f.IntVar(&c.Repeat, "repeat", c.Repeat, "warm/steady repeats")
	f.IntVar(&c.MaxOutstanding, "max-outstanding", c.MaxOutstanding, "bounded outstanding tasks")
	f.IntVar(&c.CPURounds, "cpu-rounds", c.CPURounds, "fixed SHA256 rounds per task")
	f.DurationVar(&c.Wait, "wait", c.Wait, "bounded cooperative wait per task; database checkpoints provide IO")
	f.StringVar(&c.AdminEnv, "admin-env", c.AdminEnv, "explicit admin DSN environment variable name")
	f.StringVar(&c.WorkerEnv, "worker-env", c.WorkerEnv, "explicit worker DSN environment variable name")
	output := f.String("output", "soak-smoke", "new output filename prefix; existing files preserved")
	if e := f.Parse(os.Args[2:]); e != nil {
		return e
	}
	if mode == "prepare" {
		if e := soaktest.Prepare(ctx, c); e != nil {
			fmt.Fprintln(os.Stderr, e)
			return e
		}
		fmt.Println("prepared fresh local worker role/namespace")
		return nil
	}
	r, e := soaktest.Run(ctx, c, *output)
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
	}
	if r.Config.Namespace != "" {
		_ = json.NewEncoder(os.Stdout).Encode(r)
	}
	return e
}
