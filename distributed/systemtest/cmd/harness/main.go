// harness 命令是由标准输入控制的工作进程，用于手动多主机实验。
// 示例：设置 DSN/ALLOW，再通过管道输入 JSON 行："start"、"release"、"drain"。
// 单独配置命名空间权限；此命令不执行 DDL，且除非
// 操作员显式提供 DSN，否则不连接任何主机。
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
	flag.DurationVar(&c.TTL, "ttl", 0, "lease TTL (0: 2s local default)")
	flag.DurationVar(&c.RenewInterval, "renew-interval", 0, "renew interval (0: 300ms)")
	flag.DurationVar(&c.RenewTimeout, "renew-timeout", 0, "renew operation timeout (0: 400ms)")
	flag.DurationVar(&c.OperationTimeout, "operation-timeout", 0, "coordinator operation timeout (0: 1s)")
	flag.DurationVar(&c.PollMin, "poll-min", 0, "minimum poll delay (0: 20ms)")
	flag.DurationVar(&c.PollMax, "poll-max", 0, "maximum poll delay (0: 50ms)")
	flag.Parse()
	if err := systemtest.Run(context.Background(), c, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "harness failed (credentials and backend details omitted)")
		os.Exit(1)
	}
}
