package integration_test

import (
	"os/exec"
	"time"
)

// 进程辅助函数已有各自的 CommandContext 截止时间。另行限制
// 退出或取消后的管道排空时间，避免继承的输出句柄让
// CombinedOutput 无限等待。这不会延长任何故障截止时间。
func boundedProcessOutput(cmd *exec.Cmd) ([]byte, error) {
	cmd.WaitDelay = time.Second
	return cmd.CombinedOutput()
}
