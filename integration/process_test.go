package integration_test

import (
	"os/exec"
	"time"
)

// Process helpers already have individual CommandContext deadlines. Also bound
// pipe draining after exit/cancellation so inherited output handles cannot keep
// CombinedOutput waiting indefinitely. This does not extend any fault deadline.
func boundedProcessOutput(cmd *exec.Cmd) ([]byte, error) {
	cmd.WaitDelay = time.Second
	return cmd.CombinedOutput()
}
