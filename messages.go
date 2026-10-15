package durable

import (
	"context"
	"fmt"
	"github.com/Luo-root/pulse"
)

// 未启用消息处理事务前，只接受显式手动 Ack/Stop；处理尝试和成果明确拒绝。
func (s *runState) commitMessage(ctx context.Context, req pulse.CheckpointMessageCommit) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.streamReady(ctx); err != nil {
		return err
	}
	if req.Attempt != "" || len(req.State)+len(req.Values)+len(req.Outputs) != 0 {
		return fmt.Errorf("durable: message checkpoints are not enabled")
	}
	if len(req.Inputs)+len(req.Stops) == 0 {
		return fmt.Errorf("durable: empty acknowledgement")
	}
	proposed := cloneRecord(s.record)
	for _, input := range req.Inputs {
		if input.Stream.Scope != req.Scope {
			return fmt.Errorf("durable: manual Ack scope mismatch")
		}
		if err := s.acknowledge(&proposed, input); err != nil {
			return err
		}
	}
	for _, stop := range req.Stops {
		if stop.Scope != req.Scope {
			return fmt.Errorf("durable: manual Stop scope mismatch")
		}
		key, stream, err := s.currentStream(proposed, stop, false)
		if err != nil {
			return err
		}
		stream.Stream.Stopped, stream.Pending = true, nil
		proposed.Streams[key] = stream
	}
	return s.save(ctx, proposed)
}
