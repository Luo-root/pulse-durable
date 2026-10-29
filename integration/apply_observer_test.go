package integration_test

import (
	"context"
	durable "github.com/Luo-root/pulse-durable"
)

// observeApply 仅在测试端物化提案，以设置精确故障关卡。
// 后端仍接收原始不可变批次，绝非完整记录。
func observeApply(ctx context.Context, s durable.Session, b durable.ChangeBatch, hook func(durable.RunRecord, func() error) error) (durable.CommitAck, error) {
	r, err := s.Load(ctx)
	if err != nil {
		return durable.CommitAck{}, err
	}
	if _, err = b.ApplyTo(&r); err != nil {
		return durable.CommitAck{}, err
	}
	var ack durable.CommitAck
	commit := func() error { var err error; ack, err = s.Apply(ctx, b); return err }
	if err = hook(r, commit); err != nil {
		return durable.CommitAck{}, err
	}
	return ack, nil
}
