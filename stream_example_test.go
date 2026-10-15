package durable_test

import (
	"context"
	"fmt"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)

func ExampleRuntime_managedStream() {
	if err := managedStreamExample(); err != nil {
		panic(err)
	}
	// Output: 6
}

func managedStreamExample() error {
	graph, err := pulse.New("managed-example", "有界来源与显式消息状态")
	if err != nil {
		return err
	}
	numbers := pulse.NewKey[pulse.Stream[int]]("numbers")
	if err := pulse.ProduceStream(graph, "source", "按可靠位置继续", numbers,
		func(_ *pulse.RunCtx, writer *pulse.StreamWriter[int]) error {
			for next := int(writer.Position().Sequence) + 1; next <= 3; next++ {
				if err := writer.SendAt(next, next); err != nil {
					return err
				}
			}
			return nil
		}, pulse.WithBuffer(2), pulse.WithStreamSource("example/integers", true)); err != nil {
		return err
	}
	var total int
	if err := pulse.ConsumeStream(graph, "sum", "显式状态与输入确认共同提交", numbers,
		func(rc *pulse.RunCtx, value int) error {
			previous, _, err := pulse.GetMessageState[int](rc, "sum")
			if err != nil {
				return err
			}
			total = previous + value
			return pulse.SetMessageState(rc, "sum", total)
		}, pulse.WithMessageHandling(pulse.WithMessageRetry(2, 0))); err != nil {
		return err
	}
	// MemoryStore 只演示 API；真实退出/恢复测试使用 filestore 和独立进程。
	runtime, err := durable.New(durable.NewMemoryStore(), durable.NewCodecs())
	if err != nil {
		return err
	}
	if _, err := runtime.Run(context.Background(), graph, pulse.Inputs{}, durable.RunOptions{RunID: "example", Version: "sum/1"}); err != nil {
		return err
	}
	fmt.Println(total)
	return nil
}
