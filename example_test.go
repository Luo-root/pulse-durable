package durable_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)

func ExampleRuntime_Resume() {
	// MemoryStore 演示检查点契约；真实进程退出恢复使用 filestore 或其它持久 Store。
	store := durable.NewMemoryStore()
	runtime, err := durable.New(store, durable.NewCodecs())
	if err != nil {
		panic(err)
	}
	g, err := pulse.New("document", "中断后继续")
	if err != nil {
		panic(err)
	}
	downloaded, result := pulse.NewKey[int]("downloaded"), pulse.NewKey[int]("result")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	downloadCalls, summaryCalls := 0, 0
	interrupted := false
	if err := g.Add(pulse.NewNode("download", "已确认成果不用重做", nil, pulse.Provides(downloaded), func(rc *pulse.RunCtx) error {
		downloadCalls++
		return pulse.Set(rc, downloaded, 41)
	})); err != nil {
		panic(err)
	}
	if err := g.Add(pulse.NewNode("summarize", "未确认步骤从头再进", pulse.Requires(downloaded), pulse.Provides(result), func(rc *pulse.RunCtx) error {
		summaryCalls++
		if !interrupted {
			interrupted = true
			cancel() // 确定下载检查点已确认、下游已进体之后，模拟宿主中断。
			return rc.Context().Err()
		}
		v, err := pulse.Get(rc, downloaded)
		if err != nil {
			return err
		}
		return pulse.Set(rc, result, v+1)
	})); err != nil {
		panic(err)
	}
	if err := g.DeclareOutput(pulse.NewPort(result)); err != nil {
		panic(err)
	}
	options := durable.RunOptions{RunID: "document-123", Version: "business/1"}
	if _, err := runtime.Run(ctx, g, pulse.Inputs{}, options); !errors.Is(err, context.Canceled) {
		panic(err)
	}
	e, err := runtime.Resume(context.Background(), g, pulse.Inputs{}, options)
	if err != nil {
		panic(err)
	}
	value, state, err := pulse.ReadOutput(e, result)
	if err != nil {
		panic(err)
	}
	fmt.Println("download calls:", downloadCalls)
	fmt.Println("summary calls:", summaryCalls)
	fmt.Println("result:", value, state)
	// Output:
	// download calls: 1
	// summary calls: 2
	// result: 42 ready
}
