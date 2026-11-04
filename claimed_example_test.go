package durable_test

import (
	"context"
	"fmt"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)

// 协调器的真实会话实现 RunID 和受栅栏保护的 Load/Save。此
// 示例仅为本地 MemoryStore 会话添加身份绑定。
type boundSession struct {
	durable.Session
	id string
}

func (s boundSession) RunID() string { return s.id }

func ExampleRuntime_RunClaimed() {
	ctx := context.Background()
	store := durable.NewMemoryStore()
	runtime, _ := durable.New(store, durable.NewCodecs())
	session, _ := store.Acquire(ctx, "invoice-42")
	defer session.Close() // 在调用方作出终态决策后执行，而非在 Runtime 内部执行
	graph, _ := pulse.New("invoice", "")
	_, outcome, err := runtime.RunClaimed(ctx, boundSession{session, "invoice-42"}, graph, pulse.Inputs{}, durable.RunOptions{RunID: "invoice-42", Version: "v1"})
	if err != nil {
		panic(err)
	}
	// 分布式调用方此时仍持有会话所有权，并执行受栅栏保护的 Finish。
	// Runtime 不声明或持久化协调器任务状态。
	fmt.Println(outcome)
	// Output: completed
}
