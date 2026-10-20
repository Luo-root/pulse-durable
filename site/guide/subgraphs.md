# 子图与重试

本页适用于将多个业务阶段封装为静态子图，并希望中断后复用子图内部成果的工作流。先完成[快速开始](quickstart.md)和[进程退出后恢复](recovery.md)，再运行本页的子图示例。

## 子图恢复的两种情况

可以把父挂载节点的一次尝试看作一份工作单，子图成果归属于这份工作单：

- **父尝试仍活跃**：例如宿主取消、进程退出或可靠检查点操作失败。恢复会认回原尝试及其子图作用域，复用其中已经确认的节点。
- **父尝试已经失败**：例如子图的真实业务错误使本次 Retry 尝试失败。旧作用域内的成果保留在记录中，但不能再用于新的父尝试。还有重试预算时，新尝试使用新作用域，子图重新执行。

例如子图包含“准备”和“完成”两步。在“完成”进入后发生宿主取消，恢复可以保留“准备”的结果；如果这次父尝试已经因真实业务错误失败，则下一次父尝试不能借用上一份工作单中的“准备”结果。

受管流的透传出口有独立的完成关系：空子图将入口流直接接到出口时，Sub 可以先保存成果，而实际来源继续生产。这份成果保存流引用，不替写端发布 EOF。若此时进程退出，已确认的 Sub 不重跑；声明可续读的来源从可靠 Position 继续，恢复的出口和写端共用一份端点、代际、尾部及确认账。活跃而未声明可续读的来源会被拒绝恢复；已经完成的来源只重建未确认尾部。

加载时仍会校验失败尝试下保留的旧历史，核对来源、代际和已知状态，但不会要求这些失效来源还能续读，也不会把它们后来真实失败误报为记录损坏。只有为有效作用域准备实际恢复时，才应用来源可续读和有效祖先等运行条件；旧失败作用域的节点不会因此重新执行。

## 1. 创建示例模块

需要 Go 1.25 或更高版本，并使用可写的本机目录。

```sh
mkdir durable-subgraphs
cd durable-subgraphs
go mod init example.com/durable-subgraphs
go get github.com/Luo-root/pulse-durable@a1ee8aacebed
```

下面使用固定 RunID 和 `./checkpoints`。首次执行应使用没有本例记录的新目录；再次开始新的演示应分配新 RunID，不能自动删除旧记录。

## 2. 创建 main.go

这个完整程序在同一进程中取消第一次运行，随后重建文件 Store、Runtime 和等价图。它演示活跃父尝试的子成果复用；真正退出进程的步骤见[恢复教程](recovery.md)。

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/filestore"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func newRuntime() (*durable.Runtime, error) {
	store, err := filestore.New("./checkpoints")
	if err != nil {
		return nil, err
	}
	return durable.New(store, durable.NewCodecs())
}

func buildGraph(interrupt context.CancelFunc, calls *[2]atomic.Int32) (
	graph *pulse.Graph, inputs pulse.Inputs, output pulse.Key[int], err error,
) {
	input := pulse.NewKey[int]("input")
	output = pulse.NewKey[int]("output")
	source := pulse.NewKey[int]("source")
	prepared := pulse.NewKey[int]("prepared")
	result := pulse.NewKey[int]("result")
	child, err := pulse.New("child", "Prepare and finish")
	if err != nil {
		return
	}
	if err = child.DeclareInput(pulse.NewPort(source)); err != nil {
		return
	}
	if err = child.DeclareOutput(pulse.NewPort(result)); err != nil {
		return
	}
	if err = child.Add(pulse.NewNode("prepare", "Prepare",
		pulse.Requires(source), pulse.Provides(prepared),
		func(rc *pulse.RunCtx) error {
			calls[0].Add(1)
			value, err := pulse.Get(rc, source)
			if err != nil {
				return err
			}
			return pulse.Set(rc, prepared, value)
		})); err != nil {
		return
	}
	if err = child.Add(pulse.NewNode("finish", "Finish",
		pulse.Requires(prepared), pulse.Provides(result),
		func(rc *pulse.RunCtx) error {
			calls[1].Add(1)
			value, err := pulse.Get(rc, prepared)
			if err != nil {
				return err
			}
			if interrupt != nil {
				interrupt()
				return rc.Context().Err()
			}
			return pulse.Set(rc, result, value+1)
		})); err != nil {
		return
	}
	graph, err = pulse.New("parent", "Mount a child graph")
	if err != nil {
		return
	}
	if err = graph.DeclareInput(pulse.NewPort(input)); err != nil {
		return
	}
	if err = graph.DeclareOutput(pulse.NewPort(output)); err != nil {
		return
	}
	if err = pulse.Sub(graph, "process", "Process input", []pulse.SubBind{
		pulse.In(input, source),
		pulse.Out(result, output),
	}, child, pulse.Retry(3, 0)); err != nil {
		return
	}
	err = pulse.Seed(&inputs, input, 41)
	return
}

func run() error {
	var calls [2]atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	graph, inputs, _, err := buildGraph(cancel, &calls)
	if err != nil {
		return err
	}
	runtime, err := newRuntime()
	if err != nil {
		return err
	}
	options := durable.RunOptions{RunID: "subgraph-demo-001", Version: "subgraph/1"}
	_, err = runtime.Run(ctx, graph, inputs, options)
	var checkpoint *pulse.CheckpointError
	if errors.As(err, &checkpoint) {
		return err
	}
	if !errors.Is(err, context.Canceled) {
		return fmt.Errorf("expected host cancellation, got %v", err)
	}
	before := [2]int32{calls[0].Load(), calls[1].Load()}
	fmt.Printf("Run body calls: prepare=%d finish=%d\n", before[0], before[1])
	runtime, err = newRuntime()
	if err != nil {
		return err
	}
	graph, inputs, output, err := buildGraph(nil, &calls)
	if err != nil {
		return err
	}
	execution, err := runtime.Resume(context.Background(), graph, inputs, options)
	if err != nil {
		return err
	}
	value, state, err := pulse.ReadOutput(execution, output)
	if err != nil {
		return err
	}
	if state != pulse.SlotStateReady {
		return fmt.Errorf("unexpected output state: %s", state)
	}
	fmt.Printf("Resume body calls: prepare=%d finish=%d\n",
		calls[0].Load()-before[0], calls[1].Load()-before[1])
	fmt.Println("result:", value, state)
	return nil
}
```

`pulse.In` 将父图输入绑定到子图输入，`pulse.Out` 将子图出口绑定到父图出口。`Retry(3, 0)` 安装在父挂载节点上，最多进行三次尝试，不是首次加三次重试。

## 3. 执行并检查结果

```sh
go mod tidy
go run .
```

预期输出：

```text
Run body calls: prepare=1 finish=1
Resume body calls: prepare=0 finish=1
result: 42 ready
```

第一次运行在 `finish` 读取上游后取消宿主 context，因此 `prepare` 已确认，`finish` 未确认，父 Retry 尝试仍活跃。恢复认回原作用域，只重新进入 `finish`。计数器仅用于观察，不参与恢复；真正的恢复来源是文件记录。

## Retry 预算不会因 Resume 增加

| 持久状态 | 恢复行为 |
| --- | --- |
| 当前父 Retry 尝试为 `active` | 继续同一尝试，复用有效子成果 |
| 当前父 Retry 尝试为 `failed`，还有预算 | 创建下一个尝试及新的子作用域，旧子成果不复用 |
| Retry 已耗尽，完整节点根尝试已失败 | 该 RunID 的节点预算已终结，Resume 不分配新预算 |
| 子图已完成，但父完整链尚未确认 | 只要父尝试仍活跃，可以复用子图成果，再完成父链 |
| 父完整链已经确认 | 恢复父节点成果，不重新进入父链或子图 |

宿主取消与检查点可靠失败会保留活跃尝试。节点使用自己的已取消 context，随后返回一个真实业务错误，并不自动等同于宿主中断；真实失败仍会消耗预算。恢复不是绕过 Retry 上限的入口。

自定义切面如果会改变业务语义、顺序或 Retry 参数，需要升级业务 `Version`。Durable 保存内置 Retry 的尝试边界，不会把任意自定义循环自动变成持久重试。

## 错误如何处理

- 子图输入或挂载结构改变：恢复会核对结构及实际绑定输入。使用原始条件恢复；新的业务条件应分配新 RunID。
- 真实业务错误：由 Retry 消耗剩余预算，外部副作用仍需幂等或事务处理。已经失败的父尝试中的旧子成果不会复用。
- `CheckpointError`：停止本次，处理存储、编码或回调问题后恢复。可靠失败不能被父 Retry、正常 skip 或吞错切面改成成功。
- Retry 预算耗尽：修正业务原因后，根据应用策略创建新的业务运行；不要通过反复 `Resume` 补充次数。

## 下一步

需要持久保存业务类型时，继续[值编码](codecs.md)。作用域与尝试记录的严格关联见[数据模型](../reference/data-model.md)；错误分类和可靠失败规则见[错误处理](../reference/errors.md)。
