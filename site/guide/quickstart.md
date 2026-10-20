# 快速开始

本页从一个新 Go 模块开始，完成一次 Durable `Run`，再读取声明出口。示例只有一个节点，使用 `MemoryStore` 展示最小调用流程；它不会在进程退出后保留检查点。

## 1. 准备模块

需要 Go 1.25 或更高版本，以及能够下载 Go 模块的环境。执行：

```sh
mkdir durable-quickstart
cd durable-quickstart
go mod init example.com/durable-quickstart
go get github.com/Luo-root/pulse-durable@887892d6d64f5d9c589675edea7fa12680332045
```

如果已经按[安装](installation.md)创建模块，直接在那个模块目录继续。Durable 尚无正式 release，本教程固定使用公开开发修订。

## 2. 创建 main.go

将以下完整代码保存为 `main.go`：

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	runtime, err := durable.New(durable.NewMemoryStore(), durable.NewCodecs())
	if err != nil {
		return err
	}
	input := pulse.NewKey[int]("input")
	output := pulse.NewKey[int]("output")
	graph, err := pulse.New("quickstart", "Add one")
	if err != nil {
		return err
	}
	if err := graph.DeclareInput(pulse.NewPort(input)); err != nil {
		return err
	}
	if err := graph.DeclareOutput(pulse.NewPort(output)); err != nil {
		return err
	}
	if err := graph.Add(pulse.NewNode("add", "Add one",
		pulse.Requires(input), pulse.Provides(output),
		func(rc *pulse.RunCtx) error {
			value, err := pulse.Get(rc, input)
			if err != nil {
				return err
			}
			return pulse.Set(rc, output, value+1)
		})); err != nil {
		return err
	}
	var inputs pulse.Inputs
	if err := pulse.Seed(&inputs, input, 41); err != nil {
		return err
	}
	execution, err := runtime.Run(context.Background(), graph, inputs,
		durable.RunOptions{RunID: "quickstart-001", Version: "increment/1"})
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
	fmt.Println("result:", value, state)
	return nil
}
```

程序依次完成以下操作：

1. 创建 `Runtime`，传入存储与类型编码注册表。`NewCodecs` 已包含本例使用的 `int`。
2. 声明图输入 `input` 和图出口 `output`。节点通过 `Requires`、`Provides` 声明它读取和产生的键。
3. 用 `Seed` 提供外部输入 `41`。节点的 `Get` 读取输入，`Set` 提供输出 `42`。
4. 用非空 `RunID` 与业务 `Version` 创建运行。`Run` 阻塞到本次执行结束。
5. 检查运行错误，再用 `ReadOutput` 读取本次执行的声明出口。

## 3. 执行并检查结果

在 `main.go` 所在模块目录执行：

```sh
go mod tidy
go run .
```

预期输出：

```text
result: 42 ready
```

`ready` 表示存在一个业务值。零值、空字符串或合法的 nil 值也可以是 `ready`，不能仅按值是否为零判断有无结果。正常跳过使用 `skipped` 状态；调用方应分别检查 `err` 和 `state`。

本例检查 `ready`，因为这个图明确要求得到一个加一结果。一般图的出口也可能正常处于 `skipped`，或是未喂值的可选透传出口 `pending`；这些状态本身不等同于运行失败。详细规则见[执行契约](../reference/execution.md)。

## 4. 理解这次运行的边界

节点函数里的 `Set` 不等于整个节点已持久完成。Durable 在包含切面的完整节点链结束后保存检查点，保存被确认后才向下游发布成果。

每次启动本例都会创建全新的 `MemoryStore`，所以重复执行 `go run .` 会再次执行节点。同一存储中已存在的 RunID 不能再次 `Run`；已有记录应通过 `Resume` 打开，新业务应分配新 RunID。

## 遇到错误时

- `no registered codec`：检查所有节点键和声明端口的类型。业务结构体和命名类型需要[显式注册编码器](codecs.md)。
- 出口未声明或类型不符：核对 `DeclareOutput` 与 `ReadOutput` 使用的键；内部键不能作为图出口读取。
- 运行返回错误：先处理该错误。失败执行的 `ReadOutput` 会返回失败原因，不能拿零值继续业务计算。
- 相同 RunID 冲突：确认是在创建新业务还是恢复旧业务，再选择新的 RunID 或 `Resume`。

## 下一步

继续[进程退出后恢复](recovery.md)，将存储替换为 `filestore`，实际退出第一个进程并在第二个进程中完成运行。恢复身份、版本和事件的完整约束见[执行契约](../reference/execution.md)。
