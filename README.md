[English](README_en.md) | [中文](README.md)

<p align="center">
  <img src=".github/assets/logo.svg" alt="Pulse Durable" width="360">
</p>

<p align="center">Pulse 工作流的检查点存储与恢复</p>

<p align="center">
  <a href="https://go.dev/"><img src="https://img.shields.io/badge/Go-1.25%2B-2563eb" alt="Go 1.25+"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-2563eb" alt="MIT license"></a>
  <a href="https://luo-root.github.io/pulse-durable/"><img src="https://img.shields.io/badge/docs-中文%20%7C%20English-2563eb" alt="双语文档"></a>
</p>

Pulse Durable 为 [Pulse](https://github.com/Luo-root/pulse) 工作流提供检查点存储与恢复。你仍然使用 Pulse 定义图、节点和输入输出，通过 Durable 的 `Run` 创建一次运行，通过 `Resume` 恢复已有运行。

完整节点链的检查点被存储确认后，输出才会发布给下游。恢复会复用有效作用域内已确认的节点成果，未确认的节点从函数开头重新执行。Durable 不保存 Go 调用栈，也不自动撤销数据库写入、HTTP 请求等外部副作用。

## 安装

需要 **Go 1.25 或更高版本**。Durable 当前是开发版本，尚无正式 release；下面固定到公开开发修订，便于重复安装：

```sh
go get github.com/Luo-root/pulse-durable@bd38ad7ab69f
```

该修订的 `go.mod` 固定依赖 Pulse `v0.4.2-0.20261016133921-f9f071a07499`。完整的新模块操作见[安装](site/guide/installation.md)。

## 最小示例

下面的 `main.go` 声明一个输入和一个出口，运行“加一”节点，再读取本次执行的出口。`MemoryStore` 仅用于最小演示；跨进程恢复请使用持久存储。

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

在已安装依赖的模块中执行：

```sh
go run .
```

预期输出：

```text
result: 42 ready
```

从空目录开始的完整步骤见[快速开始](site/guide/quickstart.md)。要观察真正退出进程再恢复的效果，请继续[进程退出后恢复](site/guide/recovery.md)；该教程使用文件存储和两个独立进程。

## 文档地图

在线文档：[中文](https://luo-root.github.io/pulse-durable/) · [English](https://luo-root.github.io/pulse-durable/en/)。以下链接指向仓库内的文档源文件。

| 任务 | 文档 |
| --- | --- |
| 了解定位与阅读顺序 | [指南概览](site/guide/index.md) |
| 安装开发版本并检查依赖 | [安装](site/guide/installation.md) |
| 完成第一次运行并读取出口 | [快速开始](site/guide/quickstart.md) |
| 保留检查点并在新进程中恢复 | [进程退出后恢复](site/guide/recovery.md) |
| 组合子图并理解 Retry 恢复 | [子图与重试](site/guide/subgraphs.md) |
| 保存业务结构体与自定义类型 | [值编码](site/guide/codecs.md) |
| 使用文件存储或实现 Store | [存储](site/guide/storage.md) |
| 估算 1–10000 步的运行、存储与恢复成本，复跑同任务对照 | [规模与性能](site/guide/performance.md) · [评测源码与方法](benchmarks/README.md) |
| 核对 API、记录和错误规则 | [参考概览](site/reference/index.md)、[执行契约](site/reference/execution.md)、[数据模型](site/reference/data-model.md)、[错误处理](site/reference/errors.md) |

## 当前边界

- 支持普通值、受管 `pulse.Stream[T]`、静态子图，以及节点 Retry 和消息重试预算的保存与恢复。`Resume` 延续剩余预算；已失败的父尝试下的子成果不会复用。
- 流消息先可靠追加再交付；`HandleMessage` 的输入确认、显式 `SetMessageState` 状态和输出在同一次 Save 中确认。消息贡献的普通值在恢复时认回完整节点暂存，仍等完整节点确认才发布，不重跑已 Ack 输入。活跃来源须声明 `WithStreamSource(id, true)`，按 `StreamWriter.Position()` 与 `SendAt` 续读；已完成来源会重建未确认尾部而不重跑业务体。
- `RunID` 和业务 `Version` 必填。恢复需要相同的定义、输入和编码契约；修改业务函数、闭包或切面语义时，需要升级业务版本。
- `filestore` 面向本机单宿主，依赖文件系统的独占锁和同目录原子替换。它不提供分布式租约、跨机器调度或断电持久性保证。`MemoryStore` 只验证进程内协议。
- 未确认节点的外部副作用可能重复。业务应使用幂等键、事务、去重或 outbox。Durable 不承诺 exactly-once 或 deterministic replay；宿主可按[组合指南](site/guide/composition.md)登记业务等待并 Cancel/Resume，库不内置唤醒服务。不支持 channel、函数或 `unsafe.Pointer` 值。
- 新建记录使用格式 `2`；普通格式 `1` 仍可恢复，下一次保存升级为 `2`。旧 Runtime 拒绝新格式。Codec 编码消息元素、游标和显式状态，不编码流端点；普通值中嵌套 channel 的拒绝不变。
- `RunOptions.BeforeStart` 在 Start 前供宿主登记 `pulse.SubscribeStream` 并启动读取任务；Run/Resume 仍阻塞 Wait 并持有 Session，读取任务须确认消息或显式停止。
- 检查点包含编码后的业务输出，并非加密容器。存储访问控制、备份与数据保留由宿主管理。

## 验证仓库

在仓库根目录、未启用本地工作区替换的环境中执行：

```sh
go build ./...
go vet ./...
go test -race -count=1 ./...
```

节点、子图与受管流的可运行示例集中在 [example_test.go](example_test.go)；真实进程退出与重启测试见 [integration/recovery_test.go](integration/recovery_test.go)。

## 参与项目

提交修改前请阅读[贡献指南](CONTRIBUTING.md)，社区交流遵守[行为准则](CODE_OF_CONDUCT.md)。安全漏洞请按[安全政策](SECURITY.md)私密上报。

## 许可

本项目采用 [MIT 许可](LICENSE)，Copyright © 2026 Luo-root。Logo 中的字体轮廓素材保留[第三方字体许可](.github/assets/FONT-LICENSE.txt)。
