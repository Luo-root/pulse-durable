# 进程退出后恢复

本页演示真正的两个进程：第一个进程完成上游、进入下游后退出；第二个进程重建同一张图，从文件检查点恢复并得到 `42 ready`。适合验证本机重启恢复，以及理解哪些业务函数会重新执行。

## 1. 准备独立模块

需要 Go 1.25 或更高版本，以及一个可以写入文件的本机目录。两个进程必须从同一工作目录运行，使 `./checkpoints` 指向相同存储。

```sh
mkdir durable-recovery
cd durable-recovery
go mod init example.com/durable-recovery
go get github.com/Luo-root/pulse-durable@f6b3feeb2cc58a421db3fe7048c1126f00a8aab8
```

Durable 尚无正式 release，安装命令固定到公开开发修订。使用一个尚未运行过本例的新模块目录；如果已有检查点，先阅读本页的“重复运行”说明。

## 2. 创建 main.go

将以下完整程序保存为 `main.go`。两个模式使用相同图 ID、节点 ID、端口、输入、RunID、Version 和编码器；`mode` 只控制本例的退出故障注入。

```go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/filestore"
)

func main() {
	mode := flag.String("mode", "run", "run or resume")
	flag.Parse()
	if err := execute(*mode); err != nil {
		log.Fatal(err)
	}
}

func execute(mode string) error {
	if mode != "run" && mode != "resume" {
		return fmt.Errorf("invalid mode %q: use run or resume", mode)
	}
	store, err := filestore.New("./checkpoints")
	if err != nil {
		return err
	}
	runtime, err := durable.New(store, durable.NewCodecs())
	if err != nil {
		return err
	}
	input := pulse.NewKey[int]("input")
	prepared := pulse.NewKey[int]("prepared")
	output := pulse.NewKey[int]("output")
	graph, err := pulse.New("process-recovery", "Recover after process exit")
	if err != nil {
		return err
	}
	if err := graph.DeclareInput(pulse.NewPort(input)); err != nil {
		return err
	}
	if err := graph.DeclareOutput(pulse.NewPort(output)); err != nil {
		return err
	}
	if err := graph.Add(pulse.NewNode("upstream", "Prepare input",
		pulse.Requires(input), pulse.Provides(prepared),
		func(rc *pulse.RunCtx) error {
			value, err := pulse.Get(rc, input)
			if err != nil {
				return err
			}
			fmt.Println("enter upstream:", value)
			return pulse.Set(rc, prepared, value)
		})); err != nil {
		return err
	}
	if err := graph.Add(pulse.NewNode("downstream", "Add one",
		pulse.Requires(prepared), pulse.Provides(output),
		func(rc *pulse.RunCtx) error {
			value, err := pulse.Get(rc, prepared)
			if err != nil {
				return err
			}
			fmt.Println("enter downstream:", value)
			if mode == "run" {
				fmt.Println("simulate process exit: 75")
				os.Exit(75)
			}
			return pulse.Set(rc, output, value+1)
		})); err != nil {
		return err
	}
	var inputs pulse.Inputs
	if err := pulse.Seed(&inputs, input, 41); err != nil {
		return err
	}
	options := durable.RunOptions{RunID: "recovery-demo-001", Version: "recovery/1"}
	var execution *pulse.Execution
	if mode == "run" {
		execution, err = runtime.Run(context.Background(), graph, inputs, options)
	} else {
		execution, err = runtime.Resume(context.Background(), graph, inputs, options)
	}
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

下游读取 `prepared` 后才调用 `os.Exit(75)`。Durable 在确认上游完整检查点后才发布该值，因此退出点确定在上游已保存、下游尚未完成的位置。这里不依赖延时或调度巧合。

## 3. 启动第一个进程

在模块目录执行：

```sh
go mod tidy
go run . -mode=run
```

预期业务输出：

```text
enter upstream: 41
enter downstream: 41
simulate process exit: 75
```

此命令故意失败。程序本身调用 `os.Exit(75)`；通过 `go run` 启动时，Go 工具通常额外打印 `exit status 75`，但 shell 收到的 Go 工具退出状态可能是 `1`。不要把包装命令的退出状态当作业务程序的 `75`。

`os.Exit` 不运行 Go 的 deferred 函数。进程退出后，操作系统释放文件锁，已写入的检查点仍保留在 `./checkpoints`。不要删除检查点或 `.lock` 文件。

## 4. 启动第二个进程

仍在同一模块目录执行：

```sh
go run . -mode=resume
```

预期输出：

```text
enter downstream: 41
result: 42 ready
```

第二个进程没有 `enter upstream`。把两次输出合起来观察，上游进入一次，下游进入两次。下游的函数从开头重新执行；它没有从 `os.Exit` 下一行继续。

如果需要直接观察程序退出码，可编译后启动。PowerShell：

```powershell
go build -o durable-recovery.exe .
.\durable-recovery.exe -mode=run
$LASTEXITCODE
.\durable-recovery.exe -mode=resume
```

POSIX shell：

```sh
go build -o durable-recovery .
./durable-recovery -mode=run
echo $?
./durable-recovery -mode=resume
```

直接运行时，第一次退出码为 `75`。这组命令是对第 3、4 步的另一种执行方式，应在还没有该 RunID 记录的新目录中使用，或先为新的演示分配新 RunID。

## 5. 重复运行与新业务

同一存储中的 `recovery-demo-001` 已有记录后，再次 `-mode=run` 会返回 `ErrConflict`。`Run` 负责创建，`Resume` 负责打开已有记录；两者不会自动互相切换。

再次 `-mode=resume` 会读取已完成成果，输出 `result: 42 ready`，不会重新进入这两个节点。需要开始另一项业务或重新演示退出时，应在代码中为它设置新的 RunID，然后先 `run`、再 `resume`。教程不会自动删除或覆盖旧检查点。

恢复同一业务时，应保留相同输入 `41`、业务版本、图定义和编码契约。修改图、节点、端口、Codec ID/版本或输入可能导致恢复冲突；修改函数、闭包和切面语义需要由业务主动升级 `Version`，并按新业务或迁移策略处理旧记录。

## 错误如何处理

| 现象 | 原因与操作 |
| --- | --- |
| `ErrNotFound` | 没有记录；确认先执行过 `run`，并且工作目录和 RunID 一致 |
| `ErrBusy` | 同一 RunID 正被另一个会话持有；确认原进程是否仍在运行，不要删除锁文件 |
| `ErrConflict` | 已有记录或恢复身份、版本、定义、输入不一致；核对调用方式与原始条件 |
| `CheckpointError` | 保存、加载、编码或恢复协议失败；处理底层原因，再用相同条件恢复并重读权威记录 |

保存报错时可能已经实际写入。不要手动回滚记录或盲目重复旧的保存操作；新的 `Resume` 会重新加载真实记录。详细分类见[错误处理](../reference/errors.md)。

## 接入业务时的规则

本例中的输出日志只用于观察函数进入次数。真实业务中的数据库写入、网络请求或文件操作可能发生在检查点确认前，因此仍可能重复。请使用业务幂等键、事务、去重或 outbox；Durable 不承诺 exactly-once。

`filestore` 面向本机单宿主，依赖文件锁和同目录原子替换，不承诺断电持久性或分布式运行管理。恢复后会执行未确认函数的当前代码；它不是确定性事件重放。

## 下一步

使用多阶段子图时，继续[子图与重试](subgraphs.md)，区分仍活跃的父尝试与已经失败的父尝试。需要业务结构体或其他持久后端时，阅读[值编码](codecs.md)和[存储](storage.md)。
