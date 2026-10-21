# 安装

本页适用于第一次在 Go 程序中使用 Pulse Durable。完成后，你会得到一个固定依赖版本的 Go 模块，可以继续运行[快速开始](quickstart.md)中的完整程序。

## 1. 检查前置条件

需要 Go 1.25 或更高版本。执行：

```sh
go version
```

预期版本为 `go1.25.0` 或更高。还需要能够访问 Go 模块代理或 GitHub；模块下载失败时，应先检查网络与当前 `GOPROXY` 配置。

## 2. 创建模块并安装

在希望保存示例的目录下执行，目录名也可以按需修改：

```sh
mkdir durable-quickstart
cd durable-quickstart
go mod init example.com/durable-quickstart
go get github.com/Luo-root/pulse-durable@f6b3feeb2cc58a421db3fe7048c1126f00a8aab8
```

Durable 当前没有正式 release。这条命令固定到公开开发修订，Go 会将它解析为伪版本 `v0.0.0-20261021041843-f6b3feeb2cc5` 并写入 `go.mod`。不要将它当作已发布的 `v0.1`。

按维护者 2026-10-18 确认的发布顺序，Pulse 先准备 v0.5.0，Durable 暂不发版；须先完成并验收 [#40 的正式数据库 Store](https://github.com/Luo-root/pulse-durable/issues/40)，再考虑首次发布。Pulse 独立评测模块中的 SQLite 实现不属于正式后端，也不满足该发布前置。

该修订固定依赖 Pulse：

```text
v0.4.2-0.20261021041444-b1b147d4c134
```

通常不需要单独安装 Pulse。Go 模块的版本选择可能保留现有项目中更高的 Pulse 依赖；接入已有项目时，应核对实际解析版本，并验证该组合。

## 3. 核对依赖

执行：

```sh
go list -m github.com/Luo-root/pulse-durable github.com/Luo-root/pulse
```

在本页创建的新模块中，预期看到 Durable `v0.0.0-20261021041843-f6b3feeb2cc5`，以及上面列出的 Pulse 版本。不要在安装步骤后立即执行 `go mod tidy`：尚未添加导入代码时，它会清除未使用的依赖。

如果本机启用了 `go.work`，但需要单独核对当前模块，可暂时禁用工作区。在 PowerShell 中执行：

```powershell
$env:GOWORK = 'off'
go list -m github.com/Luo-root/pulse-durable github.com/Luo-root/pulse
```

在 POSIX shell 中执行：

```sh
GOWORK=off go list -m github.com/Luo-root/pulse-durable github.com/Luo-root/pulse
```

## 4. 选择导入包

业务代码使用：

```go
import (
	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)
```

这是导入片段，不是完整程序。需要本机持久存储时，再导入 `github.com/Luo-root/pulse-durable/filestore`。`durable.NewMemoryStore()` 用于同一进程内的示例与测试；`filestore.New(dir)` 用于本机进程退出后的恢复。

## 常见问题

| 现象 | 处理方式 |
| --- | --- |
| Go 版本过低 | 使用 Go 1.25 或更高版本后重新安装 |
| 模块下载失败 | 检查网络、代理和错误中指出的模块地址，再重试下载 |
| `go list` 的 Pulse 版本不同 | 检查现有 `go.mod` 和工作区；已有较高版本会参与 Go 模块版本选择 |
| 添加代码后提示依赖缺失 | 在模块目录重新执行固定版本的 `go get`，然后执行 `go mod tidy` |
| 本地 `replace` 使结果难以复现 | 在独立模块中用公开修订验证，再决定开发工作区的引用方式 |

## 下一步

继续[快速开始](quickstart.md)，添加完整 `main.go`，运行并读取声明出口。已经完成最小运行时，可继续[进程退出后恢复](recovery.md)。升级代码或编码版本之前，先核对[执行契约](../reference/execution.md)中的兼容性规则。
