# 安装

本文说明当前**未发 tag 的开发源码**与公开安装版本的关系，需要 **Go 1.26+**，当前根模块依赖 **Pulse v0.5.2**。现行存储契约包含无格式号领域记录、wire `2`、snapshot envelope `2` 与 Task 顶层 protocol `2` / payload_format `1`。本轮根开发提交 `v0.1.1-0.20261108162459-ccc90d4b53c3` 已由 `go list -m` 解析并可下载，对应完整 Hash `ccc90d4b53c318696328cf0fbed7f876e996b7a8`；它是公开提交伪版本，不是发布 tag。子模块 go.mod 已固定该根依赖；SQLite/独立 PostgreSQL 已解析为 `v0.1.1-0.20261108164227-4babad07420f`，distributed 为 `v0.0.0-20261108164227-4babad07420f`，对应完整 Hash `4babad07420f7114729455eaf7fcff9ee5818002`。这些同为开发提交而非 tag；此前源码测试不等于完整下载包已验证。tag 状态以 [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases) 为准。历史性能页保留测量时的版本。

## 1. 检查前置条件

```sh
go version
```

预期 `go1.26.0` 或更新版本。下载模块需要访问 Go 模块代理或 GitHub；失败时先检查网络与 `GOPROXY`。

## 2. 安装根开发提交与验证子模块源码

根模块可使用已解析的公开开发 pin：

```sh
mkdir durable-quickstart
cd durable-quickstart
go mod init example.com/durable-quickstart
go get github.com/Luo-root/pulse-durable@v0.1.1-0.20261108162459-ccc90d4b53c3
```

按需安装可选模块，完整开发 pin 已由 `go list -m` 解析：

```sh
go get github.com/Luo-root/pulse-durable/sqlitestore@v0.1.1-0.20261108164227-4babad07420f
go get github.com/Luo-root/pulse-durable/postgresstore@v0.1.1-0.20261108164227-4babad07420f
go get github.com/Luo-root/pulse-durable/distributed@v0.0.0-20261108164227-4babad07420f
```

全新消费者在 `GOWORK=off`、无 replace 下完成下载、四版本核对、Store/Coordinator/worker 接口装配与 snapshot2 检查，`go test -race .` 通过（4.858s）。这是公开开发 pin 的独立消费验证，不是正式 tag 发版或对消费者包运行全部后端实库测试。源码验证见[贡献指南](https://github.com/Luo-root/pulse-durable/blob/main/CONTRIBUTING.md)。

根与子模块分别发布，根 pin 不会自动发布子模块。当前使用见 [SQLite](sqlite.md)、[PostgreSQL](postgresql.md) 与[分布式入口](https://github.com/Luo-root/pulse-durable/blob/main/distributed/README.md)。

### 历史开发 pin

旧开发 pin 不提供当前契约，不用于现行示例。历史性能页面保留当时测量版本；当前根开发 pin 以上述可下载提交为准。

## 3. 核对依赖

```sh
go list -m github.com/Luo-root/pulse-durable github.com/Luo-root/pulse
```

当前根开发安装应解析 `v0.1.1-0.20261108162459-ccc90d4b53c3`、Pulse `v0.5.2`；工作区验证时根模块可指向本地 checkout。子模块 go.mod 已固定该根 pin，完整子模块按上面的开发版本解析。已有项目可能通过 Go 模块版本选择保留更高的依赖，须核对并验证实际组合。安装后不要立即 `go mod tidy`：尚无源码导入时它会移除未使用依赖。

独立核对模块时，可暂时关闭工作区。PowerShell：

```powershell
$env:GOWORK = 'off'
go list -m github.com/Luo-root/pulse-durable github.com/Luo-root/pulse
```

POSIX shell：

```sh
GOWORK=off go list -m github.com/Luo-root/pulse-durable github.com/Luo-root/pulse
```

完成后恢复先前的环境设置。

## 4. 选择导入包

以下为导入片段，不是完整程序：

```go
import (
    "github.com/Luo-root/pulse"
    durable "github.com/Luo-root/pulse-durable"
)
```

`durable.NewMemoryStore()` 用于进程内示例；本机持久存储可以选择 `filestore` 或 [SQLite](sqlite.md)，数据库存储见 [PostgreSQL](postgresql.md)。添加完整代码后再执行 `go mod tidy`。

## 常见问题

| 现象 | 处理方式 |
| --- | --- |
| `unknown revision v0.1.0` | 核对 Releases 与模块对应 tag；发布前使用开发 pin 或源码验证，不把下载失败归为 API 错误 |
| Go 版本过低 | 使用 Go 1.26 或更高版本 |
| 模块下载失败 | 检查网络、代理和模块地址 |
| `go list` 版本不同 | 检查 `go.mod`、工作区和更高的现有依赖 |
| 本地 `replace` 干扰 | 用独立模块核对公开版本 |

## 下一步

继续[快速开始](quickstart.md)或[进程退出后恢复](recovery.md)。升级业务定义或 Codec 前先核对[执行契约](../reference/execution.md)。
