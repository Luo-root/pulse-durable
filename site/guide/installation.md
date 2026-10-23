# 安装

本文与现行教程以目标 **Durable v0.1.0** 为 API 基线，需要 **Go 1.26+**，依赖 **Pulse v0.5.0**。这是发版准备，不代表已发布：tag 状态以 [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases) 为准；相应 tag 发布前，下列 v0.1.0 命令尚不可用。历史性能页保留测量时的版本。

## 1. 检查前置条件

```sh
go version
```

预期 `go1.26.0` 或更新版本。下载模块需要访问 Go 模块代理或 GitHub；失败时先检查网络与 `GOPROXY`。

## 2. 创建模块并安装

```sh
mkdir durable-quickstart
cd durable-quickstart
go mod init example.com/durable-quickstart
go get github.com/Luo-root/pulse-durable@v0.1.0
```

可选后端按需安装，不使用它们时根模块不会引入数据库驱动：

```sh
go get github.com/Luo-root/pulse-durable/sqlitestore@v0.1.0
go get github.com/Luo-root/pulse-durable/postgresstore@v0.1.0
```

仓库包含三个独立模块，因此发布需分别提供三个 tag：

| 模块 | 仓库 tag | go get 版本 |
| --- | --- | --- |
| 根模块 | `v0.1.0` | `@v0.1.0` |
| SQLite | `sqlitestore/v0.1.0` | `@v0.1.0` |
| PostgreSQL | `postgresstore/v0.1.0` | `@v0.1.0` |

根 tag 不会自动发布子模块。这里只说明命名，不执行 tag 创建或发布。完整用法见 [SQLite](sqlite.md) 和 [PostgreSQL](postgresql.md)。

### 发布前开发 pin 备选

如果目标 tag 尚未发布，可选择此前已验证可解析的公开开发 pin；这是旧开发快照，不是 v0.1.0 发布安装验收，也不保证包含当前源码的全部修正：

```sh
go get github.com/Luo-root/pulse-durable@v0.0.0-20261023040750-1b397e91c878
go get github.com/Luo-root/pulse-durable/sqlitestore@v0.0.0-20261023051404-bc57abd1d7c3
go get github.com/Luo-root/pulse-durable/postgresstore@v0.0.0-20261023051404-bc57abd1d7c3
```

验证当前 checkout 时，按[贡献指南](https://github.com/Luo-root/pulse-durable/blob/main/CONTRIBUTING.md)使用临时 `go.work`，不在发布依赖中加入本地 `replace`。

## 3. 核对依赖

```sh
go list -m github.com/Luo-root/pulse-durable github.com/Luo-root/pulse
```

目标发布安装的新模块预期 Durable `v0.1.0`、Pulse `v0.5.0`；使用开发 pin 时预期对应伪版本。已有项目可能通过 Go 模块版本选择保留更高的依赖，须核对并验证实际组合。安装后不要立即 `go mod tidy`：尚无源码导入时它会移除未使用依赖。

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
