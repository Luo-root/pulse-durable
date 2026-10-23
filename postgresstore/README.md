# postgresstore

## 中文

可选 PostgreSQL 后端，实现 Durable `Store` / `Session`，使用 `pgx/v5` 与 `pgxpool`。目标 API 基线 **v0.1.0**，要求 **Go 1.26+**、**Pulse v0.5.0**。

这是发版准备，不代表已发布。状态以 [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases) 为准；根 `v0.1.0` 与子模块 `postgresstore/v0.1.0` tag 发布前，以下命令尚不可用：

```sh
go get github.com/Luo-root/pulse-durable@v0.1.0
go get github.com/Luo-root/pulse-durable/postgresstore@v0.1.0
```

- [PostgreSQL 完整教程](../site/guide/postgresql.md)：数据库与权限、环境变量 DSN、完整 Run/Resume 程序、读取持久化成果、Save/Load 和 Codec、连接与关闭。
- [安装与版本](../site/guide/installation.md)：三 tag 命名、发布前开发 pin 备选。
- [贡献指南](../CONTRIBUTING.md)：独立模块和源码工作区验证。根 `./...` 不包含本模块。

初始化创建并严格检查专用 `pulse_durable` schema v1。每个 Session 在 Close 前占用一条池连接；跨 Store Acquire 的池等待和 advisory lock 检测默认合计最多 30 秒，调用方更短 deadline 优先。池耗尽超时是 `context.DeadlineExceeded`，不伪报 `ErrBusy`。解锁不确定则销毁连接，清理最多 5 秒。advisory lock 不是 lease/fencing、派发或自动接管；外部副作用仍需幂等。Save 提交错误不自动重试，应停止原调用、等待所有权释放并重新 Load。不要记录 DSN 或密码；先停止业务运行再关闭 Session/Store。

**真实数据库测试使用 `PULSE_POSTGRES_TEST_DSN`，执行 `DROP SCHEMA IF EXISTS pulse_durable CASCADE` 后重建。只能使用隔离、可丢弃的测试库，绝不能使用业务库或要保留的演示库。** 无 DSN 时测试 Skip，不代表实库通过。网络故障、数据库重启、备份恢复和分布式 fencing 未完成单独验收；本机无 DSN 时只可编译，不宣称实跑。

## English

This optional PostgreSQL backend implements Durable `Store` / `Session` with `pgx/v5` and `pgxpool`. The target API baseline is **v0.1.0**, requiring **Go 1.26+**, **Pulse v0.5.0**.

This is release preparation, not a claim of publication. Check [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases). The commands above are unavailable until root `v0.1.0` and submodule `postgresstore/v0.1.0` tags are published.

- [Complete PostgreSQL tutorial](../site/en/guide/postgresql.md): database permissions, environment DSN, complete Run/Resume program, persistent output reading, Save/Load and Codec, connections and shutdown.
- [Installation and versions](../site/en/guide/installation.md): three tag names and pre-release development pin alternatives.
- [Contributing](../CONTRIBUTING.md): independent-module and source-workspace validation. Root `./...` excludes this module.

Initialization creates and strictly checks dedicated schema `pulse_durable` v1. Each Session occupies a pool connection until Close. Across Stores, Acquire pool waiting and advisory-lock detection share a default maximum of 30 seconds, with shorter caller deadlines taking precedence. Pool-exhaustion timeout is `context.DeadlineExceeded`, not fabricated `ErrBusy`. Uncertain unlock destroys the connection, with cleanup bounded to five seconds. Advisory locks are not leases/fencing, dispatch, or automatic takeover; external effects still require idempotency. Save commit errors are not automatically retried: stop the original call, wait for ownership release, and Load again. Never log DSNs or passwords; stop business execution before closing Sessions/Store.

**Real database tests use `PULSE_POSTGRES_TEST_DSN`, execute `DROP SCHEMA IF EXISTS pulse_durable CASCADE`, then recreate it. Use only an isolated disposable test database, never a business database or an example database you wish to retain.** Without a DSN, tests skip, not pass real-database acceptance. Network failures, database restarts, backup/restore, and distributed fencing have not received separate acceptance. Without a local DSN, compilation alone is not execution.
