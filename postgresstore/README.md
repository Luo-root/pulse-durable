# postgresstore

## 中文

可选 PostgreSQL 后端，实现 Durable `Store` / `Session`，使用 `pgx/v5` 与 `pgxpool`。目标 API 基线 **v0.1.0**，要求 **Go 1.26+**、**Pulse v0.5.0**。

这是发版准备，不代表已发布。状态以 [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases) 为准；根 `v0.1.0` 与子模块 `postgresstore/v0.1.0` tag 发布前，以下命令尚不可用：

```sh
go get github.com/Luo-root/pulse-durable@v0.1.0
go get github.com/Luo-root/pulse-durable/postgresstore@v0.1.0
```

- [PostgreSQL 完整教程](../site/guide/postgresql.md)：数据库与权限、环境变量 DSN、完整 Run/Resume 程序、读取持久化成果、Initialize/Apply/Load 和 Codec、连接与关闭。
- [安装与版本](../site/guide/installation.md)：三 tag 命名、发布前开发 pin 备选。
- [贡献指南](../CONTRIBUTING.md)：独立模块和源码工作区验证。根 `./...` 不包含本模块。

初始化创建并严格检查专用 `pulse_durable` schema v2。每个 Session 在 Close 前占用一条池连接；跨 Store Acquire 的池等待和 advisory lock 检测默认合计最多 30 秒，调用方更短 deadline 优先。池耗尽超时是 `context.DeadlineExceeded`，不伪报 `ErrBusy`。解锁不确定则销毁连接，清理最多 5 秒。advisory lock 不是 lease/fencing、派发或自动接管；外部副作用仍需幂等。Initialize/Apply 提交错误不自动重试，应停止原调用、等待所有权释放并重新 Load。不要记录 DSN 或密码；先停止业务运行再关闭 Session/Store。

**真实数据库测试使用 `PULSE_POSTGRES_TEST_DSN`，执行 `DROP SCHEMA IF EXISTS pulse_durable CASCADE` 后重建。只能使用隔离、可丢弃的测试库，绝不能使用业务库或要保留的演示库。** 无 DSN 时测试 Skip，不代表实库通过。网络故障、数据库重启、备份恢复和分布式 fencing 未完成单独验收；本机无 DSN 时只可编译，不宣称实跑。

### 增量布局与边界

- 仅 `Initialize` 写 revision 1 的空执行快照；`Apply` 原子插入 canonical typed batch，并用 revision + chain CAS 推进 head。无 Save 兼容路径。Load 用一致事务校验快照摘要、连续批、精确前置条件和链，返回 `durable.CloneRecord` 隔离副本。
- 旧版或不匹配 schema 直接拒绝，不迁移、不改写、不删除数据；请保留旧库供旧版读取，新版使用隔离新库。本片未实现 compaction。
- 每批遵守根包 4 MiB / 4096 操作上限；初始快照最多 4 MiB；未合成链最多 `MaxChainBatches=100000` 批、`MaxChainBytes=256 MiB` canonical wire。达到预算返回 `ErrBudget`，不丢弃或拆分原子批。通过会话的 `interface { Usage() postgresstore.Usage }` 观察批数/字节及上限；这不是 WAL 或磁盘占用测量。
- 正常 Apply 不全量复制、重放历史或编码全快照；失败后的私有暂态不对外暴露，提交未知或写入 panic 后该会话封闭，必须 Close 后重新 Acquire/Load。
- opt-in 测试包含真实 Runtime 10/100/500 条求和、公开 wire State 删除 + 两输出原子提交与失败/reopen、旧库不变、head-chain CAS/损坏、批插入故障及真实进程退出。没有执行实库时只能报告编译和非数据库测试结果。

## English

This optional PostgreSQL backend implements Durable `Store` / `Session` with `pgx/v5` and `pgxpool`. The target API baseline is **v0.1.0**, requiring **Go 1.26+**, **Pulse v0.5.0**.

This is release preparation, not a claim of publication. Check [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases). The commands above are unavailable until root `v0.1.0` and submodule `postgresstore/v0.1.0` tags are published.

- [Complete PostgreSQL tutorial](../site/en/guide/postgresql.md): database permissions, environment DSN, complete Run/Resume program, persistent output reading, Initialize/Apply/Load and Codec, connections and shutdown.
- [Installation and versions](../site/en/guide/installation.md): three tag names and pre-release development pin alternatives.
- [Contributing](../CONTRIBUTING.md): independent-module and source-workspace validation. Root `./...` excludes this module.

Initialization creates and strictly checks dedicated schema `pulse_durable` v2. Each Session occupies a pool connection until Close. Across Stores, Acquire pool waiting and advisory-lock detection share a default maximum of 30 seconds, with shorter caller deadlines taking precedence. Pool-exhaustion timeout is `context.DeadlineExceeded`, not fabricated `ErrBusy`. Uncertain unlock destroys the connection, with cleanup bounded to five seconds. Advisory locks are not leases/fencing, dispatch, or automatic takeover; external effects still require idempotency. Initialize/Apply commit errors are not automatically retried: stop the original call, wait for ownership release, and Load again. Never log DSNs or passwords; stop business execution before closing Sessions/Store.

### Incremental layout and limits

- Only `Initialize` writes the revision-1 empty execution snapshot. `Apply` inserts a canonical typed batch and advances the head using revision + chain CAS in one transaction. There is no Save compatibility path. Load validates snapshot digest, consecutive batches, exact preconditions and chain in a consistent transaction, returning an isolated `durable.CloneRecord` copy.
- Old or incompatible schemas are rejected without migration, modification or deletion. Retain old databases for old clients; use a separate new database. Compaction is not implemented in this slice.
- Root batch limits are 4 MiB / 4096 operations; initial snapshots are limited to 4 MiB. Uncompacted chains are limited to `MaxChainBatches=100000` batches and `MaxChainBytes=256 MiB` canonical wire bytes. `ErrBudget` stops writes without dropping or splitting atomic batches. The session supports `interface { Usage() postgresstore.Usage }` for retained counts/bytes and limits; this does not measure WAL or disk footprint.
- Ordinary Apply does not clone the entire record, replay history or encode a full snapshot. Tentative private state never escapes. Unknown commits and write panics invalidate the session; Close and Acquire/Load again.
- Opt-in tests cover real Runtime 10/100/500-message sums, public-wire State deletion + two-output success/failure/reopen atomicity, unchanged legacy data, head-chain CAS/corruption, batch-insert faults and actual process exit. Compilation and non-database tests do not establish database acceptance.

**Real database tests use `PULSE_POSTGRES_TEST_DSN`, execute `DROP SCHEMA IF EXISTS pulse_durable CASCADE`, then recreate it. Use only an isolated disposable test database, never a business database or an example database you wish to retain.** Without a DSN, tests skip, not pass real-database acceptance. Network failures, database restarts, backup/restore, and distributed fencing have not received separate acceptance. Without a local DSN, compilation alone is not execution.
