# postgresstore

## 中文

本页描述当前未发布 checkout：普通 `Initialize` 生成 HistorySummaries/CommitIndex/Provenance 全 nil 的记录，任一证据组件出现就必须完整闭合。Load、历史读取与快照合成校验摘要、连续 CommitIndex 和 Provenance，并将证据损坏归为 `ErrInvalidRecord`；摘要与来源不是权限凭证。领域记录不携带格式号，业务 `Version` 保留；wire 为 `2`、snapshot envelope 为 `2`，旧字节明确拒绝并保留，不自动迁移、重签或重编码，物理 schema 版本另留在后端边界。

根包 `BuildHistoryCompressionBatch` 已实现权威记录重规划、完整旧对象精确 CAS 删除、同批 AppliedRevision/CommitIndex 与恢复闭包；这不表示本后端首次权威证据物化或完整语义压缩维护入口已交付。`PlanHistoryCompression` 仍只读且 `Executable=false`；首次物化与指定位置派生回放尚未完成，注入快照测试仅验读取边界。

go.mod 已固定可下载根开发提交 `v0.1.1-0.20261108162459-ccc90d4b53c3`，它包含本轮根包契约，是公开提交伪版本而非发布 tag。当前根与独立 PostgreSQL 源码实库及中英文教程三进程验证已通过；完整 postgresstore 开发 pin 为 `v0.1.1-0.20261108164227-4babad07420f`，已可下载并通过无 replace 的独立消费者构建与 race 检查；它不是发布 tag。

可选 PostgreSQL 后端，实现 Durable `Store` / `Session`，使用 `pgx/v5` 与 `pgxpool`。当前 API 以源码 checkout 为准，要求 **Go 1.26+**、**Pulse v0.5.2**。

当前公开开发提交不是正式 tag，安装命令如下；状态以 [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases) 为准：

```sh
go get github.com/Luo-root/pulse-durable@v0.1.1-0.20261108162459-ccc90d4b53c3
go get github.com/Luo-root/pulse-durable/postgresstore@v0.1.1-0.20261108164227-4babad07420f
```

- [PostgreSQL 完整教程](../site/guide/postgresql.md)：数据库与权限、环境变量 DSN、完整 Run/Resume 程序、读取持久化成果、Initialize/Apply/Load 和 Codec、连接与关闭。
- [安装与版本](../site/guide/installation.md)：当前根开发 pin、子模块源码验证与发布边界。
- [贡献指南](../CONTRIBUTING.md)：独立模块和源码工作区验证。根 `./...` 不包含本模块。

初始化创建并严格检查专用 `pulse_durable` schema v3。每个 Session 在 Close 前占用一条池连接；跨 Store Acquire 的池等待和 advisory lock 检测默认合计最多 30 秒，调用方更短 deadline 优先。池耗尽超时是 `context.DeadlineExceeded`，不伪报 `ErrBusy`。解锁不确定则销毁连接，清理最多 5 秒。advisory lock 不是 lease/fencing、派发或自动接管；外部副作用仍需幂等。Initialize/Apply 提交错误不自动重试，应停止原调用、等待所有权释放并重新 Load。不要记录 DSN 或密码；先停止业务运行再关闭 Session/Store。

**真实数据库测试使用 `PULSE_POSTGRES_TEST_DSN`，执行 `DROP SCHEMA IF EXISTS pulse_durable CASCADE` 后重建。只能使用隔离、可丢弃的测试库，绝不能使用业务库或要保留的演示库。** 无 DSN 时测试 Skip，不代表实库通过。网络故障、数据库重启、备份恢复和分布式 fencing 未完成单独验收；本机无 DSN 时只可编译，不宣称实跑。

### 增量布局与边界

- 仅 `Initialize` 写 revision 1 的空执行快照；`Apply` 原子插入 canonical typed batch，并用 revision + chain CAS 推进 head。无 Save 兼容路径。Load 用一致事务校验快照摘要、连续批、精确前置条件和链，返回 `durable.CloneRecord` 隔离副本。
- `Maintenance` 提供 InspectStorage、精确 LoadRevision 和 CompactStorage。合成快照保留原始快照与全部不可变批次，只推进 active base / generation，不改变业务 revision、head chain 或后缀；不执行 GC。维护全程持有现有 session advisory lock，活动 Session 返回 ErrBusy，并非锁外并发构建。读取使用 Repeatable Read；提交错误可能是未知结果，先重新 InspectStorage，不自动重试。
- schema 1/2 或不匹配布局直接拒绝，不迁移、不改写、不删除数据；请保留旧库供旧版读取，新版使用隔离新库。
- 每批遵守根包 4 MiB / 4096 操作上限；初始快照最多 4 MiB；未合成链最多 `MaxChainBatches=100000` 批、`MaxChainBytes=256 MiB` canonical wire。达到预算返回 `ErrBudget`，不丢弃或拆分原子批。通过会话的 `interface { Usage() postgresstore.Usage }` 观察批数/字节及上限；这不是 WAL 或磁盘占用测量。
- 正常 Apply 不全量复制、重放历史或编码全快照；失败后的私有暂态不对外暴露，提交未知或写入 panic 后该会话封闭，必须 Close 后重新 Acquire/Load。
- opt-in 测试包含真实 Runtime 10/100/500 条求和、公开 wire State 删除 + 两输出原子提交与失败/reopen、旧库不变、head-chain CAS/损坏、批插入故障及真实进程退出。没有执行实库时只能报告编译和非数据库测试结果。

## English

This page describes the current unreleased checkout. Ordinary `Initialize` creates a record with nil HistorySummaries/CommitIndex/Provenance; any present evidence component requires complete closure. Load, exact history reads, and snapshot synthesis validate summaries, consecutive CommitIndex entries, and Provenance, reporting corrupt evidence as `ErrInvalidRecord`. Summaries and provenance grant no authority. Domain records have no format number; business `Version` remains. Wire is `2` and snapshot envelope is `2`; old bytes are explicitly rejected and preserved without automatic migration, re-signing, or re-encoding. Physical schema versions remain backend concerns.

Root `BuildHistoryCompressionBatch` provides authoritative-record replanning, exact CAS deletion using complete old objects, same-batch AppliedRevision/CommitIndex, and recovery closure. This does not establish delivery of first authoritative backend evidence materialization or a complete semantic-compression maintenance entry point. `PlanHistoryCompression` remains read-only with `Executable=false`. First materialization and positioned derived replay remain unfinished; injected snapshots test read boundaries only.

go.mod now pins downloadable root development commit `v0.1.1-0.20261108162459-ccc90d4b53c3`, which includes this round's root contract. It is a public commit pseudo-version, not a release tag. Current root and standalone PostgreSQL source passed real-database validation, including three-process runs of both language tutorials. The complete postgresstore development pin is `v0.1.1-0.20261108164227-4babad07420f`, downloadable and validated by independent consumer build/race checks without replace; it is not a release tag.

This optional PostgreSQL backend implements Durable `Store` / `Session` with `pgx/v5` and `pgxpool`. The current API is defined by this source checkout, requiring **Go 1.26+**, **Pulse v0.5.2**.

The public development commits above are installable and are not formal release tags. Check [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases) for tag status.

- [Complete PostgreSQL tutorial](../site/en/guide/postgresql.md): database permissions, environment DSN, complete Run/Resume program, persistent output reading, Initialize/Apply/Load and Codec, connections and shutdown.
- [Installation and versions](../site/en/guide/installation.md): current root development pin, submodule source validation, and release boundaries.
- [Contributing](../CONTRIBUTING.md): independent-module and source-workspace validation. Root `./...` excludes this module.

Initialization creates and strictly checks dedicated schema `pulse_durable` v3. Each Session occupies a pool connection until Close. Across Stores, Acquire pool waiting and advisory-lock detection share a default maximum of 30 seconds, with shorter caller deadlines taking precedence. Pool-exhaustion timeout is `context.DeadlineExceeded`, not fabricated `ErrBusy`. Uncertain unlock destroys the connection, with cleanup bounded to five seconds. Advisory locks are not leases/fencing, dispatch, or automatic takeover; external effects still require idempotency. Initialize/Apply commit errors are not automatically retried: stop the original call, wait for ownership release, and Load again. Never log DSNs or passwords; stop business execution before closing Sessions/Store.

### Incremental layout and limits

- Only `Initialize` writes the revision-1 empty execution snapshot. `Apply` inserts a canonical typed batch and advances the head using revision + chain CAS in one transaction. There is no Save compatibility path. Load validates snapshot digest, consecutive batches, exact preconditions and chain in a consistent transaction, returning an isolated `durable.CloneRecord` copy.
- `Maintenance` provides InspectStorage, exact LoadRevision and CompactStorage. Synthesis retains the original snapshot and every immutable batch, advancing only active base / generation without changing business revision, head chain or suffix. There is no GC. Maintenance holds the existing session advisory lock throughout; active Sessions return ErrBusy, not concurrent outside-lock builds. Reads use Repeatable Read. Commit errors may have unknown outcomes: inspect storage again before retrying.
- Schema 1/2 and incompatible layouts are rejected without migration, modification or deletion. Retain old databases for old clients; use a separate new database.
- Root batch limits are 4 MiB / 4096 operations; initial snapshots are limited to 4 MiB. Uncompacted chains are limited to `MaxChainBatches=100000` batches and `MaxChainBytes=256 MiB` canonical wire bytes. `ErrBudget` stops writes without dropping or splitting atomic batches. The session supports `interface { Usage() postgresstore.Usage }` for retained counts/bytes and limits; this does not measure WAL or disk footprint.
- Ordinary Apply does not clone the entire record, replay history or encode a full snapshot. Tentative private state never escapes. Unknown commits and write panics invalidate the session; Close and Acquire/Load again.
- Opt-in tests cover real Runtime 10/100/500-message sums, public-wire State deletion + two-output success/failure/reopen atomicity, unchanged legacy data, head-chain CAS/corruption, batch-insert faults and actual process exit. Compilation and non-database tests do not establish database acceptance.

**Real database tests use `PULSE_POSTGRES_TEST_DSN`, execute `DROP SCHEMA IF EXISTS pulse_durable CASCADE`, then recreate it. Use only an isolated disposable test database, never a business database or an example database you wish to retain.** Without a DSN, tests skip, not pass real-database acceptance. Network failures, database restarts, backup/restore, and distributed fencing have not received separate acceptance. Without a local DSN, compilation alone is not execution.
