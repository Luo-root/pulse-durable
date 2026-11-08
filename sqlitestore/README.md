# sqlitestore

## 中文

本页描述当前未发布 checkout：普通 `Initialize` 生成 HistorySummaries/CommitIndex/Provenance 全 nil 的记录，任一证据组件出现就必须完整闭合。读取与历史快照校验验证摘要、连续 CommitIndex 和 Provenance，并拒绝证据损坏；摘要与来源不是权限凭证。wire 为 `2`、snapshot envelope 为 `2`，旧字节明确拒绝并保留，不自动迁移、重签或重编码；物理 schema 版本独立留在后端边界。

根包 `BuildHistoryCompressionBatch` 已实现重规划、完整旧对象精确 CAS 删除、同批 AppliedRevision/CommitIndex 和恢复闭包；这不表示本后端首次权威证据物化或完整语义压缩维护入口已交付。`PlanHistoryCompression` 仍只读且 `Executable=false`；首次物化与指定位置派生回放尚未完成，读取测试的显式注入夹具不等于压缩执行验收。

go.mod 已固定可下载根开发提交 `v0.1.1-0.20261108162459-ccc90d4b53c3`，它包含本轮根包契约，是公开提交伪版本而非发布 tag。当前根与 SQLite 源码实库及中英文教程三进程验证已通过；完整 sqlitestore 开发 pin 为 `v0.1.1-0.20261108164227-4babad07420f`，已可下载并通过无 replace 的独立消费者构建与 race 检查；它不是发布 tag。

可选的本机 SQLite 后端，实现 Durable `Store` / `Session` 契约；独立模块使用纯 Go `modernc.org/sqlite v1.60.1`（内嵌 SQLite 3.53.4）。当前 API 以源码 checkout 为准，要求 **Go 1.26+**、**Pulse v0.5.2**。

当前公开开发提交不是正式 tag，安装命令如下；状态以 [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases) 为准：

```sh
go get github.com/Luo-root/pulse-durable@v0.1.1-0.20261108162459-ccc90d4b53c3
go get github.com/Luo-root/pulse-durable/sqlitestore@v0.1.1-0.20261108164227-4babad07420f
```

- [SQLite 教程](../site/guide/sqlite.md)：当前开发分支的 Initialize/Apply 契约、Runtime 用法及旧物理格式拒绝边界；不代表已发布版本。
- [安装与版本](../site/guide/installation.md)：当前根开发 pin、子模块源码验证与发布边界。
- [贡献指南](../CONTRIBUTING.md)：独立模块与临时源码工作区验证。根 `./...` 不包含本模块。

仅支持有 OS 文件锁的本机文件系统；不支持 SQLite URI、`:memory:`、网络共享、分布式 lease/fencing、迁移或 exactly-once 外部副作用。使用专用数据库；构造严格校验 schema，设置并读回 WAL、FULL、5000ms busy timeout。保留 `.init.lock`、`.runlocks/` 与运行中的 WAL/SHM 文件，不删锁解锁。schema 3 使用按 revision 的 lossless snapshot、不可变批、head 和 generation manifest；旧 schema 1/2 明确拒绝，不修改或删除旧数据，也不自动迁移。领域记录不携带格式号，`Version` 仍为业务身份。普通提交仅用 Apply，不重写快照；新会话首次 Load 在单一事务中校验并重放链，此后 Apply 只校验 head 和受影响对象。未合成后缀上限为 100000 批 / 256 MiB canonical wire，达到后明确停止；可通过 `durable.Maintenance` 的 `InspectStorage`、`LoadRevision` 和 `CompactStorage` 显式读取历史及合成快照，操作与限制见 [SQLite 教程](../site/guide/sqlite.md)。合成保留原业务链锚与全部历史，不执行 GC；活跃 Session 持有 runlock 时合成返回 `ErrBusy`。Initialize/Apply 报错后须等待原调用结束和所有权释放，重新 Acquire/Load 权威状态，不盲目重试。投影更新后的写入失败或未知提交会封闭原会话。关闭前先停止并等待业务运行。备份不能只复制运行中的主数据库；断电、备份恢复与迁移未完成单独验收。

## English

This page describes the current unreleased checkout. Ordinary `Initialize` creates a record with nil HistorySummaries/CommitIndex/Provenance; any present evidence component requires complete closure. Loads and historical snapshot validation check summaries, consecutive CommitIndex entries, and Provenance, rejecting corrupt evidence. Summaries and provenance grant no authority. Wire is `2` and snapshot envelope is `2`; old bytes are explicitly rejected and preserved without automatic migration, re-signing, or re-encoding. Physical schema versions remain separate backend boundaries.

Root `BuildHistoryCompressionBatch` provides replanning, exact CAS deletion using complete old objects, same-batch AppliedRevision/CommitIndex, and recovery closure. This does not establish delivery of first authoritative backend evidence materialization or a complete semantic-compression maintenance entry point. `PlanHistoryCompression` remains read-only with `Executable=false`. First materialization and positioned derived replay remain unfinished; explicitly injected read fixtures do not establish compression execution acceptance.

go.mod now pins downloadable root development commit `v0.1.1-0.20261108162459-ccc90d4b53c3`, which includes this round's root contract. It is a public commit pseudo-version, not a release tag. Current root and SQLite source passed real-database validation, including three-process runs of both language tutorials. The complete sqlitestore development pin is `v0.1.1-0.20261108164227-4babad07420f`, downloadable and validated by independent consumer build/race checks without replace; it is not a release tag.

This optional local SQLite backend implements Durable `Store` / `Session` using pure-Go `modernc.org/sqlite v1.60.1` (embedded SQLite 3.53.4). The current API is defined by this source checkout, requiring **Go 1.26+**, **Pulse v0.5.2**.

The public development commits above are installable and are not formal release tags. Check [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases) for tag status.

- [SQLite tutorial](../site/en/guide/sqlite.md): the current development branch's Initialize/Apply contract, Runtime usage, and old-layout rejection boundary; not a claim about a published version.
- [Installation and versions](../site/en/guide/installation.md): current root development pin, submodule source validation, and release boundaries.
- [Contributing](../CONTRIBUTING.md): independent-module and temporary source-workspace validation. Root `./...` excludes this module.

Use local filesystems supporting OS locks only, not SQLite URIs, `:memory:`, network shares, distributed leases/fencing, migration, or exactly-once external effects. Use a dedicated database: construction strictly checks schema and sets/reads back WAL, FULL, and 5000ms busy timeout. Keep `.init.lock`, `.runlocks/`, and live WAL/SHM files; never delete locks to unlock. Schema 3 stores lossless snapshots by revision, immutable batches, a head and a generation manifest. Old schemas 1/2 are rejected without modifying or deleting old data; there is no automatic migration. Domain records carry no format number; `Version` remains business identity. Ordinary Apply never rewrites a snapshot. A new session's first Load validates and replays the chain in one transaction; subsequent Apply checks only the head and affected objects. The uncompacted suffix stops at 100000 batches or 256 MiB of canonical wire. `durable.Maintenance` exposes InspectStorage, LoadRevision and CompactStorage for explicit history reads and snapshot synthesis; see the [SQLite tutorial](../site/en/guide/sqlite.md) for requirements. Synthesis preserves the committed business-chain anchor and all history, with no GC; it returns `ErrBusy` while an active Session holds the runlock. After an Initialize/Apply error, wait for the call and ownership release, then acquire a new session and Load authoritative state without blindly retrying. A write failure after private projection mutation, or an uncertain commit, invalidates the original session. Stop and wait for business execution before closing. Backups cannot copy only the live main database. Power-loss, backup/restore, and migration have not received separate acceptance.
