# sqlitestore

## 中文

可选的本机 SQLite 后端，实现 Durable `Store` / `Session` 契约；独立模块使用纯 Go `modernc.org/sqlite v1.60.1`（内嵌 SQLite 3.53.4）。目标 API 基线为 **v0.1.0**，要求 **Go 1.26+**、**Pulse v0.5.0**。

这是发版准备，不代表已发布。状态以 [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases) 为准；根 `v0.1.0` 与子模块 `sqlitestore/v0.1.0` tag 发布前，以下命令尚不可用：

```sh
go get github.com/Luo-root/pulse-durable@v0.1.0
go get github.com/Luo-root/pulse-durable/sqlitestore@v0.1.0
```

- [SQLite 教程](../site/guide/sqlite.md)：当前开发分支的 Initialize/Apply 契约、Runtime 用法及旧物理格式拒绝边界；不代表已发布版本。
- [安装与版本](../site/guide/installation.md)：三 tag 命名、发布前开发 pin 备选。
- [贡献指南](../CONTRIBUTING.md)：独立模块与临时源码工作区验证。根 `./...` 不包含本模块。

仅支持有 OS 文件锁的本机文件系统；不支持 SQLite URI、`:memory:`、网络共享、分布式 lease/fencing、迁移或 exactly-once 外部副作用。使用专用数据库；构造严格校验 schema，设置并读回 WAL、FULL、5000ms busy timeout。保留 `.init.lock`、`.runlocks/` 与运行中的 WAL/SHM 文件，不删锁解锁。schema 3 使用按 revision 的 lossless snapshot、不可变批、head 和 generation manifest；旧 schema 1/2 明确拒绝，不修改或删除旧数据，也不自动迁移。逻辑 RecordFormat 仍为 2。普通提交仅用 Apply，不重写快照；新会话首次 Load 在单一事务中校验并重放链，此后 Apply 只校验 head 和受影响对象。未合成后缀上限为 100000 批 / 256 MiB canonical wire，达到后明确停止；可通过 `durable.Maintenance` 的 `InspectStorage`、`LoadRevision` 和 `CompactStorage` 显式读取历史及合成快照，操作与限制见 [SQLite 教程](../site/guide/sqlite.md)。合成保留原业务链锚与全部历史，不执行 GC；活跃 Session 持有 runlock 时合成返回 `ErrBusy`。Initialize/Apply 报错后须等待原调用结束和所有权释放，重新 Acquire/Load 权威状态，不盲目重试。投影更新后的写入失败或未知提交会封闭原会话。关闭前先停止并等待业务运行。备份不能只复制运行中的主数据库；断电、备份恢复与迁移未完成单独验收。

## English

This optional local SQLite backend implements Durable `Store` / `Session` using pure-Go `modernc.org/sqlite v1.60.1` (embedded SQLite 3.53.4). The target API baseline is **v0.1.0**, requiring **Go 1.26+**, **Pulse v0.5.0**.

This is release preparation, not a claim of publication. Check [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases). The commands above are unavailable until root `v0.1.0` and submodule `sqlitestore/v0.1.0` tags are published.

- [SQLite tutorial](../site/en/guide/sqlite.md): the current development branch's Initialize/Apply contract, Runtime usage, and old-layout rejection boundary; not a claim about a published version.
- [Installation and versions](../site/en/guide/installation.md): three tag names and pre-release development pin alternatives.
- [Contributing](../CONTRIBUTING.md): independent-module and temporary source-workspace validation. Root `./...` excludes this module.

Use local filesystems supporting OS locks only, not SQLite URIs, `:memory:`, network shares, distributed leases/fencing, migration, or exactly-once external effects. Use a dedicated database: construction strictly checks schema and sets/reads back WAL, FULL, and 5000ms busy timeout. Keep `.init.lock`, `.runlocks/`, and live WAL/SHM files; never delete locks to unlock. Schema 3 stores lossless snapshots by revision, immutable batches, a head and a generation manifest. Old schemas 1/2 are rejected without modifying or deleting old data; there is no automatic migration. Logical RecordFormat remains 2. Ordinary Apply never rewrites a snapshot. A new session's first Load validates and replays the chain in one transaction; subsequent Apply checks only the head and affected objects. The uncompacted suffix stops at 100000 batches or 256 MiB of canonical wire. `durable.Maintenance` exposes InspectStorage, LoadRevision and CompactStorage for explicit history reads and snapshot synthesis; see the [SQLite tutorial](../site/en/guide/sqlite.md) for requirements. Synthesis preserves the committed business-chain anchor and all history, with no GC; it returns `ErrBusy` while an active Session holds the runlock. After an Initialize/Apply error, wait for the call and ownership release, then acquire a new session and Load authoritative state without blindly retrying. A write failure after private projection mutation, or an uncertain commit, invalidates the original session. Stop and wait for business execution before closing. Backups cannot copy only the live main database. Power-loss, backup/restore, and migration have not received separate acceptance.
