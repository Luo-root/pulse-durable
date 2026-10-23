# sqlitestore

## 中文

可选的本机 SQLite 后端，实现 Durable `Store` / `Session` 契约；独立模块使用纯 Go `modernc.org/sqlite v1.60.1`（内嵌 SQLite 3.53.4）。目标 API 基线为 **v0.1.0**，要求 **Go 1.26+**、**Pulse v0.5.0**。

这是发版准备，不代表已发布。状态以 [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases) 为准；根 `v0.1.0` 与子模块 `sqlitestore/v0.1.0` tag 发布前，以下命令尚不可用：

```sh
go get github.com/Luo-root/pulse-durable@v0.1.0
go get github.com/Luo-root/pulse-durable/sqlitestore@v0.1.0
```

- [SQLite 完整教程](../site/guide/sqlite.md)：初始化、完整 Run/Resume 程序、持久化后读取输出、Save/Load 和 Codec、路径、WAL、锁与备份。
- [安装与版本](../site/guide/installation.md)：三 tag 命名、发布前开发 pin 备选。
- [贡献指南](../CONTRIBUTING.md)：独立模块与临时源码工作区验证。根 `./...` 不包含本模块。

仅支持有 OS 文件锁的本机文件系统；不支持 SQLite URI、`:memory:`、网络共享、分布式 lease/fencing、迁移或 exactly-once 外部副作用。使用专用数据库；构造严格校验 schema，设置并读回 WAL、FULL、5000ms busy timeout。保留 `.init.lock`、`.runlocks/` 与运行中的 WAL/SHM 文件，不删锁解锁。Save 报错后须等待原调用结束和所有权释放，重新 Load 权威状态，不盲目重试旧快照。关闭前先停止并等待业务运行。备份不能只复制运行中的主数据库；断电、备份恢复与迁移未完成单独验收。

## English

This optional local SQLite backend implements Durable `Store` / `Session` using pure-Go `modernc.org/sqlite v1.60.1` (embedded SQLite 3.53.4). The target API baseline is **v0.1.0**, requiring **Go 1.26+**, **Pulse v0.5.0**.

This is release preparation, not a claim of publication. Check [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases). The commands above are unavailable until root `v0.1.0` and submodule `sqlitestore/v0.1.0` tags are published.

- [Complete SQLite tutorial](../site/en/guide/sqlite.md): initialization, complete Run/Resume program, persistent output reading, Save/Load and Codec, paths, WAL, locks, and backups.
- [Installation and versions](../site/en/guide/installation.md): three tag names and pre-release development pin alternatives.
- [Contributing](../CONTRIBUTING.md): independent-module and temporary source-workspace validation. Root `./...` excludes this module.

Use local filesystems supporting OS locks only, not SQLite URIs, `:memory:`, network shares, distributed leases/fencing, migration, or exactly-once external effects. Use a dedicated database: construction strictly checks schema and sets/reads back WAL, FULL, and 5000ms busy timeout. Keep `.init.lock`, `.runlocks/`, and live WAL/SHM files; never delete locks to unlock. After a Save error, wait for the original call and ownership release, then Load authoritative state without blindly retrying old snapshots. Stop and wait for business execution before closing. Backups cannot copy only the live main database. Power-loss, backup/restore, and migration have not received separate acceptance.
