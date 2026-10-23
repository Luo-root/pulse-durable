# postgresstore

## 中文

`postgresstore` 是 `durable.Store` 的 PostgreSQL 实现，使用 `pgx/v5` 与 `pgxpool`，要求 Go 1.26。**Durable v0.1.0 尚未发布**。当前后端源码修订的公开安装 pin 待推送后核验，不把旧子模块基线当作当前源码的可安装版本。根模块公开开发 pin 为 `v0.0.0-20261023040750-1b397e91c878`（Pulse v0.5.0）。

### 使用与边界

下面是初始化片段，需要导入 `postgresstore`、`os`，并提供 `ctx`：

```go
store, err := postgresstore.New(ctx, os.Getenv("PULSE_POSTGRES_DSN"))
if err != nil { /* handle */ }
defer store.Close()
```

DSN 直接交给 pgx 解析，支持 PostgreSQL URI 或 key/value 格式。不要把包含密码的 DSN 写入日志；本模块也不会把 DSN 放进错误信息。

首次连接会创建并严格检查 `pulse_durable` schema。固定版本为 1，元数据表为 `pulse_durable.schema_meta`，运行记录表为 `pulse_durable.runs`。记录以 `BYTEA` 保存 recordio 原始 JSON，revision 以十进制 `TEXT` 保存，因而覆盖完整 uint64 范围。陌生或错误的 schema 版本会拒绝继续使用。

每个 `Acquire` 从连接池取得一条连接，并在该连接上执行 `pg_try_advisory_lock`。同一 RunID 的竞争立即返回 `durable.ErrBusy`；不同 RunID 可以并行。Session 在 `Close` 前一直占用该连接，Load/Save 也只走该连接。Close 先释放 advisory lock，再归还连接；Store.Close 会关闭活动 Session，且可重复调用。PostgreSQL 连接断开时 advisory lock 会由服务端释放，但旧 worker 不会自动接管，也没有 fencing token、分布式租约或任务派发语义。

Save 使用短事务执行首次唯一插入或 revision CAS 更新。提交错误不自动重试，因为提交后返回错误时结果可能已经落库；调用方应停止本次运行并重新 Load 判断结果。网络故障、数据库重启、连接池耗尽等错误应由调用方按业务策略处理。

备份请使用 PostgreSQL 自身的备份工具（例如 `pg_dump`），并将 `pulse_durable` schema 纳入备份与恢复演练。

### 独立验证

根模块 `go test ./...` 不包含此独立模块。三个模块的独立依赖检查与源码 checkout 验证见[贡献指南](../CONTRIBUTING.md)。从仓库根目录执行下列 PowerShell 命令，仅创建临时工作区，不加入发布依赖 `replace`。已有 `go.work` 时停止并保留，不覆盖；若已按贡献指南创建，直接复用。

```powershell
$previousGowork = $env:GOWORK
$env:GOWORK = 'off'
go work init . ./sqlitestore ./postgresstore
$env:GOWORK = (Join-Path (Get-Location) 'go.work')
go build ./postgresstore/...
go vet ./postgresstore/...
go test -race -count=1 ./postgresstore/...
```

真实数据库测试必须显式设置 `PULSE_POSTGRES_TEST_DSN`。**测试会执行 `DROP SCHEMA IF EXISTS pulse_durable CASCADE`，再重建该 schema；只能使用隔离、可丢弃的测试数据库，绝不能使用业务库。** 未设置时数据库测试明确 Skip，不能把普通测试通过说成真实数据库通过。不要把凭据提交到仓库。

```powershell
$env:PULSE_POSTGRES_TEST_DSN = 'postgres://test_user:test_password@localhost:5432/durable_test'
go test -race -count=1 ./postgresstore/...
# 完成后恢复先前的环境设置；仅移除自己创建的临时 go.work/go.work.sum。
$env:GOWORK = $previousGowork
```

测试覆盖 schema 初始化、两 Store 同 RunID 竞争、不同 RunID、Save/Load、CAS、完整 uint64、BYTEA、关闭、跨进程竞争、连接池耗尽和池关闭。它们不替代网络故障、数据库重启、备份恢复或分布式 fencing 的单独验收。

## English

`postgresstore` implements `durable.Store` with PostgreSQL, `pgx/v5`, and `pgxpool`, requiring Go 1.26. **Durable v0.1.0 has not been released.** Public installation pins for the current backend source revision must be verified after pushing; the older submodule baseline is not an installable version of the current source. The public root development pin is `v0.0.0-20261023040750-1b397e91c878` (Pulse v0.5.0).

### Usage and boundaries

The initialization fragment above requires imports for `postgresstore` and `os`, and a supplied `ctx`. pgx parses the DSN as a PostgreSQL URI or key/value string. Never log passwords; this module does not include the DSN in errors.

Initialization creates and strictly checks schema `pulse_durable`, version 1, with tables `pulse_durable.schema_meta` and `pulse_durable.runs`. Records retain recordio's original JSON as `BYTEA`; decimal `TEXT` revisions preserve the complete uint64 range. Unknown or incorrect schema versions are rejected.

Each `Acquire` obtains a pool connection and uses `pg_try_advisory_lock` on it. Same-RunID contention immediately returns `durable.ErrBusy`; different RunIDs may proceed concurrently. Session retains that connection until Close, and Load/Save use it exclusively. Close unlocks before returning the connection; Store.Close closes active Sessions and is repeatable. PostgreSQL releases advisory locks when a connection disconnects, but an old worker does not automatically take over. There are no fencing tokens, distributed leases, or dispatch semantics.

Save uses a short transaction for an initial unique insert or revision CAS update. Commit errors are not automatically retried: a write may have committed before the error is returned. Stop the current execution and reload to determine authoritative state. The host handles network failures, database restarts, and pool exhaustion according to application policy. Use PostgreSQL backup tools such as `pg_dump`, and include `pulse_durable` in backup/restore drills.

### Independent validation

Root `go test ./...` excludes this module. See [Contributing](../CONTRIBUTING.md) for independent dependency checks and validation of all three source modules. The PowerShell commands above create a temporary source workspace from the checkout root, without release dependency `replace` directives. Preserve an existing `go.work`; reuse it if already created by the contribution procedure.

Database tests explicitly require `PULSE_POSTGRES_TEST_DSN`. **Tests execute `DROP SCHEMA IF EXISTS pulse_durable CASCADE` and recreate it. Use only an isolated disposable test database, never a business database.** Without the DSN, database tests explicitly skip; ordinary test success is not real-database acceptance. Never commit credentials. Restore previous environment settings and remove only temporary workspace files you created after validation.

Tests cover schema initialization, two-Store same-RunID contention, different RunIDs, Save/Load, CAS, full uint64, BYTEA, closing, cross-process contention, pool exhaustion, and pool shutdown. They do not replace separate acceptance for network failures, database restarts, backup/restore, or distributed fencing.
