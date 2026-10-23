# postgresstore

`postgresstore` 是 `durable.Store` 的 PostgreSQL 实现，使用 `pgx/v5` 与 `pgxpool`。

## 使用

```go
store, err := postgresstore.New(ctx, os.Getenv("PULSE_POSTGRES_DSN"))
if err != nil { /* handle */ }
defer store.Close()
```

DSN 直接交给 pgx 解析，支持 PostgreSQL URI 或 key/value 格式。不要把包含密码的 DSN 写入日志；本模块也不会把 DSN 放进错误信息。

首次连接会创建并严格检查 `pulse_durable` schema。固定版本为 1，元数据表为 `pulse_durable.schema_meta`，运行记录表为 `pulse_durable.runs`。记录以 `BYTEA` 保存 recordio 原始 JSON，revision 以十进制 `TEXT` 保存，因而覆盖完整 uint64 范围。陌生或错误的 schema 版本会拒绝继续使用。

每个 `Acquire` 从连接池取得一条连接，并在该连接上执行 `pg_try_advisory_lock`。同一 RunID 的竞争立即返回 `durable.ErrBusy`；不同 RunID 可以并行。Session 在 `Close` 前一直占用该连接，Load/Save 也只走该连接。Close 先释放 advisory lock，再归还连接；Store.Close 会关闭活动 Session，且可重复调用。PostgreSQL 连接断开时 advisory lock 会由服务端释放，但旧 worker 不会自动接管，也没有 fencing token、分布式租约或任务派发语义。

Save 使用短事务执行首次唯一插入或 revision CAS 更新。提交错误不自动重试，因为提交后返回错误时结果可能已经落库；调用方应停止本次运行并重新 Load 判断结果。网络故障、数据库重启、连接池耗尽等错误应由调用方按业务策略处理。

备份请使用 PostgreSQL 自身的备份工具（例如 `pg_dump`），并将 `pulse_durable` schema 纳入备份与恢复演练。真实数据库契约测试通过 `PULSE_POSTGRES_TEST_DSN` 显式启用；未设置时 `TestPGContract` 会明确 Skip，普通 `go test` 不连接数据库。测试会在该 DSN 中清理并重建专用的 `pulse_durable` schema，DSN 应指向测试数据库，避免误删业务数据。测试覆盖 schema 初始化、两 Store 同 RunID 竞争、不同 RunID、Save/Load、CAS、完整 uint64、BYTEA、关闭、跨进程竞争、连接池耗尽和池关闭。

使用 Go 1.26 执行：

```text
GOWORK=off go test ./...
PULSE_POSTGRES_TEST_DSN='postgres://.../testdb' GOWORK=off go test ./... -run TestPGContract -count=1
```

跨平台 PowerShell：

```powershell
$env:GOWORK='off'; $env:PULSE_POSTGRES_TEST_DSN='postgres://.../testdb'; go test ./... -run TestPGContract -count=1
```
