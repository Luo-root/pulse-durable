# PostgreSQL 使用教程

本文描述当前开发分支的破坏性 `Initialize`/`Apply` API，需要 **Go 1.26+**、**Pulse v0.5.0**。独立 PostgreSQL 后端仍在开发，本页不是已发布或本轮实库验收通过的声明。旧版 `Session.Save` 和 `RunOptions.RequireDelta` 不再提供；所有官方后端统一新协议，没有可选增量模式或整记录写入回退。`durable.New` 和 Runtime Run/Resume 不需要额外开关。

新 API 尚无本页可用的发布安装 pin，旧版 v0.1.0 不适用于下面示例。请按[贡献指南](https://github.com/Luo-root/pulse-durable/blob/main/CONTRIBUTING.md)验证本地 checkout；跨模块安装 pin 留待统一更新，发布状态见 [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases)。

## 1. 准备数据库与安装模块

使用现有 PostgreSQL 服务或自行准备隔离的示例数据库。驱动为 `pgx/v5`、连接池为 `pgxpool`；模块不创建数据库。数据库管理员须先创建数据库和登录角色，允许该角色连接数据库、在数据库中创建 `pulse_durable` schema（数据库 CREATE 权限），并拥有该专用 schema 或具备初始化建表权限；后续还需要 schema USAGE 和表的 SELECT/INSERT/UPDATE 权限。初始化会建表并严格校验 schema 版本，不提供自动迁移。不要把其他业务对象放入专用 schema。

```sh
mkdir durable-postgres
cd durable-postgres
go mod init example.com/durable-postgres
# 新 API 尚无本页可用的发布 pin；先按贡献指南配置本地源码工作区。
```

通过环境变量提供 DSN，URI 或 pgx key/value 格式均可。以下仅为示例凭据，须按本机配置修改，并根据部署配置 TLS；不要将真实 DSN 提交到仓库或写入日志。PowerShell：

```powershell
$env:PULSE_POSTGRES_DSN = 'postgres://demo_user:demo_password@localhost:5432/durable_demo'
```

POSIX shell：

```sh
export PULSE_POSTGRES_DSN='postgres://demo_user:demo_password@localhost:5432/durable_demo'
```

两个进程必须使用同一数据库与 RunID；完成后恢复原环境变量。`PULSE_POSTGRES_DSN` 是本例应用变量，与后面的破坏性测试变量不同。

## 2. 保存完整 main.go

业务逻辑与 SQLite 教程相同：先确认上游，然后在下游主动退出；`-resume` 只控制演示故障注入，在第二个进程中恢复，不改变业务版本。程序不打印 DSN，且不记录可能携带连接细节的构造错误。

```go
package main

import (
    "context"
    "errors"
    "flag"
    "fmt"
    "log"
    "os"

    "github.com/Luo-root/pulse"
    durable "github.com/Luo-root/pulse-durable"
    "github.com/Luo-root/pulse-durable/postgresstore"
)

func main() {
    resume := flag.Bool("resume", false, "resume the existing run")
    flag.Parse()
    if err := run(*resume); err != nil { log.Fatal(err) }
}

func run(resume bool) (err error) {
    ctx := context.Background()
    dsn := os.Getenv("PULSE_POSTGRES_DSN")
    if dsn == "" { return errors.New("set PULSE_POSTGRES_DSN before running") }
    store, err := postgresstore.New(ctx, dsn)
    if err != nil { return errors.New("PostgreSQL initialization failed; check credentials, connectivity and schema permissions securely") }
    defer func() { err = errors.Join(err, store.Close()) }()
    runtime, err := durable.New(store, durable.NewCodecs())
    if err != nil { return err }
    input := pulse.NewKey[int]("input")
    prepared := pulse.NewKey[int]("prepared")
    output := pulse.NewKey[int]("output")
    graph, err := pulse.New("database-recovery", "Recover a persisted workflow")
    if err != nil { return err }
    if err = graph.DeclareInput(pulse.NewPort(input)); err != nil { return err }
    if err = graph.DeclareOutput(pulse.NewPort(output)); err != nil { return err }
    if err = graph.Add(pulse.NewNode("upstream", "Prepare input",
        pulse.Requires(input), pulse.Provides(prepared),
        func(rc *pulse.RunCtx) error {
            value, err := pulse.Get(rc, input)
            if err != nil { return err }
            fmt.Println("enter upstream:", value)
            return pulse.Set(rc, prepared, value)
        })); err != nil { return err }
    if err = graph.Add(pulse.NewNode("downstream", "Add one",
        pulse.Requires(prepared), pulse.Provides(output),
        func(rc *pulse.RunCtx) error {
            value, err := pulse.Get(rc, prepared)
            if err != nil { return err }
            fmt.Println("enter downstream:", value)
            if !resume {
                fmt.Println("simulate process exit: 75")
                os.Exit(75)
            }
            return pulse.Set(rc, output, value+1)
        })); err != nil { return err }
    var inputs pulse.Inputs
    if err = pulse.Seed(&inputs, input, 41); err != nil { return err }
    options := durable.RunOptions{RunID: "database-demo-001", Version: "increment/1"}
    var execution *pulse.Execution
    if resume {
        execution, err = runtime.Resume(ctx, graph, inputs, options)
    } else {
        execution, err = runtime.Run(ctx, graph, inputs, options)
    }
    if err != nil { return err }
    value, state, err := pulse.ReadOutput(execution, output)
    if err != nil { return err }
    if state != pulse.SlotStateReady { return fmt.Errorf("unexpected output state: %s", state) }
    fmt.Println("result:", value, state)
    return nil
}
```

## 3. Run、退出与 Resume

```sh
go mod tidy
go run .
# 上一个命令故意退出后，单独执行：
go run . -resume
```

第一次预期输出：

```text
enter upstream: 41
enter downstream: 41
simulate process exit: 75
```

第二次预期输出：

```text
enter downstream: 41
result: 42 ready
```

下游在上游 Apply 确认并发布 `prepared` 后才退出。第二个进程跳过已确认上游，从函数开头执行未确认下游，最后通过 `pulse.ReadOutput` 读取新 Execution 的声明出口，而非直接解码数据库记录。`os.Exit(75)` 不执行 defer；PostgreSQL 连接断开后由服务端释放 advisory lock，检测可能不是瞬时的。`go run` 包装命令可能返回 1 并打印 `exit status 75`，不等于业务程序退出码。需要直接查看时先 `go build -o durable-postgres.exe .`，在 PowerShell 执行 `.\durable-postgres.exe`、检查 `$LASTEXITCODE`，再执行 `.\durable-postgres.exe -resume`。

再次 `-resume` 只读取已确认成果；再次 Run 同 RunID 返回 `ErrConflict`。新业务分配新 RunID，不覆盖旧记录。恢复保留相同图、输入、Version 与 Codec 契约；函数或闭包语义变化需主动升级业务 Version。若本机无 DSN，只能编译示例，不能称 PostgreSQL 已实跑或数据库验收通过。

## 4. 连接、锁与关闭

- 当前开发实现创建并严格校验 `pulse_durable` 物理 schema v3：`schema_meta`、`snapshots`、`heads`、`batches`。首快照与批 wire 存为 BYTEA，revision 为十进制 TEXT，保留完整 uint64 范围。旧 v1/v2、未知或不匹配 schema 明确拒绝，不自动删除、修改或迁移；保留旧版及其旧库，新版使用独立新库。
- 同一 Store 已持有或正在申请的 RunID 立即 `ErrBusy`。跨 Store 的 Acquire 先取得池连接，再用 `pg_try_advisory_lock` 检测；确认占用后返回 `ErrBusy`，不等待持有方释放。连接池等待与网络检测总计默认最多 **30 秒**，调用方更短 deadline 优先。池满无法检测远端占用时超时返回 `context.DeadlineExceeded`，不是 `ErrBusy`。
- 每个 Session 持续占用一条连接直到 Close，Load/Initialize/Apply 使用同一连接。不同 RunID 可以并行，但须给连接池预留容量；长业务会长时间占用连接。业务期间不持有提交事务。
- Close 确认 advisory unlock 后才归还连接；解锁失败或结果不确定时销毁连接，清理最多 **5 秒**。Store.Close 取消 pending Acquire、关闭活动 Session 并等待清理，可重复调用。先停止并等待全部 Run/Resume 和业务任务，再关闭直接获取的 Session，最后关闭 Store；Store.Close 不停止业务函数。
- advisory lock 是连接级互斥，**不是 lease、fencing token、调度或自动接管服务**。断线后旧业务函数仍可能执行外部副作用，不能由锁释放推导 exactly-once；业务仍需幂等、去重或 outbox。网络故障、数据库重启与池耗尽由宿主制定策略。
- 使用 PostgreSQL 备份工具（如 `pg_dump`）并将 `pulse_durable` 纳入恢复演练。不要日志记录 DSN 或密码；访问控制、备份、保留、TLS 与凭据由宿主管理。网络故障、数据库重启、备份恢复和分布式 fencing 未由本教程验收。

## 5. Initialize / Apply 确认、Load 与 Codec

Runtime Run 构造合法初始空执行状态，由 Initialize 发布 Revision 1；不能用它导入已有业务历史，重复初始化返回 `ErrConflict`。普通提交仅走 typed Apply：同一短事务中插入批并 CAS 推进 head，核对身份、expected/next revision 严格 +1、精确对象前置条件和链锚点；确认后返回精确 CommitAck。应用继续使用 Run/Resume，不手写业务 records 或批。

首次 Load 在一致快照中验证首快照、摘要与连续批链并重建私有投影，显式 Load 返回隔离完整记录。普通 Apply 不重写完整快照、不重放整个历史。当前未合成链预算为 100000 批和 256 MiB wire，超限返回 `ErrBudget`，不自动丢历史或拆开原子批；Store 已实现 `durable.Maintenance`：`InspectStorage` 读取 generation/base/head，`LoadRevision` 返回精确历史投影及原链锚点、不创建持久 pin，`CompactStorage` 使用 RunID、ThroughRevision 与 ExpectedGeneration 显式合成。目标须在当前 base/head 之间，generation 过期返回 `ErrConflict`。全部维护操作都取得原有 advisory lock，不能与同 RunID 的活跃 Session 并行，检测占用返回 `ErrBusy`。合成快照 digest 独立计算，保留原链 `C(R)`；发布只推进表示 generation/base，不改变业务 revision/head。全部历史、原快照和批保留，无 GC；发布后报错先 Inspect 核对状态。业务语义历史压缩、retention 与 `ReplayCreateDerived` 尚未完成，不承诺无限保留。逻辑 wire bytes 不是 PostgreSQL WAL 或物理磁盘写入量。成本边界见[检查点成本](checkpoint-cost.md)。

Initialize/Apply 报错可能发生在已落库之后，**不自动重试**。停止原调用并等待所有权释放，Acquire 同一 RunID、Load 权威状态，关闭检查 Session 后再决定 Resume；不要盲目重交旧快照或批。旧格式、损坏、权限与连接错误不能当作 `ErrNotFound` 来创建记录。

下面是返回 error 函数中的检查片段，需要已有 `ctx`、`store`，不能与同 RunID 的 Run/Resume 并行：

```go
session, err := store.Acquire(ctx, "database-demo-001")
if err != nil { return err }
record, loadErr := session.Load(ctx)
closeErr := session.Close()
if err := errors.Join(loadErr, closeErr); err != nil { return err }
fmt.Println("run:", record.RunID, "revision:", record.Revision)
```

数据库后端保存首快照和增量批，Load 返回完整投影；它不编码业务类型。`durable.NewCodecs()` 支持本例 int；自定义类型按[值编码](codecs.md)注册，恢复时保留 Codec ID/版本。channel、函数、unsafe.Pointer 不受支持；受管流端点不作为普通值保存。检查点包含业务数据，不是加密容器。

distributed PostgreSQL 是另一套后端，使用专用 `pulse_durable_distributed` schema **v5**，不与上述独立后端共用 schema；旧 v1–v4 或不匹配 schema 拒绝，不自动修改或迁移数据。其 namespace-qualified 维护 API（Coordinator.InspectStorage/CompactStorage/FindMaintenance） 用 Namespace/RunID 定位，要求数据库 `maintain` 权限，不接受 worker ownership/token 代替维护授权。合成先建立 build pin，在发布锁外重建，再校验 generation 并 CAS 发布；并行业务追加的 suffix 必须保留。mutation guard 与发布 receipt 用于核对结果，未知提交通过 `FindMaintenance` 查询，不盲重试；pin 不隐式续期。当前无对外 `LoadRevision`，不能冒称已满足 root `durable.Maintenance` 接口；也无语义压缩、retention、ReplayCreateDerived 或 GC。本页不声明本轮 distributed 实库验收全部完成，suffix budget 修复仍待验证收口。

## 6. 实库测试只能使用可丢弃测试库

根 `go test ./...` 不含独立后端，源码工作区与独立模块验证见[贡献指南](https://github.com/Luo-root/pulse-durable/blob/main/CONTRIBUTING.md)。真实数据库测试另用 **`PULSE_POSTGRES_TEST_DSN`**，会执行 **`DROP SCHEMA IF EXISTS pulse_durable CASCADE`** 再重建。**只能指向隔离、可丢弃的测试数据库，绝不能指向业务库或上述需要保留的演示库。** 不要把测试变量设为应用 DSN 的别名。

在贡献指南的临时工作区中：

```powershell
$env:PULSE_POSTGRES_TEST_DSN = 'postgres://test_user:test_password@localhost:5432/durable_test'
go test -race -count=1 ./postgresstore/...
# 完成后恢复原环境变量。
```

未设测试 DSN 时数据库测试 Skip，不代表真实数据库通过。只编译或无 DSN 的普通测试不能替代 Run/Resume 实跑。继续[存储总览](storage.md)或 [SQLite 教程](sqlite.md)。
