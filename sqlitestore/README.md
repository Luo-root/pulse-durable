# sqlitestore

## 中文

可选的本机 SQLite 文件存储，沿用 Pulse Durable 的 `Store` / `Session` 与完整 `RunRecord` 契约。独立模块要求 **Go 1.26**，驱动为纯 Go `modernc.org/sqlite v1.60.1`，内嵌 SQLite **3.53.4**。Durable 根模块统一要求 Go 1.26，依赖 Pulse v0.5.0；使用 MemoryStore 或 filestore 时不必引入 SQLite。

**Durable v0.1.0 尚未发布。** 当前后端源码修订的公开安装 pin 待推送后核验，不把旧子模块基线当作当前源码的可安装版本。根模块公开开发 pin 为 `v0.0.0-20261023040750-1b397e91c878`；源码验证使用临时 `go.work`，不向发布依赖加入本地 `replace`。三个模块的验证步骤见[贡献指南](../CONTRIBUTING.md)。

### API 与存储边界

- `New(path)` 等价于 `NewContext(context.Background(), path)`；`NewContext(ctx, path)` 支持取消数据库初始化，但文件系统调用仍受操作系统阻塞行为约束。只接受文件路径，不接受 SQLite URI 或 `:memory:`。创建父目录、解析绝对路径和符号链接；新目录 0700、新数据库文件 0600，Windows 使用 ACL。
- 构造时设置并读回验证 **WAL、synchronous=FULL (2)、busy_timeout=5000ms**。数据库必须专用：空数据库初始化为 `user_version=1`；已有 v1 必须严格匹配 DDL（忽略空白），拒绝未知版本、修改后的表结构或额外 schema 对象。不做 schema/filestore 迁移。
- 数据库旁的 `<path>.init.lock` 串行化构造与日志模式初始化，等待最多 5 秒或 context 取消；它与 `<path>.runlocks/` 中的锁文件都必须保留，不能靠删除锁文件解除竞争。运行锁按 RunID 的 SHA-256 定位；同一 RunID 跨进程竞争立即返回 `durable.ErrBusy`，不同 RunID 可同时持有会话。
- 完整记录以 JSON BLOB 保存，修订号以十进制 TEXT 精确保留 uint64 范围。元数据必须为有效 UTF-8，`Data []byte` 通过 JSON base64 无损保存；损坏 JSON、Unicode、身份或修订不一致会被拒绝。
- 首写 expected=0、Revision=1；更新 Revision=expected+1。短 `BEGIN IMMEDIATE` 内校验并插入或 SQL CAS 更新。MaxUint64 可读写为最后修订，不能再推进。业务执行不持有数据库事务；单 Store 串行数据库操作，数据库繁忙最多等待 5000ms，不做应用层自动重试。
- 不存在返回 `ErrNotFound`，CAS/写入身份冲突返回 `ErrConflict`。`Session.Close` 和 `Store.Close` 幂等；Store 关闭活动会话并关闭数据库，关闭后的访问返回 `os.ErrClosed`。先停止并等待所有 Run/Resume 与业务任务，再关闭直接获取的 Session，最后关闭 Store；Store.Close 不负责停止业务任务。
- 只适用于支持 OS 文件锁的**本机文件系统**；不要用硬链接、数据库路径别名、复制锁目录或网络共享绕过锁域。不支持分布式 lease/fencing、跨机器调度、跨后端迁移、增量记录或 exactly-once 外部副作用。
- Save 返回错误不能推断提交与否：等待原调用结束、所有权释放后通过独占 Session 重新 Load，再决定 Resume；不要用旧快照盲目重试。检查点不是加密容器。
- 宿主管理访问权限、备份和保留。WAL 模式下不能在运行中仅复制主数据库文件；使用协调停机（所有使用方关闭后）或经验证的 SQLite 一致性备份流程。保留运行中数据库、WAL/SHM 辅助文件、`.init.lock` 与 `.runlocks/`，不要在运行期间删除或替换。FULL 仍依赖 OS、设备和文件系统；**断电实测、备份恢复和迁移未完成验收**，不能将配置等同于保证。

### 完整本地运行示例 / Complete local example

在包含当前改动的仓库根目录执行下列 PowerShell 命令（Go 1.26）。仅创建未跟踪的临时工作区，不修改模块发布依赖；已有 `go.work` 时停止并保留，不覆盖。

From the current checkout root, run these PowerShell commands with Go 1.26. Use an untracked temporary workspace, not release dependency replacements. If `go.work` already exists, stop and preserve it rather than overwriting it.

```powershell
$env:GOWORK = 'off'
go work init . ./sqlitestore ./postgresstore
$env:GOWORK = (Join-Path (Get-Location) 'go.work')
mkdir sqlite-example
cd sqlite-example
go mod init example.com/sqlite-example
go mod edit -go=1.26.0
go work use .
```

保存以下完整程序为 `main.go`，执行 `go run .`，预期输出 `result: 42 ready`。首次使用示例数据库执行；再次执行会因 RunID 已存在而失败，应对已有记录使用 Resume，而非覆盖。

Save this complete program as `main.go`, then run `go run .`. Expect `result: 42 ready` with a fresh example database. Repeating Run with the same RunID fails; use Resume for an existing record rather than overwriting it.

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "log"

    "github.com/Luo-root/pulse"
    durable "github.com/Luo-root/pulse-durable"
    "github.com/Luo-root/pulse-durable/sqlitestore"
)

func main() {
    if err := run(); err != nil {
        log.Fatal(err)
    }
}

func run() (err error) {
    ctx := context.Background()
    store, err := sqlitestore.NewContext(ctx, "./state/runs.sqlite")
    if err != nil { return err }
    defer func() { err = errors.Join(err, store.Close()) }()
    runtime, err := durable.New(store, durable.NewCodecs())
    if err != nil { return err }
    input := pulse.NewKey[int]("input")
    output := pulse.NewKey[int]("output")
    graph, err := pulse.New("sqlite-example", "Add one")
    if err != nil { return err }
    if err = graph.DeclareInput(pulse.NewPort(input)); err != nil { return err }
    if err = graph.DeclareOutput(pulse.NewPort(output)); err != nil { return err }
    if err = graph.Add(pulse.NewNode("add", "Add one",
        pulse.Requires(input), pulse.Provides(output),
        func(rc *pulse.RunCtx) error {
            value, err := pulse.Get(rc, input)
            if err != nil { return err }
            return pulse.Set(rc, output, value+1)
        })); err != nil { return err }
    var inputs pulse.Inputs
    if err = pulse.Seed(&inputs, input, 41); err != nil { return err }
    execution, err := runtime.Run(ctx, graph, inputs,
        durable.RunOptions{RunID: "sqlite-example-001", Version: "increment/1"})
    if err != nil { return err }
    value, state, err := pulse.ReadOutput(execution, output)
    if err != nil { return err }
    if state != pulse.SlotStateReady { return fmt.Errorf("unexpected output state: %s", state) }
    fmt.Println("result:", value, state)
    return nil
}
```

### 独立验证 / Independent validation

根模块 `go test ./...` 不包含此独立模块。以下命令从仓库根目录开始，要求 Go 1.26；不修改发布状态。

Root `go test ./...` does not include this independent module. From the repository root, use Go 1.26. Create the temporary workspace only once; if the example above already created it, reuse it. After validation, restore the previous `GOWORK` setting and remove only the temporary workspace files you created. / 临时工作区只创建一次；若上面的示例已创建，直接复用。验证后恢复原 `GOWORK`，仅移除自己创建的临时工作区文件：

```powershell
$env:GOWORK = 'off'
go work init . ./sqlitestore ./postgresstore
$env:GOWORK = (Join-Path (Get-Location) 'go.work')
go build ./sqlitestore/...
go vet ./sqlitestore/...
go test -race -count=1 ./sqlitestore/...
```

测试覆盖运行锁的真实子进程竞争、两 Store 竞争、不同 RunID 并发、CAS、完整 uint64、损坏记录、context、关闭和数据隔离；最终实现还覆盖并发初始化与取消。它们不代替断电、网络文件系统、备份/恢复、迁移或分布式场景的单独验收。

## English

This optional local SQLite backend implements the existing Durable `Store` / `Session` and complete `RunRecord` contracts. Its independent module requires **Go 1.26**, uses pure-Go `modernc.org/sqlite v1.60.1`, and embeds **SQLite 3.53.4**. The Durable root module requires Go 1.26 and Pulse v0.5.0; MemoryStore and filestore do not pull in SQLite.

**Durable v0.1.0 has not been released.** Public installation pins for the current backend source revision must be verified after pushing; the older submodule baseline is not an installable version of the current source. The public root development pin is `v0.0.0-20261023040750-1b397e91c878`. Validate source with a temporary `go.work`, without adding local `replace` directives to release dependencies. See [Contributing](../CONTRIBUTING.md) for all three modules.

### API and operational boundaries

- `New(path)` calls `NewContext(context.Background(), path)`. `NewContext(ctx, path)` supports cancellable database initialization; filesystem calls remain subject to OS blocking behavior. Use a filename, not a SQLite URI or `:memory:`. Parents are created, absolute paths and symlinks resolved; new directories/files use 0700/0600 where applicable (Windows uses ACLs).
- Initialization sets and reads back **WAL, synchronous=FULL (2), and busy_timeout=5000ms**. Use a dedicated database: an empty schema initializes to `user_version=1`; existing v1 must match the exact DDL ignoring whitespace. Unknown versions, altered schemas, or extra schema objects are rejected. No schema or filestore migration is provided.
- The neighboring `<path>.init.lock` serializes construction and journal-mode initialization, waiting at most five seconds or context cancellation. Keep it and the `<path>.runlocks/` files: deleting lock files is not a contention remedy. Run locks use SHA-256 of RunID. Contending sessions for the same RunID immediately receive `durable.ErrBusy`, including across local processes; different RunIDs may hold sessions concurrently.
- Complete records are JSON BLOBs; decimal TEXT revisions preserve all uint64 values. Metadata must be valid UTF-8; JSON base64 preserves arbitrary `Data []byte`. Corrupt JSON, invalid Unicode, mismatched identity or revisions are rejected.
- Initial Save requires expected=0 and Revision=1; updates require Revision=expected+1. Short `BEGIN IMMEDIATE` transactions validate records and insert or SQL-CAS update them. MaxUint64 can be the final revision, but cannot advance. No transaction spans business execution. A Store serializes its database operations; SQLite contention waits at most 5000ms with no application-level automatic retry.
- Missing records return `ErrNotFound`; CAS/identity conflicts return `ErrConflict`. Session/Store Close is idempotent; Store.Close closes active sessions and SQLite, and later access returns `os.ErrClosed`. First stop and wait for all Run/Resume calls and business tasks, close directly acquired Sessions, then close Store. Store.Close does not stop business tasks.
- Use only a **local filesystem with OS file locking**. Hard links, pathname aliases, copied lock directories, and network shares must not bypass the ownership domain. No distributed leases/fencing, cross-machine dispatch, cross-backend migration, incremental records, or exactly-once external effects are provided.
- A Save error does not prove whether it committed. Wait for the original call and ownership release, acquire a Session and Load authoritative state before deciding to Resume. Never blindly retry an old snapshot. Checkpoints are not encrypted containers.
- The host owns permissions, backups and retention. With WAL, copying only the live main database file is not a consistent backup. Use coordinated shutdown of all users or a separately verified SQLite-consistent backup procedure. Do not delete or replace live database/WAL/SHM files, `.init.lock`, or `.runlocks/`. FULL still depends on the OS, device and filesystem: **power-loss, backup/restore and migration acceptance remain unverified**.

The complete local setup, executable example and independent validation commands above apply to both languages. Tests cover real subprocess lock contention, two Stores, different RunIDs, CAS, full uint64 revisions, corruption, context, closing and isolation; the final implementation also covers concurrent initialization and cancellation. These checks do not establish power-loss, network-filesystem, backup/restore, migration or distributed-scenario guarantees.
