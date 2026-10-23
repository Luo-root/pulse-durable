# SQLite 使用教程

本文以目标 **Durable v0.1.0** 为 API 基线，需要 **Go 1.26+**、**Pulse v0.5.0**。这是发版准备，不代表已发布；状态以 [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases) 为准，相应 tag 发布前安装命令尚不可用。开发 pin 备选与三 tag 命名见[安装](installation.md)。

## 1. 安装独立模块

在新的本机目录执行：

```sh
mkdir durable-sqlite
cd durable-sqlite
go mod init example.com/durable-sqlite
go get github.com/Luo-root/pulse-durable@v0.1.0
go get github.com/Luo-root/pulse-durable/sqlitestore@v0.1.0
```

SQLite 驱动为纯 Go `modernc.org/sqlite v1.60.1`，内嵌 SQLite 3.53.4，不需要单独安装 SQLite 服务。不要把本例数据库与业务表混用：初始化会严格检查专用 schema，拒绝未知版本、修改的 DDL 或额外 schema 对象。

## 2. 保存完整 main.go

程序先确认上游，然后在下游主动退出；`-resume` 在新进程中重建同一业务并恢复。两个数据库教程共享业务逻辑，仅后端初始化不同。`-resume` 只是故障注入开关，不是业务版本变化。

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
    "github.com/Luo-root/pulse-durable/sqlitestore"
)

func main() {
    resume := flag.Bool("resume", false, "resume the existing run")
    flag.Parse()
    if err := run(*resume); err != nil { log.Fatal(err) }
}

func run(resume bool) (err error) {
    ctx := context.Background()
    store, err := sqlitestore.NewContext(ctx, "./state/runs.sqlite")
    if err != nil { return err }
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

## 3. 启动两个进程

两个进程必须从同一工作目录运行，使相对路径指向同一数据库：

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

第一次故意失败。`os.Exit(75)` 不执行 defer；`go run` 通常打印 `exit status 75`，但包装工具返回 shell 的状态可能为 1。要直接观察 75，先 `go build -o durable-sqlite.exe .`，再在 PowerShell 中执行 `.\durable-sqlite.exe` 并检查 `$LASTEXITCODE`，随后执行 `.\durable-sqlite.exe -resume`。

第二次预期输出：

```text
enter downstream: 41
result: 42 ready
```

下游只能在上游 Save 确认、输出发布后读到 `prepared`，因此退出时上游已经持久保存。恢复跳过上游，重新进入未确认的下游；`pulse.ReadOutput` 从新的 Execution 读取已声明出口，不是直接解码数据库 BLOB。再次 `-resume` 只输出结果；再次无 flag 调用 Run 会因同 RunID 已存在返回 `ErrConflict`。新业务应分配新 RunID，不删除旧记录绕过冲突。恢复须保留相同图、输入、Version 和 Codec 契约。

## 4. 路径、WAL、锁与备份

- `New(path)` 等价于 `NewContext(context.Background(), path)`；后者支持取消初始化。只接受本机文件路径，不接受 SQLite URI 或 `:memory:`。会创建父目录，解析绝对路径和符号链接；新目录/文件为 0700/0600，Windows 使用 ACL。
- 初始化设置并读回验证 **WAL、synchronous=FULL (2)、busy_timeout=5000ms**。短 `BEGIN IMMEDIATE` 事务做插入或 revision CAS，业务执行期间不持有数据库事务；单 Store 串行数据库操作，繁忙最多等待 5 秒，不自动重试。
- `<path>.init.lock` 串行化初始化，最多等 5 秒或 context 取消；`<path>.runlocks/` 按 RunID 的 SHA-256 保存 OS 锁。同 RunID 跨本机进程竞争立即 `ErrBusy`；不同 RunID 可以持有各自 Session。不要删除锁文件“解锁”，不要用硬链接、路径别名或复制锁目录绕开锁域。
- 仅支持有 OS 文件锁的本机文件系统，不支持网络共享、跨机器调度、分布式 lease/fencing 或 exactly-once 外部副作用。进程退出可恢复不等于断电保证；FULL 仍依赖 OS、设备和文件系统。
- 停止并等待全部 Run/Resume 与业务任务，再关闭直接获取的 Session，最后关闭 Store。Store.Close 不负责停止业务函数。本例正常返回时 defer 关闭 Store；故意退出时由 OS 释放锁。
- WAL 模式下运行中不能仅复制主数据库文件作为备份。协调所有使用方停机关闭后备份，或使用单独验证的 SQLite 一致性备份流程。运行期间保留数据库、WAL/SHM、`.init.lock` 与 `.runlocks/`，不要删除或替换。权限、备份和保留由宿主管理；断电、备份恢复、迁移未完成单独验收。

## 5. Save 确认、Load 与 Codec

后端保存完整 RunRecord 的 JSON BLOB，revision 为十进制 TEXT，保留完整 uint64 范围；它不负责业务值编码。`durable.NewCodecs()` 处理本例的 int；结构体和自定义类型按[值编码](codecs.md)注册，恢复时保留 Codec ID/版本。channel、函数、unsafe.Pointer 不受支持；受管流端点不是普通持久值。检查点包含业务数据，不是加密容器。

Save 首次 expected=0、Revision=1，后续 Revision=expected+1；冲突返回 `ErrConflict`。**Save 返回错误不证明未提交**：停止原调用，等待运行和所有权释放，再 Acquire 同一 RunID、Load 权威记录，关闭检查 Session 后决定 Resume。不要用旧快照自动重试 Save，也不要把权限或损坏误判为 `ErrNotFound` 后创建新记录。下面是放在返回 error 的函数中的检查片段，需要已有 `ctx`、`store`；不要与同 RunID 的 Run/Resume 并行：

```go
session, err := store.Acquire(ctx, "database-demo-001")
if err != nil { return err }
record, loadErr := session.Load(ctx)
closeErr := session.Close()
if err := errors.Join(loadErr, closeErr); err != nil { return err }
fmt.Println("run:", record.RunID, "revision:", record.Revision)
```

源码验证见[贡献指南](https://github.com/Luo-root/pulse-durable/blob/main/CONTRIBUTING.md)：根 `./...` 不含独立后端模块，发布前 checkout 验证须使用其临时工作区步骤，而不是声称目标 tag 已可安装。继续[存储总览](storage.md)或 [PostgreSQL 教程](postgresql.md)。
