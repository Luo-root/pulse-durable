[English](README_en.md) | [中文](README.md)

# Distributed

为 Pulse Durable 提供整工作流任务提交、PostgreSQL 协调与有容量上限的 worker。生产者持久提交输入，worker 按精确版本领取任务，在本机执行整张图；租约失效后，兼容 worker 可从已确认检查点接管。

| 本机执行 | 分布式执行 |
| --- | --- |
| 宿主调用 Durable `Run` / `Resume`，提供图和输入 | 生产者 `NewTask` + `Submit`，worker `Registry` + `Run` |
| 宿主决定何时恢复 | worker 领取 queued 或租约到期的 running 任务 |
| 使用 Durable Store | 使用独立协调 schema 和带 fencing 的检查点会话 |

这里分配的是不同 RunID，不会把同一张图的节点拆到不同机器。`Submit` 成功表示持久接收，不表示业务完成。

## 源码接入

需要 **Go 1.26+、PostgreSQL 16+**。`distributed` 是独立模块，当前尚无发布 tag。以下直接使用源码工作区，不依赖不存在的 `distributed/v0.2.0`：

```sh
git clone https://github.com/Luo-root/pulse-durable.git
mkdir distributed-demo
cd distributed-demo
go mod init example.com/distributed-demo
go mod edit -go=1.26.0
go mod edit -require=github.com/Luo-root/pulse@v0.5.0
go mod edit -require=github.com/Luo-root/pulse-durable@v0.1.1-0.20261024124930-5296b17b7011
go mod edit -require=github.com/Luo-root/pulse-durable/distributed@v0.0.0
go mod edit -replace=github.com/Luo-root/pulse-durable/distributed=../pulse-durable/distributed
```

`v0.0.0` 只是本地 replace 的占位版本，不是下载地址。上述 Pulse 与 Durable 版本与当前 [go.mod](go.mod) 一致；源码检出必须包含 `distributed/`。若父目录已有 `go.work`，请将示例和 `distributed/` 纳入工作区，或为这些命令设置 `GOWORK=off`。

## PostgreSQL 与最小程序

先由数据库管理员创建数据库、独立的 schema owner 和登录角色 `pulse_app`，并授予二者数据库 CONNECT。owner 需要在目标库创建 schema；`pulse_app` 必须为非 owner、非超级用户，不得有 CREATEDB、CREATEROLE、BYPASSRLS、owner 角色成员关系或协调表直接访问权限。密码通过数据库的安全配置流程设置，不写入源码。

仅从环境注入连接串：

- `PULSE_ADMIN_DSN`：迁移专用 owner 凭据，仅迁移进程持有。
- `PULSE_WORKER_DSN`：`pulse_app` 凭据；示例的提交、查询、取消和 worker 共用它。

保存为 `main.go`。示例注册一个结构体 Codec，将文档从输入复制到出口：

```go
package main

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "fmt"
    "log"
    "os"
    "os/signal"
    "time"

    "github.com/Luo-root/pulse"
    durable "github.com/Luo-root/pulse-durable"
    d "github.com/Luo-root/pulse-durable/distributed"
    "github.com/Luo-root/pulse-durable/distributed/postgres"
    "github.com/Luo-root/pulse-durable/distributed/worker"
)

type Document struct { Text string `json:"text"` }
var id = d.Identity{Namespace: "documents", RunID: "document-001"}

func assembly() (d.Assembly, error) {
    g, err := pulse.New("document", "Copy a document")
    if err != nil { return d.Assembly{}, err }
    input := pulse.NewKey[Document]("input")
    output := pulse.NewKey[Document]("output")
    if err = g.DeclareInput(pulse.NewPort(input)); err != nil { return d.Assembly{}, err }
    if err = g.DeclareOutput(pulse.NewPort(output)); err != nil { return d.Assembly{}, err }
    if err = g.Add(pulse.NewNode("copy", "Copy", pulse.Requires(input), pulse.Provides(output),
        func(rc *pulse.RunCtx) error {
            v, err := pulse.Get(rc, input)
            if err != nil { return err }
            return pulse.Set(rc, output, v)
        })); err != nil { return d.Assembly{}, err }
    keys := pulse.NewRegistry()
    if err = pulse.RegisterKey(keys, input); err != nil { return d.Assembly{}, err }
    codecs := durable.NewCodecs()
    if err = durable.RegisterJSON[Document](codecs, "example/document", "1"); err != nil { return d.Assembly{}, err }
    return d.Assembly{Graph: g, Keys: keys, Codecs: codecs}, nil
}

func adminMigrate(ctx context.Context) error {
    return postgres.Migrate(ctx, os.Getenv("PULSE_ADMIN_DSN"), postgres.Grant{
        Role: "pulse_app", Namespace: id.Namespace,
        Permissions: []string{"submit", "query", "worker", "cancel"},
    })
}

func workerNew(ctx context.Context, c *postgres.Coordinator) error {
    // A second New creates a separate pool, not merely another goroutine.
    renew, err := postgres.New(ctx, os.Getenv("PULSE_WORKER_DSN"))
    if err != nil { return err }
    defer renew.Close()
    registry := &d.Registry{}
    if _, err = registry.Register("copy", "document/1", assembly); err != nil { return err }
    w, err := worker.New(worker.Config{
        Coordinator: c, Renewer: renew, Registry: registry,
        Namespace: id.Namespace, WorkerID: "local-1", Capacity: 4, BatchMax: 2,
        TTL: 15*time.Second, RenewInterval: 5*time.Second,
        OperationTimeout: 2*time.Second, RenewTimeout: 2*time.Second,
        PollMin: 100*time.Millisecond, PollMax: 500*time.Millisecond,
    })
    if err != nil { return err }
    stop, stopSignals := signal.NotifyContext(ctx, os.Interrupt)
    defer stopSignals()
    runCtx, cancel := context.WithCancel(ctx)
    defer cancel()
    done := make(chan error, 1)
    go func() { done <- w.Run(runCtx) }()
    select {
    case err = <-done: return err
    case <-stop.Done():
    }
    // Keep Run and renewal alive while draining, then stop the local loop.
    drainCtx, finish := context.WithTimeout(context.Background(), 30*time.Second)
    defer finish()
    if err = w.Drain(drainCtx); err != nil { return err }
    cancel()
    <-done
    return nil
}

func run() error {
    if len(os.Args) != 2 { return fmt.Errorf("usage: demo migrate|submit|worker|query|cancel") }
    ctx := context.Background()
    if os.Args[1] == "migrate" { return adminMigrate(ctx) }
    c, err := postgres.New(ctx, os.Getenv("PULSE_WORKER_DSN"))
    if err != nil { return err }
    defer c.Close()
    switch os.Args[1] {
    case "submit":
        a, err := assembly()
        if err != nil { return err }
        var in pulse.Inputs
        if err = pulse.Seed(&in, pulse.NewKey[Document]("input"), Document{Text: "Hello"}); err != nil { return err }
        task, err := d.NewTask(ctx, id, "copy", "document/1", a.Graph, in, a.Codecs)
        if err != nil { return err }
        result, err := c.Submit(ctx, task)
        if err != nil { return err }
        fmt.Println(result.Snapshot.Status, "deduplicated:", result.Deduplicated)
    case "worker":
        return workerNew(ctx, c)
    case "query":
        s, err := c.Get(ctx, id)
        if err != nil { return err }
        fmt.Println(s.Status, "epoch:", s.Epoch, "revision:", s.Revision, "reason:", s.Reason)
    case "cancel":
        // Retain the same ID/digest when resolving this logical cancellation.
        h := sha256.Sum256([]byte("cancel:documents:document-001"))
        s, err := c.Cancel(ctx, id, d.Mutation{ID: "cancel-document-001", Digest: hex.EncodeToString(h[:])})
        if err != nil { return err }
        fmt.Println(s.Status)
    default:
        return fmt.Errorf("unknown command: %s", os.Args[1])
    }
    return nil
}

func main() {
    if err := run(); err != nil { log.Fatal(err) }
}
```

```sh
go mod tidy
go build -o demo .
# 仅迁移进程注入 PULSE_ADMIN_DSN：
./demo migrate
# 后续进程只需 PULSE_WORKER_DSN：
./demo submit
./demo query
./demo worker
```

在另一终端执行 `./demo query`，完成后应为 `succeeded`；worker 持续轮询，按 Ctrl+C 排空后退出。Windows 使用 `demo.exe`。在新任务领取前执行 `./demo cancel` 可观察 `canceled`；取消已完成任务只返回已有终态。重新演示请改用新的 RunID。

`Migrate` 显式安装或验证全新物理格式 3 的 `pulse_durable_distributed` schema；旧格式拒绝且原数据不改，需使用新库。逻辑记录仍为 format 2。检查点强制使用 InitializeOwned/ApplyOwned，以初始快照、不可变批和带 fencing 的 revision/chain head CAS 提交。`Migrate` 同时执行 namespace 授权；`New` 只验证并连接，不执行 DDL。示例为简化使用一个应用角色；实际部署可通过多个 `postgres.Grant` 分离 submit、query、worker、cancel 权限，worker 同时授予 query 以支持恢复查询。不要以管理员凭据调用 `New`，也不要将旧 `postgresstore` 的表或会话用于这些任务。

## 路由与升级

- `Registry.Register` 按 TaskType、BusinessVersion、图定义指纹、完整 Codec 清单摘要、协议及记录/载荷格式精确匹配，不做版本范围、latest 回退或代码下载。
- `assembly` 每次构造新的图、类型化根输入 keys 与 Codecs；构造器和 Codec 必须确定且无业务副作用。业务函数和切面由应用部署，不会随任务序列化；改变它们的语义须升级 `document/1` 这样的业务版本。
- 自定义类型用 `RegisterJSON[T]` 或 Codec 显式注册稳定 TypeID/Version，覆盖内部、子图和流相关的所有键；编码契约变化要升级 Codec 版本。图结构相同不代表业务函数相同。
- 生产者与 worker 安装相同注册契约。滚动升级时保留旧注册项处理旧任务；没有兼容 worker 的任务保持 queued。领取后校验失败可转为 blocked；当前 API 不提供通用 unblock/requeue 管理操作，不要直接改表。
- 同 `(Namespace, RunID)` 的相同规范任务重复提交返回 `Deduplicated=true`，保留检查点和终态；内容不同返回 `ErrSubmitConflict`。终态身份不自动回收，RunID 不复用。

输入必须能编码成有限载荷：最大 4 MiB，任务封装最大 8 MiB，Codec 清单最多 4096 项；检查点最大 16 MiB。不要提交函数、channel 或进程内地址。

## 生命周期与查询

| 操作 | 语义 |
| --- | --- |
| `Capacity` / `BatchMax` | 本地预留槽位总上限 / 单次领取上限；未决领取和排空中的执行也占槽 |
| `Pause()` / `Resume()` | 停止 / 恢复新领取，已有任务继续执行和续租 |
| `Drain(ctx)` | 永久停止新领取，保持续租并等待槽位清空；超时本身不取消任务 |
| `Run(ctx)` | 单次使用的阻塞循环；context 取消停止本机执行，不等于业务 Cancel |
| `Coordinator.Cancel` | 在数据库持久取消，与 Finish 竞争；不撤销已发生的外部副作用 |
| `Get` / `List` | 查询权威状态，观察不会续租，也不授予执行权限 |

`List` 使用 `ListRequest{Namespace: "documents", Status: d.Queued, Limit: 100}`；后续页用最后一个任务的 RunID 设置 `AfterRunID`。状态为 queued、running、blocked、succeeded、failed、canceled；`Snapshot.Reason` 是脱敏代码，`Revision` 是检查点顺序，不是所有权代数。

worker 为每次启动生成 BootID，领取分配新 LeaseID 与递增 Epoch。续租使用独立 `postgres.New` 连接池，需为它预留真实数据库连接预算，单独开 goroutine 不等于连接隔离。示例时间参数用于演示，部署时按数据库延迟和业务取消耗时调整。优雅关闭先 Drain，再取消 Run，最后关闭两个池；不响应取消的业务代码或未决领取可能阻止排空，`Run` 返回也不保证这些代码已经退出，`Active()` 可观察仍保留的槽位。示例排空 30 秒失败即退出，不把它报告为排空成功。

写入返回 `ErrUnknown` 表示提交结果不确定，不是业务失败。Submit 可用完全相同的任务重试；Cancel 保留原 Mutation ID/Digest 并查询状态或使用 PostgreSQL receipt API 裁决。worker 自行处理未决 Claim 和受保护写入，不要另起领取循环重放，也不要因网络失败将任务标为 failed。

## 部署前提

协调操作在单个权威 PostgreSQL 写主库上执行，持行锁后采样数据库时间，并用有效租约、BootID、LeaseID、Epoch 和 Revision 拒绝旧 owner 写入。数据库时钟须避免回拨；实现检测到行时间倒退会拒绝操作，不能据此推导任意时钟回拨仍安全。备份回退、丢失已确认事务或双主写入会破坏所有权历史，恢复时应隔离旧 worker 并使用新的 namespace；本模块不负责数据库 HA。fencing 保护协调状态和检查点，不会终止失联进程的业务代码：外部副作用必须以稳定 `(Namespace, RunID, 业务操作身份)` 做幂等，不能用每次改变的 LeaseID；外部系统支持 fencing 时再校验 Epoch。

## 验证入口

示例可以 `go test -run '^$' .` 只编译而不连接数据库。本模块的 [systemtest](systemtest)、[networktest](networktest)、[crossmachine](crossmachine) 与 [soaktest](soaktest) 包提供真实数据库、故障及跨进程验证入口；运行条件和观测结果应按对应测试配置核对。性能与验收数据统一见[规模与性能](../site/guide/performance.md)，本文不将单次示例或编译检查视为部署验收。
