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
go mod edit -require=github.com/Luo-root/pulse@v0.5.2
go mod edit -require=github.com/Luo-root/pulse-durable@v0.1.1-0.20261108162459-ccc90d4b53c3
go mod edit -require=github.com/Luo-root/pulse-durable/distributed@v0.0.0
go work init . ../pulse-durable ../pulse-durable/distributed
```

`v0.0.0` 只是工作区模块的占位版本，不是下载地址。上述 Pulse 与 Durable 版本与当前 [go.mod](go.mod) 一致；根开发 pin `v0.1.1-0.20261108162459-ccc90d4b53c3` 已解析可下载，包含本轮根包契约，是提交伪版本而非发布 tag。完整 distributed 开发 pin 为 `v0.0.0-20261108164227-4babad07420f`，已可下载并通过无 replace 的独立消费者接口装配、snapshot2 和 race 检查。源码联调同时包含示例、当前根 checkout 和 `distributed/`；使用公开根 pin 的独立模块 build/vet 需显式 `GOWORK=off`，本轮四模块 `GOWORK=off` 下载/build/vet/race 已通过，含 standalone PostgreSQL 实库；这些结果不等于完整子模块已正式发布。若已有 `go.work`，保留它并把需要的目录纳入现有工作区，令联调命令的 `GOWORK` 指向该文件，不覆盖已有配置。

当前可用公开开发 pin 独立接入（非 tag）：

```sh
go get github.com/Luo-root/pulse-durable/distributed@v0.0.0-20261108164227-4babad07420f
```

## 物理合成入口

PostgreSQL 安装五个独立的 `jsonb` 入口：`storage_inspect`、`storage_compact_begin`、`storage_compact_publish`、`storage_compact_abort`、`storage_compact_find`，不再安装 `maintenance(text,jsonb)` 或兼容分派器。Go 仍提供 `InspectStorage`、`CompactStorage`、`FindMaintenance`；内部仅调用固定 SQL 白名单，不提供任意 SQL 执行。

所有入口按 `session_user` 校验命名空间 `maintain` 权限，拒绝 worker token，按身份 advisory → run 行锁顺序执行，不锁 lease。内部锁定/时钟 helper 不授予客户端或 PUBLIC 执行权限，直接表 DML 仍禁止。构建在数据库锁外完成；发布验证 generation/CAS、保留并发 suffix 并重算预算。永久 guard 不因 abort/过期释放；find 的 building/closed/expired 不是发布回执。发布结果未知时只能查询回执，不自动 abort 或盲重试。

`Migrate` 的签名覆盖全部新函数及 helper，授权覆盖五个入口；已有旧布局不会原地升级，签名不符时拒绝。这里没有恢复 schema 版本链，也不承诺旧 SQL 入口兼容。

## 历史引用 pin

`postgres.Coordinator` 另行实现独立的 `distributed.HistoryPinner` 与 `HistoryPinMutationReader`，不扩张 Coordinator 接口。固定 SQL 入口为 `pin_create`、`pin_renew`、`pin_release`、`pin_find`、`pin_find_mutation`；内部 helper 不授权应用角色或 PUBLIC。

有限创建需要 `replay`，长期创建需要 `maintain`；续期必须是原 `session_user` holder 且仍有当前权限，长期相关操作需要 `maintain`。`HolderDigest` 是绑定摘要，不是凭证；`maintain` 可代释放但仍须版本 CAS。当前状态与不可变 receipt 查询需要 `query`。PinID 永久不复用，Mutation.ID 在同 Identity 的所有 pin 写操作中共享，完整规范摘要由 SQL 重算。nil 续期 TTL 保留原策略，过期/释放不复活；未知结果只能通过精确 receipt 裁决，absent 不授权盲重放。

历史 pin 仅保护已存在 revision+chain 引用，不实现 GC、retention、summary 或 replay。当前开发布局增加三张表与入口；`Migrate` 仅新库安装或精确验证，不升级旧布局、不自动删库。真实测试过滤器为 `go test -count=1 -v ./postgres -run '^TestPostgresHistoryPin$'`，必须显式设置专用 `PULSE_DISTRIBUTED_TEST_DSN` 与 `PULSE_DISTRIBUTED_TEST_ALLOW_DESTRUCTIVE=yes`；未配置时 skip，不算实库验收。

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

`Migrate` 显式安装或验证当前 `pulse_durable_distributed` 布局，使用固定 layout marker `pulse-durable-distributed-layout-v1`、catalog signature 以及 owner/`SECURITY DEFINER`/ACL 安全检查，再执行 namespace 授权。布局不匹配时只拒绝，不写入、删除或迁移旧数据，需使用新库。开发期不维护递增物理 schema 版本链。领域记录无格式号，业务 `Version` 保留；当前 wire `2`、snapshot envelope `2`、Task 顶层 protocol `2` / payload_format `1` 与 budget 协议 `2` 均留在边界。固定 marker 不改号，旧 catalog signature 不匹配仍拒绝；旧 wire/snapshot/Task 原字节保留，不自动迁移、重签或重编码。检查点强制使用 InitializeOwned/ApplyOwned，以初始快照、不可变批和带 fencing 的 revision/chain head CAS 提交。`New` 只验证并连接，不执行 DDL。示例为简化使用一个应用角色；实际部署可通过多个 `postgres.Grant` 分离 submit、query、worker、cancel 权限，worker 同时授予 query 以支持恢复查询。不要以管理员凭据调用 `New`，也不要将旧 `postgresstore` 的表或会话用于这些任务。

## 路由与升级

- `Registry.Register` 只按 TaskType、BusinessVersion、Definition（图定义指纹）、ManifestDigest（完整 Codec 清单摘要）四项业务 Route 精确匹配，不做版本范围、latest 回退或代码下载。Task 的顶层 `protocol=2` / `payload_format=1` 在调用 Factory 或 Codec 前校验，不进入注册、查询或 worker 能力匹配；旧 route.record_format 与旧封装明确拒绝。
- `assembly` 每次构造新的图、类型化根输入 keys 与 Codecs；构造器和 Codec 必须确定且无业务副作用。业务函数和切面由应用部署，不会随任务序列化；改变它们的语义须升级 `document/1` 这样的业务版本。
- 自定义类型用 `RegisterJSON[T]` 或 Codec 显式注册稳定 TypeID/Version，覆盖内部、子图和流相关的所有键；编码契约变化要升级 Codec 版本。图结构相同不代表业务函数相同。
- 同一协议内滚动业务升级时保留旧业务注册项处理旧任务；没有匹配 worker 的任务保持 queued。首版 protocol `1` Task 不能通过增加 Route、改版本字段或重新编码自动升级，必须原样保留并由旧版及隔离存储处理。领取后校验失败可转为 blocked；当前 API 不提供通用 unblock/requeue 管理操作，不要直接改表。
- 同 `(Namespace, RunID)` 的相同规范任务重复提交返回 `Deduplicated=true`，保留检查点和终态；内容不同返回 `ErrSubmitConflict`。终态身份不自动回收，RunID 不复用。

输入必须能编码成有限载荷：最大 4 MiB，任务封装最大 8 MiB，Codec 清单最多 4096 项；检查点最大 16 MiB。PostgreSQL 的 16 MiB 预算按当前完整 `EncodeSnapshot` 字节数计算，保留 nil/empty、base64、JSON 转义和 revision 位数。每次 Apply 仅计数变更对象；数据库从实际 wire 和当前对象/容器账本独立计算，与 head、batch、guard、receipt、event 原子发布。越界返回 `postgres.ErrSnapshotLimit`（可用 `errors.Is(err, durable.ErrLimitExceeded)`），拒绝不产生这些发布。压缩历史不会重置当前快照预算；活跃日志的 256 MiB/100000 批预算仍独立存在。Load 校验账本与重放后的快照大小，不一致时拒绝，不自动修复。不要提交函数、channel 或进程内地址。

PostgreSQL 预算协议为 `snapshot_budget_version=2`，对应 snapshot envelope `2` / batch wire `2`；每个 API/maintenance 请求都要求该版本，已连接的旧客户端也会安全拒绝。`New` 在连接数据库前检查根模块是否提供 `ChangeBatch.SnapshotSizeDelta`，缺少时返回 `ErrSnapshotBudgetUnsupported`；该检查不证明旧根模块理解当前编码。go.mod 已固定包含本轮根包契约的公开开发 pin `v0.1.1-0.20261108162459-ccc90d4b53c3`，它是可下载开发提交而非 tag。独立模块 build/vet 使用 `GOWORK=off` 验证实际根依赖，源码联合回归使用包含根模块与 `distributed` 的工作区；完整 distributed 开发 pin 为 `v0.0.0-20261108164227-4babad07420f`，不是发布 tag。

## 语义历史能力边界

可选 HistorySummaries/CommitIndex/Provenance 全 nil 是普通记录，任一出现须形成完整闭包；摘要和来源不是执行或删除凭证。根包不可变 `historySummaryRegistry` 以 Retry/Message 两个内置 handler 统一解释摘要；未知 Kind 只可在底层只读路径保留 opaque 证据，恢复或修改前拒绝。

根包 `BuildHistoryCompressionBatch` 已提供当前权威记录重规划、完整旧对象精确 CAS 删除、同批 AppliedRevision/CommitIndex 与恢复闭包，`PlanHistoryCompression` 仍只读且 `Executable=false`。这不表示 distributed 已交付首次权威证据物化、语义压缩执行、压缩 floor/永久 receipt 或指定位置派生回放；这些仍未完成。物理合成、GC、retention、语义压缩与 Replay 分别验收，现有 mutation/maintenance receipt 不等于语义压缩永久回执。

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

PostgreSQL 的 `Coordinator.FindMutation(ctx, MutationQuery)` 使用独立 `mutation_receipts`；永久 `mutation_guards` 防止身份复用，`events` 仅用于诊断，不能代替回执。查询提供 Identity、Mutation ID/Digest、Operation 和闭区间 RequestedWindow；ExpectedEpoch/ExpectedRevision 为可选指针，nil 表示未知，非 nil（包括 0）要求精确匹配。已知值应显式传入；Cancel 可留 nil 并读取已提交值，不从后来的 Get 猜测。

- `Found`：找到请求窗口内的精确 mutation 回执，仅证明该协调 mutation 已提交，不证明外部副作用，也不授予 lease/执行权限。
- `AbsentWithinWindow`：请求窗口完全被权威 Coverage 覆盖，未找到匹配回执；不是从未提交的证明。
- `Expired`：请求下界早于连续覆盖下界，且本次观察已达到请求上界；不是业务重放授权。请求上界晚于观察时间会被拒绝，而不是判为 Expired。

Coverage.From 来自数据库持久 `receipt_coverage_floor`，Coverage.Through 是持运行锁后采样的数据库观察上界，并持久到 `receipt_observed_through`，不使用客户端时间。仍保留的回执可早于 coverage floor；覆盖内应有却缺失的回执会报错，不冒充正常缺席。该查询会写 observation watermark，必须连接可读写主库，不能使用只读事务或副本；权限仍是 namespace `query`，应用无直接表 DML 权限。缺回执、缺席或 Expired 均不授权业务重放。

历史 pin 由独立 `HistoryPinner` 能力提供：`PinHistory` 的有限 TTL 需要 `replay`，长期 pin 需要 `maintain`；`FindPin` / `FindPinMutation` 只需目标 namespace 的 `query`。query-only 角色可以查询 pin 和回执，不获得 pin 创建、续期、释放或物理维护权限。pin 查询会在运行锁下采样并保存观测水位，仍需可读写主库；`HolderDigest` 不是凭据，可信主体来自数据库 `session_user`。

PostgreSQL 已提供保留策略持久化与版本 CAS：`GetRetentionPolicy` 在首次读取时初始化默认策略，`PreviewRetentionPolicy` 登记供 `UpdateRetentionPolicy` 校验的拟议策略绑定，这两条初始化/策略预览路径需要可写事务。独立的 `PreviewRetentionCandidates` 只读当前候选、逻辑行数/字节数、保护原因与分页游标，不创建策略或预览记录，可在已初始化策略的 PostgreSQL READ ONLY 事务中执行。它只提供观察结果，不是固定数据快照或删除授权；后续执行必须重新锁定、采样并复验。以上保留入口需要 namespace `retain` 权限，无应用表 DML 权限。nil TTL 仍表示无限保留，guard/运行身份不受 TTL 回收。实际 retention 删除、GC、业务 replay 和语义历史压缩仍未实现。

## 部署前提

协调操作在单个权威 PostgreSQL 写主库上执行，持行锁后采样数据库时间，并用有效租约、BootID、LeaseID、Epoch 和 Revision 拒绝旧 owner 写入。数据库时钟须避免回拨；实现检测到行时间倒退会拒绝操作，不能据此推导任意时钟回拨仍安全。备份回退、丢失已确认事务或双主写入会破坏所有权历史，恢复时应隔离旧 worker 并使用新的 namespace；本模块不负责数据库 HA。fencing 保护协调状态和检查点，不会终止失联进程的业务代码：外部副作用必须以稳定 `(Namespace, RunID, 业务操作身份)` 做幂等，不能用每次改变的 LeaseID；外部系统支持 fencing 时再校验 Epoch。

## 验证入口

示例可以 `go test -run '^$' .` 只编译而不连接数据库。本模块的 [systemtest](systemtest)、[networktest](networktest)、[crossmachine](crossmachine) 与 [soaktest](soaktest) 包提供真实数据库、故障及跨进程验证入口；运行条件和观测结果应按对应测试配置核对。当前性能对照统一见[生产性能对比](../site/guide/production-comparison.md)，历史规模与版本见[规模与性能](../site/guide/performance.md)。本文不将单次示例或编译检查视为部署验收。本轮 distributed PostgreSQL 新库整套 race 已通过（491.245s），包含容量（199.98s）、精确边界（232.29s）、retention 修复和 500 消息/1506 回执与批次（34.95s）；空库 DDL 通过（0.775s）。这是当前源码实库验证，不等于完整 CI 或正式发布。

源码联合回归使用同时包含根模块与 distributed 的工作区；独立 build/vet 对公开根 pin 使用 `GOWORK=off`。设置 `PULSE_DISTRIBUTED_TEST_DSN` 指向专用可丢弃库、`PULSE_DISTRIBUTED_TEST_ALLOW_DESTRUCTIVE=yes` 后，从 distributed 目录执行整套实库 race；无 DSN 的跳过不算实库通过。保留原有时钟、租约、恢复、容量和 retention 断言。

```sh
go test -mod=readonly -race -count=1 -timeout=900s -v ./postgres
```
