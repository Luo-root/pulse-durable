# 观测与稳定运行身份

本页面向需要从日志判断“新重试，还是恢复了上次未完成尝试”的服务开发者。示例以目标 Durable `v0.1.0` 为 API 基线，依赖 Pulse `v0.5.0`，要求 Go 1.26；发布状态以 [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases) 为准，tag 发布前[安装命令](installation.md)尚不可用。默认构建不使用本地 replace。

## 区分运行、进程与尝试

| 身份 | 来源与用途 | 跨进程恢复 |
|---|---|---|
| `durable.run_id` | 宿主传入的 `RunOptions.RunID`；使用 `durable.AttrRunID` 键 | 同一业务运行保持不变 |
| `pulse.attempt.scope` / `pulse.attempt.parent_scope` | Durable 实际分配或认回的持久尝试游标 | 认回活跃尝试时 Scope、ParentScope 和编号不变 |
| Record 的 `TraceID` | 每次调用装配时注入或生成的追踪关联 | 示例每轮新建；若跨调用串联追踪，策略由宿主显式决定 |
| Record 的 `HostID` | 宿主提供的执行者标识 | 换执行者可以不同，同一宿主也可以相同 |
| `pulse.execution` | Core 每次创建 Execution 的进程内身份 | 不能当作持久 RunID；不同进程可能得到相同文本 |
| `pulse.aspect.boundary` | 本次 Execution + NodeID 内的切面调用编号 | 仅关联本轮嵌套事件，不能代替持久 Scope |

`pulse.graph`、非空 `pulse.path` 和 `pulse.node` 仍用于识别图、挂载位置和节点。需要跨恢复找同一次业务时，先用稳定 RunID，再用真实 Scope/ParentScope 对齐尝试，不从 Execution ID 或路径重建持久身份。

## 一次装配稳定 RunID

RecordObserver 已实现 Core 的可选切面观察接口。业务保持原有 `pulse.Retry(3, 0)`，安装观察者后自动产出 `pulse.aspect` 记录，无需每个节点手写日志。Durable 只提供属性键；下面的 `runIDSink` 是宿主代码，每次 Run/Resume 为相同 RunID 装配一次即可。

`Record.Attrs` 有引用语义，Sink 应只读消费收到的记录。包装出口须先通过 Range 复制四种支持的标量，再补 RunID，避免影响调用方或 MultiSink 的其他成员：

<<< ../../observation_example_test.go#runIDSink

下面是同一份已编译 Example 的装配与读取部分，使用 MemoryStore 演示一次失败后成功；进程退出恢复使用下一节的真实 filestore 验证。

<<< ../../observation_example_test.go#observation

完整源码位于仓库根 `observation_example_test.go`，含包声明和导入。在此源码分支根目录运行：

```sh
GOWORK=off go test -count=1 -run '^ExampleRuntime_Run_observation$' .
```

PowerShell 使用 `$env:GOWORK = 'off'` 后执行同一 go test。输出断言是 attempt 1 与 attempt 2 都为 new。MemorySink 可换为 LineSink、SlogSink 或宿主自己的出口；LineSink 结束时仍需检查 Flush。

## 真实退出与恢复会看到什么

`integration/observation_recovery_test.go` 使用三个真实进程和同一 filestore 目录，所有 Record 都经过 RunID 包装，进程分别配置可区分的 HostID/TraceID：

1. 第一个进程装原有 Retry(3, 0)。attempt 1 业务失败，确认失败后分配 attempt 2；进入记录已同步写盘、持久记录仍为 active 时直接退出。阶段为 `started → attempt_started(1,new) → attempt_finished(1,failed) → retry(2,new) → attempt_started(2,new)`。
2. 新进程 Resume。桥在实际 active 分支返回 `CheckpointAttempt.Resumed=true`，认回相同 Scope/Index 2；阶段为 `started → attempt_started(2,resumed) → attempt_finished(2,completed) → finished`，没有新 retry 事件。
3. 成功后再启动一个进程 Resume。完整节点成果直接恢复，业务体和切面都不再执行，没有历史切面记录；RecordObserver 仅产图开始/结束记录，恢复事实由 Durable 的 `OnEvent(EventRestored)` 提供。

这个回归核对真实持久游标、输出和预算：业务体进入次数共 3 次，修订号 `5→6→6`，没有为恢复补充新尝试。测试有子进程 deadline；把 active 分支的 Resumed 故意改为 false 的 overlay，会因 resumed 被误报为 new 而失败。

```sh
GOWORK=off go test -race -count=1 -timeout=90s -v -run '^TestRetryObservationAcrossProcessExit$' ./integration
```

`Resumed` 只是桥返回的可选观察事实，不进入 RunRecord JSON。新分配返回 false、活跃认回返回 true；旧桥不提供时 Core 记为 unknown，不猜测。attempt 从 1 开始且 limit 包括首次，首次成功不会产生 retry。Scope 与 ParentScope 直接来自返回游标，耗时只覆盖本进程实际观测到的调用。

## 保留原有语义

正常 skip 是缺少结果，结束记录为 skipped、Err=nil，不重试；宿主取消保持活跃游标，可按原恢复规则继续。panic 仍是失败并遵守原 Retry 预算，可靠检查点失败不能被改写为 skip。完整节点 Commit 发生在完整切面链结束后，某层切面 completed 不代表节点已持久确认。

Observer 五种生命周期回调次数不变；额外切面观察回调的 panic 不改变业务结果。Durable 的 `RunOptions.OnEvent` 是已有的恢复回调，其错误仍会停止本次恢复，不能把两种回调的错误契约混同。自定义切面只有通过 `pulse.NamedAspect("approval", aspect)` 显式包装才自动记录进出，审批原因等业务事实由应用自行报告；消息级重试不混入节点尝试编号。

下一步见[子图与重试恢复](subgraphs.md)、[进程退出与恢复](recovery.md)及 [Core 观测参考](https://luo-root.github.io/pulse/packages/observe/)。
