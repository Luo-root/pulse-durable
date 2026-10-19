# 执行契约

本页定义 Runtime、运行身份、恢复兼容性和恢复事件的规则。完整调用程序见[快速开始](../guide/quickstart.md)和[进程退出后恢复](../guide/recovery.md)。

## Runtime

`durable.New(store, codecs)` 创建 Runtime，不读取存储，也不执行业务。Store 和 Codecs 必须非 nil；Store 的 typed nil 也会被拒绝。

同一个 Runtime 可以并发运行不同 RunID。每次 `Run` 或 `Resume` 创建新的 `pulse.Execution`，在启动、等待及收尾期间持有同一 RunID 的独占 Session，然后关闭 Session。

| 方法 | 记录前置条件 | 行为 |
| --- | --- | --- |
| `Run(ctx, graph, inputs, options)` | RunID 尚无记录 | 初始化记录，启动新执行，阻塞等待本次执行结束 |
| `Resume(ctx, graph, inputs, options)` | RunID 已有记录 | 加载并验证记录，恢复有效成果，启动新执行并等待结束 |

`Run` 遇到已有记录返回 `ErrConflict`；`Resume` 遇到不存在的记录返回 `ErrNotFound`。方法不会自动切换。不同 Store 是否共享所有权与数据，由后端决定。

两者都返回 `(*pulse.Execution, error)`。前置阶段失败可能尚未创建 Execution；后续失败尽可能返回 Execution 供 `Snapshot`、`Err` 查询。应先检查返回错误，不能仅按 Execution 是否存在判断成功。恢复校验或回调也可能在图尚未启动时失败。

## RunOptions

| 字段 | 规则 |
| --- | --- |
| `RunID string` | 必填，非空且为有效 UTF-8 文本；非法 RunID 在访问 Store 前作为参数错误拒绝 |
| `Version string` | 必填，非空且为有效 UTF-8 文本，不要求 SemVer；非法文本在访问 Store 前作为参数错误拒绝，恢复时须与记录一致 |
| `ExecOptions []pulse.ExecOption` | 本次执行使用的 Pulse 选项，例如本次 Observer |
| `OnEvent func(Event) error` | 可选，同步报告 Durable 恢复事实；错误或 panic 停止本次执行 |
| `BeforeStart func(*pulse.Execution) error` | 可选，在恢复装配后、Start 前登记宿主流订阅；错误或 panic 取消执行并释放 Session |

context 必须非 nil；调用入口已取消或到期的 context 会被拒绝。图须有效，普通键、消息元素、游标和显式状态的真实类型须有合适 Codec；受管流端点只存 descriptor。宿主可在 BeforeStart 中 SubscribeStream 并启动读取任务，任务在 Start 后 Receive、Ack 或 Stop，并在执行取消时退出。Run/Resume 持续持有会话并等待读取义务完成。

RunID 表示一项业务运行的身份，应保持稳定。新业务分配新 RunID；恢复旧业务保留原 RunID。不能借助更换 Version、输入或定义，把旧 RunID 当成另一项业务使用。

## 恢复兼容性

恢复需要宿主重新提供图定义、输入、Codecs 和 RunOptions。Durable 不从记录中构建业务函数，也不保存 Go 调用栈。

| 检查项 | 核对内容 | 变化的处理 |
| --- | --- | --- |
| 运行身份与业务版本 | RunID、Version 与记录完全一致 | 原条件恢复，或明确创建新业务运行 |
| 定义指纹 | 完整 Topology、所有节点与端口，以及由真实 Go 类型查得的 Codec ID/版本 | 定义漂移会拒绝恢复；重建等价定义 |
| 根输入指纹 | 输入值的状态、类型契约和确定性编码 | 重供相同输入；不要将新输入与旧成果混用 |
| 子图实际输入 | 每次打开子作用域时的绑定输入指纹 | 已有作用域输入不同会拒绝恢复 |
| 记录完整性 | 格式、修订号、作用域、尝试关联、依赖闭合、输出集合、编码校验和及可解码性 | 损坏或不一致按可靠恢复错误停止 |

完整 Topology 也参与哈希，不应假定修改描述等可见定义字段一定兼容。Codec 注册按实际 Go 类型匹配，ID 与版本参与持久契约；仅改变 Go 类型名称或注册形状时，也应重新验证兼容性。

函数体、闭包内容、固定绑定值以及切面顺序、参数和语义不靠结构指纹完整判断。宿主须为这些变化升级业务 `Version`。固定绑定值还会影响子图打开时的实际输入核对，但这不能代替业务版本管理。

输入编码必须确定：相同值每次产生相同字节。不要让 Codec 依赖随机数、时间或可变外部状态，否则同一业务输入可能无法恢复。

## 确认、重入与 Retry

普通确认边界是完整节点链，包含切面。业务函数返回成功或执行了 `Set`，都不能单独证明节点已经确认。完整成果、节点根尝试的 `confirmed` 和写端成功终态在同一次 Save 中保存。受管流追加、`HandleMessage` 的输入确认/显式状态/成果另有增量事务；流消息在对应存储确认后才交付，不能等同于完整节点已经完成。消息贡献的普通值恢复到完整节点暂存外帧，仍等完整节点确认才发布；恢复不重做已 Ack 输入。

恢复复用有效作用域中的已确认节点，不重新执行它们的完整链。未确认节点从函数开头执行，因此确认前发生的外部副作用可能重复。

子图作用域属于父节点当前最深尝试。父尝试为 `active` 时可认回原作用域；父尝试为 `failed` 时，旧后代不可复用。Retry 还有预算时，下一个尝试使用新的作用域。

完整节点根尝试的预算为一次，内置 Retry 在其内部保存自己的序号和上限。真实业务失败耗尽完整节点预算后，反复 `Resume` 不会补充 Retry 次数。内层成功在根确认前仍保持活跃。操作示例见[子图与重试](../guide/subgraphs.md)。

恢复发现当前图作用域内已有最终失败的节点根时，不重跑已确认别名或重新打开失败来源。子作用域通过精确的 `*pulse.CheckpointScopeFailure{Scope: request.Attempt}` 在装配恢复成果之前交回父完整链；父切面可以接住业务失败，父 Retry 仍按持久剩余预算决定下一轮。信号不携带原业务错误身份或文本，不代替 Store/Codec/校验错误。

根 Resume 在完整校验与旧 partial 收敛之后、解码根恢复成果之前返回同类业务信号。返回的 Execution 尚未 Start，Snapshot 仍为 pending；不发 restored/GraphStarted、不调用 BeforeStart，也不提供 ReadOutput 结果。必须以 Runtime 返回错误判断已确认失败，不能把 pending 快照当成可继续运行或恢复成功。

宿主取消、退出或可靠失败不会仅因中断就将仍活跃的尝试确认成业务失败；不确定保存的实际结果仍以重读为准。节点自身取消局部 context 后返回真实业务错误，仍可能是应确认的业务失败，不能仅凭错误链中存在 `context.Canceled` 判断为宿主中断。

## 读取出口

`pulse.ReadOutput(execution, key)` 读取本次执行的声明出口，键须与 `DeclareOutput` 同名同类型。它不等待执行完成，不创建槽位，也不读取任意内部键。

| 情况 | 返回语义 |
| --- | --- |
| 成功收尾，出口 `ready` | 业务值、`ready`、nil error；零值与合法 nil 仍保持 ready |
| 成功收尾，出口 `skipped` | 类型零值、`skipped`、nil error；正常缺失 |
| 成功收尾，可选透传出口未喂值 | 类型零值、`pending`、nil error |
| 尚未收尾 | 生命周期错误，立即返回，不提供部分值 |
| 失败或真实取消 | 返回失败原因，值为零值、状态为空 |
| 未声明出口或类型不符 | 包装 `pulse.ErrUndeclared` 的错误 |

先检查 `err`，再按业务规则处理 `state`。不能把正常 skip 升格为失败，也不能把失败返回的零值当作正常缺失。读取 map、slice、指针等不会深拷贝，宿主仍负责后续的并发访问。

## 恢复事件

`EventRestored` 的值为 `"restored"`，表示一个已确认节点的成果被选入本次恢复计划。Event 包含 `RunID`、`Scope`、`Graph`、`Path`、`NodeID` 和 `Kind`，不包含业务值、编码字节或虚构的业务耗时。

事件在对应图启动前同步发出；多个运行或子图可能并发调用同一回调，回调须并发安全。不要在回调中同步等待同一个执行完成，否则会阻塞启动。一次新的 Resume 可能再次报告相同成果，事件不提供一次性投递保证。

以下是选项工厂的局部声明，需要导入 `fmt`、`sync` 和 `durable`，并把返回值传给相应的 `Run`/`Resume`：

```go
func observedOptions(runID string) durable.RunOptions {
	var mu sync.Mutex
	return durable.RunOptions{
		RunID: runID, Version: "recovery/1",
		OnEvent: func(event durable.Event) error {
			mu.Lock()
			defer mu.Unlock()
			fmt.Println("restored node:", event.NodeID)
			return nil
		},
	}
}
```

回调返回错误或 panic 会按可靠恢复错误停止本次，已确认数据不回滚。错误正文脱敏，原错误原因通过 `Unwrap` 保留；`panic(error)` 同样保留原因。虽然事件不带业务值，RunID 和其他标识仍可能含业务信息，宿主应按日志策略选择与脱敏字段。

## 下一步

实现存储或检查记录时，阅读[数据模型](data-model.md)。决定失败后的动作时，阅读[错误处理](errors.md)。
