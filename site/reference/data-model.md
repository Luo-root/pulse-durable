# 数据模型

本页定义 Durable 的持久记录、编码状态与 Store/Session 协议。用于实现后端、调查恢复问题或核对自定义 Codec。业务操作见[存储](../guide/storage.md)和[值编码](../guide/codecs.md)。

## RunRecord

当前 `RecordFormat` 为 `2`，新建普通与流式运行均使用该版本。Runtime 的逻辑校验仍识别不含增量字段的普通格式 `1`，后续业务提交使用 `2`；这不表示新版耐久后端会读取或迁移旧物理 JSON/schema。新版 Initialize 只接受当前逻辑格式；旧物理格式必须明确拒绝并保留原数据，使用旧版读取或另建新目录/新库。旧 Runtime 会拒绝新格式，避免忽略流字段后覆盖。RunRecord 只包含可序列化数据，不保存函数、Go 调用栈或自动采集的业务错误文本。

| 字段 | 含义 |
| --- | --- |
| `Format int` | 记录格式版本，恢复时须为当前支持的格式 |
| `RunID string` | 业务运行身份，须与 Session 和调用一致 |
| `Revision uint64` | CAS 修订号，从 `1` 开始，不能为零 |
| `Version string` | 宿主的业务语义版本 |
| `Definition string` | 完整定义的 SHA-256 指纹 |
| `Inputs string` | 根输入的 SHA-256 指纹；不是可直接还原业务输入的输入副本 |
| `RootScope string` | 根图的持久随机身份 |
| `Scopes map[string]ScopeRecord` | 图作用域及其完整节点成果 |
| `Attempts map[string]AttemptRecord` | 节点根尝试与 Retry 尝试游标 |
| `Streams map[string]StreamRecord` | 逻辑来源、读取组、消息日志及确认前沿 |
| `Messages map[string]MessageAttemptRecord` | 每条输入的持久失败预算与处理状态 |
| `Managed map[string]ManagedRecord` | 处理节点显式状态及已提交普通值 |

RootScope 与尝试身份是 16 字节随机数的十六进制表示。它们保存在记录中，不能按图路径、内存计数或进程启动顺序重新生成。恢复仍须由宿主提供原始外部输入。

## ScopeRecord

| 字段 | 含义 |
| --- | --- |
| `Graph string`、`Path string` | 定义中的图身份与静态挂载位置 |
| `Inputs string` | 该作用域实际输入的指纹 |
| `Nodes map[string]NodeRecord` | 此作用域中已经确认的完整节点成果 |
| `Bindings bool` | 子图指纹按稳定 In 绑定与固定值生成；false 保留原格式的输入值指纹 |

根作用域的输入指纹与 RunRecord.Inputs 一致。子图作用域直接使用父节点当前最深 Attempt 的身份，挂载路径仅描述位置，不能代替一次父尝试的持久身份。

同一张子图在不同挂载位置或不同父尝试中具有不同执行作用域。父尝试失败后，旧作用域留在记录中，但不再用于新的尝试。

## AttemptRecord

| 字段 | 含义 |
| --- | --- |
| `Parent string` | 本层图 Scope，或外层 Attempt 的身份 |
| `NodeID string` | 对应节点 |
| `Boundary string` | 根尝试为空；内置 Retry 使用固定切面位置表示边界 |
| `Index int` | 从 `1` 开始的本层尝试序号 |
| `Limit int` | 本层正数预算，Index 不超过 Limit |
| `State AttemptState` | `active`、`failed` 或 `confirmed` |

| 状态 | 语义 |
| --- | --- |
| `AttemptActive` | 可以认回原尝试；内层成功在根提交前也保持 active |
| `AttemptFailed` | 本次尝试已经真实失败，所有后代成果失效，不能复用 |
| `AttemptConfirmed` | 节点根尝试与完整 NodeRecord 在同一次 Apply 中确认 |

节点根尝试的 Boundary 为空，Limit 为 `1`。Retry 尝试 Parent 指向外层 Attempt，并保存自己的序号和上限。相同 Parent、NodeID、Boundary 下，序号连续，上限一致，较早尝试必须已经失败。

确认的是节点根尝试；Retry 内层成功并不独立成为 `confirmed`。记录中的父链须能一直追溯到根作用域，不能缺失、形成环或指向不匹配的节点。失败祖先导致成果逻辑失效，不等于记录格式损坏；恢复仍会校验旧作用域的可解码性与依赖完整性。

## NodeRecord

| 字段 | 含义 |
| --- | --- |
| `NodeID string` | 对应定义中的节点 |
| `Attempt string` | 此节点的已确认根尝试身份 |
| `Reason string` | 仅为 `completed` 或 `skipped` |
| `Outputs []ValueRecord` | 恰好覆盖该节点全部 Provides，不能缺少或重复 |

NodeRecord 代表完整节点链的确认成果，不是“函数已经进入”或“调用过 Set”的标记。真实失败保存在 Attempt 状态中，不会伪造成 skipped NodeRecord。

普通值恢复仍要求上游完整节点记录闭合。受管流依赖允许引用已可靠打开的来源，包括沿静态 In/Out 接线传递的来源，不要求来源业务体已经完成；其稳定身份、来源尝试和全部祖先须有效。根尝试仍须与 Scope、NodeID 及 NodeRecord 正确关联，并在同一次 Apply 中确认。

## ValueRecord

| 字段 | 含义 |
| --- | --- |
| `Name string` | 输出键名 |
| `TypeID string`、`Version string` | 对应实际 Go 类型的 Codec 契约 |
| `State string` | 仅为 `ready` 或 `skipped` |
| `Digest string` | ready 编码字节的 SHA-256 校验和 |
| `Data []byte` | ready 的不透明编码字节，在记录 JSON 中按字节字段保存 |

`ready` 与 `skipped` 分别描述有值和正常无值，不由 Data 的长度推断：

- ready 的零值、空字符串、typed nil 均保留 ready。声明为接口时，nil 接口也可以是 ready。
- 自定义 Codec 可以输出零字节，仍须保存并验证该字节串的校验和。
- skipped 不含编码值，Data 长度为零，Digest 为空。正常 skip 可以正常完成运行。
- `pending` 不是节点检查点的输出状态；可选透传出口的 pending 是本次 Pulse 执行的出口语义。

ready 恢复时会核对 Codec ID/版本、摘要格式与实际字节校验和，并验证解码值可赋给声明类型。校验和用于检测缺失与损坏，不是加密或防篡改认证。

## 增量流与消息事务

`ValueRecord.Stream` 是与 Data/Digest 互斥的 `StreamDescriptorRecord`，TypeID/Version 描述元素类型。descriptor 保存来源身份、真实写端根尝试、容量和已确认终态；恢复用 Core 的泛型 factory 重建端点，不把端点交给 Codec。

`StreamRecord` 的身份由 Scope/NodeID/Key/Group 和 Generation 构成，另存 SourceID/Resumable、LastSequence、AckedThrough、Pending 和 Cursor。Pending 按序保存仍需确认的消息，稀疏 Ack 不会把最大序号误当连续前沿。恢复一次提升一个代际，同轮重复 Open 认回同一代际；旧交付不能确认新代际。

活跃来源必须显式可续读，业务从 `StreamWriter.Position()` 返回的位置开始并用 `SendAt` 保存新位置。completed 来源保留完整 NodeRecord，恢复只重建未确认尾部；已确认来源的尾部仍可 Ack。失败来源仍拒绝 Open/Send/Ack，失败祖先、伪造引用和未来代际均拒绝。合法已确认的 Sub 别名可在历史身份、来源根已 failed 和有效祖先核验后，恢复为无消息、无 Open 的只读失败端点，让更深一层生产者的父链自行处理业务失败；这不重跑别名、不升级代际、不授权确认，也不接受 canceled 为业务失败。

真实失败的 `Finish` 将实际写端根尝试和该根全部已打开流的 failed 终态在同一次 Apply 中确认，不提前确认父 Sub 或父 Retry 的结论。Core 后续逐个 Finish 和 FailAttempt 只做严格幂等收尾；旧来源仍不得 Open、Send 或 Ack，位置、游标和代际不得改变。canceled 终态不是这条业务失败事实。

旧格式 2 可能只保存部分 failed 终态而根仍 active。Resume 先完整校验全部历史，再以一次可靠 CAS 收敛仍有效作用域内同根全部端点和根失败；修复 Apply 报错立即停止，由下次 Load 认回。不修改 confirmed/failed 祖先下的旧成果，不猜修损坏历史；同根 failed 与 canceled/成功终态混合视为不一致。普通格式 1 仍按原兼容路径，不允许增量字段。该修复不增加存储字段，也不补充任何 Retry 预算。

`MessageAttemptRecord` 保存输入来源/组/序号、处理父尝试、Index/Limit 和状态；重启不补预算。`ManagedRecord.State` 是显式完整快照，Values 是已到达普通值。消息输入 Ack、状态、普通值和输出消息在一次 typed ChangeBatch Apply 中原子确认；前置条件校验失败不发布批内任何变更，提交结果不确定时停止并由新会话 Load 裁决。Stop 显式结束读取义务，不伪装逐条 Ack。外部副作用仍需要业务幂等、事务或 outbox。

每个 confirmed 消息尝试都必须对应以 Scope/Parent/NodeID 定位、归属字段相同的 `ManagedRecord`；反向也要求每个容器至少有一条同归属的 confirmed 消息。即使 State 和 Values 都为空，合法消息提交也保存这个带身份的容器。恢复若发现容器缺失或归属错误，会在恢复事件、Start、业务调用及 Revision/Generation 推进前拒绝，不把丢失的状态当作初始零值。手动 Ack 不创建消息尝试，不要求容器；尚未确认的 active/failed 消息也不要求已有容器，格式 1 的无消息记录仍可恢复。历史 confirmed 记录不因父尝试或祖先后来失败、输入流代际后来提升而损坏，但失败祖先下的成果不能复用。这些是记录完整性检查，不表示健康 filestore 会自行丢失容器，也不是对任意篡改的认证。

消息已确认、完整节点仍 active 时，根 `CheckpointAttempt.Values` 从同 Scope/Parent/NodeID 的 ManagedRecord.Values 解码普通成果（ready 或 skipped），恢复进 Core 的完整链暂存外帧，完整节点 Commit 后才发布执行槽。认回不增加修订号、不重新处理已 Ack 输入；失败祖先不允许认回。流引用、未声明或重复键、类型/编码版本错误和 pending 都被拒绝；Retry 及 OpenMessage 的嵌套 CheckpointAttempt.Values 始终为空。此接缝不增加存储字段。

## Codec 契约

以下是公开接口的签名示意，依赖标准库 `reflect`；不是完整业务程序：

```go
type Codec interface {
	Type() reflect.Type
	ID() string
	Version() string
	Encode(any) ([]byte, error)
	Decode([]byte) (any, error)
}
```

注册按 `reflect.Type` 比较。Type、ID、Version 必须有效且非空；重复类型或 ID 返回 `ErrConflict`，不替换旧项。零值 `Codecs` 可注册自定义项；`NewCodecs` 额外提供内置标量和 `[]byte` 的 JSON 编码器。

Encode/Decode 必须同步、并发安全，不修改业务值，相同值编码相同字节。Decode 返回值须可赋给 Type。channel、函数与 `unsafe.Pointer`，包括嵌套声明和接口动态值，均被拒绝。

`RegisterJSON[T]` 只适合递归具体类型。含接口的字段或容器需要自定义 Codec；标准 JSON 不能重建它们的动态 Go 类型。JSON 编码要求字符串、字段与 map 键有效 UTF-8；任意字节可用 `[]byte` 或自定义编码。JSON `null` 对 pointer/slice/map 保留 typed nil，对标量拒绝。未知结构体字段、尾随内容及非法原始 UTF-8 会被拒绝。其他标准库 JSON 限制仍适用。

## Store 与 Session

以下是公开接口的签名示意。实际类型位于 `durable` 包；`RunRecord` 指本页定义的记录，`context` 为标准库包：

```go
type Store interface {
	Acquire(context.Context, string) (Session, error)
}

type Session interface {
	Load(context.Context) (RunRecord, error)
	Initialize(context.Context, RunRecord) error
	Apply(context.Context, ChangeBatch) (CommitAck, error)
	Close() error
}
```

Store 须并发安全，不同 RunID 可以独立使用。Acquire 检测到同一 RunID 已被持有时返回 `ErrBusy`，不等待该运行释放所有权；连接容量及网络检测仍可能需要等待，具体边界由后端说明。Session 从 Acquire 到 Close 独占该运行。PostgreSQL 的本地 pending 检查、30 秒申请上限及关闭清理边界见[存储指南](../guide/storage.md)。

Load 校验完整性后返回隔离完整投影；仅无检查点时返回 `ErrNotFound`，不能把旧格式、损坏或权限错误当作不存在。Initialize 只在检查点不存在时原子发布 Revision `1` 的合法初始空执行状态，核对会话 RunID、当前逻辑格式、业务身份及空根作用域；不是导入历史记录的入口。业务通过 Runtime Run 构造初始状态，不应手写业务 records。

Apply 是唯一普通写入口：typed ChangeBatch 原子核对身份、业务版本、expected/next revision 严格加一及精确对象前置条件，成功返回与批头精确匹配的 CommitAck。前置条件或 CAS 冲突返回 `ErrConflict`，不发布部分变更；uint64 修订号耗尽必须拒绝。没有兼容 Save、可选 DeltaSession、RequireDelta 或全量回退。

耐久后端保存初始快照、连续不可变批及权威 head。快照摘要、批 wire 摘要、父修订号和链锚点由后端验证与维护；物理链不属于 ChangeHeader。新会话必须权威加载并校验链，普通 Apply 不得每次重放整个历史或重写完整快照。Initialize 可以写完整初始快照；显式快照合成属于单独维护能力，不是普通提交的回退。

`durable.Maintenance` 是 Store 级接口，MemoryStore、FileStore、SQLite 与独立 PostgreSQL 已实现；第三方 Store 可不提供：

```go
type Maintenance interface {
	InspectStorage(context.Context, string) (StorageState, error)
	LoadRevision(context.Context, string, uint64) (RevisionImage, error)
	CompactStorage(context.Context, CompactStorageRequest) (StorageState, error)
}
```

`StorageState` 包含 RunID、Generation、BaseRevision/BaseChain 与 HeadRevision/HeadChain，是一次原子观察而非后续操作的授权。Generation 从 1 开始，仅表示发布时改变，普通 Apply 只改变业务 head。`RevisionImage` 返回隔离的 Record 和精确修订号的 Chain，不以更新记录替代请求，不建立持久 pin。`CompactStorageRequest` 含 RunID、正数 ThroughRevision/ExpectedGeneration；generation 必须仍匹配，目标须在当前 base/head 之间，否则 `ErrConflict`。

合成修订号 R 的新快照摘要独立于原提交链 `C(R)`，必须保留该锚点，不由新摘要重置链；发布只改变表示 generation/base，业务 revision/head 不变，后续 suffix 保留。所有历史修订、原快照与批全部保留，无 GC。保留链缺失或损坏是 `ErrInvalidRecord`，不能冒充 `ErrHistoryExpired`；后者为显式 retention 删除后的语义预留，`ErrPinned` 为保护历史阻止回收预留。未支持能力可用 `ErrUnsupportedCapability`，维护预算超限用 `ErrLimitExceeded`。物理合成不执行业务代码；业务语义历史压缩、retention 与 `ReplayCreateDerived` 尚未完成。

FileStore 与独立 PostgreSQL 的全部维护操作使用运行独占锁，活跃 Session 返回 `ErrBusy`；SQLite 的 InspectStorage/LoadRevision 使用一致读事务，不需要 runlock，但 CompactStorage 排他。MemoryStore 在锁外重建、发布时校验 generation，仍不耐久。构建取消不发布；发布后错误或取消可能结果不确定，应重新 Inspect 核对 generation/base，不盲重试。distributed PostgreSQL 使用独立的 namespace-qualified 维护 API（Coordinator.InspectStorage/CompactStorage/FindMaintenance）、maintain 授权与 pin/receipt；当前无对外 LoadRevision，不能视为满足上述 root 接口。

Runtime 仅在精确 CommitAck 确认后推进内存状态和发布成果。Initialize/Apply 可能已提交却返回错误；此时停止，不回滚或盲重试，等待所有权释放后由新会话 Load 裁决。Close 幂等、等待在途操作退出并释放会话所有权；关闭后的 Load/Initialize/Apply 必须返回错误。

## 内置 Store 的具体保证

MemoryStore 的零值可用，Initialize 复制初始记录，Apply 校验后仅修改受影响对象，Load 返回隔离副本。它只提供进程内互斥与 CAS，不提供跨进程所有权或退出持久性。

FileStore 必须通过 `filestore.New` 创建。它使用每个 RunID 的 OS 文件锁，锁在 Close 或进程退出时释放，锁文件保留。提供 Windows，以及 Linux、macOS、FreeBSD、OpenBSD、NetBSD、DragonFly 的锁实现；其他平台拒绝取得会话，不退化为无锁运行。仍须使用支持所需锁与同目录原子替换的文件系统。

FileStore 使用不可变首快照和批文件，以同目录同步和原子替换的小 `.head` 发布权威状态，不再以单个 `.json` 为权威。未被 head 引用的文件不能认作已提交；head 发布后确认失败可能造成提交结果不确定。当前 `.head` manifest 为物理格式 `2`，旧 manifest `1` 和旧 `.json` 均明确拒绝，不自动清除、修改或迁移；新版使用新目录。SQLite 与独立 PostgreSQL 为物理 schema `3`，distributed PostgreSQL 为独立 schema `6`，旧 v1–v5 或不匹配 schema 明确拒绝，不写入、删除或迁移旧数据，新版使用新库。逻辑记录 format `2` 与协调 protocol `1` 不因此改变。单宿主保证不扩展为分布式租约、fencing 或断电持久性。增量写入也不承诺所有规模都更快，测量范围见[检查点成本](../guide/checkpoint-cost.md)。

distributed mutation 的身份、结果凭据与诊断分开保存：永久 `mutation_guards`、独立 `mutation_receipts` 与诊断 `events`。`Coordinator.FindMutation(ctx, MutationQuery)` 匹配 Identity、Mutation ID/Digest、Operation 与闭区间 RequestedWindow；ExpectedEpoch/ExpectedRevision 可选指针 nil 表示未知，非 nil（含 0）精确匹配。`Found` 只裁决精确协调 mutation，不证明外部副作用或授予 lease/执行权限；`AbsentWithinWindow` 要求完整覆盖请求窗口；`Expired` 要求请求下界早于覆盖且观察已达到请求上界，未来上界会被拒绝。缺回执、缺席或过期均不证明从未提交，也不授权业务重放。

Coverage.From 来自数据库持久 `receipt_coverage_floor`；Coverage.Through 是持运行锁采样的数据库观察上界，并持久为 `receipt_observed_through`，不使用客户端时钟。保留回执可早于 floor，覆盖内应有却缺失的回执报错。该 query 需可读写主库，不可用只读事务或副本；权限仍为 namespace `query`，不授予应用直接表 DML。RetentionPolicy 仅提供可选 TTL 校验/CAS 请求契约，不代表已实现策略落库、实际 retention 删除或 GC；replay 与语义压缩仍未完成。

所有元数据字符串及 map 键必须有效 UTF-8。写前拒绝非法元数据，保留旧文件与 Revision；读时拒绝非法原始 UTF-8 和未配对 JSON Unicode 代理项。合法 Unicode 保留，Data 字节不受元数据 UTF-8 限制。

## 下一步

处理损坏、冲突或保存不确定时，阅读[错误处理](errors.md)。实际检查文件记录的程序见[存储](../guide/storage.md)，业务版本与定义兼容性见[执行契约](execution.md)。
