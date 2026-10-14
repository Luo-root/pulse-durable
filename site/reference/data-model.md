# 数据模型

本页定义 Durable 的持久记录、编码状态与 Store/Session 协议。用于实现后端、调查恢复问题或核对自定义 Codec。业务操作见[存储](../guide/storage.md)和[值编码](../guide/codecs.md)。

## RunRecord

当前 `RecordFormat` 为 `1`。RunRecord 只包含可序列化数据，不保存函数、Go 调用栈或自动采集的业务错误文本。

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

RootScope 与尝试身份是 16 字节随机数的十六进制表示。它们保存在记录中，不能按图路径、内存计数或进程启动顺序重新生成。恢复仍须由宿主提供原始外部输入。

## ScopeRecord

| 字段 | 含义 |
| --- | --- |
| `Graph string`、`Path string` | 定义中的图身份与静态挂载位置 |
| `Inputs string` | 该作用域实际输入的指纹 |
| `Nodes map[string]NodeRecord` | 此作用域中已经确认的完整节点成果 |

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
| `AttemptConfirmed` | 节点根尝试与完整 NodeRecord 在同一次 Save 中确认 |

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

恢复要求记录的依赖闭合：如果一个已确认节点依赖图内上游产物，则其上游节点也须有确认记录。根尝试必须与 Scope、NodeID 及 NodeRecord 正确关联，并在同一次 Save 中确认。

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
	Save(context.Context, RunRecord, uint64) error
	Close() error
}
```

Store 须并发安全，不同 RunID 可以独立使用。同一 RunID 已被持有时，Acquire 立即返回 `ErrBusy`；Session 从 Acquire 到 Close 独占该运行。

Load 返回隔离副本，无记录时返回 `ErrNotFound`。Save 原子比较并交换，核对记录 RunID 与会话一致：

| 保存 | expectedRevision | 新记录 Revision |
| --- | --- | --- |
| 首次 | `0` | `1` |
| 后续 | 当前修订号 | 当前修订号加 `1` |

条件不符返回 `ErrConflict`，旧记录不变。修订号不能溢出。Close 释放所有权，可重复调用；关闭后的 Load/Save 必须返回错误。

Runtime 每次变更复制 proposed record，只有 Save 成功才推进自己的内存记录。Save 可能已写成却返回错误；此时停止本次，不回滚或自动重试，下一次 Resume 必须重新 Load。后端不能只提供无 CAS 的普通覆盖写。

## 内置 Store 的具体保证

MemoryStore 的零值可用，Load/Save 会复制记录，只提供进程内互斥与 CAS，不提供跨进程所有权或退出持久性。

FileStore 必须通过 `filestore.New` 创建。它使用每个 RunID 的 OS 文件锁，锁在 Close 或进程退出时释放，锁文件保留。提供 Windows，以及 Linux、macOS、FreeBSD、OpenBSD、NetBSD、DragonFly 的锁实现；其他平台拒绝取得会话，不退化为无锁运行。仍须使用支持所需锁与同目录原子替换的文件系统。

文件记录在同目录暂存、同步后替换；替换后确认失败可能造成提交结果不确定。本机单宿主保证不扩展为分布式租约、fencing 或断电持久性。

所有元数据字符串及 map 键必须有效 UTF-8。写前拒绝非法元数据，保留旧文件与 Revision；读时拒绝非法原始 UTF-8 和未配对 JSON Unicode 代理项。合法 Unicode 保留，Data 字节不受元数据 UTF-8 限制。

## 下一步

处理损坏、冲突或保存不确定时，阅读[错误处理](errors.md)。实际检查文件记录的程序见[存储](../guide/storage.md)，业务版本与定义兼容性见[执行契约](execution.md)。
