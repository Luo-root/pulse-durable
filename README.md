# pulse-durable

[English](README_en.md)

`github.com/Luo-root/pulse-durable` 是独立的 Pulse 检查点执行库：定义仍可复用，`Run` / `Resume` 每次创建新的 `pulse.Execution`。完整节点链确认存储后才发布输出；恢复直接复用确认成果，未确认节点从函数开头重新进入。

首期方案 A 保存 Retry 游标和子图作用域。父尝试未结束时，恢复认回原作用域并复用已经确认的子节点；父 Retry 明确失败后，新尝试使用新作用域，旧子成果不会复活。根完整链只有一次预算，最终真实失败的 RunID 不能通过反复 Resume 补充 Retry 次数。

## 使用

需要 Go 1.25+。以下示例用 MemoryStore 展示协议；跨进程恢复应使用 `filestore.New(dir)` 或另一个真正持久的 Store。

```go
package example

import (
    "context"

    "github.com/Luo-root/pulse"
    durable "github.com/Luo-root/pulse-durable"
)

func execute(ctx context.Context, store durable.Store, resume bool) (*pulse.Execution, error) {
    codecs := durable.NewCodecs() // bool、string、整数、浮点、[]byte
    rt, err := durable.New(store, codecs)
    if err != nil {
        return nil, err
    }
    input := pulse.NewKey[int]("input")
    output := pulse.NewKey[int]("output")
    g, err := pulse.New("example", "加一")
    if err != nil {
        return nil, err
    }
    if err := g.DeclareInput(pulse.NewPort(input)); err != nil {
        return nil, err
    }
    if err := g.DeclareOutput(pulse.NewPort(output)); err != nil {
        return nil, err
    }
    if err := g.Add(pulse.NewNode("add", "加一", pulse.Requires(input), pulse.Provides(output),
        func(rc *pulse.RunCtx) error {
            v, err := pulse.Get(rc, input)
            if err != nil {
                return err
            }
            return pulse.Set(rc, output, v+1)
        })); err != nil {
        return nil, err
    }
    var in pulse.Inputs
    if err := pulse.Seed(&in, input, 41); err != nil {
        return nil, err
    }
    options := durable.RunOptions{RunID: "order-123", Version: "business/1"}
    if resume {
        return rt.Resume(ctx, g, in, options)
    }
    return rt.Run(ctx, g, in, options)
}
```

RunID 必须是非空有效 UTF-8 文本，Run/Resume 在访问 Store 之前拒绝非法 RunID，保持参数错误分类。`Run` 只创建新记录，重复 RunID 返回 `ErrConflict`。`Resume` 要求记录存在，并重供相同输入。两者阻塞到本次执行结束，并尽可能返回可查询的 Execution；成功后用 `pulse.ReadOutput(e, key)` 读取声明出口。`MemoryStore` 构造器是 `durable.NewMemoryStore()`。

可直接复跑的示例：

- [基础 Run / Resume](example_test.go)：用 MemoryStore 展示已确认节点复用和未确认节点重进。
- [父图挂载文档处理子图](subgraph_example_test.go)：用真实 filestore，在当前目录创建独占目录并在结束时清理，演示 `In` / `Out` 接线和父图出口读取。
- [真实进程退出 / 重启验收](integration/recovery_test.go)：子进程在持久边界直接退出，新进程重建 Graph 并恢复，覆盖子图、Retry 和不确定提交等场景。

子图示例通过 `pulse.Sub` 静态挂载 C1 下载 → C2 提取 → C3 摘要（用字符串变换模拟业务体），父挂载步骤安装 `Retry(3, 0)`。第一轮在 C1/C2 检查点已确认、C3 已进体并读到 C2 后取消宿主 context，各步执行次数为 1/1/1。随后重建 filestore、Runtime 和等价 Graph，重供相同输入，用相同 RunID 与 Version 调用 Resume：父 Retry 认回原 active 尝试作用域，恢复阶段次数为 0/0/1，累计为 1/1/2，最终用 `pulse.ReadOutput` 从父图声明出口取得摘要。

该示例只在同一进程内重建对象；真正退出 / 重启的证据见上面的 integration 测试。未确认业务体及其外部副作用可能重做，调用方仍须提供幂等、事务或 outbox。

在本仓库根目录执行：

```text
go test -run '^ExampleRuntime_Resume$' -v .
go test -run ExampleRuntime_Resume_subgraph -v .
go test -run TestCheckpointRecoveryAcrossProcessExit -v ./integration
```

## 版本、编码与存储

业务 `Version` 显式必填。结构指纹覆盖完整 Topology、全部节点与端口的真实类型所对应的 Codec ID/版本；函数、闭包、固定绑定值以及 Aspect 顺序/参数/语义变更由宿主升级业务版本。子图再次打开还会核对实际绑定输入，不能把新输入和旧输出混用。

自定义类型使用 `durable.RegisterJSON[T](codecs, "稳定类型ID", "编码版本")` 或实现 `Codec`。注册按 `reflect.Type` 比较，重复类型或 ID 不替换。编码须确定、同步、并发安全且不修改业务值；接口类型（包括结构体或容器内的 interface）须用自定义 Codec 恢复其动态类型，JSON 便捷入口递归拒绝这类声明。JSON 字符串、嵌套字段及 map 键须为有效 UTF-8，防止静默替换及输入指纹碰撞；任意字节使用 []byte 或自定义 Codec。自定义 Codec 可以使用零字节编码，ready 状态与校验和仍须完整保存。`ready` 的 typed nil 和声明为 interface 的 nil 是值；零值也保持 ready。`skipped` 没有编码值，正常跳过不使运行失败。JSON `null` 对 pointer/slice/map 保留 typed nil，对标量拒绝。

Store 提供 `Acquire(ctx, runID)` 独占会话；同 RunID 立即返回 `ErrBusy`。Session 的 `Save(ctx, record, expectedRevision)` 原子 CAS：新记录修订号 1 / expected 0，后续修订号必须为 expected+1。`Close()` 释放所有权。RunRecord 是可 JSON 存取的纯数据，值记录包含编码校验和，不保存业务错误文本。文件 Store 要求全部元数据字符串和 map 键为有效 UTF-8：写前拒绝非法元数据，旧文件字节与 Revision 不变；读取也拒绝原始非法 UTF-8 和未配对的 JSON Unicode 代理项。合法 Unicode 原样保留，编码值的 `Data []byte` 是不透明字节，不受 UTF-8 限制。它不是加密容器，存储目录和备份的访问控制由宿主管理。

每次变更先复制 proposed record，Save 成功才更新内存。Save 可能实际写成却报错：本次停止，不撤销、不自动重试，后续 Resume 重新读取权威记录。Store / Codec / 恢复回调的实际可靠故障统一为 `*pulse.CheckpointError`，根恢复前置操作与子图相同；参数错误、正常记录冲突与所有权占用仍保留原分类。Codec 和回调错误的公开正文脱敏，`errors.Is/As` 经 `Unwrap` 保留原原因，`panic(error)` 也保留 cause。可靠故障不能被父 Retry、业务 skip 或外层切面吞掉；宿主取消及可靠失败保留 active 尝试。业务清理节点 context 后返回真实错误，仍确认根失败，Resume 不补充根预算。`RunOptions.OnEvent` 同步报告 `EventRestored`，不携带值、不伪造 Pulse 业务耗时；回调需并发安全，错误或 panic 停止本次，已确认数据不回滚。

## 边界与验证

首期只支持非流式值，不支持 channel、持久等待或分布式运行管理。未确认节点可能再次产生外部副作用，需宿主提供幂等、事务、防重或 outbox；不承诺 deterministic replay 或 exactly-once。MemoryStore 只证明进程内协议；文件后端面向本机单宿主，其 OS 锁和退出/重启验证见 `filestore/` 与 `integration/`。

所需 Pulse 桥接变更随对应 PR 评审，依赖修订由 `go.mod` 固定；本地联调可通过独立的 `go.work` 指向 Pulse 工作树。这不是桥接 API 已发布的声明，两仓发布由维护者决定。

```text
go build ./...
go vet ./...
go test -race -count=1 ./...
```
