# 错误处理

本页用于决定 Run/Resume 失败后的操作。先识别可靠检查点错误，再检查具体原因；不能只根据错误文本或一个底层 sentinel 判断业务是否成功。

## 错误分类

| 类别 | 判定与场景 | 处理方式 |
| --- | --- | --- |
| 参数或定义错误 | nil Runtime/context/图、空 RunID/Version、非法 RunID UTF-8、无效图、缺少 Codec、不支持的类型或流式声明 | 修正调用或配置；这些前置契约错误不归为 CheckpointError |
| `durable.ErrBusy` | Acquire 时同一 RunID 已有活动会话 | 由宿主协调现有所有者；Acquire 不等待，不能删除锁文件解除占用 |
| `durable.ErrNotFound` | Resume 加载时没有该 RunID 的记录 | 核对存储与身份；确实是新业务时使用 Run |
| `durable.ErrConflict` | RunID 已存在，或根恢复身份、Version、定义、输入不一致；也用于注册或 CAS 冲突 | 核对原始契约与调用方式，不覆盖旧记录 |
| `*pulse.CheckpointError` | Store、Codec、恢复协议或恢复回调的可靠操作失败 | 停止本次，处理原因；后续 Resume 重新读取权威记录 |
| 宿主取消或到期 | 当前宿主 context 确实被取消或到期 | 用有效的新 context 恢复原运行；保留原身份与兼容条件 |
| 真实业务失败 | 节点或业务切面返回真实错误 | 消耗已有 Retry 预算；最终失败不会通过 Resume 获得新预算 |

一个 `CheckpointError` 的原因链仍可能匹配 `ErrConflict`、`context.Canceled` 或 `pulse.ErrSkipped`。例如 CAS 冲突发生在可靠保存边界时，它仍首先是可靠失败；不能降为普通参数冲突或正常跳过。根 Run/Resume 的正常身份冲突、记录不存在和占用保持原分类。

## 用 errors.Is/As 检查

不要依赖 `err == durable.ErrBusy` 或解析显示文本。下面是分类函数的局部声明，需要导入 `errors`、`pulse` 与 `durable`。返回标签用于宿主选择处理路径，不代替运行结果或 context 核对。

```go
func classify(err error) string {
	if err == nil {
		return "success"
	}
	var checkpoint *pulse.CheckpointError
	if errors.As(err, &checkpoint) {
		return "checkpoint failure"
	}
	if errors.Is(err, durable.ErrBusy) {
		return "run busy"
	}
	if errors.Is(err, durable.ErrNotFound) {
		return "record not found"
	}
	if errors.Is(err, durable.ErrConflict) {
		return "contract conflict"
	}
	return "argument, cancellation, or business error"
}
```

先识别 `CheckpointError`，随后可通过 `errors.Is/As` 检查其底层原因，定位具体修复动作。`CheckpointError.Operation` 描述可靠操作，`Err` 与 `Unwrap` 保留错误链。

## 保存结果不确定

Save 报错有两种可能：

1. **没有提交**：磁盘或后端仍保留旧记录，节点尚未确认。
2. **已经提交**：记录已更新，只是在随后的同步或确认步骤报错。

Runtime 停止本次执行，不把 proposed record 当成自己的已确认内存状态，不自动回滚或重复 Save。不要据此用旧记录覆盖后端，也不要盲目重试旧 expectedRevision。

处理原因后，新建或复用 Runtime 调用 Resume。它重新 Load：实际已经确认的有效成果会复用；实际仍未确认的节点会重做。Save 返回错误本身不能证明某个业务动作必须重做或不重做。

已确认成果不会因为后续错误而回滚。关闭 Session 失败也会作为 CheckpointError 与返回错误合并；即使实际 Execution 已完成，Runtime 调用仍可能因收尾错误返回失败。此时应处理返回错误并查明存储/所有权状态，不能仅按 Snapshot 的 completed 判定整个调用无误。

## 可靠失败不会变成正常跳过

正常 skip 表示没有值，是可以成功收尾的业务结果。可靠检查点失败表示无法安全确认或恢复运行，两者不能混用。

当 Store、Codec 或恢复回调返回的原因链包含 `pulse.ErrSkipped`，外层 `CheckpointError` 仍保持失败语义。父 Retry 不能把可靠失败当作普通业务错误继续重试；外层业务切面即使吞掉内层返回错误，也不能使整个执行成功。不要自行用一个 `errors.Is(err, pulse.ErrSkipped)` 分支将这些错误降为 skip。

失败执行读取出口时，`ReadOutput` 返回失败原因、零值和空状态。不能把这个零值当作正常缺失继续计算。正常 skipped 出口则返回零值、`skipped` 和 nil error。

## 取消与真实业务失败

是否为宿主中断，应结合当前宿主 context 和实际失败分类判断。错误链中有 `context.Canceled` 或 `context.DeadlineExceeded`，并不单独证明宿主取消。例如恢复回调返回一个包装 context 错误，仍是可靠失败；节点取消自己的局部 context 后返回一个真实业务错误，仍可能确认真实失败。

宿主中断与可靠失败不会仅因中断把仍活跃的尝试变成业务失败；不确定保存的状态以再次 Load 为准。已确认的业务失败会推进失败游标，旧父尝试的后代不可复用。

完整节点根尝试真实失败后，该 RunID 的节点预算已经终结。即使问题后来修好了，Resume 也不会分配新的根预算；如果业务需要重新开始，应根据应用策略创建新 RunID。内置 Retry 的次数是持久契约，不是每个进程各自重新计数。

## 错误正文与日志

Codec 和恢复回调的错误或 panic 转换为只含操作信息的显示文本，不拼入业务值、编码字节或原始 panic 文本。返回的 error 原因仍可通过 Unwrap 检查，`panic(error)` 也保留原因；非 error panic 不提供原始文本。

Store 返回的错误也通过操作包装保留原因并控制正文；Store panic 被转换为操作失败，不应依赖其 panic 原因被保留。普通业务错误、运行标识和宿主自行解包的 cause 不自动获得同样的脱敏保证。日志应明确选择字段，避免输出业务值或原始底层错误中的敏感信息。

RunRecord 不自动保存业务错误文本，但会保存编码输出。错误脱敏不等于检查点数据加密，目录与备份仍须按业务要求控制访问。

## 故障排查顺序

1. 保留 RunID、业务版本、存储位置和返回错误分类，避免修改原记录。
2. 存在 CheckpointError 时先定位可靠操作与底层原因；不要自动启动新的业务运行。
3. Busy 时核对当前所有者；NotFound 时核对目录和身份；Conflict 时核对方法、版本、定义与输入。
4. 修复后按原兼容条件 Resume，由重新加载的记录决定有效成果和剩余预算。
5. 新业务条件或需要新的完整预算时，明确分配新 RunID，并继续管理旧记录的保留策略。

## 下一步

实际恢复操作见[进程退出后恢复](../guide/recovery.md)。版本核对见[执行契约](execution.md)，CAS 与记录完整性见[数据模型](data-model.md)。
