# 指南概览

Pulse Durable 为 Pulse 图增加检查点存储与恢复。适合已经有明确节点边界、希望在程序中断后复用已完成步骤的工作流，例如分阶段文档处理、批量任务或长时间计算。

Pulse 负责图定义、依赖调度、切面和本次执行的观测；Durable 负责保存完整节点成果、尝试状态，并在新执行中恢复有效成果。宿主仍负责启动进程、提供输入、选择运行身份和处理外部副作用。

## 开始之前

需要 Go 1.25 或更高版本，以及能够下载 Go 模块的环境。Durable 尚无正式 release，教程固定使用公开开发修订。你不需要先实现自己的存储后端：最小示例使用 `MemoryStore`，恢复教程使用自带的 `filestore`。

如果还不熟悉 Pulse 的图、类型化 Key 和声明出口，可以先阅读 [Pulse 快速开始](https://luo-root.github.io/pulse/guide/quickstart)。Durable 教程会给出自己的完整代码与依赖版本。

## 按顺序完成第一次恢复

1. [安装](installation.md)：创建模块，固定依赖，检查 Go 与 Pulse 版本。
2. [快速开始](quickstart.md)：运行一个节点，读取值为 `42`、状态为 `ready` 的声明出口。
3. [进程退出后恢复](recovery.md)：在下游节点主动退出进程，再用新进程读取检查点并完成运行。

第三步中，上游只进入一次，下游在退出前后各进入一次。这是理解 Durable 的关键：已确认节点直接复用；未确认节点从函数开头重做。

## 将恢复接入业务

| 需求 | 操作页 |
| --- | --- |
| 将多个阶段组合为子图，并保留 Retry 进度 | [子图与重试](subgraphs.md) |
| 保存结构体、命名类型或特殊编码 | [值编码](codecs.md) |
| 选择存储目录，检查锁占用，接入其他后端 | [存储](storage.md) |

业务函数已经调用外部系统，却尚未完成检查点确认时，恢复可能再次调用它。请在接入业务时确定幂等键、事务或 outbox 的边界。

## 核对规则与处理错误

任务页讲操作过程；[参考](../reference/index.md)集中定义 API 和持久记录的约束：

- [执行契约](../reference/execution.md)：RunID、Version、恢复兼容性、取消和恢复事件。
- [数据模型](../reference/data-model.md)：作用域、尝试、值状态、编码与 Store 的 CAS 协议。
- [错误处理](../reference/errors.md)：参数错误、占用、冲突和 `CheckpointError` 的不同处理方式。

遇到失败时，先按错误分类查明原因。存储保存报错可能发生在实际写入之后，应停止本次运行并在恢复时重读记录。重复调用 `Resume` 不会增加已耗尽的 Retry 预算。

## 当前适用范围

支持普通值、受管流、显式消息状态和静态子图；流的可靠边界见[数据模型](../reference/data-model.md)。`filestore` 支持本机进程退出后的恢复；`MemoryStore` 仅在同一进程存活期间保存记录。Durable 不提供持久等待、分布式运行管理、deterministic replay 或 exactly-once 外部副作用保证。

下一步：[安装](installation.md)。
