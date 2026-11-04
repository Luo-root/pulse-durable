# 统一生产框架对照

## 2026-11-01：消息索引与文件历史专项

本节仅比较 Durable 两项修改前后的指定路径，不更新 Pulse／Eino／Temporal 的跨框架排名。基线为 Durable `16d7bf9fbee1b496fb1ffb95a25f62ee2f19e6fe`，对照为本次源码修改；两侧都使用公开 Pulse `v0.5.2`、Go `1.26.10`、Windows amd64、Intel Core i9-14900HX、`GOMAXPROCS=2` 和 `GOWORK=off`。基准串行执行，期间暂停本轮 PG、race 和其他基准；未清空文件缓存，也未控制所有系统进程。以下均为全部样本的中位数，不是线上分位数。

消息对照在同一基线源码上仅覆盖 `attempts.go`、`runtime.go`、`delta.go`、`messages.go`，隔离消息索引变更。`BenchmarkMessageIndexRuntimeManaged` 运行完整受管消息处理，校验结果并保留全部 confirmed 消息历史；每个样本运行一次，每组 3 个样本。新索引在权威加载或初始化确认后建一次，普通确认仅更新本批消息，正常查询未命中不重建。

| 消息数 | 旧扫描 ms/运行 | 增量索引 ms/运行 | 旧累计分配 MB/运行 | 新累计分配 MB/运行 |
| ---: | ---: | ---: | ---: | ---: |
| 128 | 9.851 | 7.027 | 8.824 | 4.478 |
| 512 | 96.115 | 14.951 | 88.586 | 17.730 |
| 2048 | 1434.037 | 62.993 | 1209.992 | 70.779 |

文件对照固定 origin/base 为 revision 1，只增加保留批数 H；准备写入不计时，每份夹具计一次操作，每组 5 个样本。公共对照夹具允许旧实现按其原规则推进 generation，新实现按已批准契约执行 samebase no-op，避免连续测量改变旧实现的历史规模。`LoadRevision(1)` 直接读取 origin；samebase 请求仍校验当前所需元数据和快照。完整审计不在这两条快捷路径内。

| 保留批数 H | 旧 LoadRevision(1) ms | 新 LoadRevision(1) ms | 旧 samebase ms | 新 samebase ms |
| ---: | ---: | ---: | ---: | ---: |
| 128 | 35.081 | 0.706 | 36.454 | 0.682 |
| 256 | 73.571 | 0.744 | 69.565 | 0.684 |
| 512 | 261.411 | 0.734 | 164.402 | 0.714 |
| 1024 | 299.611 | 0.697 | 280.153 | 0.705 |

首轮离群样本未删除。H=1024 的旧／新读取范围分别为 275.522–12019.477 ms 与 0.640–9.358 ms；128 消息的新旧耗时范围有重叠，不能从三个样本推断小规模的统计显著性。H=1024 读取的累计分配中位数从 75,295,696 B 降为 16,832 B。MB 均为十进制；累计分配不等于存活堆、RSS、WAL、网络或设备 I/O。索引另有随消息组数增长的常驻元数据；这些结果不证明整个 Runtime／Apply 均为线性或所有历史查询均为常数成本。当前路径与完整 `AuditHistory` 的职责见[存储](storage.md)。

## 2026-10-17／23：跨框架历史记录

> 2026-10-23 新测量：[正式版 SQLite 与恢复报告](https://github.com/Luo-root/pulse/blob/main/site/reference/sqlite-formal-20261023.md)。Go 1.26.0、Core v0.5.0、Durable/官方 sqlitestore v0.1.0；有效 171 条，36 次恢复全 ok，另保留 45 条 Temporal checkpoint 配置错误，不归为其性能失败，整轮 COMPLETED_WITH_ERRORS。仅更新可靠确认、Temporal 原生持久化基线与恢复，未重跑完整执行/观测/资源。本页历史身份、后端与数字不变。

新一轮固定使用 **Pulse／Durable、Eino、Temporal**，执行、观测、资源、可靠确认与恢复始终同三列。[完整报告与四图](https://github.com/Luo-root/pulse/blob/main/site/reference/production-comparison.md)、[234组精简CSV](https://github.com/Luo-root/pulse/blob/main/benchmarks/reports/2026-10-17-production/summary.csv)、[版本与原始哈希摘要](https://github.com/Luo-root/pulse/blob/main/benchmarks/reports/2026-10-17-production/identity.json)和[复跑代码](https://github.com/Luo-root/pulse/tree/main/benchmarks/production)统一保存在 Pulse，不复制整套原始产物到两仓。关联 [Pulse #427](https://github.com/Luo-root/pulse/issues/427)、[本仓 #41](https://github.com/Luo-root/pulse-durable/issues/41)。

测量日期2026-10-17。被测公开 Pulse 为 `v0.4.2-0.20261017111210-aa02dd107a43`，Durable 为 `v0.0.0-20261017105818-71c693fc82ef`；Eino v0.9.21，Temporal SDK v1.50.0 / CLI v1.9.1 / Server v1.32.0。共同业务宿主用Go1.26.0，官方Server二进制构建为Go1.26.8。独立评测模块同时固定两仓最新版，无本地replace；Durable生产go.mod仍保留原依赖，不将组合验收冒充正式依赖升级。

## 可靠维度怎么读

| 维度 | Pulse / Durable | Eino | Temporal |
|---|---|---|---|
| 普通执行 | Pulse内存执行 | compose Graph内存执行 | 原生持久化Service/Activity |
| 可靠保存 | Store确认完整节点链后发布 | 普通成功不自动逐节点保存；主动interrupt后保存checkpoint | 原生事件历史与Activity确认 |
| SQLite | 评测宿主，经现有Store扩展 | 评测宿主，经CheckPointStore扩展 | CLI内嵌真实Server |
| 进程恢复 | 确认前缀后强杀宿主，再Resume | 主动checkpoint后退出，新进程恢复 | 无在途Activity前缀后强杀worker；另测Server重开 |

SQLite配置WAL/FULL，保留不同事务布局与写入粒度；Durable评测宿主把完整RunRecord编码为JSON，存入SQLite的record BLOB列并按Revision做CAS；只借filestore系统锁提供本机运行独占，不调用其Load/Save，也不额外保存第二份JSON运行文件。这是测试适配器，不能宣传为已交付的生产SQLite后端、分布式租约或fencing。Eino普通组没有同义逐节点确认测量不代表扩展机制不能承载持久应用；主动中断不等于任意crash恢复。

长链、rounds=1可靠复用中位数（ms）：

| 节点数 | Pulse + Durable | Eino | Temporal |
|---|---:|---|---:|
| 1 | 7.1326 | 另测主动中断 | 253.826 |
| 10 | 45.5552 | 另测主动中断 | 197.6637 |
| 100 | 503.4885 | 另测主动中断 | 5049.4483 |
| 1000 | 8553.1903 | 另测主动中断 | — |
| 10000 | — | 另测主动中断 | — |

Durable重复3次；Temporal引用本轮原生持久化同点的5批结果，绝非第二份同义实验。`—`为整案例没有完成成本，不填零或外推。Durable万节点三种图均触及60s预算，不能说这轮已经验收万步可靠规模。

## 结果和证据边界

四矩阵共1062条计划身份、234组，947完成、25触及时间边界、90不再重复，无业务结果校验错误；36次真实恢复全部完成。恢复长链N=10/100/1000，rounds=256；确认前缀分别5/10/10。Pulse父进程另读SQLite和业务审计，Eino另读并哈希blob，Temporal独立客户端读原生历史确认没有在途Activity。最终结果和前缀不重跑均核对，不承诺任意在途副作用恰好一次。

普通案例包含首次加复用两整轮，恢复只继续已确认前缀后的后缀；恢复成功不表示普通整案例必在同一预算内完成。存储长度包含DB/WAL/SHM，不能当作累计写盘或每步成本。Go累计分配不等于RSS；Temporal执行进程分配不含独立Server，资源对比另用相同两轮工作量并统计整个进程树。

最新公开Core与Durable组合build/vet/全量race（含真实进程退出恢复）通过。新适配器全量race、真实Temporal错误/取消复用/历史并发、采样器独立反例与审查通过。完整报告还保留了Temporal服务不重开反而恢复较慢等不利或反常点，不在没有控制变量诊断时给因果结论。

[旧规模报告](./performance.md)和[旧检查点成本](./checkpoint-cost.md)保留历史版本与数字；它们不参与本轮统一框架排名。原始JSONL、日志、数据库、二进制及完整本机清单留仓库外证据包，不入Git。此次未发正式tag/release。
