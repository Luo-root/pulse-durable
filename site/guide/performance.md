# 规模与性能

> 本页是旧版本的历史样本。合并后最新版与 Eino、Temporal 固定同三列的新对比见[统一生产框架对照](./production-comparison.md)，覆盖执行、观测、资源、可靠确认和恢复。

这份报告测量同一个长链任务在 1、10、100、1000、10000 个业务步骤下的成本：加上检查点、可靠记录和进程恢复以后，要付出多少时间、累计分配和写出。它与 [Pulse 纯内存报告](https://github.com/Luo-root/pulse/blob/main/site/reference/performance.md) 分组阅读，不能混成同一张速度榜。

测量日期为 **2026-10-17**，固定公开 Durable `v0.0.0-20261016143640-e47e51eda579`、Core `v0.4.2-0.20261016143631-7d70e1e14771`，对照 go-workflows [v1.4.2](https://github.com/cschleiden/go-workflows/releases/tag/v1.4.2) 与该版本依赖的 pure-Go SQLite `v1.27.0`。这是当时开发版本和各自后端配置的历史基线，不代表后续统一框架的生产对比；两种文件后端的事务与断电保证也不同。这不是尚未创建的 Durable v0.1.0/Core v0.5.0 正式 tag；也不改写[历史成本页](checkpoint-cost.md)的版本和数字。

## 本机结果：内存和文件成本明显分开

以下为轻计算 **chain、rounds=1、run** 的整任务耗时中位数，单位 **ms**。有数值的点均为 5 个有效样本，例外会显式标 n。“SQLite”指 go-workflows 使用固定版本的 pure-Go SQLite 后端，不是单独测试数据库。

| 步骤数 | Durable memory | SQLite memory | Durable file | SQLite file |
|---:|---:|---:|---:|---:|
| 1 | 0.0694 | 3.51 | 46.5 | 18.4 |
| 10 | 0.334 | 21.6 | 368 | 119 |
| 100 | 6.69 | 198 | 3440 | 1120 |
| 1000 | 565 | 2140 | 无有效样本：timeout | 11600 |
| 10000 | 无有效样本：timeout | 无有效样本：timeout | 无有效样本：timeout | 无有效样本：timeout |

在完成到 1000 步的测点上，MemoryStore 的耗时低于 SQLite 内存库，但分配增长必须一起看：1000 步的累计分配中位数，Durable 为 **1730MiB**，go-workflows 为 **55MiB**。这是整轮不断分配的总量，不是同时占着这么多内存。Durable 文件组在 1000 步的主矩阵中没有取得有效样本；预热和正式运行共用 60s 案例 context，不能据此声称单次执行必定超过 60s。

![按后端分开的普通整任务耗时](/performance/2026-10-17/workflow-run-r1-time.png)

![累计 Go 分配而非峰值内存](/performance/2026-10-17/workflow-run-r1-allocations.png)

![每步固定增加到 256 轮计算的结果](/performance/2026-10-17/workflow-run-r256-time.png)

## 记录增长：最终文件小，不等于保存成本小

下表来自单独的 growth 仪表化测量，rounds=1、不预热。complete 点是 3 次完成运行的汇总；partial 点只保留真正尝试并确认的前缀，不能当作请求规模已经做完。1 和 10 步的结果也保留在原始矩阵和图中。

| 后端 | 请求步骤数 | 状态 / 保留样本数 | 已确认步骤 | 成功 Save | 累计逻辑 JSON / MiB | 留存 JSON / KiB |
|---|---:|---|---:|---:|---:|---:|
| memory | 100 | complete: ok, n=3 | 100 | 201 | 3.58 | 42.8 |
| memory | 1000 | complete: ok, n=3 | 1000 | 2001 | 352 | 428 |
| memory | 10000 | partial: resource_limit, n=1 | 2 | 3786 | 1020 | 554 |
| file | 100 | complete: ok, n=3 | 100 | 201 | 3.59 | 42.8 |
| file | 1000 | complete: ok, n=3 | 1000 | 2001 | 352 | 428 |
| file | 10000 | partial: timeout, n=1 | 0 | 2977 | 632 | 435 |

还有一个必须保留的大规模成功点：go-workflows 的 SQLite 内存组在关闭预热的 growth 中，10000 步 3 次都完成，中位数约 **51s**，历史事件 **30003** 条。普通 run 要在同一案例期限内同时容纳独立预热和正式运行，因此主表没有有效值，不等于它完全不能跑完 10000 步；两种模式的计时范围分别保留。

SQLite 文件组的统计范围不同：下面是关闭前的当前文件长度与历史条数，不是累计写出量。SQLite 内存库没有磁盘文件长度，这里不填一个零来代替。

| 请求步骤数 | 状态 / 保留样本数 | 已确认步骤 | 历史事件数 | 关闭前 DB + WAL / MiB |
|---:|---|---:|---:|---:|
| 100 | complete: ok, n=3 | 100 | 303 | 4.17 |
| 1000 | complete: ok, n=3 | 1000 | 3003 | 5.5 |
| 10000 | partial: timeout, n=1 | 4676 | 14031 | 10.8 |

![不同统计范围的增长指标，部分前缀单独标记](/performance/2026-10-17/storage-growth.png)

当前机制解释了为什么需要关注增长：[runState.save](https://github.com/Luo-root/pulse-durable/blob/e47e51eda57911df84922beca29b840fa8b6f2fd/attempts.go#L33) 会克隆完整候选交给 Store，确认成功后再克隆成自己的记录；[MemoryStore.Save](https://github.com/Luo-root/pulse-durable/blob/e47e51eda57911df84922beca29b840fa8b6f2fd/memory.go#L70) 还要保留一份隔离副本；[filestore.Save](https://github.com/Luo-root/pulse-durable/blob/e47e51eda57911df84922beca29b840fa8b6f2fd/filestore/store.go#L163) 每次先读/解码旧记录校验 Revision，再编码、同步并替换完整新记录。每确认一个节点，登记表就更长。尝试打开本身也会保存记录：固定版本的 [Core 检查点链](https://github.com/Luo-root/pulse/blob/7d70e1e1477192669d5008d47729e3c8c19a336f/checkpoint_run.go#L296) 在内层等待前驱之前就打开根 attempt，所以还没等到业务输入的节点也可能增长尝试表。Save 次数不能当成已经完成同样多的业务步骤。这是读码得到的机制证据，没有用 profiler 分配各函数的耗时百分比；先保留本次基线，再单独评审增量存储或减少复制。

## 真正重启与已完成重开

下表单位为 **新进程中操作耗时 ms**，不含进程启动和图/后端准备；有数值的点均为 3 个有效样本，例外显式标 n。resume 包含后半段业务执行和每步 fsync 审计，不是单独测“读一份快照”。前缀列仅适用于 resume；reopen 在准备阶段已完成全部 N 步。

| 步骤数 | 退出前要求确认的前缀 | Durable resume | SQLite resume | Durable 已完成重开 | SQLite 已完成查询 |
|---:|---:|---:|---:|---:|---:|
| 10 | 5 | 81 | 81.4 | 0.887 | 0.452 |
| 100 | 50 | 900 | 765 | 2.22 | 1.72 |
| 1000 | 500 | 11400 | 6970 | 14.6 | 15.4 |

![中途恢复并完成剩余审计业务](/performance/2026-10-17/workflow-resume-r1-time.png)

![两种不同的已完成重开动作](/performance/2026-10-17/workflow-reopen-r1-time.png)

成功的 resume 点都经过父进程独立核对持久前缀，且两进程合并的磁盘审计中每个业务 ID 恰好一次；超时点不算该规模恢复通过。完成后重开的两列是不同的原生动作，不据此排等价恢复能力的快慢。进程启动、setup、剩余操作以及清理/IPC 的墙钟时间在原始 JSON 中分别保留；未采集分配量仍为空。

## 覆盖与使用建议

| 矩阵 | 计划 / 保留条数 | 状态计数 |
|---|---:|---|
| workflow | 200 / 200 | not_repeated=40, ok=150, timeout=10 |
| growth | 60 / 60 | not_repeated=6, ok=51, resource_limit=1, timeout=2 |
| recovery | 36 / 36 | ok=36 |

两引擎、两后端的 run/growth 都实际尝试了五档请求规模。触及预算的点保留原状态，没有替未完成的 10000 步编一个耗时。对于大量极细的持久步骤，应把整份记录复制/保存的成本计入设计，并按自己的业务规模验收；小规模正确不等于大规模成本合适。优化属于后续单独改动，仍须保留确认、身份和恢复契约。

仓内汇总：[普通运行 CSV](https://github.com/Luo-root/pulse-durable/blob/main/benchmarks/reports/2026-10-17/workflow-summary.csv) · [增长耗时 CSV](https://github.com/Luo-root/pulse-durable/blob/main/benchmarks/reports/2026-10-17/growth-summary.csv) · [存储增长 CSV](https://github.com/Luo-root/pulse-durable/blob/main/benchmarks/reports/2026-10-17/storage-summary.csv) · [恢复与重开 CSV](https://github.com/Luo-root/pulse-durable/blob/main/benchmarks/reports/2026-10-17/recovery-summary.csv)。[精简证据说明](https://github.com/Luo-root/pulse-durable/blob/main/benchmarks/reports/2026-10-17/README.md)保留版本、环境、原始 JSONL 哈希与采集连续性。完整原始 JSONL、manifest、模块 Origin、机器说明、覆盖/校验记录和其他图保存在外部证据包 `pulse-scale-baseline-2026-10-17.zip`，不随源码仓分发；此处没有公开下载链接。

## 共同任务与分组

每步使用相同 uint64 计算，首步输入 1，以后为 `1 + 上一步输出`；计算轮数为 1 或 256。一个 Pulse 节点对应一个 Activity，不把整张图装进对方一个 Activity。节点数就是业务步骤数，长链有 N−1 条边、关键路径 N，不能用这组结果证明宽图吞吐。结果与独立 Python oracle 一致。

| 分组 | Durable | go-workflows | 能说明什么 |
|---|---|---|---|
| memory | MemoryStore 的隔离 Go 记录 | 命名 shared-cache SQLite 内存库，保留连接 | 进程内协议、编码/事务与调度成本；不证明退出后恢复 |
| file | 完整 JSON 快照，文件 Sync 后替换 | SQLite 文件、WAL、实际 synchronous=2/FULL | 本机文件后端成本；两者事务/断电保证不同 |
| resume | 已确认一半后真正退出，新进程 Resume | 已确认历史后退出，新 worker 继续原实例 | 这个精确中断点上的前缀复用与剩余完成 |
| reopen | 重建、验证并恢复已完成 Execution | 查询已完成实例结果，无 worker | 两个不同原生动作的各自成本 |

filestore 不承诺断电持久性，MemoryStore 不证明跨进程持久化，SQLite 内存库也不是 Go map。普通成功路径没有 Retry、skip 或外部副作用，不把这组测试推成外部 exactly-once。

## 普通运行、增长和恢复分别计量

`run` 主计时从提交新运行开始，到确认完成并读取正确结果结束。Graph/Codec/Runtime、注册、空库迁移和 worker 准备在外，单列 setup；Durable Runtime 为本轮生成检查点模型的工作仍在内。每迭代用全新后端，预热也用另一份全新后端，不把旧的完成结果当新运行。检查存储及关闭资源在计时外，但它们必须成功才接受样本。

`growth` 是单独的仪表化测量，不参与普通运行延迟排名。Durable 包装器额外 JSON 编码，累计成功 Save 的逻辑 JSON 字节，记录最终 JSON 大小、Save 次数与确认节点数，并施加每个隔离运行 **1GiB** 预算。SQLite 在计时后、关闭连接前报告历史条数、当前 DB/WAL 长度；关闭时 checkpoint 可能改变文件长度。它们不是相同的累计写出指标，更不是设备物理 I/O。

这就像一份越来越长的登记表：最终只留一份文件，不代表只写了一遍；如果每次确认都复制/保存整份，累计工作会比最后那张表大很多。不能只看最终文件大小推断能否承担大规模持久步骤，也不能把测出来的增长直接写成所有场景的复杂度定律。

恢复使用真实子进程。Durable 在 Save 成功、向下游发布前 `os.Exit(73)`；go-workflows 在成功历史事务后确认 ActivityCompleted，并暂时挡住下一条 Activity 的领取。父进程独立读取持久前缀，再启新进程继续；磁盘审计核对每个业务 ID 在两个进程间总共一次。worker 名字固定为 `workflow-bench-single-host`，衡量同宿主重启，不是跨机器领取/租约接管。此测试没有覆盖“下一条外部副作用已发生但尚未确认”的窗口。

恢复时间拆分进程启动、后端/图准备、恢复及剩余业务、检查/清理/IPC。业务审计每步 fsync，计入恢复剩余执行，因此只能与同样审计的恢复场景比较。恢复/重开没有采集分配量，留空，不写零。普通 run 和 growth 没有磁盘业务审计。

## 配置和测量边界

机器为 i9-14900HX，24 核/32 逻辑线程、约 32GiB 内存，Windows 11 `10.0.26200`、Go `1.25.0`、D: NTFS。固定 `GOMAXPROCS=4` 和业务并发上限 4；链上实际业务只能一次一步。对照串行执行，不与本机编译、race、站点构建或 profiler 并跑；缓存未清空，电源、温度及无关程序不受严格控制。

SQLite 使用公开 WithDB 构造器、单连接并保留一个 idle 连接。原生内存便捷构造器上限 2，但永久借走一条保活，剩余可用业务连接同为一条。实际记录的 file/memory journal 分别为 wal/memory，synchronous=2、busy_timeout=5000、cache_size=-2000、page_size=4096、wal_autocheckpoint=1000。

Workflow/Activity 各一 poller、1ms 轮询、上限 4；完成查询同为 1ms，历史条数上限随规模提高。默认 200ms 轮询与结果等待退避会显著影响小任务，所以这是公开的低延迟单机配置，不是默认配置成绩。更频繁轮询消耗 CPU/SQL；进程 CPU 时间随原始 JSON 保存。固定版本 Worker.New 未使用 SingleWorkerMode，未把这个字段当成实际优化。

主矩阵每点计划 5 个样本；独立增长与恢复矩阵计划 3 个。显示有效样本数、中位数与观察到的 min–max，不伪造线上 p95/p99。Go 累计分配不是峰值内存或 RSS。Windows 操作耗时用 QPC，启动/setup/warmup 用 Go 时钟；CPU 计数粒度较粗，小任务可能为 0，不能理解为没有 CPU 消耗。

60s 案例 context 包含 setup 和预热，65s 父进程硬上限会清理整棵子进程树。正常 run 不额外编码计数，只有时间上限；1GiB JSON 预算仅用于 Durable growth。到达边界后保留真实状态与可获得的确认进度，后续计划重复标记 `not_repeated`。超时不能改成“单次操作必定超过 60s”，未完成的 10000 步不补零或外推。

## 复跑和检查

[隔离评测模块与完整方法](https://github.com/Luo-root/pulse-durable/blob/main/benchmarks/README.md)给出命令、固定依赖和原生边界。统一采样/绘图工具位于 Pulse 的 `benchmarks/tools`；正常复跑把原始 JSONL、模块版本/校验和、二进制 SHA256、机器说明、汇总和图写入 Git 已忽略的 `benchmarks/results/`，完整证据单独归档，源码仓只选取精简 CSV 和报告实际引用的 PNG。Go JSON 未给出的 Origin 另由 `module-origins.json` 查询同版本代理元数据；未提供/获取失败明确标记。

适配器先通过两引擎、两后端、1/2/10/100 步、两档计算的次数/结果/并发检查及 race；10/100 步真实退出恢复和完成后重开也通过。独立复审确认并修正 URI 特殊字符导致共库、恢复误用 growth 预算两处适配器问题，原反例复测通过。Linux/Windows 的生产回归与新增适配器 CI 均通过；这些是正确性检查，不是共享 CI 上的性能成绩。

对照依赖仅在独立 benchmark 模块，生产 go.mod、存储格式与执行协议均未修改。此报告先保留当前成本；后续若优化，需要重跑相同任务，不能先修改实现后覆盖不利基线。
