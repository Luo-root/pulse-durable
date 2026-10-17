# 持久工作流的规模成本

[Durable #37](https://github.com/Luo-root/pulse-durable/issues/37) 的隔离评测，与 [Pulse #418](https://github.com/Luo-root/pulse/issues/418) 配套。`workflow/` 是独立 Go 模块；数据库和对照框架不进入 Durable 生产模块。英文：[README_en.md](README_en.md)。

## 相同任务与不同保证

首轮完整矩阵使用长链：1、10、100、1000、10000 个业务步骤。一个 Pulse 节点对应一个 go-workflows Activity；不把整张图塞进对照引擎的一个 Activity。第 0 步输入为 uint64(1)，以后每步输入为 `1 + 上一步输出`。业务函数、rounds=1/256 两档与末端 checksum 沿用 [Core 评测规范](https://github.com/Luo-root/pulse/blob/main/benchmarks/README.md)。每个请求固定同一业务并发上限和 GOMAXPROCS；长链关键路径等于业务步骤数，不能用它证明宽图吞吐。

| 分组 | Durable | go-workflows |
|---|---|---|
| `memory` | MemoryStore 的隔离 Go 记录 | 命名 shared-cache SQLite 内存库，保留连接；并非 Go map |
| `file` | filestore 完整 JSON 快照，文件 Sync 后替换 | SQLite 文件、请求 WAL、不覆盖 synchronous；实际 PRAGMA 随样本保存 |

两种持久后端完成相同业务任务，但存储事务、历史格式、重放与断电保证并不相同。filestore 不承诺断电持久性；MemoryStore 不证明进程重启后的恢复。内存组和文件组分别读，不与纯内存 DAG 排成同一张榜。

固定版本：

- Durable `v0.0.0-20261016143640-e47e51eda579`。
- Pulse `v0.4.2-0.20261016143631-7d70e1e14771`。
- go-workflows `v1.4.2`，其 pure-Go SQLite 依赖固定为 `modernc.org/sqlite v1.27.0`。

模块无 replace。运行前记录版本、校验和与二进制 SHA256，不使用 `@latest`；补充的 `module-origins.json` 查询同版本公共代理实际提供的 Origin，未提供或获取失败的条目明确标记，不编造 Hash。

## 计时边界

`run` 从新运行提交开始，到确认完成并读取正确末端值结束。Graph、Codec、Runtime、SQLite 空库/迁移、注册和 worker 准备在主计时外，单列 `setup_ns`；Durable Runtime 内部为本轮生成检查点模型和执行的成本仍在主计时中。预热使用独立后端，每个正式迭代也使用新的独立后端和相同 RunID，避免某方复用同一轮已经完成的结果。收尾和存储检查在计时外，但必须完成才把样本标为成功。

对照采用单机低延迟设置：Workflow/Activity 各 1 个 poller、1ms 轮询、相同业务并发上限；完成状态也按 1ms 查询。默认 200ms 轮询和结果等待的指数退避会显著影响小任务，因此这不是默认配置成绩。更频繁的轮询和 SQL 查询会增加 CPU 开销，`cpu_ns_per_op` 单列进程 CPU 时间；Windows CPU 计数较粗，小任务可能读到 0，不能解释成不消耗 CPU。

SQLite 内存和文件组都通过公开 WithDB 构造器设置单连接并保留空闲连接。原生内存便捷构造器使用 2 连接；这里选择固定单连接的本机配置，实际值与 journal_mode、synchronous、busy_timeout、缓存、页大小随原始 JSON 保留在外部证据包。SingleWorkerMode 选项在固定版本的 Worker.New 中未产生实际效果，不把设置了该字段当成可靠性或性能保证。最大历史条数按规模提高，以免 10000 条默认上限提前终止大图。

`growth` 是独立仪表化场景：Durable Store 包装器在每次 Save 前额外 JSON 编码，记录 Save 次数、成功 Save 的累计逻辑 JSON 字节和最终记录大小，并执行累计字节预算。这些额外成本不混入普通 `run` 成绩。SQLite 在计时结束后、连接关闭前报告历史事件条数及当前数据库/WAL 文件长度；关闭连接后的 WAL checkpoint 可能改变这两个长度。未取得等价的累计物理或逻辑写入计数，不能把两者直接当作相同的写放大指标。

`resume` 由父进程启动子进程，在半数步骤已经持久确认后 `os.Exit(73)`；新进程加载同一记录/实例继续。Durable 钉在 Save 成功、下游发布前；go-workflows 钉在已确认历史事务，并临时挡住下一条 Activity 的领取，避免把下一条未完成租约的等待混入此场景。磁盘上的业务审计检查每个步骤在两个进程间总共只执行一次。worker 名字固定为 `workflow-bench-single-host`，衡量同宿主重启，不测跨机器领取/租约接管。该精确中断点不代表任意外部副作用的 exactly-once。

恢复结果分列子进程启动、后端/Graph 准备、恢复及剩余业务、检查/清理/IPC。业务审计每次 fsync，会增加剩余业务耗时，因此恢复测量只与同样启用审计的恢复场景比较。该模式不报告未采集的分配量。

`reopen` 单列已完成运行再次打开。Durable 原生动作是重建/验证/恢复 Execution；go-workflows 原生动作是查询完成实例结果，不启动 worker。这不是两个完全相同的 Resume 操作，不给出二者恢复能力的快慢结论。

## 构建与复跑

先设置 `GOWORK=off` 和 `GOFLAGS=-buildvcs=false`，保持无 replace，使用 Go 1.25 自动工具链。从本仓根目录，在隔离模块验证：

```sh
cd benchmarks/workflow
go mod download
go build -o workflow.exe .
go vet ./...
go test -race -count=1 -timeout=180s ./...
```

Linux/macOS 将可执行文件命名为 `workflow`。真实恢复测试会启动刚编译的测试可执行文件，不需要外部服务或凭据。

使用 Pulse 仓库的统一采样器（根据两仓位置调整路径）：

```sh
python ../pulse/benchmarks/tools/run_matrix.py --suite workflow --module benchmarks/workflow --binary benchmarks/workflow/workflow.exe --out benchmarks/results/local-workflow --sizes 1,10,100,1000,10000 --rounds 1 --repeats 5 --procs 4 --parallelism 4
python ../pulse/benchmarks/tools/capture_origins.py benchmarks/results/local-workflow/manifest.json --out benchmarks/results/local-workflow/module-origins.json
python ../pulse/benchmarks/tools/summarize.py benchmarks/results/local-workflow/samples.jsonl --out benchmarks/results/local-workflow/figures
```

也可只跑一个精确场景：

```sh
benchmarks/workflow/workflow.exe -engine durable -backend file -nodes 100 -rounds 1 -mode growth -warmup=false -max-write-bytes 1073741824 -timeout 60s -dir benchmarks/results/local-scratch
benchmarks/workflow/workflow.exe -engine go-workflows -backend file -nodes 100 -rounds 1 -mode resume -timeout 60s -dir benchmarks/results/local-scratch
```

正常 `run` 不额外编码计数，只有时间边界；1GiB 逻辑字节保护只用于 Durable `growth`。context 不能强行中断同步编码/落盘，统一采样器另设 65 秒父进程硬超时并清理整棵子进程树。超时、超预算与未测分别保存，未完成的 10000 步不补零、不从小规模外推。

## 读数据与保留规则

[2026-10-17 历史报告](../site/guide/performance.md)使用上面的固定版本和后端配置，不代表后续统一框架的生产对比。[精简证据说明](reports/2026-10-17/README.md)记录环境、原始 JSONL 哈希和采集连续性；源码仓只保留以下汇总及报告实际引用的 6 张 PNG：

- [普通运行汇总](reports/2026-10-17/workflow-summary.csv)：rounds=1/256，按引擎、后端和规模分组。
- [增长耗时汇总](reports/2026-10-17/growth-summary.csv)与[存储增长汇总](reports/2026-10-17/storage-summary.csv)：后者区分 complete/partial，不把未完成前缀当成完整运行。
- [恢复与重开汇总](reports/2026-10-17/recovery-summary.csv)：没有采集的分配量保持空白。

汇总保留有效样本数、状态、中位数和观察到的 min–max。完整原始 JSONL、manifest、模块 Origin、机器说明、覆盖/校验记录、诊断和其他图保存在外部证据包 `pulse-scale-baseline-2026-10-17.zip`，不随源码仓分发；此处不提供未发布的下载链接。

正常复跑仍向 Git 已忽略的 `benchmarks/results/` 写入原始 JSONL、manifest、汇总和图。将完整运行证据单独归档；不要提交该目录、执行文件、业务记录、审计、SQLite/WAL、本机路径清单或重复图。更新发布报告时，核对原始数据后只选取必要 CSV 和实际引用图。

Go 累计分配不是峰值内存，文件长度不是累计写出量，重复批次的 min–max 不是线上延迟分位数。[原 checkpoint-cost 页](../site/guide/checkpoint-cost.md)保留其原版本与历史数字。
