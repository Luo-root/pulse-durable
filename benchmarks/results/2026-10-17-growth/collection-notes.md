# Collection continuity / 采集连续性

The first outer shell command reached its 3600-second tool runtime limit after the DAG and ordinary workflow suites had completed and 48 growth rows had been flushed. This is an outer collection interruption, separate from the 60-second case context and 65-second sample-process watchdog recorded in the manifests. No engine error is inferred from that shell termination.

After confirming that no Python runner or workflow sample process remained, collection continued with the existing `run_matrix.py --resume`, identical matrix configuration and the same frozen executable. Completed suites were retained untouched. Existing flushed rows were not replaced; any in-flight operation without a retained complete row was restarted and is not assigned a fabricated performance value. The recovery suite then ran in sequence. Ordinary timing, compilation, race tests, profiling and site builds did not run concurrently.

首条外层采集命令达到工具的 3600 秒总运行上限时，DAG 与普通持久工作流矩阵已经完成，growth 已落盘 48 条记录。它与 manifest 中的 60 秒案例 context、65 秒样本进程硬上限分开记录，不将外层中断解释成引擎错误。

确认没有残留采样器或 workflow 进程后，使用原 `run_matrix.py --resume`、相同配置和冻结二进制续跑。已完成组不重跑，已落盘行不覆盖；中断时若有尚未形成完整记录的操作，重新启动该测点，不为它编造耗时。恢复组按顺序接续运行。正式计时没有与编译、race、profile 或站点构建并跑。
