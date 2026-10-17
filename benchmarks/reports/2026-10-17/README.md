# 2026-10-17 historical evidence / 历史证据

This is the unchanged historical baseline, not the later production comparison under a unified framework. / 本目录保留原历史基线，不代表后续统一框架的生产对比。

- Versions / 版本: Durable `v0.0.0-20261016143640-e47e51eda579`; Pulse `v0.4.2-0.20261016143631-7d70e1e14771`; go-workflows `v1.4.2`; pure-Go SQLite `v1.27.0`.
- Environment / 环境: i9-14900HX, 24 cores / 32 logical processors, about 32GiB RAM, Windows 11 `10.0.26200`, Go `1.25.0`, NTFS, GOMAXPROCS=4, business concurrency / 业务并发上限 4.
- Harness commit / 评测代码提交: `ba2e6cb4cdffce917a4025e0bb9bb67ea2c2d6da`; no module replacements / 无模块替换.
- Frozen executable SHA256 / 冻结可执行文件哈希: `f58f9651f8ab412e8f094620a0b74012d0dd8684ec3c87a741af80156522c7dc`.

| Summary / 汇总 | Scope / 范围 |
|---|---|
| [workflow-summary.csv](workflow-summary.csv) | Ordinary run / 普通运行; rounds=1/256; 200 retained rows / 原始记录 200 条 |
| [growth-summary.csv](growth-summary.csv) | Instrumented growth / 仪表化增长; rounds=1; 60 retained rows / 原始记录 60 条 |
| [storage-summary.csv](storage-summary.csv) | Complete/partial growth, distinct write metrics / 完整与部分增长、不同写入指标 |
| [recovery-summary.csv](recovery-summary.csv) | Real restart and completed reopen / 真正重启与完成后重开; 36 retained rows / 原始记录 36 条 |

Raw `samples.jsonl` SHA256 / 原始文件哈希:

| Dataset / 数据集 | SHA256 |
|---|---|
| workflow | `abefda291a6cefc560da4bb106f1857da85689617f18b8e9733381289342af08` |
| growth | `41ed5e9cea3fc96343a532064a2cb4bb9b12da8b92711705df4d164725d42e68` |
| recovery | `3d4855364faa2a9fe37af8c87822d47be4c866a02cca5f1cb6e38b76edf2ae82` |

Full raw data, manifests, module origins, acceptance checks and other figures are in the external archive `pulse-scale-baseline-2026-10-17.zip`, not distributed with source. No public download URL has been published here. / 完整原始数据、manifest、模块来源、校验记录和其他图保存在同名外部证据包，不随源码分发，此处无公开下载链接。

Collection continuity: the first outer shell reached its 3600s limit after 48 growth rows had been flushed. Collection resumed with the same configuration and frozen executable after verifying no sampler or child remained. Retained rows were not overwritten; an unfinished row was restarted without an invented value. This differs from the 60s case context and 65s process watchdog. / 采集连续性：外层 shell 在 growth 落盘 48 条后触及 3600 秒限制；确认无残留采样进程后，以原配置和冻结二进制续跑，已留存行不覆盖，未完成行重启且不编造成绩。该外层中断与 60 秒案例期限、65 秒进程硬上限分开。

See the [Chinese method](../../README.md), [English method](../../README_en.md) and their linked reports for backend guarantees, failure points and timing limits. Normal reruns still write full evidence to ignored `benchmarks/results/`. / 后端保证、失败点和计时限制见双语方法及其报告链接；正常复跑仍向已忽略的 `benchmarks/results/` 写完整证据。
