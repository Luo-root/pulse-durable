# Unified production workflow comparison

## 2026-11-01: message index and file history measurements

This section compares two specific Durable paths before and after the current source changes. It does not update the Pulse/Eino/Temporal ranking. The baseline is Durable `16d7bf9fbee1b496fb1ffb95a25f62ee2f19e6fe`. Both variants use public Pulse `v0.5.2`, Go `1.26.10`, Windows amd64, Intel Core i9-14900HX, `GOMAXPROCS=2`, and `GOWORK=off`. Benchmarks ran serially while this round's PostgreSQL tests, race tests, and other benchmarks were paused. Filesystem caches were not cleared and not all system processes were controlled. All figures are medians over every sample, not production latency percentiles.

The message comparison overlays only `attempts.go`, `runtime.go`, `delta.go`, and `messages.go` onto the same baseline source to isolate the index change. `BenchmarkMessageIndexRuntimeManaged` runs complete managed-message processing, checks the result, and retains all confirmed message history. Each sample executes one run, with three samples per point. The new index is built once after authoritative loading or confirmed initialization; ordinary confirmation updates only this batch's messages, and normal lookup misses do not rebuild it.

| Messages | Old scan ms/run | Incremental index ms/run | Old allocated MB/run | New allocated MB/run |
| ---: | ---: | ---: | ---: | ---: |
| 128 | 9.851 | 7.027 | 8.824 | 4.478 |
| 512 | 96.115 | 14.951 | 88.586 | 17.730 |
| 2048 | 1434.037 | 62.993 | 1209.992 | 70.779 |

The file comparison fixes origin/base at revision 1 and increases only retained batch count H. Preparation writes are excluded, each fixture measures one operation, and each point has five samples. The common comparison fixture allows the old implementation to advance generation under its original behavior and the new implementation to perform its approved same-base no-op, avoiding repeated measurements that grow the old representation history. `LoadRevision(1)` reads the origin directly; same-base requests still validate the required current metadata and snapshot. Complete auditing is separate from these paths.

| Retained batches H | Old LoadRevision(1) ms | New LoadRevision(1) ms | Old same-base ms | New same-base ms |
| ---: | ---: | ---: | ---: | ---: |
| 128 | 35.081 | 0.706 | 36.454 | 0.682 |
| 256 | 73.571 | 0.744 | 69.565 | 0.684 |
| 512 | 261.411 | 0.734 | 164.402 | 0.714 |
| 1024 | 299.611 | 0.697 | 280.153 | 0.705 |

First-sample outliers were retained. At H=1024, old/new read ranges were 275.522–12019.477 ms and 0.640–9.358 ms. The 128-message timing ranges overlap; three samples do not establish a statistically significant small-scale speedup. Median allocated bytes for the H=1024 read fell from 75,295,696 B to 16,832 B. MB is decimal. Cumulative allocation is not live heap, RSS, WAL, network traffic, or device I/O. The index also retains metadata proportional to message groups; these figures do not establish linear cost for the complete Runtime/Apply or constant cost for every historical query. See [Storage](storage.md) for the boundary between targeted paths and complete `AuditHistory`.

## 2026-10-17/23: historical framework comparison

> New collection, 2026-10-23: [formal-release SQLite and recovery report](https://github.com/Luo-root/pulse/blob/main/site/en/reference/sqlite-formal-20261023.md). Go 1.26.0, Core v0.5.0, Durable/official sqlitestore v0.1.0; 171 valid records and all 36 recoveries ok. Another 45 Temporal checkpoint configuration errors remain as diagnostics, not performance failures; overall status: COMPLETED_WITH_ERRORS. Only reliable confirmation, the Temporal native-persistence baseline and recovery were rerun, not full execution/observation/resources. Historical identities, backends and numbers below remain unchanged.

The new round keeps **Pulse/Durable, Eino and Temporal** throughout execution, observation, resources, reliable confirmation and recovery. The [full report/four figures](https://github.com/Luo-root/pulse/blob/main/site/en/reference/production-comparison.md), [234-group compact CSV](https://github.com/Luo-root/pulse/blob/main/benchmarks/reports/2026-10-17-production/summary.csv), [versions/raw hashes](https://github.com/Luo-root/pulse/blob/main/benchmarks/reports/2026-10-17-production/identity.json) and [reproduction code](https://github.com/Luo-root/pulse/tree/main/benchmarks/production) live together in Pulse rather than duplicating raw artifacts in both repositories. See [Pulse #427](https://github.com/Luo-root/pulse/issues/427) and [Durable #41](https://github.com/Luo-root/pulse-durable/issues/41).

Measured on 2026-10-17. Public Pulse `v0.4.2-0.20261017111210-aa02dd107a43`, Durable `v0.0.0-20261017105818-71c693fc82ef`, Eino v0.9.21, Temporal SDK v1.50.0 / CLI v1.9.1 / Server v1.32.0. The shared benchmark host uses Go1.26.0; the official Server binary was built with Go1.26.8. The isolated benchmark pins both latest repositories without local replace. Durable's production go.mod retains its original dependency: combined validation is not a production dependency upgrade.

## Reading reliable dimensions

| Dimension | Pulse / Durable | Eino | Temporal |
|---|---|---|---|
| Native execution | Pulse in memory | compose Graph in memory | Native persistent Service/Activities |
| Reliable save | Store confirms complete node chains before publication | No automatic per-node save on ordinary success; checkpoint on active interrupt | Native event history and Activity confirmation |
| SQLite | Benchmark host through existing Store | Benchmark host through CheckPointStore | Real Server embedded in CLI |
| Process recovery | Kill host after confirmed prefix, then Resume | Exit after active checkpoint; new-process resume | No-in-flight prefix, worker kill; Server restart separately |

SQLite uses WAL/FULL while retaining different transaction layouts and save granularity. The Durable benchmark host encodes the complete RunRecord as JSON in SQLite's record BLOB column, with Revision-based CAS. It borrows filestore OS locks for local ownership only, never calls its Load/Save methods, and does not save a second JSON run file. This is a benchmark adapter, not a newly delivered production SQLite backend, distributed lease or fencing. No equivalent ordinary per-node confirmation measurement for Eino does not mean its extension mechanisms cannot support persistent applications. Active interruption is distinct from arbitrary-crash recovery.

chain/rounds=1 reliable reuse medians (ms):

| Nodes | Pulse + Durable | Eino | Temporal |
|---|---:|---|---:|
| 1 | 7.1326 | Active interrupt separately | 253.826 |
| 10 | 45.5552 | Active interrupt separately | 197.6637 |
| 100 | 503.4885 | Active interrupt separately | 5049.4483 |
| 1000 | 8553.1903 | Active interrupt separately | — |
| 10000 | — | Active interrupt separately | — |

Durable uses three repeats; Temporal references this round's native durable points with five batches, rather than a second equivalent experiment. `—` is an incomplete whole-case cost, not zero or an extrapolation. All three ten-thousand-node Durable shapes hit the 60s case budget, so this round does not accept ten-thousand-step reliable scale.

## Results and evidence boundaries

The four matrices retain all 1062 planned identities and 234 groups: 947 completed, 25 time limits, 90 not_repeated, no business-result validation errors; all 36 recovery scenarios completed. Recovery uses chain N=10/100/1000, rounds=256, confirmed prefixes 5/10/10. The parent independently reads Pulse SQLite and execution audit, hashes Eino's saved blob, or uses a separate Temporal client to confirm native history with no in-flight Activity. Final values and no prefix rerun are checked; arbitrary in-flight external effects are not promised exactly once.

Native cases include a first run plus a full reuse, while recovery only completes the suffix. Successful recovery does not imply native completion within the same budget. Store lengths include DB/WAL/SHM, not cumulative writes or per-step costs. Cumulative Go allocations differ from RSS; Temporal's process-local allocations exclude Server. Resources use separate equal-work two-run cases and the complete process tree.

Latest public Core with Durable passed build/vet/full race and real process-exit recovery. New adapters passed full race, live Temporal failure/cancellation reuse/history concurrency checks, and independent sampler counterexamples/review. The full report retains unfavorable or surprising results, including slower recovery with the Temporal Server left alive, without claiming a cause absent controlled diagnosis.

The [older scale report](./performance.md) and [checkpoint cost page](./checkpoint-cost.md) preserve historical versions and numbers; they do not enter this unified comparison. Raw JSONL, logs, databases, binaries and full machine inventories remain in external evidence rather than Git. No release tag was created.
