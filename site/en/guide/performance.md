# Scale and performance

This report measures the same chain at 1, 10, 100, 1000 and 10000 business steps: time, cumulative allocations and writes after adding checkpoints, reliable records and recovery. Read it separately from [Pulse's in-memory report](https://github.com/Luo-root/pulse/blob/main/site/en/reference/performance.md), rather than one combined speed ranking.

Measured on **2026-10-17**, pinning public Durable `v0.0.0-20261016143640-e47e51eda579`, Core `v0.4.2-0.20261016143631-7d70e1e14771`, go-workflows [v1.4.2](https://github.com/cschleiden/go-workflows/releases/tag/v1.4.2) and its pure-Go SQLite `v1.27.0` dependency. This is a historical baseline of those development versions and backend settings, not the later production comparison under a unified framework. The file backends also differ in transaction and power-loss guarantees. These are not uncreated Durable v0.1.0/Core v0.5.0 release tags. The [historical cost page](checkpoint-cost.md) retains its original versions and measurements.

## Local results: memory and file costs diverge

For the lightweight **chain, rounds=1, run** task, whole-task median times are in **ms**. Every numeric point has five valid samples unless an explicit n is shown. “SQLite” means go-workflows with the pinned pure-Go SQLite backend, not a standalone database microbenchmark.

| Steps | Durable memory | SQLite memory | Durable file | SQLite file |
|---:|---:|---:|---:|---:|
| 1 | 0.0694 | 3.51 | 46.5 | 18.4 |
| 10 | 0.334 | 21.6 | 368 | 119 |
| 100 | 6.69 | 198 | 3440 | 1120 |
| 1000 | 565 | 2140 | No valid sample: timeout | 11600 |
| 10000 | No valid sample: timeout | No valid sample: timeout | No valid sample: timeout | No valid sample: timeout |

MemoryStore is faster than SQLite memory at the completed points through 1000 steps, but allocation growth matters: at 1000 steps the medians are **1730MiB** for Durable versus **55MiB** for go-workflows. These are cumulative allocations throughout one run, not simultaneous resident memory. Durable file has no valid primary sample at 1000 steps within this case budget; its one warmup shares the 60-second context with the measured run. Read timeout as a bounded unsuccessful case, not a measured one-operation lower bound.

![Ordinary whole-task time, separately by backend](/performance/2026-10-17/workflow-run-r1-time.png)

![Cumulative Go allocations, not peak memory](/performance/2026-10-17/workflow-run-r1-allocations.png)

![The same task with 256 computation rounds per step](/performance/2026-10-17/workflow-run-r256-time.png)

## Storage growth: final size hides repeated saves

The table below is the separate instrumented growth experiment, rounds=1, with no warmup. Complete points summarize three runs; partial points retain only attempted prefixes. No partial result is reported as a completed requested size. The first two sizes, 1 and 10, are also present in the raw matrix and figures.

| Backend | Requested steps | State / retained samples | Confirmed steps | Successful Saves | Cumulative logical JSON / MiB | Retained JSON / KiB |
|---|---:|---|---:|---:|---:|---:|
| memory | 100 | complete: ok, n=3 | 100 | 201 | 3.58 | 42.8 |
| memory | 1000 | complete: ok, n=3 | 1000 | 2001 | 352 | 428 |
| memory | 10000 | partial: resource_limit, n=1 | 2 | 3786 | 1020 | 554 |
| file | 100 | complete: ok, n=3 | 100 | 201 | 3.59 | 42.8 |
| file | 1000 | complete: ok, n=3 | 1000 | 2001 | 352 | 428 |
| file | 10000 | partial: timeout, n=1 | 0 | 2977 | 632 | 435 |

An important completed large point remains visible: go-workflows with SQLite memory completed all three 10000-step growth runs with warmup disabled, at approximately **51s** median and **30003** history events. Its primary run has no valid measurement because that case must fit both isolated warmup and the measured run into one deadline. This is not evidence that it cannot complete 10000 steps at all; the two modes retain their separate timing scopes.

SQLite file measurements have a different scope: these are current file lengths and history counts before close, not cumulative write bytes. SQLite memory has no disk-file length, so it is not filled with zero here.

| Requested steps | State / retained samples | Confirmed steps | History events | DB + WAL before close / MiB |
|---:|---|---:|---:|---:|
| 100 | complete: ok, n=3 | 100 | 303 | 4.17 |
| 1000 | complete: ok, n=3 | 1000 | 3003 | 5.5 |
| 10000 | partial: timeout, n=1 | 4676 | 14031 | 10.8 |

![Distinct growth metrics with partial prefixes marked separately](/performance/2026-10-17/storage-growth.png)

The current mechanism explains why growth deserves attention: [runState.save](https://github.com/Luo-root/pulse-durable/blob/e47e51eda57911df84922beca29b840fa8b6f2fd/attempts.go#L33) clones the full proposed record for Store and again after confirmation; [MemoryStore.Save](https://github.com/Luo-root/pulse-durable/blob/e47e51eda57911df84922beca29b840fa8b6f2fd/memory.go#L70) takes another isolated clone; [filestore.Save](https://github.com/Luo-root/pulse-durable/blob/e47e51eda57911df84922beca29b840fa8b6f2fd/filestore/store.go#L163) rereads/decodes the prior record for Revision validation and encodes, syncs and replaces the full new record. Each confirmed node lengthens the record. Attempt opening also saves a record. In the pinned [Core checkpoint chain](https://github.com/Luo-root/pulse/blob/7d70e1e1477192669d5008d47729e3c8c19a336f/checkpoint_run.go#L296), the root attempt opens before the inner chain waits for dependencies; nodes still waiting for input can therefore lengthen the attempt table. Save counts do not mean the same number of business steps completed. This is source-based mechanism evidence, not a profile assigning a percentage of runtime to a function. Preserve this baseline before separately evaluating incremental storage or fewer copies.

## Real restart and completed reopen

Values are **ms for the restarted child's operation**, excluding process launch and graph/backend preparation. Numeric points have three valid samples unless n is shown. Resume additionally performs the remaining half of the business steps with fsynced audits; it is not an isolated snapshot-load microbenchmark. The prefix column applies to resume; completed reopen is seeded with all N steps.

| Steps | Confirmed prefix before exit | Durable resume | SQLite resume | Durable completed reopen | SQLite completed-result query |
|---:|---:|---:|---:|---:|---:|
| 10 | 5 | 81 | 81.4 | 0.887 | 0.452 |
| 100 | 50 | 900 | 765 | 2.22 | 1.72 |
| 1000 | 500 | 11400 | 6970 | 14.6 | 15.4 |

![Resume plus remaining audited work](/performance/2026-10-17/workflow-resume-r1-time.png)

![Two different completed-run reopen operations](/performance/2026-10-17/workflow-reopen-r1-time.png)

At successful resume points, the parent verified the persisted half-prefix and the combined disk audit verified every business ID once across both processes. A timeout is not a passed recovery test at that size. Completed reopen compares different native operations, so the values do not rank equivalent recovery capability. Raw JSON separates launch, setup, remaining operation and cleanup/IPC wall times; unmeasured allocation metrics remain empty.

## Coverage and practical reading

| Matrix | Planned / retained samples | Status counts |
|---|---:|---|
| workflow | 200 / 200 | not_repeated=40, ok=150, timeout=10 |
| growth | 60 / 60 | not_repeated=6, ok=51, resource_limit=1, timeout=2 |
| recovery | 36 / 36 | ok=36 |

All five requested run/growth sizes were actually attempted for both backends and engines. Budget-limited points remain visible; no successful duration was invented for an unfinished 10000-step case. For many tiny durable steps, budget full-record copying/saving and validate the application's own scale before adopting the abstraction. Correctness at smaller sizes does not establish acceptable large-scale cost. Optimizing this mechanism is a separate change, preserving confirmation, ownership and recovery contracts.

Repository summaries: [ordinary-run CSV](https://github.com/Luo-root/pulse-durable/blob/main/benchmarks/reports/2026-10-17/workflow-summary.csv) · [growth-time CSV](https://github.com/Luo-root/pulse-durable/blob/main/benchmarks/reports/2026-10-17/growth-summary.csv) · [storage-growth CSV](https://github.com/Luo-root/pulse-durable/blob/main/benchmarks/reports/2026-10-17/storage-summary.csv) · [recovery/reopen CSV](https://github.com/Luo-root/pulse-durable/blob/main/benchmarks/reports/2026-10-17/recovery-summary.csv). The [compact evidence notes](https://github.com/Luo-root/pulse-durable/blob/main/benchmarks/reports/2026-10-17/README.md) retain versions, environment, raw JSONL hashes and collection continuity. Full raw JSONL, manifests, module Origin metadata, machine details, coverage/acceptance checks and other figures remain in the external archive `pulse-scale-baseline-2026-10-17.zip`, not distributed with source. No public download URL is supplied here.

## Common task and groups

Each step performs the same uint64 computation. The first input is 1; later inputs are `1 + previous output`, for 1 or 256 compute rounds. One Pulse node corresponds to one Activity, not an entire graph inside an Activity. N counts business steps, with N−1 edges and critical path N; the chain cannot establish wide-graph throughput. An independent Python oracle checks results.

| Group | Durable | go-workflows | What it establishes |
|---|---|---|---|
| memory | Isolated Go records in MemoryStore | Named shared-cache SQLite in memory with a retained connection | In-process protocol, encoding/transaction and scheduling costs, not exit/restart persistence |
| file | Full JSON snapshots, file Sync then replacement | SQLite file, WAL, actual synchronous=2/FULL | Local backend cost with differing transaction/power-loss guarantees |
| resume | Actual exit after half the steps are confirmed; new-process Resume | Exit after confirmed history; new worker continues the instance | Confirmed-prefix reuse and remaining work at this precise interruption point |
| reopen | Rebuild, validate and restore a completed Execution | Query a completed result with no worker | Costs of different native operations |

Filestore does not promise power-loss durability. MemoryStore does not establish cross-process persistence, and SQLite memory is not a Go map. The common successful task has no retry, skip or external effect, and does not establish external exactly-once behavior.

## Separate run, growth and recovery

`run` measures submission through confirmed completion and correct result reading. Graph/Codec/Runtime, registration, empty-database migrations and worker preparation are excluded and reported as setup; Durable Runtime's per-run checkpoint-model work remains included. Every iteration uses a fresh backend, and warmup uses another fresh backend. Old completed results never count as new runs. Inspection and resource closure are outside timing but must succeed for a valid sample.

`growth` is separately instrumented and does not rank ordinary-run latency. The Durable wrapper performs extra JSON encoding, accumulating bytes passed to successful Saves, final JSON size, Save counts and confirmed steps, with a **1GiB** budget per isolated run. After timing and before close, SQLite reports history count and current DB/WAL lengths; close-time checkpointing can change file lengths. These are different cumulative-write scopes, neither representing physical device I/O.

Think of an expanding registration form: retaining only the final file does not mean it was written only once. Copying/saving the whole form at each confirmation can cost far more than the final form's size. Final file size alone does not establish suitability for many durable steps, and observed growth is not a universal complexity law.

Recovery uses actual child processes. Durable calls `os.Exit(73)` after successful Save and before downstream publication. The go-workflows hook confirms ActivityCompleted after a successful history transaction and briefly blocks the next Activity pickup. The parent independently reads the persistent prefix, then launches a new process. A disk audit checks each business ID exactly once across both processes. The worker name remains `workflow-bench-single-host`: this measures same-host restart, not cross-machine claiming/lease takeover. It does not test the window where an external effect occurs before its confirmation.

Recovery separates startup, backend/graph preparation, restore plus remaining work, and inspection/cleanup/IPC. Business-entry audits fsync and are included in remaining-work time; compare only similarly audited recovery scenarios. Recovery/reopen allocation counters are unmeasured and empty, not zero. Ordinary run and growth do not use disk business audits.

## Configuration and limits

i9-14900HX, 24 cores/32 logical processors, approximately 32GiB RAM, Windows 11 `10.0.26200`, Go `1.25.0`, D: NTFS. `GOMAXPROCS=4` and business concurrency cap 4; a chain still runs one business step at a time. Engines execute serially without concurrent local compilation, race tests, site builds or profiling. Caches are not flushed; power, temperature and unrelated programs are not tightly controlled.

SQLite uses the public WithDB constructor with one connection and one retained idle connection. Its native in-memory convenience constructor allows two connections but permanently borrows one to keep the database alive, leaving one usable business connection. Recorded file/memory journal modes are wal/memory, with synchronous=2, busy_timeout=5000, cache_size=-2000, page_size=4096 and wal_autocheckpoint=1000.

Workflow/Activity each use one poller at 1ms, concurrency cap 4; completion polling is also 1ms, with a raised history limit for larger tasks. Default 200ms polling and result-wait backoff affect tiny tasks, so these are explicit low-latency single-host settings, not default-configuration results. Frequent polling costs CPU/SQL work; raw JSON retains process CPU. The pinned Worker.New ignores SingleWorkerMode; the field is not treated as an effective optimization.

The primary matrix plans five samples per point; growth and recovery plan three. Report valid sample counts, medians and observed min–max, not invented production p95/p99. Cumulative Go allocations are not peak memory or RSS. Windows operation timing uses QPC; startup/setup/warmup use Go time. Coarse CPU counters can report zero for short cases, which does not mean no CPU work.

The 60-second case context includes setup and warmup; a 65-second parent deadline cleans up the entire process tree. Ordinary run does no extra encoding for write metering and is time-bounded only; the 1GiB JSON budget applies solely to Durable growth. Limits retain their actual status and available confirmation progress, with later repetitions marked `not_repeated`. A timeout is not proof that one operation necessarily exceeds 60 seconds. Incomplete 10000-step runs are neither zero-filled nor extrapolated.

## Reproduce and check

The [isolated suite and full method](https://github.com/Luo-root/pulse-durable/blob/main/benchmarks/README_en.md) contain commands, pins and native boundaries. Shared tools live in Pulse's `benchmarks/tools`. Normal reruns write raw JSONL, module versions/checksums, binary SHA256, machine details, summaries and figures to Git-ignored `benchmarks/results/`. Archive complete evidence separately; the source repository selects only compact CSVs and PNGs used by the report. Separate `module-origins.json` queries same-version proxy metadata for Origin fields absent from Go's JSON; missing or failed entries are explicit.

Adapters first passed per-step counts/results/concurrency and race checks across two engines, two backends, 1/2/10/100 steps and both workloads. Real exit/restart and completed reopen passed at 10/100 steps. Independent review confirmed and fixed special-character URI database reuse and an unintended growth budget in recovery; original counterexamples passed afterward. Production and new-adapter CI passed on Linux/Windows. These are correctness checks, not performance measurements from shared CI machines.

Comparison dependencies remain in a separate module. Production go.mod, storage format and execution protocol are unchanged. This report retains current costs first; later optimizations must rerun the same task rather than overwrite an unfavorable baseline.
