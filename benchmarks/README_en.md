# Durable workflow scale costs

The isolated suite for [Durable #37](https://github.com/Luo-root/pulse-durable/issues/37), paired with [Pulse #418](https://github.com/Luo-root/pulse/issues/418). `workflow/` is a separate Go module; comparison frameworks and databases do not enter the production module. Chinese: [README.md](README.md).

## Same task, different guarantees

The first complete matrix uses chains of 1, 10, 100, 1000 and 10000 business steps. One Pulse node corresponds to one go-workflows Activity, rather than an entire Pulse graph inside one Activity. The first input is uint64(1); each subsequent input is `1 + previous output`. The compute function, rounds=1/256 workloads and terminal checksum match the [Core specification](https://github.com/Luo-root/pulse/blob/main/benchmarks/README_en.md). Fix GOMAXPROCS and business concurrency for each matrix. The chain's critical path equals its step count; it does not establish wide-graph throughput.

| Group | Durable | go-workflows |
|---|---|---|
| `memory` | Isolated Go records in MemoryStore | Named shared-cache SQLite in-memory database with a retained connection, not a Go map |
| `file` | Full JSON snapshots, file Sync then replacement | SQLite file, WAL requested, synchronous not overridden; actual PRAGMAs retained per sample |

Both persistent backends complete the same business task, but differ in transactions, history format, replay and power-loss guarantees. Filestore does not promise power-loss durability. MemoryStore does not demonstrate process-restart recovery. Read memory and file groups separately; do not rank them alongside pure in-memory DAG engines.

Pinned versions:

- Durable `v0.0.0-20261016143640-e47e51eda579`.
- Pulse `v0.4.2-0.20261016143631-7d70e1e14771`.
- go-workflows `v1.4.2`, with pure-Go `modernc.org/sqlite v1.27.0`.

There are no replacements. Retain versions, module checksums and binary SHA256 before sampling, without `@latest`. Supplementary `module-origins.json` queries the same-version proxy's actual Origin metadata; missing or failed entries are explicit and hashes are never invented.

## Timing boundaries

`run` measures new-run submission through confirmed completion and correct terminal-output reading. Graph, Codec, Runtime, empty SQLite database/migrations, registration and worker preparation are outside the operation timer and reported as `setup_ns`. Durable Runtime's per-run checkpoint-model and execution work remain timed. Warmup uses a separate backend; every measured iteration also uses a fresh isolated backend and the same RunID. No engine reads a previous completed result as a new run. Inspection and cleanup are outside timing, but must succeed before a sample is considered successful.

The comparison uses an explicit low-latency single-host configuration: one Workflow and one Activity poller, each polling every 1ms, with the same business concurrency cap; completion state is also polled every 1ms. Default 200ms polling and exponential result-wait backoff substantially affect small tasks, so these are not default-configuration results. Frequent polling and SQL queries cost CPU. `cpu_ns_per_op` separately measures process CPU; Windows accounting is coarse and can return zero for short operations, which does not mean zero CPU consumption.

Both SQLite groups use the public WithDB constructor with one connection and a retained idle connection. The native in-memory convenience constructor uses two connections; the suite selects a fixed single-connection local configuration and exposes it alongside journal_mode, synchronous, busy_timeout, cache and page settings. The pinned Worker.New does not act on SingleWorkerMode; setting that field is not treated as a guarantee. The history-event limit is raised with graph size to prevent the default 10000-event limit from ending large runs prematurely.

`growth` is a separate instrumented scenario. A Durable Store wrapper performs an extra JSON encoding before each Save, counting Save calls, cumulative JSON bytes passed to successful Saves and final record size, with a cumulative logical-byte budget. Do not mix these extra costs into ordinary `run` rankings. After timing and before closing connections, SQLite reports event counts and current database/WAL file lengths; a WAL checkpoint at close can change those lengths. It does not provide an equivalent cumulative physical/logical-write counter. These are different metrics, not directly comparable write-amplification figures.

`resume` launches a child process and calls `os.Exit(73)` after half the steps are durably confirmed. A new process continues the same record/instance. Durable stops after successful Save and before downstream publication. The go-workflows hook stops after a confirmed history transaction and briefly prevents the next Activity from being claimed, avoiding an unrelated unfinished lease wait. A disk audit checks that every business step ran exactly once across both processes. The worker name stays `workflow-bench-single-host`, measuring same-host restart rather than cross-machine claiming/lease takeover. This precise crash boundary does not establish exactly-once external side effects.

Recovery separates process startup, backend/Graph preparation, restore plus remaining work, and inspection/cleanup/IPC. Business-entry audits fsync and increase remaining-work latency, so compare recovery only with similarly audited recovery samples. This mode leaves unmeasured allocation counters empty.

`reopen` separately opens an already completed run. Durable rebuilds/validates/restores an Execution; go-workflows queries the completed instance result without starting a worker. These are different native operations and do not establish a relative recovery-performance claim.

## Build and reproduce

From this repository root, validate the separate module first:

```sh
cd benchmarks/workflow
go mod download
go build -o workflow.exe .
go vet ./...
go test -race -count=1 -timeout=180s ./...
```

Set `GOWORK=off` and retain no replacements. On Linux/macOS, name the executable `workflow`. Recovery tests launch the compiled test executable; no external service or credentials are needed.

Use the shared sampler from the Pulse repository, adjusting paths for your checkouts:

```sh
python ../pulse/benchmarks/tools/run_matrix.py --suite workflow --module benchmarks/workflow --binary benchmarks/workflow/workflow.exe --out benchmarks/results/local-workflow --sizes 1,10,100,1000,10000 --rounds 1 --repeats 5 --procs 4 --parallelism 4
python ../pulse/benchmarks/tools/capture_origins.py benchmarks/results/local-workflow/manifest.json --out benchmarks/results/local-workflow/module-origins.json
python ../pulse/benchmarks/tools/summarize.py benchmarks/results/local-workflow/samples.jsonl --out benchmarks/results/local-workflow/figures
```

Or run one precise scenario:

```sh
benchmarks/workflow/workflow.exe -engine durable -backend file -nodes 100 -rounds 1 -mode growth -warmup=false -max-write-bytes 1073741824 -timeout 60s -dir benchmarks/results/local-scratch
benchmarks/workflow/workflow.exe -engine go-workflows -backend file -nodes 100 -rounds 1 -mode resume -timeout 60s -dir benchmarks/results/local-scratch
```

Ordinary `run` does no extra encoding for write metering and is time-bounded only; the 1GiB logical-byte guard applies to Durable `growth`. Context cancellation cannot forcibly interrupt synchronous encoding/storage, so the sampler adds a 65-second parent deadline and cleans up the contained process tree. Retain timeout, resource-limit and unmeasured states separately. Never fill an incomplete 10000-step point with zero or an extrapolated value.

Keep `manifest.json`, `samples.jsonl`, summaries and SVGs; do not commit checkpoint, audit, SQLite or WAL files from `work/`. Cumulative Go allocations are not peak memory, current file length is not cumulative writes, and repeated-batch min–max is not a production latency percentile. See the site performance guide for actual measurements; the [historical checkpoint-cost page](../site/en/guide/checkpoint-cost.md) keeps its original versions and numbers.
