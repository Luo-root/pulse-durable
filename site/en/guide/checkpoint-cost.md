# Checkpoint cost and history growth

This page retains **historical full-snapshot write measurements** for [Pulse #411](https://github.com/Luo-root/pulse/issues/411), distinguishing individual checkpoint costs, cumulative logical writes, and retained storage. These are not performance results for current incremental backends and define no product throughput target or business retention period. See [Storage](storage.md) for guarantees and the [Data model](../reference/data-model.md) for record fields.

## Current development protocol versus historical data

The development branch uniformly uses `Session.Initialize`/`Apply`: Initialize publishes only a valid initial empty execution state at Revision 1. Ordinary commits atomically apply typed ChangeBatch operations with identity, consecutive revision, and exact precondition checks; Runtime publishes results only after a successful CommitAck. MemoryStore maintains a private non-durable projection. Filestore stores an initial snapshot, immutable batches, and an atomically replaced small head. SQLite and standalone PostgreSQL insert batches and CAS the head within transactions. Applications use `durable.New` and Run/Resume, not handwritten business records or batches, and do not set `RequireDelta`; the old Save interface and full-record fallback are removed. Initialization or commit errors may follow persistence: stop and adjudicate with Load from a new session, never blindly retry.

Physical-format upgrades neither migrate nor automatically delete or overwrite old JSON/databases. Preserve old data with the old version and isolated storage; use a new directory/database for the new version. Standalone PostgreSQL advisory locks are not distributed/postgres token/epoch/lease fencing; the latter's design establishes neither the former's capabilities nor database acceptance. PostgreSQL remains under development in this round; this page claims neither real-database acceptance nor publication.

Incremental commits reduce full-record rewrites, not history automatically or Load to constant cost. Initial Load validates the snapshot, digests, and consecutive batch chain to rebuild an isolated projection. Budget batch logs, message-attempt history, and business retention separately. The current SQLite cost fixture's `fullSnapshotObserver` is a **simplified observer** writing complete projections to another SQLite database, not an exact old-version benchmark. Incremental storage can cost more at 10 messages; do not claim every scale is faster. No unverified new figures are added here; keep current comparisons on [Production performance comparison](production-comparison.md), without relabeling these historical tables as new results.

Save, single-JSON-file descriptions, and formulas below refer to the 2026-10-16 implementation. Summed file lengths are logical bytes, not device I/O; SQLite database/WAL file sizes likewise do not establish actual device writes.

See [scale and performance](performance.md) for equal-task comparisons at 1–10000 ordinary business steps, current pinned revisions and actual process-recovery costs. This page keeps its original historical measurements.

## What remains after Pending is empty

`StreamRecord.Pending` holds messages with outstanding reading obligations. Acknowledgement removes a message from Pending, while its `MessageAttemptRecord` remains in `RunRecord.Messages`, preserving identity, attempt budget, and confirmation state. That record does not itself contain the encoded message payload. Explicit state and ordinary outputs can also retain business values. Empty Pending therefore does not mean an empty record.

The new `TestRecordGrowthHistorySurvivesDrainAndResume` completes eight messages through a real filestore, checks that Pending is empty and the contiguous acknowledgement frontier is eight, and verifies eight distinct confirmed history entries. It then creates a new Store and Runtime, resumes from the file, and checks unchanged history, the same output string, and no additional execution of either node body. This reopens files within one process; existing integration tests cover process exit and recovery.

At that historical baseline, a mutation path typically clones the record to build a proposal. `runState.save` also clones the record passed to Session and, after successful persistence, clones it into its confirmed state. `cloneRecord` traverses maps and copies slices and encoded bytes; immutable strings are reused. A filestore update reads and decodes the complete previous JSON to check Revision, validates metadata, encodes the complete new record, writes a temporary file, syncs it, and atomically replaces the record. Only the current JSON file is retained, rather than a separate file for every revision.

## Environment and method

Measured on 2026-10-16, using Durable runtime baseline `c80d8e86e570608603380f4f15a96ad19055dab9` and pinned Pulse dependency `v0.4.2-0.20261015113730-990607e5593a`. The machine ran Windows 11 `10.0.26200`, amd64, Go `1.25.0`, Intel Core i9-14900HX, and default `GOMAXPROCS=32`; records were on the local D: NTFS volume. Filesystem caches were not cleared and other system load was not controlled.

These measurements describe the historical baseline above, not the dependency used by every current build. See the repository's `go.mod` for the current pin. Remeasure costs after dependency or implementation upgrades rather than replacing the versions recorded here.

There are two groups. Fixture generation is excluded from component timings:

- **Component matrix:** first complete eight real string messages through MemoryStore to obtain a record template, then construct 8, 128, or 1024 confirmed history entries. `drained` has no Pending messages; `backlog` adds the same number of Pending messages on top of that history. String length independently varies between 32 and 4096 bytes. The template also retains one integer state and the final string as an ordinary result. Constructed records measure copying, encoding, and backend persistence; they do not establish large-run Runtime recovery correctness.
- **Component operations:** `Clone` calls the real `cloneRecord` through a test-only entry point; `JSON` calls `json.Marshal`; `FileSave` calls an actual filestore Session.Save. FileSave seeds Revision 1 outside the timer and performs ten updates inside it, including the old file read/decode, Unicode/metadata checks, new JSON encoding, file sync, and replacement. It excludes Acquire, Close, and Runtime cloning. JSON encoding is already included in FileSave, so adding the two timing columns would double-count encoding.
- **Real pipeline:** execute source → HandleMessage sink through filestore with buffer capacity one and no failures or retries. Each message updates cumulative byte state; the last publishes an ordinary string result. Run 8 or 128 messages with both string sizes. After every successful Save, Stat reads the actual file length for a cumulative byte count. That instrumentation is included only in pipeline timings, not component FileSave timings.

Each component sample uses exactly ten operations, repeated for three samples; each pipeline sample uses exactly one run, repeated for three samples. Tables report the median of each metric across those samples. Timing is the median of three sample means, not a production latency percentile. JSON buffer reuse, GC, file sync, and scheduling can all affect these short samples.

Real pipelines stop at 128 messages; 1024-message costs use constructed component records only. The largest constructed JSON is about 6.12 MB, and each FileSave sample performs only ten updates. This avoids repeatedly rewriting an ever-growing large pipeline merely to demonstrate growth. Tests use no elapsed-time performance thresholds.

## Component results

`history` counts confirmed message attempts. In `backlog`, Pending also equals history. `payload B` is the original Go string length; its encoding also includes JSON quotes, and `ValueRecord.Data` is base64 in the outer JSON. All byte counts are decimal bytes.

| State | history | payload B | JSON B | Clone µs/op | JSON µs/op | FileSave ms/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| drained | 8 | 32 | 5246 | 2.15 | 9.61 | 3.41 |
| drained | 8 | 4096 | 16082 | 2.46 | 12.58 | 16.95 |
| drained | 128 | 32 | 42962 | 6.56 | 68.71 | 18.52 |
| drained | 128 | 4096 | 53794 | 8.90 | 79.20 | 17.07 |
| drained | 1024 | 32 | 325233 | 47.42 | 563.82 | 23.00 |
| drained | 1024 | 4096 | 336069 | 47.89 | 576.90 | 22.03 |
| backlog | 8 | 32 | 7074 | 3.03 | 9.53 | 5.19 |
| backlog | 8 | 4096 | 61238 | 10.85 | 42.45 | 17.69 |
| backlog | 128 | 32 | 72158 | 16.09 | 97.40 | 18.04 |
| backlog | 128 | 4096 | 776238 | 136.90 | 427.27 | 21.18 |
| backlog | 1024 | 32 | 559741 | 172.69 | 824.94 | 24.55 |
| backlog | 1024 | 4096 | 6116561 | 760.49 | 3548.68 | 52.53 |

JSON B uses Revision 1. The FileSave file is measured at Revision 11 after updates, so it is one byte longer because of the additional revision digit. Every FileSave sub-benchmark checks the final file length against that record's JSON length outside the timer.

Allocation cells are **B/op / allocs/op**, also independently taking each metric's median across the three samples. They report cumulative heap allocation per operation, not live heap, peak memory, or disk usage.

| State | history | payload B | Clone | JSON | FileSave |
| --- | ---: | ---: | ---: | ---: | ---: |
| drained | 8 | 32 | 6624 / 32 | 10548 / 39 | 29272 / 258 |
| drained | 8 | 4096 | 16256 / 32 | 26130 / 39 | 67155 / 259 |
| drained | 128 | 32 | 32184 / 154 | 91975 / 280 | 209712 / 1830 |
| drained | 128 | 4096 | 41816 / 154 | 98354 / 280 | 245724 / 1832 |
| drained | 1024 | 32 | 223584 / 1052 | 656551 / 2073 | 1767019 / 13497 |
| drained | 1024 | 4096 | 233216 / 1052 | 659623 / 2072 | 1756244 / 13496 |
| backlog | 8 | 32 | 8928 / 49 | 13370 / 39 | 39529 / 303 |
| backlog | 8 | 4096 | 57088 / 49 | 93676 / 40 | 225337 / 304 |
| backlog | 128 | 32 | 69048 / 411 | 129652 / 280 | 348704 / 2481 |
| backlog | 128 | 4096 | 695131 / 411 | 808414 / 278 | 3266685 / 2491 |
| backlog | 1024 | 32 | 510305 / 3101 | 998968 / 2073 | 2992355 / 18627 |
| backlog | 1024 | 4096 | 5451520 / 3101 | 7816263 / 2073 | 25778228 / 18635 |

With no Pending, 1024 history entries occupy about 325–336 KB of JSON. Increasing the original string from 32 to 4096 bytes does not attach another large string to every history entry. With an additional 1024 large Pending strings, the record is about 6.12 MB, one clone allocates about 5.45 MB, and FileSave allocates about 25.78 MB. The evidence supports treating history count and unacknowledged payload size as separate dimensions, rather than counting messages alone.

## Real pipeline: final file versus cumulative writes

Every row executes real HandleMessage processing and filestore saves, and checks the ordinary output. Cumulative bytes sum the actual JSON file lengths after successful saves. Ranges cover the three runs because producer/consumer scheduling slightly changes intermediate records. These counts exclude reads, lock files, filesystem metadata, and physical device writes.

| Messages | payload B | Seconds/run | Saves/run | Final JSON B | Written B/run | Allocated B/run | Allocations/run |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 32 | 0.571 | 30 | 5258 | 89336–89551 | 1174128 | 8259 |
| 8 | 4096 | 0.510 | 30 | 16094 | 197967 | 1987960 | 8302 |
| 128 | 32 | 6.679 | 390 | 42975 | 8542754–8542967 | 70409880 | 557854 |
| 128 | 4096 | 6.192 | 390 | 53807 | 9951499 | 81472648 | 557816 |

Pipeline RunIDs are longer and final revisions differ from component fixtures, so final JSON byte counts differ slightly. The final 128-message record is about 43–54 KB, while generating it wrote about 8.54–9.95 MB of JSON cumulatively. These answer different questions: how much remains on disk, and how much this run wrote through repeated saves. Confirmation history grows over time and every write contains the complete record at that point. Empty Pending at the end does not undo earlier writes.

This is a bounded, serial measurement on one machine. File caches, sync, and scheduling can make some larger-payload timings lower than smaller ones. That does not show that larger messages are faster, and these numbers cannot establish concurrent or distributed throughput or a product SLO.

## Estimate record count and retention separately

Start with the actual application shape for each run:

```text
S_run = fixed metadata + history bytes(H) + Pending bytes(P, encoded size) + state and ordinary results
W_run = S_1 + S_2 + ... + S_K
D_JSON = sum of current S_run across all retained runs
```

`H` is retained attempt count, `P` is unacknowledged message count, and `K` is the number of successful saves. Retries can make attempt count exceed successful message count. `S_run` is the current snapshot size; `W_run` is cumulative logical JSON written throughout that run. If each step adds fixed-size history and saves the complete record, final size grows with step count while cumulative bytes include a term like `1 + 2 + ... + H`. The final file alone cannot determine that sum, and the sum is not a direct elapsed-time growth law.

If completed runs arrive at approximately `λ` records/day and the business retains them for `T` days, a simplified steady-arrival estimate is:

```text
Retained completed records R ≈ λ × T
D_JSON ≈ active-record JSON total + λ × T × mean completed-record size
```

Retention therefore multiplies **record count and per-record size**. For example, 1000 component records of the same shape, each with 1024 history entries, no Pending, and a 32-byte string, contain `1000 × 325233 = 325233000` JSON bytes. That is capacity arithmetic, not a measured 1000-record load test. Add lock files, filesystem allocation, backups, and the application's own archives separately.

Longer retention after completion increases occupancy; it does not cause additional Saves for an unchanged record. A long-running workflow that keeps processing messages, however, continues growing history within its own record. Budget these separately. The host chooses retention based on recovery, audit, and cost requirements; this page chooses no duration and supplies no safe contract for deleting individual historical attempts.

First collect active/completed run counts, history and Pending sizes, and save frequency for the target environment. Repeat the matrix there and compare it with host budgets. These measurements establish growth and the sources of complete-record write cost; actual target scale and acceptable cost should determine whether the storage approach needs to change.

## Reproduce

These commands refer to `record_growth_filestore_test.go` and `record_growth_test.go` at the historical baseline above. Use that measured commit and dependency in an isolated checkout: same-named tests on the development branch have been adapted to the new protocol and cannot reproduce old Save costs. Do not roll back or overwrite the current workspace. Run the commands from the historical checkout root, placing temporary files in its sibling `validation-scale` directory; the recorded measurements likewise used separate validation storage.

```powershell
$env:GOWORK = 'off'
$env:GOFLAGS = '-buildvcs=false'
$scaleArtifacts = [System.IO.Path]::GetFullPath('../validation-scale')
$env:TEMP = Join-Path $scaleArtifacts 'tmp'
$env:TMP = $env:TEMP
$env:GOTMPDIR = Join-Path $scaleArtifacts 'go-tmp'
[void][System.IO.Directory]::CreateDirectory($env:TEMP)
[void][System.IO.Directory]::CreateDirectory($env:GOTMPDIR)

go test -run '^TestRecordGrowth' -count=1 -v -timeout=60s .
go test -run '^$' -bench '^BenchmarkRecordGrowth$' -benchtime=10x -count=3 -benchmem -timeout=180s .
go test -run '^$' -bench '^BenchmarkRecordGrowthPipeline$' -benchtime=1x -count=3 -benchmem -timeout=180s .
```

Bound message count, payload, and operation count before expanding the matrix. Retention estimates do not need a benchmark that waits for days: use the application's record arrival rate and measured snapshot sizes in the formula, then validate the target storage environment as needed. The [recovery tutorial](recovery.md) explains how business code resumes existing runs.
