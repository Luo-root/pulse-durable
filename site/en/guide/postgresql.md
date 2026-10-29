# PostgreSQL tutorial

This page describes the current development branch's breaking `Initialize`/`Apply` API, requiring **Go 1.26+** and **Pulse v0.5.0**. The standalone PostgreSQL backend remains under development; this page claims neither publication nor real-database acceptance in this round. `Session.Save` and `RunOptions.RequireDelta` have been removed. All official backends use the new protocol without optional delta mode or full-record fallback. `durable.New` and Runtime Run/Resume require no opt-in.

No published installation pin for the new API is supplied here, and old v0.1.0 does not apply to the example below. Validate the local checkout per [Contributing](https://github.com/Luo-root/pulse-durable/blob/main/CONTRIBUTING.md). Cross-module pins await a coordinated update; check [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases) for publication.

## 1. Prepare a database and install modules

Use an existing PostgreSQL service or prepare an isolated example database. The driver is `pgx/v5`, with `pgxpool`; the module does not create databases. An administrator must create the database and login role first. Allow the role to connect and create the `pulse_durable` schema (database CREATE permission), and own that dedicated schema or have initialization table-creation privileges. Subsequent operations also need schema USAGE and table SELECT/INSERT/UPDATE permissions. Initialization creates tables and strictly checks schema versions; it does not migrate schemas. Do not put unrelated business objects in the dedicated schema.

```sh
mkdir durable-postgres
cd durable-postgres
go mod init example.com/durable-postgres
# No published pin for this API is supplied here; configure the source workspace per Contributing.
```

Supply a DSN through the environment, using a URI or pgx key/value syntax. These are example credentials: adapt them to your service and configure TLS for your deployment. Never commit a real DSN or log it. PowerShell:

```powershell
$env:PULSE_POSTGRES_DSN = 'postgres://demo_user:demo_password@localhost:5432/durable_demo'
```

POSIX shell:

```sh
export PULSE_POSTGRES_DSN='postgres://demo_user:demo_password@localhost:5432/durable_demo'
```

Both processes must use the same database and RunID. Restore previous environment settings afterward. `PULSE_POSTGRES_DSN` is this example's application variable, distinct from the destructive-test variable below.

## 2. Save the complete main.go

The business logic matches the SQLite tutorial: confirm upstream, then deliberately exit downstream. `-resume` controls fault injection only; it recovers in a second process without changing the business version. The program never prints the DSN and does not log constructor errors that could contain connection details.

```go
package main

import (
    "context"
    "errors"
    "flag"
    "fmt"
    "log"
    "os"

    "github.com/Luo-root/pulse"
    durable "github.com/Luo-root/pulse-durable"
    "github.com/Luo-root/pulse-durable/postgresstore"
)

func main() {
    resume := flag.Bool("resume", false, "resume the existing run")
    flag.Parse()
    if err := run(*resume); err != nil { log.Fatal(err) }
}

func run(resume bool) (err error) {
    ctx := context.Background()
    dsn := os.Getenv("PULSE_POSTGRES_DSN")
    if dsn == "" { return errors.New("set PULSE_POSTGRES_DSN before running") }
    store, err := postgresstore.New(ctx, dsn)
    if err != nil { return errors.New("PostgreSQL initialization failed; check credentials, connectivity and schema permissions securely") }
    defer func() { err = errors.Join(err, store.Close()) }()
    runtime, err := durable.New(store, durable.NewCodecs())
    if err != nil { return err }
    input := pulse.NewKey[int]("input")
    prepared := pulse.NewKey[int]("prepared")
    output := pulse.NewKey[int]("output")
    graph, err := pulse.New("database-recovery", "Recover a persisted workflow")
    if err != nil { return err }
    if err = graph.DeclareInput(pulse.NewPort(input)); err != nil { return err }
    if err = graph.DeclareOutput(pulse.NewPort(output)); err != nil { return err }
    if err = graph.Add(pulse.NewNode("upstream", "Prepare input",
        pulse.Requires(input), pulse.Provides(prepared),
        func(rc *pulse.RunCtx) error {
            value, err := pulse.Get(rc, input)
            if err != nil { return err }
            fmt.Println("enter upstream:", value)
            return pulse.Set(rc, prepared, value)
        })); err != nil { return err }
    if err = graph.Add(pulse.NewNode("downstream", "Add one",
        pulse.Requires(prepared), pulse.Provides(output),
        func(rc *pulse.RunCtx) error {
            value, err := pulse.Get(rc, prepared)
            if err != nil { return err }
            fmt.Println("enter downstream:", value)
            if !resume {
                fmt.Println("simulate process exit: 75")
                os.Exit(75)
            }
            return pulse.Set(rc, output, value+1)
        })); err != nil { return err }
    var inputs pulse.Inputs
    if err = pulse.Seed(&inputs, input, 41); err != nil { return err }
    options := durable.RunOptions{RunID: "database-demo-001", Version: "increment/1"}
    var execution *pulse.Execution
    if resume {
        execution, err = runtime.Resume(ctx, graph, inputs, options)
    } else {
        execution, err = runtime.Run(ctx, graph, inputs, options)
    }
    if err != nil { return err }
    value, state, err := pulse.ReadOutput(execution, output)
    if err != nil { return err }
    if state != pulse.SlotStateReady { return fmt.Errorf("unexpected output state: %s", state) }
    fmt.Println("result:", value, state)
    return nil
}
```

## 3. Run, exit, and Resume

```sh
go mod tidy
go run .
# After the deliberate exit, execute separately:
go run . -resume
```

Expected first output:

```text
enter upstream: 41
enter downstream: 41
simulate process exit: 75
```

Expected second output:

```text
enter downstream: 41
result: 42 ready
```

Downstream exits only after upstream Apply confirms and publishes `prepared`. The second process skips confirmed upstream, restarts unconfirmed downstream from its function beginning, and uses `pulse.ReadOutput` on the new Execution's declared outlet, rather than decoding database records directly. `os.Exit(75)` skips defer. PostgreSQL releases advisory locks after detecting connection loss, which may not be instantaneous. The `go run` wrapper may return 1 and print `exit status 75`, rather than returning the program's exit code. To inspect it directly, first `go build -o durable-postgres.exe .`, run `.\durable-postgres.exe` in PowerShell, inspect `$LASTEXITCODE`, then run `.\durable-postgres.exe -resume`.

Another `-resume` reads confirmed results only; another Run with this RunID returns `ErrConflict`. Use new RunIDs for new business without overwriting old records. Preserve graph, inputs, Version, and Codec contracts for recovery; proactively change business Version when function or closure semantics change. Without a local DSN, the example can only be compiled, not claimed as a PostgreSQL execution or database acceptance.

## 4. Connections, locks, and shutdown

- The development implementation creates and strictly checks physical schema `pulse_durable` v3: `schema_meta`, `snapshots`, `heads`, and `batches`. Initial snapshots and batch wire use BYTEA; decimal TEXT revisions preserve the uint64 range. Old v1/v2, unknown, or incompatible schemas are explicitly rejected, never automatically deleted, modified, or migrated. Preserve the old version with its database and use a separate new database for the new version.
- Same-Store held or pending RunIDs immediately return `ErrBusy`. Across Stores, Acquire obtains a pool connection, then checks `pg_try_advisory_lock`. Detected contention returns `ErrBusy`, without waiting for the owner to release. Pool waiting and network checks share a default maximum of **30 seconds**, with shorter caller deadlines taking precedence. If pool exhaustion prevents remote ownership detection, timeout returns `context.DeadlineExceeded`, not `ErrBusy`.
- Every Session occupies one connection until Close, with Load/Initialize/Apply using that connection. Different RunIDs can proceed concurrently within pool capacity; long business executions retain connections. Business execution does not hold a commit transaction.
- Close returns a connection only after confirmed advisory unlock. Failed or uncertain unlock destroys the connection; cleanup is bounded to **five seconds**. Store.Close cancels pending Acquire, closes active Sessions, waits for cleanup, and is repeatable. Stop and wait for all Run/Resume calls and business tasks, close directly acquired Sessions, then close Store. Store.Close does not stop business functions.
- Advisory locks provide connection-level exclusion, **not leases, fencing tokens, dispatch, or automatic takeover**. After disconnect, old business functions can still produce external effects; lock release does not establish exactly-once. Applications still need idempotency, deduplication, or an outbox. Hosts define policies for network failures, database restarts, and pool exhaustion.
- Use PostgreSQL backup tools such as `pg_dump`, including `pulse_durable` in restore drills. Never log DSNs or passwords. Hosts own access control, backups, retention, TLS, and credentials. This tutorial does not establish acceptance for network failures, database restarts, backup/restore, or distributed fencing.

## 5. Initialize / Apply confirmation, Load, and Codec

Runtime Run constructs valid initial empty execution state, published at Revision 1 by Initialize. It cannot import existing business history; repeated initialization returns `ErrConflict`. Ordinary commits only use typed Apply: one short transaction inserts the batch and CAS-updates the head, checking identity, strictly consecutive expected/next revisions, exact object preconditions, and chain anchors, then returns an exact CommitAck. Applications keep using Run/Resume rather than handwriting business records or batches.

Initial Load verifies the initial snapshot, digests, and consecutive batch chain in a consistent snapshot to rebuild a private projection; explicit Load returns an isolated complete record. Ordinary Apply neither rewrites a complete snapshot nor replays all history. The current uncompacted chain budget is 100000 batches and 256 MiB of wire data; exceeding it returns `ErrBudget`, without dropping history or splitting atomic batches. The Store implements `durable.Maintenance`: InspectStorage observes generation/base/head; LoadRevision returns an exact historical projection and original chain anchor without creating a persistent pin; CompactStorage explicitly synthesizes using RunID, ThroughRevision, and ExpectedGeneration. The target must be between the current base/head; a stale generation returns `ErrConflict`. All maintenance operations take the existing advisory lock, cannot run alongside an active Session for that RunID, and return `ErrBusy` on detected contention. Snapshot digests are calculated independently, preserving original `C(R)`. Publication advances representation generation/base only, not business revision/head. All history, original snapshots, and batches remain retained without GC. Inspect state after publication errors. Semantic history compression, retention, and `ReplayCreateDerived` remain unfinished, with no unlimited retention promise. Logical wire bytes are not PostgreSQL WAL or physical disk writes. See [Checkpoint cost](checkpoint-cost.md) for cost boundaries.

Initialize/Apply errors can follow successful database commits and are **not automatically retried**. Stop the original call and wait for ownership release, Acquire the same RunID, Load authoritative state, then close the inspection Session before deciding to Resume. Never blindly resubmit old snapshots or batches. Old formats, corruption, permissions, and connection errors are not `ErrNotFound` and must not trigger record creation.

This inspection fragment belongs in an error-returning function with existing `ctx` and `store`; do not run it concurrently with that RunID's Run/Resume:

```go
session, err := store.Acquire(ctx, "database-demo-001")
if err != nil { return err }
record, loadErr := session.Load(ctx)
closeErr := session.Close()
if err := errors.Join(loadErr, closeErr); err != nil { return err }
fmt.Println("run:", record.RunID, "revision:", record.Revision)
```

The database backend stores the initial snapshot and incremental batches; Load returns a complete projection. It does not encode business types. `durable.NewCodecs()` handles the example int; register custom types through [Value codecs](codecs.md) and preserve Codec IDs/versions for recovery. Channels, functions, and unsafe.Pointer are unsupported; managed stream endpoints are not saved as ordinary values. Checkpoints contain business data and are not encrypted containers.

Distributed PostgreSQL is a separate backend with dedicated `pulse_durable_distributed` schema **v6**, not the standalone schema above. Old v1–v5 or incompatible schemas are explicitly rejected without writing, deleting, or migrating old data; use a new database. Logical record format 2 and coordination protocol 1 remain unchanged and independent of the physical schema version. Its namespace-qualified maintenance API (Coordinator.InspectStorage/CompactStorage/FindMaintenance) uses Namespace/RunID and requires database `maintain` permission; worker ownership/tokens cannot substitute for maintenance authorization. Synthesis captures a build pin, rebuilds outside the publication lock, then validates generation and CAS-publishes, preserving concurrently appended business suffixes. A mutation guard and publication receipt support outcome checks; use `FindMaintenance` after unknown commits rather than blindly retrying. Pins are not implicitly renewed. There is currently no public LoadRevision, so this API must not be claimed to satisfy root `durable.Maintenance`. It also supplies no semantic compression, retention, ReplayCreateDerived, or GC. This page does not claim that the complete distributed real-database suite passed this round; local strict-clock checks remain a risk requiring separate validation closure.

Business mutation receipts are separate from those maintenance receipts: `mutation_receipts` stores receipts, permanent `mutation_guards` prevent identity reuse, and `events` are diagnostic only. `Coordinator.FindMutation(ctx, MutationQuery)` matches Identity, Mutation ID/Digest, Operation, and a closed RequestedWindow. ExpectedEpoch/ExpectedRevision are optional pointers: nil means unknown; non-nil, including 0, requires an exact match. Do not guess them from a later Get. Results are `Found` (the exact mutation committed), `AbsentWithinWindow` (the request is fully covered but no matching receipt exists), or `Expired` (the request starts before coverage and observation has reached its upper bound). A future requested upper bound is rejected, not Expired.

Coverage.From is the persisted database `receipt_coverage_floor`; Coverage.Through is database observation time sampled under the run lock and persisted as `receipt_observed_through`, not derived from client time. Retained receipts may predate the floor; a receipt missing where required inside coverage is an error. This query writes a watermark and requires a read/write primary, not a replica or read-only transaction. It still requires only namespace `query` permission, without direct application table DML. Missing receipts, AbsentWithinWindow, and Expired neither prove non-commit nor authorize business replay. Found resolves only the exact coordinator mutation, not external effects or lease/execution authority. RetentionPolicy TTL validation/CAS request contracts do not implement persisted policy updates, actual retention deletion, or GC; replay and semantic compression remain unfinished.

## 6. Real-database tests require a disposable test database

Root `go test ./...` excludes this independent backend. See [Contributing](https://github.com/Luo-root/pulse-durable/blob/main/CONTRIBUTING.md) for source-workspace and independent-module validation. Real database tests use separate **`PULSE_POSTGRES_TEST_DSN`** and execute **`DROP SCHEMA IF EXISTS pulse_durable CASCADE`**, then recreate it. **Only use an isolated disposable test database, never a business database or the example database you want to preserve.** Do not alias the test variable to your application DSN.

In the contribution guide's temporary workspace:

```powershell
$env:PULSE_POSTGRES_TEST_DSN = 'postgres://test_user:test_password@localhost:5432/durable_test'
go test -race -count=1 ./postgresstore/...
# Restore the previous environment variable afterward.
```

Without the test DSN, database tests skip rather than pass. Compilation or ordinary tests without a DSN do not establish Run/Resume execution. Continue with [Storage](storage.md) or the [SQLite tutorial](sqlite.md).
