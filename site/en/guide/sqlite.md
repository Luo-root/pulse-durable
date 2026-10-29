# SQLite tutorial

This page describes the current development branch's breaking `Initialize`/`Apply` API, requiring **Go 1.26+** and **Pulse v0.5.0**; it does not describe a published release. `Session.Save` and `RunOptions.RequireDelta` have been removed. All official backends implement the new protocol, with no optional delta mode or full-record write fallback. The example uses `durable.New` and Runtime Run/Resume without an opt-in.

No published installation pin for this new API is supplied here; the old v0.1.0 is not a substitute for this branch. Follow [Contributing](https://github.com/Luo-root/pulse-durable/blob/main/CONTRIBUTING.md) to validate a local checkout and check [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases) for publication. Installation pins will be updated together across modules; do not treat the program below as a verified example for an old tag.

## 1. Install the independent module

In a new local directory:

```sh
mkdir durable-sqlite
cd durable-sqlite
go mod init example.com/durable-sqlite
# No published pin for this API is supplied here; configure the source workspace per Contributing.
```

The pure-Go driver `modernc.org/sqlite v1.60.1` embeds SQLite 3.53.4; no SQLite service installation is needed. Use a dedicated database, not one containing business tables: initialization strictly checks physical schema v3 and rejects old/unknown versions, modified DDL, or extra schema objects. It never automatically deletes, modifies, or migrates old data. Preserve old history with the old version/database; use a new directory/database file for the new version, rather than clearing or overwriting data to bypass rejection.

## 2. Save the complete main.go

The program confirms an upstream node, then deliberately exits inside the downstream node. `-resume` rebuilds the same business definition in another process and recovers it. Both database tutorials share business logic and differ only in backend initialization. `-resume` controls fault injection, not business versioning.

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
    "github.com/Luo-root/pulse-durable/sqlitestore"
)

func main() {
    resume := flag.Bool("resume", false, "resume the existing run")
    flag.Parse()
    if err := run(*resume); err != nil { log.Fatal(err) }
}

func run(resume bool) (err error) {
    ctx := context.Background()
    store, err := sqlitestore.NewContext(ctx, "./state/runs.sqlite")
    if err != nil { return err }
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

## 3. Start two processes

Use the same working directory for both processes so the relative path resolves to the same database:

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

The first command deliberately fails. `os.Exit(75)` skips defer. `go run` normally prints `exit status 75`, but its wrapper may return 1 to the shell. To observe 75 directly, first `go build -o durable-sqlite.exe .`, run `.\durable-sqlite.exe` in PowerShell and inspect `$LASTEXITCODE`, then run `.\durable-sqlite.exe -resume`.

Expected second output:

```text
enter downstream: 41
result: 42 ready
```

The downstream node can read `prepared` only after the upstream Apply confirms and publishes its output. Thus the upstream result is persistent at the exit point. Resume skips upstream and re-enters unconfirmed downstream. `pulse.ReadOutput` reads a declared outlet from the new Execution; it does not directly decode a database BLOB. Another `-resume` prints only the result; another Run without the flag returns `ErrConflict` because that RunID already exists. Assign a new RunID for new business, rather than deleting old records. Recovery needs the same graph, inputs, Version, and Codec contract.

## 4. Paths, WAL, locks, and backups

- `New(path)` calls `NewContext(context.Background(), path)`; the latter supports cancelling initialization. Use a local filename, not a SQLite URI or `:memory:`. Parents are created, absolute paths and symlinks resolved; new directories/files use 0700/0600, with ACLs on Windows.
- Initialization sets and reads back **WAL, synchronous=FULL (2), busy_timeout=5000ms**. Short `BEGIN IMMEDIATE` transactions insert or perform revision CAS. Business execution holds no database transaction. One Store serializes database operations; contention waits at most five seconds, without automatic retries.
- `<path>.init.lock` serializes initialization, waiting at most five seconds or context cancellation. `<path>.runlocks/` contains OS locks indexed by RunID SHA-256. Same-RunID contention across local processes immediately returns `ErrBusy`; different RunIDs can hold separate Sessions. Never delete lock files to “unlock,” or bypass the lock domain with hard links, pathname aliases, or copied lock directories.
- Only local filesystems supporting OS file locks are supported, not network shares, cross-machine dispatch, distributed leases/fencing, or exactly-once external effects. Process-exit recovery does not establish power-loss guarantees; FULL still depends on OS, device, and filesystem.
- Stop and wait for all Run/Resume calls and business tasks, close directly acquired Sessions, then close Store. Store.Close does not stop business functions. The example defers Store.Close on normal return; the deliberate exit relies on OS lock release.
- With WAL, copying only the live main database file is not a consistent backup. Coordinate shutdown and closure of all users, or use a separately verified SQLite-consistent backup procedure. Keep live database/WAL/SHM files, `.init.lock`, and `.runlocks/`; do not delete or replace them. The host owns permissions, backups, and retention. Power-loss, backup/restore, and migration have not received separate acceptance.

## 5. Initialize / Apply confirmation, Load, and Codec

Physical schema v3 uses `durable_snapshots`, `durable_batches`, `durable_heads`, and `durable_manifests` for original/synthesized snapshots, consecutive immutable batches, authoritative revision/chain, and representation generation/base. Decimal TEXT revisions preserve the uint64 range. Each Apply inserts a batch and CAS-updates the head within one transaction, without rewriting the complete RunRecord. Initial Load verifies digests and chains and replays within a consistent transaction; explicit Load returns an isolated complete projection, while ordinary Apply uses a session-private projection. The current uncompacted chain is limited to 100000 batches and 256 MiB of wire data; exceeding the budget rejects commits without dropping history or splitting atomic batches. The Store implements `durable.Maintenance` for explicit synthesis: InspectStorage first obtains the generation, then CompactStorage accepts `CompactStorageRequest{RunID, ThroughRevision, ExpectedGeneration}`. The target must be between the current base/head; a changed generation returns `ErrConflict`. Synthesis returns `ErrBusy` when a business Session holds the runlock. InspectStorage and LoadRevision instead use consistent read transactions without a runlock; the latter returns an isolated exact historical projection and chain anchor without creating a persistent pin. The new physical snapshot has an independent digest and preserves original `C(R)`. Only representation generation/base advances; business revision/head is unchanged, all history remains retained, and there is no GC. Inspect state after publication errors. Semantic history compression, retention, and `ReplayCreateDerived` remain unfinished, with no unlimited retention promise. Logical wire bytes are not SQLite WAL, physical writes, or measured reclaimed space. See [Checkpoint cost](checkpoint-cost.md). The backend does not encode business values. `durable.NewCodecs()` handles the example's int; register structs and custom types as described in [Value codecs](codecs.md), preserving Codec IDs/versions for recovery. Channels, functions, and unsafe.Pointer are unsupported; managed stream endpoints are not ordinary persistent values. Checkpoints contain business data and are not encrypted containers.

Runtime Run constructs valid initial state and calls Initialize at Revision=1; this is not an import path for existing business history. Repeated initialization returns `ErrConflict`. Subsequent typed Apply checks identity, strictly consecutive expected/next revisions, and object preconditions, returning an exact CommitAck. This is a backend protocol, not a request for applications to handwrite records or batches. **An Initialize/Apply error does not prove non-commit**: stop the original call, wait for execution and ownership release, Acquire the same RunID and Load authoritative state, then close the inspection Session before deciding to Resume. Never automatically retry old snapshots or batches, or treat old-format, permission, or corruption errors as `ErrNotFound` and create a record. This inspection fragment belongs in a function returning error, with existing `ctx` and `store`; do not run it concurrently with the same RunID's Run/Resume:

```go
session, err := store.Acquire(ctx, "database-demo-001")
if err != nil { return err }
record, loadErr := session.Load(ctx)
closeErr := session.Close()
if err := errors.Join(loadErr, closeErr); err != nil { return err }
fmt.Println("run:", record.RunID, "revision:", record.Revision)
```

See [Contributing](https://github.com/Luo-root/pulse-durable/blob/main/CONTRIBUTING.md) for source validation: root `./...` excludes independent backends. Before publication, validate the checkout with its temporary workspace steps, not a claim that target tags are installable. Continue with [Storage](storage.md) or the [PostgreSQL tutorial](postgresql.md).
