# Storage

Use this page to choose a checkpoint directory, inspect an existing run, or integrate a custom backend. Complete the [recovery tutorial](recovery.md) first to obtain a record that this page's inspection program can read.

## 1. Choose storage

| Store | Use | Guarantee scope |
| --- | --- | --- |
| `durable.NewMemoryStore()` | Examples, unit tests, and in-process protocol checks | In-process exclusive sessions and CAS; records do not survive process exit |
| `filestore.New(dir)` | Local workflow exit and restart recovery | Local cross-process file locking per RunID, file records, and atomic replacement |
| `sqlitestore.New(path)` | A local SQLite file with transactional initial snapshot and incremental batches | Local OS locking per RunID; short SQLite `BEGIN IMMEDIATE` transactions, FULL synchronous mode, and a 5000 ms busy timeout |
| `postgresstore.New(ctx, dsn)` | PostgreSQL checkpoint database | Session advisory lock per RunID; initial snapshot/batch BYTEA and short transactional head CAS, without fencing or dispatch guarantees |
| Custom `durable.Store` | Environments with explicit host storage and coordination requirements | Must implement Store/Session contracts and separately declare persistence and ownership guarantees |

`filestore` requires exclusive file locking and atomic replacement within a directory. It does not provide distributed leases, fencing, dispatch across machines, or a power-loss durability guarantee. Placing the directory on a network share does not by itself make it distributed run management.

This page describes the development branch's mandatory Initialize/Apply API, requiring Go **1.26+** and Pulse **v0.5.0**, not old v0.1.0 interfaces or a claim that the new API is published. There is no compatibility Save, RequireDelta, or full-record fallback. Applications use `durable.New` and Runtime Run/Resume without opt-in. Installation pins await a coordinated cross-module update; validate the local checkout per [Contributing](https://github.com/Luo-root/pulse-durable/blob/main/CONTRIBUTING.md) and check [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases) for publication. `sqlitestore` and `postgresstore` are independent modules; see the [SQLite tutorial](sqlite.md) and [PostgreSQL tutorial](postgresql.md) for complete programs and operations. Standalone PostgreSQL remains under development; this page does not claim real-database acceptance in this round.

`postgresstore` uses pgx/v5 and initializes and strictly checks a dedicated `pulse_durable` schema. Within one Store, held or pending RunIDs return `ErrBusy` immediately. Across Stores, acquisition must obtain a connection and complete the `pg_try_advisory_lock` network check; detected contention returns `ErrBusy` without waiting for lock release. Pool capacity and network checks share a default maximum wait of 30 seconds, with a shorter caller deadline taking precedence. Pool exhaustion that prevents checking remote ownership returns `context.DeadlineExceeded`, not a fabricated `ErrBusy`. Store.Close cancels pending acquisitions and waits for resource cleanup; failed or uncertain unlocks destroy the connection, with cleanup bounded to five seconds. The host owns the DSN; never log passwords. Real database tests use `PULSE_POSTGRES_TEST_DSN` and execute `DROP SCHEMA IF EXISTS pulse_durable CASCADE`: **use only an isolated test database, never a business database**. Without a DSN, database tests skip rather than pass. See [`postgresstore/README.md`](https://github.com/Luo-root/pulse-durable/blob/main/postgresstore/README.md) for source validation, usage, and boundaries.

`sqlitestore`: The driver `modernc.org/sqlite v1.60.1` embeds SQLite 3.53.4; `NewContext(ctx, path)` supports cancellable database initialization. Construction sets and reads back WAL, FULL (2), and busy_timeout=5000ms. Use a dedicated database: physical schema v2 DDL is checked strictly and extra schema objects are rejected. Old schemas are explicitly rejected, not automatically deleted, modified, or migrated; use a new database. Keep the neighboring `.init.lock` construction lock. `New(path)` accepts only a database filename, creates its parent directory, and creates a neighboring `.runlocks/` directory; new directories/files use 0700/0600 where applicable (Windows uses ACLs). Separate tables store the initial snapshot and immutable batches; the head stores authoritative revision and chain, with `revision` as decimal TEXT. Apply inserts a batch and CAS-updates the head in one transaction without rewriting a complete record. Short transactions use `BEGIN IMMEDIATE`, SQLite busy timeout is 5000 ms, and business execution never holds a database transaction. It is for local filesystems with OS file locking only; distributed coordination, migration, and cross-backend import are unsupported.

See the repository [`sqlitestore/README.md`](https://github.com/Luo-root/pulse-durable/blob/main/sqlitestore/README.md) for initialization and a runnable integration fragment. Stop business execution and close Sessions before closing the Store; after an Initialize/Apply error, reload through the Store contract instead of inferring whether it committed. FULL synchronous mode is not a completed power-loss test acceptance; the host owns backup, restore, and directory retention.

## 2. Configure a directory

This initialization fragment belongs in a function returning `error`. It needs imports for `durable` and `filestore`; follow it with Runtime `Run` or `Resume`:

```go
store, err := filestore.New("./checkpoints")
if err != nil {
	return err
}
runtime, err := durable.New(store, durable.NewCodecs())
if err != nil {
	return err
}
```

The directory path string must not be empty; the directory itself may be empty. `New` creates it when necessary and resolves a relative path to an absolute path when creating the Store. A new process creating another Store must still resolve to the original checkpoint directory.

On platforms with Unix permission bits, new directories use `0700` and new files use `0600`. Existing directory permissions are unchanged. Windows uses directory ACLs. The host manages directory access, backups, and retention; checkpoints are not encrypted containers.

Each RunID is located by SHA-256 of its original text, rather than inserted directly into a path. The new layout uses an initial `.snapshot-<id>`, immutable `.batch-<id>` files, and an authoritative `.head` manifest. Ownership still uses `.lock`. Persistent lock files are normal; OS locking provides exclusion until Close or process exit. Never delete lock files to resolve `ErrBusy`, which can create two exclusive domains.

Initialize publishes the initial snapshot and head. Ordinary Apply writes and syncs an immutable batch, then atomically replaces the small head in the same directory, without rewriting all history. Load follows only head references to verify snapshot digests, consecutive revisions, batch digests, and parent chains; neither mtime nor directory ordering establishes commits. A batch written before head publication is an uncommitted orphan, not a recoverable business result. Confirmation failures after head publication are uncertain commits. Explicit snapshot compaction is a separate maintenance capability, currently not implemented. Rewriting snapshots after every Apply is not compaction; automatic history compression and unlimited retention are not promised.

Detected old `.json` returns an explicit legacy-format error, without automatic deletion, overwrite, or migration. Preserve the old version and isolated directory when old data is needed; use a new directory for the new version. After locking, Acquire may still clean this run's regular temporary files exactly matching `<SHA-256>-<uint32 decimal>.tmp` (no leading zeroes except `0`), but never adopts unreferenced immutable files as commits. Cleanup does not recurse or touch other runs or `.lock`; failure returns an error and releases the lock. The host owns permissions and retention. See [Checkpoint cost](checkpoint-cost.md) for performance and logical-byte measurement boundaries; incremental storage does not promise every scale is faster.

## 3. Read an existing record

Run both processes from the [recovery tutorial](recovery.md) and keep `durable-recovery/checkpoints`. In a sibling directory, create an inspection module:

```sh
mkdir durable-storage
cd durable-storage
go mod init example.com/durable-storage
# Installation pins await a coordinated update; configure the current checkout per Contributing.
```

Requires Go 1.26 or later. Save this complete program as `main.go`:

```go
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"

	"github.com/Luo-root/pulse-durable/filestore"
)

func main() {
	dir := flag.String("dir", "../durable-recovery/checkpoints", "checkpoint directory")
	runID := flag.String("run-id", "recovery-demo-001", "existing run ID")
	flag.Parse()
	if err := inspect(*dir, *runID); err != nil {
		log.Fatal(err)
	}
}

func inspect(dir, runID string) (err error) {
	store, err := filestore.New(dir)
	if err != nil {
		return err
	}
	session, err := store.Acquire(context.Background(), runID)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, session.Close()) }()
	record, err := session.Load(context.Background())
	if err != nil {
		return err
	}
	fmt.Println("run:", record.RunID)
	fmt.Println("record format:", record.Format)
	fmt.Println("revision:", record.Revision)
	fmt.Println("scopes:", len(record.Scopes))
	fmt.Println("attempts:", len(record.Attempts))
	return nil
}
```

Run:

```sh
go mod tidy
go run . -dir ../durable-recovery/checkpoints -run-id recovery-demo-001
```

The input must use the new layout generated by the current development branch; old `.json` from a previous tutorial version is not compatible input. For the same simple business graph, expect run identity `recovery-demo-001`, logical format `2`, a positive revision, and one root scope plus two node root attempts after completion. The inspection tool only uses Acquire/Load/Close, prints metadata rather than encoded business values, and neither initializes nor commits changes. Logical RecordFormat and backend physical format are separate boundaries: recognizing a logical format does not automatically upgrade an old physical directory.

Reading also needs an exclusive session. If the business process still owns the same RunID, inspection immediately returns `ErrBusy`. `filestore.New` creates a missing directory, so a mistyped path may produce an empty directory followed by `ErrNotFound`. Check the path; absence of a record does not mean business work completed.

## 4. Integrate a custom Store

Applications usually use Runtime. Backend implementations must follow these steps:

1. `Acquire(ctx, runID)` obtains an exclusive session for the RunID. Detected ownership returns `ErrBusy` without waiting for that run to release ownership; backend contracts specify waits for connection capacity and network checks. Different RunIDs can be used independently.
2. `Load(ctx)` returns an isolated copy, or `ErrNotFound` if no record exists. Permission, corruption, and read failures must not masquerade as absence.
3. `Initialize(ctx, record)` atomically publishes valid initial empty execution state at Revision `1` only when no checkpoint exists, checking current format, RunID, and business identity. Existing checkpoints return `ErrConflict`; this cannot import business history.
4. `Apply(ctx, batch) (CommitAck, error)` is the sole ordinary commit entry point, atomically checking exact typed-change preconditions, identity, and strictly consecutive revisions. Durable backends persist the batch and CAS the head/chain; failures cannot publish partial batches, and revision overflow is rejected. New sessions authoritatively load the chain; ordinary Apply must not replay all history or perform full-record overwrites.
5. `Close()` is idempotent, waits for in-flight operations, and releases session ownership. Load/Initialize/Apply after close must fail. Store must be concurrency safe; optional-interface detection cannot fall back to old full-record writes.

RunRecord contains serializable data only. Scope and attempt identities are persisted random IDs; do not regenerate them from graph paths or local counters. See the [Data model](../reference/data-model.md) for field constraints and associations.

## 5. Handle uncertain saves

A save can finish writing and then fail during a confirmation step. For example, a head may have been replaced before subsequent confirmation fails. A returned error therefore does not prove the disk still holds the old record.

Runtime stops this execution without advancing its confirmed in-memory record, rolling back, or automatically retrying that write. Resolve the cause, wait for the original call to finish, and confirm ownership has been released. Check the original directory and RunID, then reload authoritative state through an exclusive session; close that inspection session before calling Runtime.

Use `Resume` for an existing valid, compatible record: reuse confirmed results and handle unconfirmed nodes under recovery rules, while retaining terminal-state and original Retry-budget constraints. Continue `Run` with the original inputs, version, and Codec contract only after confirming that this new `Run` never established its initial record. For example, initial input encoding can return `CheckpointError` before Initialize. Conversely, a failed Initialize may already have committed; do not infer absence or automatically turn `ErrNotFound` into `Run`. Investigate permission, corruption, and read failures instead of treating them as absence. See [Error handling](../reference/errors.md) for the decision procedure.

Custom backends must also permit and document this situation. Do not automatically repeat a failed save using an old revision, or overwrite the actual record with an old in-memory snapshot.

## Handle errors

- `ErrBusy`: identify the session or process that still owns the run. Let the host's coordination policy release ownership; do not delete lock files.
- `ErrNotFound`: check the directory and RunID, and whether the application ever created a record.
- `ErrConflict`: check the CAS revision or run identity. At a Runtime reliable-save boundary, it is also wrapped as `CheckpointError`.
- Permission, format, Unicode, or I/O errors: preserve the evidence and resolve the cause. A read failure must not cause a new record to overwrite the existing file.

Before writing, FileStore checks UTF-8 in every metadata string and map key. Invalid metadata leaves the previous file and Revision unchanged. Loading rejects invalid raw UTF-8 and unpaired JSON Unicode surrogates. Encoded `Data []byte` is opaque and is not restricted by metadata UTF-8 rules.

## Next steps

Check the Store/Session protocol in the [Data model](../reference/data-model.md) and reliable error categories in [Error handling](../reference/errors.md). Return to the [recovery tutorial](recovery.md) to integrate a business run.
