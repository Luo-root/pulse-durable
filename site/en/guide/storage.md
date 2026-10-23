# Storage

Use this page to choose a checkpoint directory, inspect an existing run, or integrate a custom backend. Complete the [recovery tutorial](recovery.md) first to obtain a record that this page's inspection program can read.

## 1. Choose storage

| Store | Use | Guarantee scope |
| --- | --- | --- |
| `durable.NewMemoryStore()` | Examples, unit tests, and in-process protocol checks | In-process exclusive sessions and CAS; records do not survive process exit |
| `filestore.New(dir)` | Local workflow exit and restart recovery | Local cross-process file locking per RunID, file records, and atomic replacement |
| `sqlitestore.New(path)` | A local SQLite file with complete JSON records and short transactional CAS | Local OS locking per RunID; short SQLite `BEGIN IMMEDIATE` transactions, FULL synchronous mode, and a 5000 ms busy timeout |
| `postgresstore.New(ctx, dsn)` | PostgreSQL checkpoint database | Session advisory lock per RunID; complete JSON BYTEA records and short transactional revision CAS, without fencing or dispatch guarantees |
| Custom `durable.Store` | Environments with explicit host storage and coordination requirements | Must implement Store/Session contracts and separately declare persistence and ownership guarantees |

`filestore` requires exclusive file locking and atomic replacement within a directory. It does not provide distributed leases, fencing, dispatch across machines, or a power-loss durability guarantee. Placing the directory on a network share does not by itself make it distributed run management.

`sqlitestore` and `postgresstore` are independent Go 1.26 modules. Public installation pins for the current backend revisions must be verified after pushing. For now, validate source using the temporary `go.work` in [Contributing](https://github.com/Luo-root/pulse-durable/blob/main/CONTRIBUTING.md), not release dependency `replace` directives. Durable `v0.1.0` has not been released.

`postgresstore` uses pgx/v5 and initializes and strictly checks a dedicated `pulse_durable` schema. Within one Store, held or pending RunIDs return `ErrBusy` immediately. Across Stores, acquisition must obtain a connection and complete the `pg_try_advisory_lock` network check; detected contention returns `ErrBusy` without waiting for lock release. Pool capacity and network checks share a default maximum wait of 30 seconds, with a shorter caller deadline taking precedence. Pool exhaustion that prevents checking remote ownership returns `context.DeadlineExceeded`, not a fabricated `ErrBusy`. Store.Close cancels pending acquisitions and waits for resource cleanup; failed or uncertain unlocks destroy the connection, with cleanup bounded to five seconds. The host owns the DSN; never log passwords. Real database tests use `PULSE_POSTGRES_TEST_DSN` and execute `DROP SCHEMA IF EXISTS pulse_durable CASCADE`: **use only an isolated test database, never a business database**. Without a DSN, database tests skip rather than pass. See [`postgresstore/README.md`](https://github.com/Luo-root/pulse-durable/blob/main/postgresstore/README.md) for source validation, usage, and boundaries.

`sqlitestore`: The driver `modernc.org/sqlite v1.60.1` embeds SQLite 3.53.4; `NewContext(ctx, path)` supports cancellable database initialization. Construction sets and reads back WAL, FULL (2), and busy_timeout=5000ms. Use a dedicated database: v1 DDL is checked strictly and extra schema objects are rejected. Keep the neighboring `.init.lock` construction lock. `New(path)` accepts only a database filename, creates its parent directory, and creates a neighboring `.runlocks/` directory; new directories/files use 0700/0600 where applicable (Windows uses ACLs). A complete `RunRecord` is stored as a JSON BLOB and `revision` as decimal TEXT. Short transactions use `BEGIN IMMEDIATE`, SQLite busy timeout is 5000 ms, and business execution never holds a database transaction. It is for local filesystems with OS file locking only; distributed coordination, migration, and cross-backend import are unsupported.

See the repository [`sqlitestore/README.md`](https://github.com/Luo-root/pulse-durable/blob/main/sqlitestore/README.md) for initialization and a runnable integration fragment. Stop business execution and close Sessions before closing the Store; after a Save error, reload through the Store contract instead of inferring whether it committed. FULL synchronous mode is not a completed power-loss test acceptance; the host owns backup, restore, and directory retention.

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

Each RunID is located by SHA-256 of its original text, rather than inserted directly into a path. Records use `.json`, and ownership uses `.lock`. Persistent lock files are normal. Ownership comes from the operating system lock and is released on session close or process exit. Deleting a lock file can create two separate exclusive domains; it is not a way to resolve `ErrBusy`.

A save writes the complete new record to a temporary file in the same directory, syncs it, then replaces `.json`. If the process exits before replacement, the next `Acquire` removes leftover temporary files after taking that RunID's lock. The original `.json` remains authoritative; uncommitted records are never recovered from temporary files. Cleanup matches only regular files named `<SHA-256>-<uint32 decimal>.tmp` for this run (no leading zeroes except `0`). It does not recurse or touch other runs, similar names, or `.lock` files. Directory-read or removal failures return an error and release the lock. Temporary files may remain until that run next acquires its lock successfully; directory permissions and retention remain the host's responsibility.

## 3. Read an existing record

Run both processes from the [recovery tutorial](recovery.md) and keep `durable-recovery/checkpoints`. In a sibling directory, create an inspection module:

```sh
mkdir durable-storage
cd durable-storage
go mod init example.com/durable-storage
go get github.com/Luo-root/pulse-durable@v0.0.0-20261023040750-1b397e91c878
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

Expect run identity `recovery-demo-001`, format `2`, and a positive revision. New ordinary and streaming runs both use format `2`; existing ordinary format `1` records remain readable and upgrade to `2` on their next save. After the recovery tutorial's simple graph completes, it contains one root scope and two node root attempts. The inspection tool prints metadata, not encoded business values, and does not call `Save`.

Reading also needs an exclusive session. If the business process still owns the same RunID, inspection immediately returns `ErrBusy`. `filestore.New` creates a missing directory, so a mistyped path may produce an empty directory followed by `ErrNotFound`. Check the path; absence of a record does not mean business work completed.

## 4. Integrate a custom Store

Applications usually use Runtime. Backend implementations must follow these steps:

1. `Acquire(ctx, runID)` obtains an exclusive session for the RunID. Detected ownership returns `ErrBusy` without waiting for that run to release ownership; backend contracts specify waits for connection capacity and network checks. Different RunIDs can be used independently.
2. `Load(ctx)` returns an isolated copy, or `ErrNotFound` if no record exists. Permission, corruption, and read failures must not masquerade as absence.
3. `Save(ctx, record, expectedRevision)` performs atomic compare-and-swap. The first expected revision is `0` and the new Revision is `1`; later Revision must equal expected plus one, with the record RunID matching the session.
4. Return `ErrConflict` on a failed CAS condition and leave the old record unchanged. Successful records must be fully readable; partial updates cannot be visible.
5. `Close()` releases ownership and permits repeated calls. `Load` and `Save` after close must fail. Store must be concurrency safe.

RunRecord contains serializable data only. Scope and attempt identities are persisted random IDs; do not regenerate them from graph paths or local counters. See the [Data model](../reference/data-model.md) for field constraints and associations.

## 5. Handle uncertain saves

A save can finish writing and then fail during a confirmation step. For example, a record file may have been replaced before directory sync fails. A returned error therefore does not prove the disk still holds the old record.

Runtime stops this execution without advancing its confirmed in-memory record, rolling back, or automatically retrying that write. Resolve the cause, wait for the original call to finish, and confirm ownership has been released. Check the original directory and RunID, then reload authoritative state through an exclusive session; close that inspection session before calling Runtime.

Use `Resume` for an existing valid, compatible record: reuse confirmed results and handle unconfirmed nodes under recovery rules, while retaining terminal-state and original Retry-budget constraints. Continue `Run` with the original inputs, version, and Codec contract only after confirming that this new `Run` never established its initial record. For example, initial input encoding can return `CheckpointError` before the first Save. Conversely, a failed initial Save may already have committed; do not infer absence or automatically turn `ErrNotFound` into `Run`. Investigate permission, corruption, and read failures instead of treating them as absence. See [Error handling](../reference/errors.md) for the decision procedure.

Custom backends must also permit and document this situation. Do not automatically repeat a failed save using an old revision, or overwrite the actual record with an old in-memory snapshot.

## Handle errors

- `ErrBusy`: identify the session or process that still owns the run. Let the host's coordination policy release ownership; do not delete lock files.
- `ErrNotFound`: check the directory and RunID, and whether the application ever created a record.
- `ErrConflict`: check the CAS revision or run identity. At a Runtime reliable-save boundary, it is also wrapped as `CheckpointError`.
- Permission, format, Unicode, or I/O errors: preserve the evidence and resolve the cause. A read failure must not cause a new record to overwrite the existing file.

Before writing, FileStore checks UTF-8 in every metadata string and map key. Invalid metadata leaves the previous file and Revision unchanged. Loading rejects invalid raw UTF-8 and unpaired JSON Unicode surrogates. Encoded `Data []byte` is opaque and is not restricted by metadata UTF-8 rules.

## Next steps

Check the Store/Session protocol in the [Data model](../reference/data-model.md) and reliable error categories in [Error handling](../reference/errors.md). Return to the [recovery tutorial](recovery.md) to integrate a business run.
