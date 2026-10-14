# Guide overview

Pulse Durable adds checkpoint storage and recovery to Pulse graphs. It suits workflows with explicit node boundaries that need to reuse completed steps after an interruption, such as document processing stages, batch tasks, or long computations.

Pulse owns graph definitions, dependency scheduling, Aspects, and observation of the current execution. Durable saves complete node results and attempt state, then restores valid results into a new execution. The host still starts processes, supplies inputs, selects run identities, and handles external effects.

## Before you start

You need Go 1.25 or later and access to Go module downloads. Durable has no formal release yet; the tutorials pin a public development revision. You do not need to implement storage: the minimal example uses `MemoryStore`, and the recovery tutorial uses the included `filestore`.

If graphs, typed Keys, and declared outlets are new to you, start with the [Pulse quick start](https://luo-root.github.io/pulse/en/guide/quickstart). The Durable tutorials provide their own complete programs and dependency versions.

## Complete your first recovery

1. [Installation](installation.md): create a module, pin the dependency, and check Go and Pulse versions.
2. [Quick start](quickstart.md): run one node and read a declared outlet with value `42` and state `ready`.
3. [Recovery after process exit](recovery.md): exit the process inside a downstream node, then load the checkpoint in a new process and finish the run.

In the third step, the upstream body runs once and the downstream body runs once before exit and once after restart. This is Durable's central rule: reuse confirmed nodes and restart unconfirmed nodes from the beginning of their functions.

## Add recovery to your application

| Need | Guide |
| --- | --- |
| Compose stages into a child graph and retain Retry progress | [Subgraphs and retries](subgraphs.md) |
| Persist structs, named types, or special encodings | [Value codecs](codecs.md) |
| Choose a directory, investigate lock contention, or add a backend | [Storage](storage.md) |

If a business function calls an external system before its checkpoint is confirmed, recovery may call that function again. Establish idempotency keys, transaction boundaries, or an outbox when integrating your application.

## Check rules and handle errors

The guides explain tasks. The [reference](../reference/index.md) defines API and persistent record constraints:

- [Execution contract](../reference/execution.md): RunID, Version, recovery compatibility, cancellation, and recovery events.
- [Data model](../reference/data-model.md): scopes, attempts, value states, codecs, and the Store CAS protocol.
- [Error handling](../reference/errors.md): different responses to argument errors, contention, conflicts, and `CheckpointError`.

When a call fails, identify its error category first. A storage save may report an error after committing; stop this execution and reload the record when recovering. Repeated `Resume` calls do not increase an exhausted Retry budget.

## Current scope

Supports non-streaming values and static child graphs. `filestore` supports recovery after local process exit; `MemoryStore` retains records only while the same process is alive. Durable does not provide durable waits, distributed run management, deterministic replay, or exactly-once external effects.

Next: [Installation](installation.md).
