# pulse-durable

[中文](README.md)

`github.com/Luo-root/pulse-durable` is an independent checkpoint execution library for Pulse. Definitions remain reusable; every `Run` / `Resume` creates a fresh `pulse.Execution`. A complete node chain publishes its outputs after storage confirms its checkpoint. Recovery reuses confirmed results and restarts unconfirmed nodes from the beginning of their functions.

The first implementation, plan A, persists Retry cursors and child graph scopes. An interrupted active parent attempt reclaims its scope and reuses confirmed child nodes. A failed parent Retry attempt invalidates its descendants; the next attempt gets a new scope. A root node chain has one complete execution budget: repeatedly calling Resume cannot replenish the Retry budget of a run that ended in a real failure.

## Usage

Requires Go 1.25+. Use `durable.NewMemoryStore()` to exercise the protocol, or `filestore.New(dir)` / another persistent Store for recovery across processes. With an assembled graph and seeded `pulse.Inputs`:

```go
codecs := durable.NewCodecs()
rt, err := durable.New(store, codecs)
if err != nil {
    return err
}
options := durable.RunOptions{RunID: "order-123", Version: "business/1"}
e, err := rt.Run(ctx, g, in, options)
if err != nil {
    return err
}
value, state, err := pulse.ReadOutput(e, output)
```

For recovery, call `rt.Resume(ctx, g, in, options)` with the same RunID, business version, definition, and inputs. Both methods block until this execution finishes and return its handle when available, including on failure. Run requires a new record (`ErrConflict` if it exists); Resume requires an existing record (`ErrNotFound` otherwise). See the [Chinese README](README.md) for a complete graph assembly example.

Runnable examples:

- [Basic Run / Resume](example_test.go): uses MemoryStore to demonstrate reusing confirmed nodes and re-entering unconfirmed nodes.
- [A parent workflow with a document-processing child](subgraph_example_test.go): uses a real filestore, creates an exclusive directory under the current directory and cleans it up afterward, and demonstrates `In` / `Out` wiring and reading a parent outlet.
- [Actual process exit / restart validation](integration/recovery_test.go): a child process exits at a persistent boundary, then a new process rebuilds the Graph and resumes. It covers child graphs, Retry, and uncertain commits.

The child graph example statically mounts C1 download → C2 extract → C3 summarize with `pulse.Sub`; string transformations stand in for business operations. The parent mount uses `Retry(3, 0)`. The first Run cancels the host context after C1/C2 checkpoints are confirmed and C3 has entered its body and read C2, producing body call counts of 1/1/1. It then rebuilds filestore, Runtime, and an equivalent Graph, supplies the same inputs, and calls Resume with the same RunID and Version. The parent Retry reclaims the original active attempt scope: Resume calls the bodies 0/0/1 times, for totals of 1/1/2. Finally, `pulse.ReadOutput` reads the summary from the parent's declared outlet.

This example rebuilds objects within one process. Actual exit / restart evidence is in the integration test linked above. Unconfirmed bodies and their external effects may repeat; callers still need idempotency, transactions, or an outbox.

Run from this repository's root:

```text
go test -run '^ExampleRuntime_Resume$' -v .
go test -run ExampleRuntime_Resume_subgraph -v .
go test -run TestCheckpointRecoveryAcrossProcessExit -v ./integration
```

## Versions, codecs, and storage

The explicit business `Version` is required. The definition fingerprint covers the entire Topology plus Codec IDs/versions resolved by the actual Go types of all node keys and ports. Functions, closures, fixed binding values, and Aspect ordering/parameters/semantics require a business version change. Opening a child scope also compares its actual bound input fingerprint.

`NewCodecs` includes JSON codecs for bool, string, integer/float types, and []byte. Register business types with `durable.RegisterJSON[T](codecs, "stable-type-id", "codec-version")`, or implement `Codec`. Registration uses `reflect.Type`; duplicate types or IDs cannot replace existing entries. Encoding must be deterministic, synchronous, concurrency safe, and must not mutate business values. Interfaces, including nested fields and container elements, need a custom codec to reconstruct dynamic types; the JSON convenience entry rejects these declarations recursively. JSON strings, nested fields, and map keys must be valid UTF-8 to avoid silent replacement and input fingerprint collisions; use []byte or a custom codec for arbitrary bytes. A custom codec may use a zero-byte encoding, still confirmed with ready state and a checksum. Ready typed nil, nil for a declared interface, and zero values remain ready. Skipped outputs contain no encoded value, and normal skips do not fail the run. JSON null preserves typed nil for pointers/slices/maps and is rejected for scalar types.

Store provides `Acquire(ctx, runID)` with exclusive ownership and immediate `ErrBusy` for an active session. Session provides `Load`, atomic CAS `Save(ctx, record, expectedRevision)`, and `Close`. The initial revision is 1 with expected 0; later revisions are expected+1. RunRecord contains only JSON data, including encoded value checksums and no automatically captured business error text. The record is not encrypted; hosts manage access to storage and backups.

Every mutation clones a proposed record and updates runtime memory only after Save succeeds. A Save may actually commit and then return an error: this execution stops without rollback or automatic retry; Resume reloads the authoritative record. Store, Codec, and recovery callback errors or panics are reliability failures even when they contain a skip sentinel or are swallowed by an outer Aspect. Host cancellation and reliability failures leave attempts active. `RunOptions.OnEvent` reports `EventRestored` synchronously without values or invented Pulse business timings. It must be concurrency safe; an error or panic stops this execution without undoing confirmed data.

## Scope and validation

The first phase supports non-streaming values. Channels, durable waits, and distributed run management are outside this phase. Unconfirmed nodes may repeat external effects; hosts need idempotency keys, transactions, deduplication, or an outbox. This library does not promise deterministic replay or exactly-once effects. MemoryStore validates the in-process protocol only. The file backend targets one local host; OS locking and actual process exit/restart validation belong to `filestore/` and `integration/`.

The required Pulse bridge changes are reviewed in the corresponding PR, with the dependency revision pinned in `go.mod`. Local integration can point to a Pulse working tree through a separate `go.work`. This does not claim the bridge APIs have been released; releases remain the maintainer's decision.

```text
go build ./...
go vet ./...
go test -race -count=1 ./...
```
