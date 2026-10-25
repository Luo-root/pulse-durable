[English](README_en.md) | [中文](README.md)

# Distributed

Whole-workflow submission, PostgreSQL coordination, and capacity-bounded workers for Pulse Durable. Producers persist inputs; workers claim an exact compatible version and execute the entire graph locally. After a lease expires, a compatible worker can take over from confirmed checkpoints.

| Local execution | Distributed execution |
| --- | --- |
| The host calls Durable `Run` / `Resume` with graph and inputs | Producers use `NewTask` + `Submit`; workers use `Registry` + `Run` |
| The host decides when to resume | Workers claim queued tasks or expired running tasks |
| Uses a Durable Store | Uses a separate coordination schema and fenced checkpoint sessions |

Work is distributed across RunIDs, not across nodes of a single graph. Successful submission means durable acceptance, not business completion.

## Use the source module

Requires **Go 1.26+ and PostgreSQL 16+**. `distributed` is an independent module without a release tag yet. Use a source workspace rather than an unavailable `distributed/v0.2.0`:

```sh
git clone https://github.com/Luo-root/pulse-durable.git
mkdir distributed-demo
cd distributed-demo
go mod init example.com/distributed-demo
go mod edit -go=1.26.0
go mod edit -require=github.com/Luo-root/pulse@v0.5.0
go mod edit -require=github.com/Luo-root/pulse-durable@v0.1.1-0.20261024124930-5296b17b7011
go mod edit -require=github.com/Luo-root/pulse-durable/distributed@v0.0.0
go mod edit -replace=github.com/Luo-root/pulse-durable/distributed=../pulse-durable/distributed
```

`v0.0.0` is only a placeholder for the local replacement, not a downloadable release. The Pulse and Durable pins match the current [go.mod](go.mod); the checkout must contain `distributed/`. If a parent directory already has `go.work`, add the demo and `distributed/` to it, or set `GOWORK=off` for these commands.

## PostgreSQL and a minimal program

Have a database administrator provision the database, a dedicated schema owner, and a separate login role named `pulse_app`, with database CONNECT for both. The owner needs permission to create a schema in the target database. `pulse_app` must not own the schema, be a superuser, have CREATEDB/CREATEROLE/BYPASSRLS, belong to the owner role, or have direct access to coordination tables. Set passwords through your secure database provisioning process, not in source code.

Supply connection strings only through the environment:

- `PULSE_ADMIN_DSN`: owner credentials, available only to the migration process.
- `PULSE_WORKER_DSN`: `pulse_app` credentials, shared by submission, query, cancellation, and worker commands in this demo.

Save this as `main.go`. It registers a struct Codec and copies a document from input to output:

```go
package main

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "fmt"
    "log"
    "os"
    "os/signal"
    "time"

    "github.com/Luo-root/pulse"
    durable "github.com/Luo-root/pulse-durable"
    d "github.com/Luo-root/pulse-durable/distributed"
    "github.com/Luo-root/pulse-durable/distributed/postgres"
    "github.com/Luo-root/pulse-durable/distributed/worker"
)

type Document struct { Text string `json:"text"` }
var id = d.Identity{Namespace: "documents", RunID: "document-001"}

func assembly() (d.Assembly, error) {
    g, err := pulse.New("document", "Copy a document")
    if err != nil { return d.Assembly{}, err }
    input := pulse.NewKey[Document]("input")
    output := pulse.NewKey[Document]("output")
    if err = g.DeclareInput(pulse.NewPort(input)); err != nil { return d.Assembly{}, err }
    if err = g.DeclareOutput(pulse.NewPort(output)); err != nil { return d.Assembly{}, err }
    if err = g.Add(pulse.NewNode("copy", "Copy", pulse.Requires(input), pulse.Provides(output),
        func(rc *pulse.RunCtx) error {
            v, err := pulse.Get(rc, input)
            if err != nil { return err }
            return pulse.Set(rc, output, v)
        })); err != nil { return d.Assembly{}, err }
    keys := pulse.NewRegistry()
    if err = pulse.RegisterKey(keys, input); err != nil { return d.Assembly{}, err }
    codecs := durable.NewCodecs()
    if err = durable.RegisterJSON[Document](codecs, "example/document", "1"); err != nil { return d.Assembly{}, err }
    return d.Assembly{Graph: g, Keys: keys, Codecs: codecs}, nil
}

func adminMigrate(ctx context.Context) error {
    return postgres.Migrate(ctx, os.Getenv("PULSE_ADMIN_DSN"), postgres.Grant{
        Role: "pulse_app", Namespace: id.Namespace,
        Permissions: []string{"submit", "query", "worker", "cancel"},
    })
}

func workerNew(ctx context.Context, c *postgres.Coordinator) error {
    // A second New creates a separate pool, not merely another goroutine.
    renew, err := postgres.New(ctx, os.Getenv("PULSE_WORKER_DSN"))
    if err != nil { return err }
    defer renew.Close()
    registry := &d.Registry{}
    if _, err = registry.Register("copy", "document/1", assembly); err != nil { return err }
    w, err := worker.New(worker.Config{
        Coordinator: c, Renewer: renew, Registry: registry,
        Namespace: id.Namespace, WorkerID: "local-1", Capacity: 4, BatchMax: 2,
        TTL: 15*time.Second, RenewInterval: 5*time.Second,
        OperationTimeout: 2*time.Second, RenewTimeout: 2*time.Second,
        PollMin: 100*time.Millisecond, PollMax: 500*time.Millisecond,
    })
    if err != nil { return err }
    stop, stopSignals := signal.NotifyContext(ctx, os.Interrupt)
    defer stopSignals()
    runCtx, cancel := context.WithCancel(ctx)
    defer cancel()
    done := make(chan error, 1)
    go func() { done <- w.Run(runCtx) }()
    select {
    case err = <-done: return err
    case <-stop.Done():
    }
    // Keep Run and renewal alive while draining, then stop the local loop.
    drainCtx, finish := context.WithTimeout(context.Background(), 30*time.Second)
    defer finish()
    if err = w.Drain(drainCtx); err != nil { return err }
    cancel()
    <-done
    return nil
}

func run() error {
    if len(os.Args) != 2 { return fmt.Errorf("usage: demo migrate|submit|worker|query|cancel") }
    ctx := context.Background()
    if os.Args[1] == "migrate" { return adminMigrate(ctx) }
    c, err := postgres.New(ctx, os.Getenv("PULSE_WORKER_DSN"))
    if err != nil { return err }
    defer c.Close()
    switch os.Args[1] {
    case "submit":
        a, err := assembly()
        if err != nil { return err }
        var in pulse.Inputs
        if err = pulse.Seed(&in, pulse.NewKey[Document]("input"), Document{Text: "Hello"}); err != nil { return err }
        task, err := d.NewTask(ctx, id, "copy", "document/1", a.Graph, in, a.Codecs)
        if err != nil { return err }
        result, err := c.Submit(ctx, task)
        if err != nil { return err }
        fmt.Println(result.Snapshot.Status, "deduplicated:", result.Deduplicated)
    case "worker":
        return workerNew(ctx, c)
    case "query":
        s, err := c.Get(ctx, id)
        if err != nil { return err }
        fmt.Println(s.Status, "epoch:", s.Epoch, "revision:", s.Revision, "reason:", s.Reason)
    case "cancel":
        // Retain the same ID/digest when resolving this logical cancellation.
        h := sha256.Sum256([]byte("cancel:documents:document-001"))
        s, err := c.Cancel(ctx, id, d.Mutation{ID: "cancel-document-001", Digest: hex.EncodeToString(h[:])})
        if err != nil { return err }
        fmt.Println(s.Status)
    default:
        return fmt.Errorf("unknown command: %s", os.Args[1])
    }
    return nil
}

func main() {
    if err := run(); err != nil { log.Fatal(err) }
}
```

```sh
go mod tidy
go build -o demo .
# Supply PULSE_ADMIN_DSN only to the migration process:
./demo migrate
# Subsequent processes need only PULSE_WORKER_DSN:
./demo submit
./demo query
./demo worker
```

Run `./demo query` in another terminal; the completed task should be `succeeded`. The worker keeps polling; press Ctrl+C to drain and exit. Use `demo.exe` on Windows. To observe `canceled`, run `./demo cancel` before a new task is claimed; canceling a completed task returns its existing terminal state. Use a new RunID for another run.

`Migrate` explicitly installs or upgrades the separate `pulse_durable_distributed` schema and grants namespace permissions. `New` validates and connects without DDL. The demo uses one application role for simplicity; deploy separate submit/query/worker/cancel roles with multiple `postgres.Grant` entries as needed. Give workers query permission too, for recovery queries. Do not call `New` with administrator credentials or use legacy `postgresstore` tables or sessions for these tasks.

## Routing and upgrades

- `Registry.Register` matches TaskType, BusinessVersion, graph definition fingerprint, complete Codec manifest digest, protocol, and record/payload formats exactly. There are no version ranges, latest fallback, or automatic code downloads.
- `assembly` constructs fresh graphs, typed root input keys, and Codecs. Factories and Codecs must be deterministic and free of business side effects. The application deploys functions and aspects; they are not serialized into tasks. Change the explicit business version, such as `document/1`, when their semantics change.
- Register custom types with `RegisterJSON[T]` or a Codec using stable TypeID/Version values, covering all keys including internal, subgraph, and stream keys. Bump the Codec version when its encoding contract changes. Identical graph structure does not imply identical business functions.
- Install matching contracts on producers and workers. Keep old registrations available during rolling upgrades to process old tasks. Tasks without compatible workers stay queued; validation failures after claiming can become blocked. The current API has no general unblock/requeue administration operation; do not edit tables directly.
- Resubmitting the same canonical task under `(Namespace, RunID)` returns `Deduplicated=true` without resetting checkpoints or terminal status. Different content returns `ErrSubmitConflict`. Terminal identities are not automatically reclaimed; do not reuse RunIDs.

Inputs must have a finite encoding: payloads are limited to 4 MiB, envelopes to 8 MiB, Codec manifests to 4096 entries, and checkpoints to 16 MiB. Do not submit functions, channels, or process-local addresses.

## Lifecycle and queries

| Operation | Meaning |
| --- | --- |
| `Capacity` / `BatchMax` | Total local reservation limit / maximum claim batch; uncertain claims and draining executions occupy slots |
| `Pause()` / `Resume()` | Stop / resume admission; existing tasks continue executing and renewing |
| `Drain(ctx)` | Permanently stop admission, retain renewal, and wait for all reservations; its deadline does not itself cancel tasks |
| `Run(ctx)` | Single-use blocking loop; canceling its context stops local execution, not a business Cancel |
| `Coordinator.Cancel` | Persist cancellation, racing Finish under database coordination; does not undo external effects |
| `Get` / `List` | Observe authoritative state without renewal or execution permission |

Use `ListRequest{Namespace: "documents", Status: d.Queued, Limit: 100}` with `List`; set `AfterRunID` to the last task's RunID for the next page. States are queued, running, blocked, succeeded, failed, and canceled. `Snapshot.Reason` is a sanitized code; `Revision` orders checkpoints, not ownership generations.

Workers generate a BootID on startup; claims assign fresh LeaseIDs and increasing Epochs. Use a separate `postgres.New` pool for renewal and reserve actual database connection capacity for it: another goroutine alone does not provide isolation. The demo timing values are illustrative; tune them for database latency and business cancellation behavior. Graceful shutdown drains first, then cancels Run, then closes both pools. Non-cooperative business code or uncertain claims can prevent draining; Run returning does not prove those functions have exited. `Active()` exposes retained reservations. The demo exits on a 30-second drain failure rather than reporting successful draining.

`ErrUnknown` means an uncertain commit, not business failure. Retry Submit only with the identical task. Preserve the original Mutation ID/Digest for Cancel and inspect state or use the PostgreSQL receipt API to resolve it. Workers handle uncertain claims and protected writes internally; do not replay them through a second claim loop or mark tasks failed because of network errors.

## Deployment assumptions

Coordination uses one authoritative PostgreSQL write primary. Operations lock the task row before sampling database time and fence stale owners with the valid lease, BootID, LeaseID, Epoch, and Revision. Prevent database clock rollback; detected row-time regression rejects operations but does not establish safety under arbitrary clock rollback. Restoring old backups, losing acknowledged transactions, or allowing two writable primaries breaks ownership history: isolate old workers and use a fresh namespace during recovery. Database HA is outside this module. Fencing protects coordination state and checkpoints, not arbitrary code in a disconnected process. External effects require stable `(Namespace, RunID, business operation)` idempotency keys, never the changing LeaseID; validate Epoch at external systems only when they support fencing.

## Validation entry points

Use `go test -run '^$' .` to compile the demo without connecting to a database. The module's [systemtest](systemtest), [networktest](networktest), [crossmachine](crossmachine), and [soaktest](soaktest) packages provide real-database, fault, and cross-process validation entry points; check each test's configuration and observed results. Performance and acceptance data belong on the single [Scale and performance](../site/en/guide/performance.md) page. A demo run or compile check is not deployment acceptance.
