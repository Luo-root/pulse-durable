# Execution contract

This page defines Runtime, run identity, recovery compatibility, and recovery event rules. Complete calling programs are in [Quick start](../guide/quickstart.md) and [Recovery after process exit](../guide/recovery.md).

## Runtime

`durable.New(store, codecs)` constructs a Runtime without reading storage or executing business code. Store and Codecs must be non-nil; a typed nil Store is rejected too.

One Runtime may run different RunIDs concurrently. Each `Run` or `Resume` creates a new `pulse.Execution`, holds the RunID's exclusive Session throughout startup, waiting, and cleanup, then closes the Session.

| Method | Record precondition | Behavior |
| --- | --- | --- |
| `Run(ctx, graph, inputs, options)` | No record for this RunID | Initialize a record, start a new execution, and block until it finishes |
| `Resume(ctx, graph, inputs, options)` | A record exists for this RunID | Load and validate the record, restore valid results, start a new execution, and wait |

`Run` returns `ErrConflict` for an existing record. `Resume` returns `ErrNotFound` for a missing one. The methods do not switch automatically. The backend defines whether different Store instances share data and ownership.

Both return `(*pulse.Execution, error)`. A preflight failure may occur before an Execution exists; later failures return the Execution when possible for `Snapshot` and `Err` inspection. Check the returned error first. An existing Execution does not prove success, and validation or callbacks can fail before the graph starts.

## RunOptions

| Field | Rule |
| --- | --- |
| `RunID string` | Required, nonempty valid UTF-8 text; invalid RunID is rejected as an argument error before accessing Store |
| `Version string` | Required, nonempty valid UTF-8 text; SemVer is not required. Invalid text is rejected as an argument error before accessing Store; it must match the record on recovery |
| `ExecOptions []pulse.ExecOption` | Pulse options for this execution, such as its Observer |
| `OnEvent func(Event) error` | Optional synchronous callback for Durable recovery facts; an error or panic stops this execution |
| `BeforeStart func(*pulse.Execution) error` | Optional host stream registration after restore assembly and before Start; errors or panics cancel execution and release the Session |

The context must be non-nil. A context already canceled or expired at entry is rejected. The graph must be valid. Ordinary keys, message elements, cursors, and explicit state need codecs for their actual Go types; managed endpoints persist only descriptors. In BeforeStart, the host can SubscribeStream and launch a reader that receives after Start, acknowledges or stops, and exits on execution cancellation. Run/Resume retain the Session and wait for reading obligations.

RunID identifies one business run and should remain stable. Allocate a new RunID for new work; retain the original RunID for recovery. Changing Version, inputs, or the definition does not let an old RunID represent different work.

## Recovery compatibility

The host must supply the graph definition, inputs, Codecs, and RunOptions again. Durable does not reconstruct business functions from the record or save the Go call stack.

| Check | Compared content | Response to change |
| --- | --- | --- |
| Run identity and business version | RunID and Version exactly match the record | Recover with original conditions or explicitly create new business work |
| Definition fingerprint | Complete Topology, every node and port, and Codec IDs/versions resolved by actual Go types | Definition drift is rejected; rebuild an equivalent definition |
| Root input fingerprint | Input states, type contracts, and deterministic encodings | Supply the same inputs; do not mix new inputs with old results |
| Actual child inputs | Bound input fingerprint when opening each child scope | Different inputs for an existing scope are rejected |
| Record integrity | Format, revision, scopes, attempt associations, dependency closure, output sets, checksums, and decodability | Corruption or inconsistency stops recovery as a reliable failure |

The complete Topology participates in the hash. Do not assume changes to visible definition fields such as descriptions are compatible. Codec registration matches actual Go types; IDs and versions form the persistent contract. Revalidate compatibility when changing Go type names or registration shape too.

Function bodies, closure contents, fixed bindings, and Aspect ordering, parameters, and semantics are not fully captured by the structural fingerprint. The host must change business `Version` for these changes. Fixed binding values also affect actual child input validation when opening a scope, but that does not replace business version management.

Input encoding must be deterministic: equal values produce equal bytes every time. Codecs depending on randomness, time, or mutable external state may prevent recovery even for the same business input.

## Confirmation, re-entry, and Retry

Ordinary confirmation covers the complete node chain, including Aspects. Returning success or calling `Set` does not by itself prove node confirmation. Complete results, the node root attempt's `confirmed` state, and successful writer terminals share one Save. Managed appends and `HandleMessage` input acknowledgements, explicit state, and results use separate incremental transactions. Stream delivery follows the corresponding storage confirmation; it does not mean the complete node has finished. Ordinary message values restore into the outer complete-node staging frame and publish after complete node confirmation, without replaying acknowledged inputs.

Recovery reuses confirmed nodes in valid scopes without executing their complete chains again. Unconfirmed nodes restart from the beginning of their functions, so external effects occurring before confirmation may repeat.

A child scope belongs to its parent node's deepest current attempt. An `active` parent attempt can reclaim that scope. A `failed` parent attempt makes old descendants unavailable for reuse. If Retry budget remains, the next attempt gets a new scope.

A complete node root attempt has a budget of one. Built-in Retry persists its own index and limit inside that root attempt. Once real business failure exhausts the complete node budget, repeated `Resume` calls cannot replenish retries. Inner success remains active until root confirmation. See [Subgraphs and retries](../guide/subgraphs.md) for the example.

Host cancellation, exit, or reliable failure does not mark an otherwise active attempt as a business failure merely because execution was interrupted. Uncertain-save outcomes still follow the reloaded record. A node that cancels its local context and returns a real business error may still have a business failure to confirm. The presence of `context.Canceled` in an error chain alone cannot identify host interruption.

## Read outlets

`pulse.ReadOutput(execution, key)` reads a declared outlet of this execution. The key must have the same name and type as `DeclareOutput`. It does not wait for completion, create slots, or read arbitrary internal keys.

| Condition | Return semantics |
| --- | --- |
| Successful cleanup, outlet `ready` | Business value, `ready`, nil error; zero and valid nil values remain ready |
| Successful cleanup, outlet `skipped` | Type zero value, `skipped`, nil error; normal absence |
| Successful cleanup, optional pass-through outlet without an input | Type zero value, `pending`, nil error |
| Execution has not finished cleanup | Immediate lifecycle error with no partial value |
| Failure or real cancellation | Failure cause, zero value, empty state |
| Undeclared outlet or wrong type | Error wrapping `pulse.ErrUndeclared` |

Check `err` first, then handle `state` according to the application. Do not promote normal skip to failure or treat a zero value from a failed read as normal absence. Map, slice, and pointer results are not deep copies; the host owns subsequent concurrent access.

## Recovery events

`EventRestored` is `"restored"` and means a confirmed node result was selected into this recovery plan. Event contains `RunID`, `Scope`, `Graph`, `Path`, `NodeID`, and `Kind`, without business values, encoded bytes, or invented business durations.

Events are emitted synchronously before the corresponding graph starts. Multiple runs or child graphs can invoke one callback concurrently, so it must be concurrency safe. Do not synchronously wait for the same execution to finish inside the callback: that blocks startup. Another Resume may report the same result again; events have no once-only delivery guarantee.

This local options factory needs imports for `fmt`, `sync`, and `durable`. Pass its return value to the relevant `Run`/`Resume`:

```go
func observedOptions(runID string) durable.RunOptions {
	var mu sync.Mutex
	return durable.RunOptions{
		RunID: runID, Version: "recovery/1",
		OnEvent: func(event durable.Event) error {
			mu.Lock()
			defer mu.Unlock()
			fmt.Println("restored node:", event.NodeID)
			return nil
		},
	}
}
```

A callback error or panic stops this execution as a reliable recovery failure without rolling back confirmed data. Public error text is sanitized, while `Unwrap` preserves original error causes, including `panic(error)`. Events contain no business values, but RunID and other identifiers may still carry business information; select and sanitize fields according to the host's logging policy.

## Next steps

Read the [Data model](data-model.md) to implement storage or inspect records. Use [Error handling](errors.md) to decide what to do after a failure.
