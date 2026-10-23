# Reference overview

Use this section to check APIs, persistent records, and error semantics. Start with the [guide](../guide/index.md) if you are new to Durable. Reference pages do not replace the complete procedures in [Quick start](../guide/quickstart.md) and the [recovery tutorial](../guide/recovery.md).

## Find a rule

| Question | Reference |
| --- | --- |
| What are the preconditions for Run and Resume? | [Execution contract](execution.md) |
| Can I recover after changing functions, the graph, inputs, or codecs? | [Execution contract](execution.md) |
| When are recovery events emitted? | [Execution contract](execution.md) |
| How do node results, child scopes, and Retry attempts relate? | [Data model](data-model.md) |
| How are ready, skipped, nil, and empty encodings distinguished? | [Data model](data-model.md) |
| What are Store ownership and CAS requirements? | [Data model](data-model.md) |
| How do I handle Busy, Conflict, and CheckpointError? | [Error handling](errors.md) |
| What if saving commits and then reports an error? | [Error handling](errors.md) |

## Three core rules

1. **Publish after reliable confirmation**: ordinary results confirm after the full node chain, including Aspects. Managed appends and message transactions have separate incremental boundaries. Publish each result after its storage confirmation.
2. **Restore valid results**: confirmed nodes in valid scopes are not re-entered. Unconfirmed nodes restart from the beginning of their functions. Child results beneath a failed parent attempt cannot serve a new attempt.
3. **Reload the authority**: a save error may occur after commit. Stop this execution and reload the authoritative record during recovery; the returned error alone cannot prove the write did not commit.

Normal skip means no business value and can finish successfully. Reliable storage or codec failure remains failure, even when its cause chain contains `pulse.ErrSkipped`.

## Version and scope

This reference describes the current source contracts. Root-module tutorials pin public development version `v0.0.0-20261023040750-1b397e91c878`, which depends on Pulse `v0.5.0` and requires Go 1.26 or later. Durable `v0.1.0` has not been released. Current independent backend sources are not the older submodule baseline at that root pin; public backend installation pins must be verified after pushing the current revisions. See [Contributing](https://github.com/Luo-root/pulse-durable/blob/main/CONTRIBUTING.md) for source validation. Historical performance pages retain their stated measurement versions.

Supports ordinary values, managed streams, explicit state, static child graphs, and built-in Retry and message budgets. MemoryStore provides only the in-process protocol; FileStore targets one local host. Hosts can compose approval waits and per-round tool calls with [aspects and Cancel/Resume](../guide/composition.md). A built-in wake-up service, distributed run management, deterministic replay, and exactly-once external effects are outside the current implementation.

## Next steps

Read the [Execution contract](execution.md) to check run parameters, or the [Data model](data-model.md) to implement a backend or Codec. For an existing error, start with [Error handling](errors.md), then decide whether to recover or create new business work.
