# Error handling

Use this page to decide what to do after Run/Resume fails. Identify reliable checkpoint failures before checking individual causes. Error text or a single underlying sentinel cannot determine business success.

## Error categories

| Category | Detection and scenarios | Action |
| --- | --- | --- |
| Argument or definition error | Nil Runtime/context/graph, empty RunID/Version, invalid RunID UTF-8, invalid graph, missing Codec, unsupported type, or streaming declaration | Fix the call or configuration; these preflight contract errors are not CheckpointError |
| `durable.ErrBusy` | An active session owns the RunID during Acquire | Coordinate with the existing owner; Acquire does not wait, and deleting lock files does not resolve contention |
| `durable.ErrNotFound` | Resume loads a RunID with no record | Check storage and identity; use Run for genuinely new work |
| `durable.ErrConflict` | Existing RunID, or mismatched root recovery identity, Version, definition, or inputs; also registration and CAS conflicts | Check original contracts and the method; do not overwrite old records |
| `*pulse.CheckpointError` | Reliable Store, Codec, recovery protocol, or callback operation fails | Stop this execution, resolve the cause, then reload the authoritative record with Resume |
| Host cancellation or deadline | The current host context was actually canceled or expired | Recover the same run with a valid new context and the original compatible conditions |
| Real business failure | A node or business Aspect returns a real error | Consume the existing Retry budget; Resume does not replenish it after final failure |

A `CheckpointError` cause chain may also match `ErrConflict`, `context.Canceled`, or `pulse.ErrSkipped`. For example, a CAS conflict at a reliable-save boundary remains a reliable failure. Do not downgrade it to an ordinary argument conflict or normal skip. Normal root Run/Resume identity conflicts, missing records, and ownership contention retain their original categories.

## Check with errors.Is/As

Do not rely on `err == durable.ErrBusy` or parse display text. This local classifier needs imports for `errors`, `pulse`, and `durable`. Its labels help the host choose handling paths; they do not replace execution result or context checks.

```go
func classify(err error) string {
	if err == nil {
		return "success"
	}
	var checkpoint *pulse.CheckpointError
	if errors.As(err, &checkpoint) {
		return "checkpoint failure"
	}
	if errors.Is(err, durable.ErrBusy) {
		return "run busy"
	}
	if errors.Is(err, durable.ErrNotFound) {
		return "record not found"
	}
	if errors.Is(err, durable.ErrConflict) {
		return "contract conflict"
	}
	return "argument, cancellation, or business error"
}
```

Identify `CheckpointError` first, then use `errors.Is/As` to inspect its underlying cause and select a repair action. `CheckpointError.Operation` identifies the reliable operation; `Err` and `Unwrap` preserve its error chain.

## Uncertain saves

A failed Save has two possible outcomes:

1. **No commit**: the disk or backend still has the previous record, and the node remains unconfirmed.
2. **Commit completed**: the record was updated, but a later sync or confirmation step failed.

Runtime stops this execution without treating the proposed record as its confirmed in-memory state, automatically rolling back, or repeating Save. Do not overwrite the backend with the previous record or blindly retry an old expectedRevision.

After resolving the cause, call Resume with a new or reused Runtime. It reloads: valid results that were actually confirmed are reused, and nodes that remain unconfirmed run again. A returned Save error alone cannot prove that a particular business action must or must not repeat.

Later errors do not roll back confirmed results. A Session close failure is also joined into the returned error as CheckpointError. Even when the actual Execution completed, the Runtime call can fail during cleanup. Handle the returned error and investigate storage/ownership state; a completed Snapshot alone does not prove the entire call was error-free.

## Reliable failures cannot become normal skips

Normal skip means no value and is a business outcome that can finish successfully. Reliable checkpoint failure means execution cannot be safely confirmed or restored. These meanings are distinct.

If a Store, Codec, or recovery callback cause chain contains `pulse.ErrSkipped`, the surrounding `CheckpointError` still means failure. A parent Retry cannot treat a reliable failure as an ordinary business error and keep retrying. Even an outer business Aspect that swallows the inner error cannot make the whole execution succeed. Do not use a lone `errors.Is(err, pulse.ErrSkipped)` branch to downgrade such failures to skip.

Reading an outlet from a failed execution returns the failure cause, a zero value, and an empty state. Do not treat that zero value as normal absence. A normally skipped outlet returns a zero value, `skipped`, and nil error.

## Cancellation and real business failure

Identify host interruption using the current host context and the actual failure category together. The presence of `context.Canceled` or `context.DeadlineExceeded` in a chain does not by itself prove host cancellation. A recovery callback wrapping a context error still has a reliable failure. A node that cancels its local context and returns a real business error may still have a real failure to confirm.

Host interruption and reliable failures do not mark active attempts as business failures merely because execution was interrupted. Uncertain-save state follows the next Load. A confirmed business failure advances the failure cursor, and descendants of the old parent attempt cannot be reused.

Once a complete node root attempt has a real failure, this RunID's node budget is finished. Even if the cause is later fixed, Resume does not allocate a new root budget. Start with a new RunID according to application policy if work must begin again. Built-in Retry limits are persistent contracts, not counters restarted in every process.

## Error text and logging

Codec and recovery callback errors and panics become display text containing operation information, without business values, encoded bytes, or original panic text. Original error causes remain inspectable through Unwrap, including `panic(error)`. Non-error panics do not expose their original text.

Store errors also preserve returned causes through an operation wrapper that controls display text. Store panics become operation failures; do not rely on their panic cause being retained. Ordinary business errors, run identifiers, and causes that the host explicitly unwraps do not automatically have the same sanitization guarantee. Select log fields deliberately and avoid exposing business values or sensitive data in raw underlying errors.

RunRecord does not automatically save business error text, but it does save encoded outputs. Sanitized errors do not mean encrypted checkpoint data. Directory and backup access still need application controls.

## Troubleshooting order

1. Preserve RunID, business version, storage location, and the returned error category without changing the original record.
2. For CheckpointError, identify the reliable operation and underlying cause first. Do not automatically start new business work.
3. For Busy, inspect ownership; for NotFound, inspect directory and identity; for Conflict, inspect the method, version, definition, and inputs.
4. Resume with the original compatible conditions after repair, letting the reloaded record determine valid results and remaining budget.
5. Explicitly allocate a new RunID for new business conditions or a new complete budget, while managing retention of the old record.

## Next steps

See [Recovery after process exit](../guide/recovery.md) for the procedure, the [Execution contract](execution.md) for version checks, and the [Data model](data-model.md) for CAS and record integrity.
