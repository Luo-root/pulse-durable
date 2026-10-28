# Data model

This page defines persistent records, encoding states, and Store/Session contracts. Use it when implementing a backend, investigating recovery, or checking a custom Codec. For application procedures, see [Storage](../guide/storage.md) and [Value codecs](../guide/codecs.md).

## RunRecord

The current `RecordFormat` is `2`, used for all new ordinary and streaming runs. Runtime's logical validation still recognizes ordinary format `1` without incremental fields, with subsequent business commits using `2`. This does not mean new durable backends read or migrate old physical JSON/schemas. New Initialize accepts only the current logical format; old physical formats must be explicitly rejected with data preserved. Read them with the old version or use a new directory/database. Old Runtime versions reject the new format, preventing an overwrite after silently ignoring stream fields. RunRecord contains serializable data only, without functions, Go call stacks, or automatically captured business error text.

| Field | Meaning |
| --- | --- |
| `Format int` | Record format version, which must be supported by the current recovery implementation |
| `RunID string` | Business run identity, matching the Session and call |
| `Revision uint64` | CAS revision, starting at `1`, never zero |
| `Version string` | Host business semantics version |
| `Definition string` | SHA-256 fingerprint of the complete definition |
| `Inputs string` | SHA-256 fingerprint of root inputs; not an input copy from which business inputs can be reconstructed |
| `RootScope string` | Persistent random identity of the root graph |
| `Scopes map[string]ScopeRecord` | Graph scopes and complete node results |
| `Attempts map[string]AttemptRecord` | Node root attempts and Retry cursors |
| `Streams map[string]StreamRecord` | Logical sources, reading groups, message logs, and acknowledgement frontiers |
| `Messages map[string]MessageAttemptRecord` | Persistent per-input failure budgets and processing state |
| `Managed map[string]ManagedRecord` | Explicit processor state and committed ordinary values |

RootScope and attempt identities are hexadecimal representations of 16 random bytes. They are persisted, not regenerated from graph paths, in-memory counters, or process startup order. The host must still supply original external inputs during recovery.

## ScopeRecord

| Field | Meaning |
| --- | --- |
| `Graph string`, `Path string` | Graph identity and static mount location in the definition |
| `Inputs string` | Actual input fingerprint for this scope |
| `Nodes map[string]NodeRecord` | Confirmed complete node results within the scope |
| `Bindings bool` | Child fingerprint uses stable In bindings and fixed values; false retains legacy input-value fingerprints |

The root scope input fingerprint matches RunRecord.Inputs. A child scope directly uses its parent node's deepest current Attempt identity. Mount paths describe locations; they do not replace the persistent identity of a particular parent attempt.

The same child graph has different execution scopes at different mounts or in different parent attempts. After parent attempt failure, its old scope remains in the record but cannot serve a new attempt.

## AttemptRecord

| Field | Meaning |
| --- | --- |
| `Parent string` | Identity of this graph's Scope or the outer Attempt |
| `NodeID string` | Associated node |
| `Boundary string` | Empty for a root attempt; built-in Retry uses a fixed Aspect position as its boundary |
| `Index int` | Attempt index at this layer, starting at `1` |
| `Limit int` | Positive budget at this layer; Index must not exceed Limit |
| `State AttemptState` | `active`, `failed`, or `confirmed` |

| State | Meaning |
| --- | --- |
| `AttemptActive` | The same attempt can be reclaimed; inner success stays active until root commit |
| `AttemptFailed` | The attempt has a real failure; all descendant results are invalid for reuse |
| `AttemptConfirmed` | The node root attempt and complete NodeRecord were confirmed in the same Apply |

A node root attempt has an empty Boundary and Limit `1`. A Retry attempt points to an outer Attempt and stores its own index and limit. Within one Parent/NodeID/Boundary group, indexes are consecutive, limits match, and earlier attempts must have failed.

Confirmation applies to the node root attempt. Inner Retry success does not independently become `confirmed`. Parent chains must reach the root scope without missing links, cycles, or mismatched nodes. A failed ancestor makes results logically invalid for reuse, not structurally corrupt. Recovery still validates old scopes for decodability and dependency completeness.

## NodeRecord

| Field | Meaning |
| --- | --- |
| `NodeID string` | Node in the definition |
| `Attempt string` | Identity of this node's confirmed root attempt |
| `Reason string` | Only `completed` or `skipped` |
| `Outputs []ValueRecord` | Exactly all Provides of the node, without omissions or duplicates |

NodeRecord represents confirmation of the complete node chain. It is not a marker for entering the body or calling Set. Real failures are recorded in Attempt state, not disguised as skipped NodeRecords.

Ordinary values still require complete producer records. A managed stream dependency may refer to a durably opened source, including a source connected through static In/Out bindings, before its body completes. Its stable identity, source attempt, and all ancestors must be valid. The root attempt must still match the Scope, NodeID, and NodeRecord, and be confirmed in the same Apply.

## ValueRecord

| Field | Meaning |
| --- | --- |
| `Name string` | Output key name |
| `TypeID string`, `Version string` | Codec contract for the actual Go type |
| `State string` | Only `ready` or `skipped` |
| `Digest string` | SHA-256 checksum of ready encoded bytes |
| `Data []byte` | Opaque ready encoded bytes, persisted as a byte field in record JSON |

`ready` and `skipped` distinguish a value from normal absence. Data length does not determine state:

- Zero values, empty strings, and typed nil remain ready. A nil interface can also be ready for a declared interface type.
- A custom Codec may return zero bytes. The checksum for that byte sequence must still be saved and validated.
- skipped has no encoded value: Data length is zero and Digest is empty. Normal skip can complete the run successfully.
- `pending` is not a node checkpoint output state. Pending optional pass-through outlets belong to the current Pulse execution's outlet semantics.

Restoring ready values checks Codec ID/version, digest format, the actual byte checksum, and whether the decoded value is assignable to the declared type. The checksum detects missing or corrupt bytes; it is not encryption or tamper authentication.

## Incremental streams and message transactions

`ValueRecord.Stream` contains a `StreamDescriptorRecord`, mutually exclusive with Data/Digest; TypeID/Version describe the element type. A descriptor records the source identity, actual writer root attempt, capacity, and confirmed terminal. Recovery uses Core's generic factory to rebuild endpoints without passing endpoints to a Codec.

`StreamRecord` identifies Scope/NodeID/Key/Group plus Generation, and stores SourceID/Resumable, LastSequence, AckedThrough, Pending, and Cursor. Pending holds ordered messages with reading obligations; sparse acknowledgements do not mistake the highest acknowledged sequence for a continuous frontier. Recovery advances generation once; repeated Open calls in the same execution reclaim that generation. Old deliveries cannot acknowledge a new generation.

Active sources must explicitly support continuation. Business code starts from `StreamWriter.Position()` and persists positions with `SendAt`. Completed sources retain their complete NodeRecord; recovery rebuilds their unacknowledged tails without rerunning the body. A confirmed source still permits tail acknowledgements. Failed sources still reject Open/Send/Ack; failed ancestors, forged references, and future generations remain rejected. A legitimate confirmed Sub alias may restore a read-only failed endpoint, with no messages or Open, after verification of historical identity, the failed source root, and valid ancestry. This allows a deeper producer's parent chain to handle its business failure without rerunning the alias, advancing generation, authorizing acknowledgement, or treating canceled as business failure.

A real-failure `Finish` confirms the actual writer root attempt and failed terminals of all streams opened by that root in one Apply, without deciding the parent Sub or parent Retry result. Core's subsequent per-output Finish and FailAttempt calls perform strictly idempotent cleanup only. Old sources still cannot Open, Send, or Ack, or change their position, cursor, or generation. A canceled terminal is not this business-failure fact.

Legacy format 2 may contain only some failed terminals with an active root. Resume validates all history first, then uses one reliable CAS to reconcile all same-root endpoints and root failure in still-valid scopes. A repair Apply error stops execution; the next Load determines authority. Results beneath confirmed/failed ancestors remain untouched, and corrupt history is never guessed or repaired; mixed failed and canceled/successful sibling terminals are inconsistent. Ordinary format 1 retains its compatibility path and cannot contain incremental fields. This repair adds no storage fields or Retry budget.

`MessageAttemptRecord` stores source/group/sequence, processing parent attempt, Index/Limit, and state; restarts do not replenish budgets. `ManagedRecord.State` is an explicit full snapshot; Values are arrived ordinary outputs. Input acknowledgement, state, ordinary values, and output messages commit atomically in one typed ChangeBatch Apply. Failed precondition validation publishes no changes from the batch; uncertain commits stop execution and a new session Load determines authority. Stop explicitly ends reading obligations without pretending to acknowledge every message. External effects still require application idempotency, transactions, or an outbox.

Every confirmed message attempt requires a `ManagedRecord` at its Scope/Parent/NodeID key with matching ownership fields; conversely, every container requires at least one confirmed message with that ownership. A valid message commit persists this identity-bearing container even when both State and Values are empty. Recovery rejects a missing or mismatched container before recovery events, Start, business calls, or Revision/Generation advances, rather than treating lost state as an initial zero value. Manual Ack creates no message attempt and requires no container; unconfirmed active/failed messages also need no existing container, and message-free format 1 records remain recoverable. Historical confirmed records do not become corrupt when a parent attempt or ancestor later fails or the input stream advances generation, although results under failed ancestors cannot be reused. These are record-completeness checks, not a claim that a healthy filestore loses containers or authentication against arbitrary tampering.

When messages are confirmed but their complete node remains active, root `CheckpointAttempt.Values` decodes ordinary ready or skipped results from ManagedRecord.Values with the same Scope/Parent/NodeID. Core restores them into the outer complete-chain staging frame and publishes execution slots after complete node Commit. Reclaiming does not advance revision or replay acknowledged inputs; failed ancestors prohibit recovery. Stream references, undeclared or duplicate keys, type/codec-version mismatches, and pending values are rejected. Retry and OpenMessage nested CheckpointAttempt.Values remain empty. This bridge adds no storage fields.

## Codec contract

These public interface signatures need standard library `reflect`. They are not a complete application:

```go
type Codec interface {
	Type() reflect.Type
	ID() string
	Version() string
	Encode(any) ([]byte, error)
	Decode([]byte) (any, error)
}
```

Registration compares `reflect.Type`. Type, ID, and Version must be valid and nonempty. Duplicate types or IDs return `ErrConflict` without replacement. Zero-value `Codecs` accepts custom entries; `NewCodecs` additionally supplies built-in scalar and `[]byte` JSON codecs.

Encode/Decode must be synchronous, concurrency safe, and not mutate business values. Equal values produce equal bytes. Decode must return a value assignable to Type. Channels, functions, and `unsafe.Pointer`, including nested declarations and interface dynamic values, are rejected.

`RegisterJSON[T]` is for recursively concrete types. Interface fields or containers need a custom Codec because standard JSON does not reconstruct their dynamic Go types. JSON encoding requires valid UTF-8 strings, fields, and map keys. Use `[]byte` or custom encoding for arbitrary bytes. JSON `null` preserves typed nil for pointers/slices/maps and is rejected for scalars. Unknown struct fields, trailing content, and invalid raw UTF-8 are rejected. Other standard library JSON limitations still apply.

## Store and Session

These are the public interface signatures. Actual types live in package `durable`; `RunRecord` is the record described here, and `context` is the standard library package:

```go
type Store interface {
	Acquire(context.Context, string) (Session, error)
}

type Session interface {
	Load(context.Context) (RunRecord, error)
	Initialize(context.Context, RunRecord) error
	Apply(context.Context, ChangeBatch) (CommitAck, error)
	Close() error
}
```

Store must be concurrency safe, with independent use of different RunIDs. Acquire returns `ErrBusy` when it detects that a RunID is owned, without waiting for that run to release ownership. Connection capacity and network detection may still require waiting, as specified by the backend. Session exclusively owns that run from Acquire until Close. See [Storage](../guide/storage.md) for PostgreSQL's local pending check, 30-second acquisition bound, and shutdown cleanup.

Load validates integrity and returns an isolated complete projection. Only an absent checkpoint returns `ErrNotFound`; old formats, corruption, and permission errors are not absence. Initialize atomically publishes a valid initial empty execution state at Revision `1` only when no checkpoint exists, checking the session RunID, current logical format, business identity, and empty root scope. It is not a history import path. Applications let Runtime Run construct initial state rather than handwriting business records.

Apply is the sole ordinary write entry point: typed ChangeBatch atomically checks identity, business version, strictly consecutive expected/next revisions, and exact object preconditions, returning a CommitAck that exactly matches its header. Precondition or CAS conflicts return `ErrConflict` without partial publication; exhausted uint64 revisions must be rejected. There is no compatibility Save, optional DeltaSession, RequireDelta, or full-record fallback.

Durable backends retain an initial snapshot, consecutive immutable batches, and an authoritative head. Backends verify and maintain snapshot digests, batch-wire digests, parent revisions, and chain anchors; the physical chain is not part of ChangeHeader. New sessions must authoritatively load and validate the chain; ordinary Apply must not replay all history or rewrite a complete snapshot on every commit. Initialize may write a complete initial snapshot. Explicit snapshot compaction is a separate maintenance capability, currently not implemented, not an ordinary-commit fallback. Physical compaction is distinct from semantic history compression and retention.

Runtime advances in-memory state and publishes results only after an exact CommitAck. Initialize/Apply may commit and then return an error: stop without rollback or blind retries, wait for ownership release, then adjudicate through Load from a new session. Close is idempotent, waits for in-flight operations, and releases session ownership; Load/Initialize/Apply after close must fail.

## Concrete built-in Store guarantees

MemoryStore's zero value is usable. Initialize copies the initial record, Apply validates then updates affected objects only, and Load returns an isolated copy. It provides only in-process locking and CAS, without cross-process ownership or persistence after exit.

FileStore must be created with `filestore.New`. It uses one OS file lock per RunID, released on Close or process exit while the lock file remains. Lock implementations exist for Windows, Linux, macOS, FreeBSD, OpenBSD, NetBSD, and DragonFly. Other platforms reject session acquisition rather than running without locks. The filesystem must still support the required locks and atomic replacement within a directory.

FileStore uses immutable initial snapshot and batch files, publishing authority through a small `.head` synced and atomically replaced within the same directory, rather than a single authoritative `.json`. Files not referenced by the head cannot be adopted as committed; confirmation failure after head publication can make commit status uncertain. Old `.json` is explicitly rejected, never automatically cleared or migrated; use a new directory. Single-host guarantees do not extend to distributed leases, fencing, or power-loss durability. Incremental writes do not promise every scale is faster; see [Checkpoint cost](../guide/checkpoint-cost.md) for measurement boundaries.

All metadata strings and map keys must be valid UTF-8. Invalid metadata is rejected before writing, preserving the old file and Revision. Loading rejects invalid raw UTF-8 and unpaired JSON Unicode surrogates. Valid Unicode is preserved; Data bytes are not restricted by metadata UTF-8 rules.

## Next steps

Read [Error handling](errors.md) for corruption, conflicts, and uncertain saves. The [Storage](../guide/storage.md) guide provides a record inspection program; the [Execution contract](execution.md) defines business version and definition compatibility.
