# Observation and stable run identity

This page is for service developers who need to tell a fresh retry from recovery of an unfinished attempt. The example targets public Durable revision `9fda63212a51fc04e44e771cb09e06a9a06b1c7b`, pinned by [installation](installation.md); there is no formal release. This revision pins public Core development commit `995752b4614a129064e2e274264e0f1a73f6f6f6` (`v0.4.2-0.20261020102630-995752b4614a`), with no local replace in default builds.

## Distinguish runs, processes and attempts

| Identity | Source and purpose | Across process recovery |
|---|---|---|
| `durable.run_id` | Host-supplied `RunOptions.RunID`, keyed by `durable.AttrRunID` | Unchanged for the same business run |
| `pulse.attempt.scope` / `pulse.attempt.parent_scope` | Persistent attempt cursors actually allocated or reclaimed by Durable | Reclaiming an active attempt retains Scope, ParentScope and index |
| Record `TraceID` | Tracing association supplied or generated at each invocation | This example creates one per invocation; hosts explicitly choose any tracing relationship across invocations |
| Record `HostID` | Host-supplied executor identity | May differ on takeover or remain the same on one host |
| `pulse.execution` | Core's process-local identity for each new Execution | Cannot replace a durable RunID; different processes may produce identical text |
| `pulse.aspect.boundary` | Aspect invocation number within Execution + NodeID | Associates nesting in this invocation; cannot replace a persistent Scope |

`pulse.graph`, nonempty `pulse.path`, and `pulse.node` still identify the definition, mount and node. Correlate a business run across recovery using its stable RunID and the actual Scope/ParentScope. Do not reconstruct persistent identity from Execution ID or paths.

## Attach RunID once during setup

RecordObserver implements Core's optional aspect observation interface. Existing `pulse.Retry(3, 0)` usage automatically emits `pulse.aspect` records after installing the observer, without logging in every node. Durable supplies only the attribute key. The following `runIDSink` is host code, assembled once per Run/Resume using the same RunID.

Record.Attrs has reference semantics, and a Sink consumes its input read-only. A wrapper must copy the four supported scalar types with Range before adding RunID, preserving the caller's record and other MultiSink members:

<<< ../../../observation_example_test.go#runIDSink

The setup and readout below come from the same compiled Example. MemoryStore demonstrates failure followed by success in one invocation; the next section verifies process recovery with real filestore data.

<<< ../../../observation_example_test.go#observation

The complete source, including package and imports, is `observation_example_test.go` at the repository root. From this source branch, run:

```sh
GOWORK=off go test -count=1 -run '^ExampleRuntime_Run_observation$' .
```

In PowerShell, set `$env:GOWORK = 'off'` and run the same go test command. The output asserts that attempts 1 and 2 are both new. Replace MemorySink with LineSink, SlogSink or a host sink as needed; LineSink still requires checking Flush at finalization.

## What actual exit and recovery report

`integration/observation_recovery_test.go` runs three real processes against one filestore directory. Every Record passes through RunID enrichment, and each process configures distinguishable HostID/TraceID values:

1. The first process installs the existing Retry(3, 0). Attempt 1 fails, its failure is confirmed, then attempt 2 is allocated. After synchronously writing the entry record while the persistent attempt remains active, the process exits directly. Phases are `started → attempt_started(1,new) → attempt_finished(1,failed) → retry(2,new) → attempt_started(2,new)`.
2. A fresh process calls Resume. The bridge returns `CheckpointAttempt.Resumed=true` from the actual active branch and reclaims Scope/Index 2. Phases are `started → attempt_started(2,resumed) → attempt_finished(2,completed) → finished`, with no new retry event.
3. Another process resumes after successful completion. The complete node result is restored without entering its body or aspects or replaying aspect history. RecordObserver emits only graph started/finished records; Durable's `OnEvent(EventRestored)` reports the restoration fact.

The regression compares authoritative cursors, output and budget. Body entries total 3, revisions are `5→6→6`, and recovery allocates no extra attempts. Each subprocess has a deadline. An overlay changing the active branch's Resumed marker to false makes this regression fail because resumed is mislabeled as new.

```sh
GOWORK=off go test -race -count=1 -timeout=90s -v -run '^TestRetryObservationAcrossProcessExit$' ./integration
```

Resumed is optional bridge observation metadata and never enters RunRecord JSON. Fresh allocation returns false; active reclamation returns true. Core records unknown when an older bridge omits it. Attempts start at 1, limit includes the first try, and first-attempt success emits no retry. Scope and ParentScope come directly from returned cursors. Duration covers only the invocation observed in this process.

## Preserve existing semantics

Normal skipping represents an absent result, ends as skipped with Err=nil, and never retries. Host cancellation retains the active cursor under existing recovery rules. Panic remains failure and follows the original Retry budget; reliable checkpoint failure cannot become a normal skip. Complete-node Commit follows the full aspect chain, so one aspect reporting completed does not prove durable node confirmation.

The five Observer lifecycle callback counts remain unchanged, and optional aspect observer panic does not change business results. Durable's existing `RunOptions.OnEvent` recovery callback still stops recovery on error; keep those error contracts distinct. Custom aspects automatically report entry/exit only when explicitly wrapped with `pulse.NamedAspect("approval", aspect)`. Application code supplies facts such as approval reasons. Message retries do not share node attempt numbering.

Continue with [subgraphs and retries](subgraphs.md), [process exit and recovery](recovery.md), and the [Core observation reference](https://luo-root.github.io/pulse/en/packages/observe/).
