# Composing approvals and tool calls with aspects

Pulse owns graph execution and confirmed publication; Durable owns checkpoints and recovery. The host can compose approval state, event correlation, tool registries, and business loops using the existing extension mechanisms. This page describes two tested compositions and their limits.

## Waiting for approval and resuming

Build a normal static graph with a build step and a deployment step. Put an aspect around deployment: first read the confirmed build result, then check the application's approval record. Call `next(rc)` only after approval.

Waiting before `next` has not acquired the node's business quota. A short wait can directly observe `rc.Context().Done()` while independent nodes continue. The aspect owns the meaning of the condition. The node Snapshot may still be pending; this does not mean the engine has registered an approval task.

For a long wait that must release the process and its exclusive Session, the host can implement this protocol:

1. Reliably save an application wait record with a stable RunID, step, and approval correlation identifier.
2. Cancel this Execution and wait for Run to return, releasing its resources and Session. The application task remains waiting, while this Execution ends canceled.
3. When approval arrives, rebuild the same graph version with the same inputs and call Resume with the original RunID.
4. The unfinished node re-enters its aspect, checks approval again, and advances. Confirmed upstream work is not repeated.

The wait record and checkpoint are separate commit locations. The host must reconcile interruption after wait registration but before cancellation, and coordinate duplicate wake-ups for the same RunID. The example stores application records in local files and checks readability after normal process exit. It does not claim power-loss durability or an atomic transaction across the two records.

**Do not substitute an ordinary “still waiting” business error for host cancellation.** An ordinary error is a failure and confirms a failed attempt; Resume cannot replenish an exhausted root attempt budget. Normal waiting should not be labeled skipped either: skip means this step has no value in this run, not that approval remains pending.

The repository's real subprocess test starts a pausing process, a process that resumes after approval, and another that resumes the completed run. It checks that build executes once, the approval aspect enters twice, deployment executes once, and the stable root scope is preserved:

```sh
go test -race -count=1 -timeout=60s -run '^Test(ApprovalAspectResumeAcrossProcesses|BusinessWaitingErrorDoesNotSuspendRun)$' ./integration
```

See [integration/approval_resume_test.go](https://github.com/Luo-root/pulse-durable/blob/main/integration/approval_resume_test.go). This demonstrates that an aspect and Cancel/Resume can implement this host protocol. Built-in approval catalogs, Signal, a unified wake-up service, and distributed claiming or leases require their own designs.

## Calling Graph tools inside a loop node

A ReAct-shaped node can keep local loop variables and select tools according to business decisions. Register a tool as a function that creates a fresh Execution from a reusable Graph, supplies this call's Inputs, waits, and reads the declared outlet. Calling the same tool graph twice creates two independent Executions without changing the definition or reopening slots.

Pulse's [composition tests](https://github.com/Luo-root/pulse/blob/main/tests/composition_scenarios_test.go) use a deterministic decision stand-in to call a tool graph twice. They also check that an approval aspect leaves room for independent work with a quota of one. No remote model is involved. A tool called as an independent root has its own execution environment; it does not inherit the whole-tree quota or durable scope of a static Sub mount.

If the loop node exits before its complete checkpoint is confirmed, that node restarts at the beginning. Recovering each completed tool call separately requires stable round or call identities and persistent records, for example explicit independent Durable runs. A normal in-memory loop does not automatically provide that guarantee. External effects still require business idempotency or a transaction protocol.
