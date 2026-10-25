package worker

// Operational contract
//
// New requires explicit capacity, timing and a Renewer. Use a dedicated database
// pool for Renewer: worker goroutines cannot guarantee renewal priority inside a
// shared exhausted pool. Every coordinator call has a context deadline; the
// backend must honor cancellation. Factories/codecs and Observe are trusted host
// code and must terminate promptly. Business effects still require idempotency.
//
// Run is single-use. Pause stops admission (a Claim already admitted may complete)
// while active sessions keep renewing. Resume cannot reverse Drain. Drain stops
// admission without canceling active executions and returns only when all local
// reservations quiesce, or its caller's deadline expires. Canceling Run's context
// is host shutdown, not business cancellation; it stops local execution and lets
// uncertain leases expire. An uncooperative execution can outlive Run, remains in
// Active, and prevents Drain from reporting success. No lease is released before
// the actual execution and in-flight saves return.
//
// Business cancellation is Coordinator.Cancel, not Pause, Drain or host context
// cancellation. The coordinator fences subsequent operations; renewal rejection
// cancels local execution. This package does not infer a canceled terminal state
// from context.Canceled. Completed/failed terminal writes require Runtime outcomes.
//
// Unknown Claim uses the original IDs exclusively. ResolveClaim serializes with
// the original request: only a durable Closed acknowledgement frees that slot;
// a returned Claim still requires successful Renew before first execution.
// Unknown or invalid resolution responses retain the reservation for retry.
// FindClaim is not used as negative evidence. Renewal/Save uncertainty never
// restarts the same token. Shutdown retains unresolved reservations, and a Worker
// is single-use. Finish/Block/Release errors are reported and left to
// authoritative expiry/administrative reconciliation, never blindly replayed.
//
// Events use fixed reason codes and identities, without payloads or arbitrary
// errors. IDs belong in logs/traces, not default metric labels. Observer panics
// are contained; blocking observers are a host contract violation. No database,
// OS-process, cross-machine or external side-effect guarantee is established by
// this package's fake-coordinator tests.
