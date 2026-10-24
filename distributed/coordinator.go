package distributed

import (
	"context"
	"time"

	durable "github.com/Luo-root/pulse-durable"
)

type Status string

const (
	Queued    Status = "queued"
	Running   Status = "running"
	Blocked   Status = "blocked"
	Succeeded Status = "succeeded"
	Failed    Status = "failed"
	Canceled  Status = "canceled"
)

// Snapshot is an authoritative observation, never an execution permission.
// Reason is a stable sanitized code, never codec errors or business payload text.
type Snapshot struct {
	Task       Task
	Status     Status
	Epoch      int64
	Revision   uint64
	LeaseUntil time.Time
	CreatedAt  time.Time
	UpdatedAt  time.Time
	Reason     string
}
type SubmitResult struct {
	Snapshot     Snapshot
	Deduplicated bool
}
type Worker struct {
	ID     string
	BootID string
}

// Token is immutable per claim: fresh random >=128-bit LeaseID, boot and positive
// monotonically increasing Epoch. Namespace is always part of ownership identity.
type Token struct {
	Identity Identity
	Worker   Worker
	LeaseID  string
	Epoch    int64
}
type ClaimRequest struct {
	Namespace    string
	Worker       Worker
	Capabilities []Route
	Limit        int
	TTL          time.Duration
	// LeaseIDs are caller-generated independent random >=128-bit identities, one
	// per requested slot (len must equal Limit). A returned row uses exactly one
	// of these IDs; unfilled IDs remain unused. Retain all IDs on unknown commit.
	LeaseIDs []string
}
type Claim struct {
	Snapshot Snapshot
	Token    Token
}
type Mutation struct {
	ID     string
	Digest string
}
type SaveRequest struct {
	Token            Token
	ExpectedRevision uint64
	Record           durable.RunRecord
	Mutation         Mutation
}
type FinishRequest struct {
	Token            Token
	ExpectedRevision uint64
	Outcome          durable.Outcome
	Mutation         Mutation
}
type ListRequest struct {
	Namespace  string
	Status     Status
	Limit      int
	AfterRunID string
}

// Coordinator is the future persistent backend contract, NOT an implementation.
// All ownership operations atomically lock the authoritative row, sample DB time
// AFTER lock acquisition, check running+boot+lease+epoch+unexpired, and mutate in
// that transaction. Client clocks and a prior token read never authorize writes.
//
// Submit inserts queued/epoch=0 once by (namespace,run_id); on conflict compare
// canonical immutable fields AND digest via CompareSubmission, retaining all
// status/record/lease state. RunIDs/terminal rows are not automatically recycled.
// Claim only exact capabilities, at most Limit, queued or expired running; it
// increments epoch (reject overflow), assigns a fresh lease and preserves record.
// Each requested slot supplies its own fresh LeaseID before Claim. A backend
// rejects reused IDs, including retries; do not re-Claim an uncertain request.
// An uncertain Claim must be queried with FindClaim using each original lease ID;
// do not start until ownership is re-confirmed with Renew. Never replay with a
// new ID. Every write may return ErrUnknown: stop, read authority, don't infer
// business failure or automatically replay. All mutations have stable IDs/digests.
//
// LoadOwned returns ErrNotFound ONLY for an absent record under valid ownership.
// SaveOwned also checks revision CAS, exact task identity/version/definition/inputs
// in actual encoded record, bounds and JSON format; first revision=1, then +1.
// Finish accepts only Runtime OutcomeCompleted/OutcomeFailed and matching final
// revision; the runtime must confirm all root outputs/final failure first. It
// atomically sets immutable succeeded/failed. Cancel races Finish under same lock
// and returns existing terminal state idempotently. Block uses a sanitized reason
// for incompatible/corrupt claims, preventing infinite reclaim. Release preserves
// epoch/record and requeues ONLY after local execution and in-flight saves exit.
// Close is local resource cleanup, never an implicit Release. Reads don't renew.
// Implementations must enforce namespace authorization, bounded calls/records,
// fencing on EVERY write and retention, not adapt an unfenced legacy Store.
type Coordinator interface {
	Submit(context.Context, Task) (SubmitResult, error)
	Get(context.Context, Identity) (Snapshot, error)
	List(context.Context, ListRequest) ([]Snapshot, error)
	Claim(context.Context, ClaimRequest) ([]Claim, error)
	FindClaim(context.Context, string, Worker, string) ([]Claim, error)
	Renew(context.Context, Token, time.Duration) (time.Time, error)
	LoadOwned(context.Context, Token) (durable.RunRecord, error)
	SaveOwned(context.Context, SaveRequest) error
	Finish(context.Context, FinishRequest) (Snapshot, error)
	Release(context.Context, Token, Mutation) error
	Block(context.Context, Token, string, Mutation) error
	Cancel(context.Context, Identity, Mutation) (Snapshot, error)
}

// PullPolicy bounds admission, not DB ownership. Active includes pending claims,
// unconfirmed commits, running executions and draining work until all saves exit.
// Serialize reservation + Claim locally: recomputing the same free slots in two
// concurrent pollers is not admission. Paused stops new claims but renews existing
// work; Draining additionally indicates shutdown intent, never business cancel.
type PullPolicy struct {
	Capacity int
	BatchMax int
	Paused   bool
	Draining bool
}

func (p PullPolicy) Limit(active int) (int, error) {
	if p.Capacity < 1 || p.BatchMax < 1 || p.BatchMax > 1024 || active < 0 || active > p.Capacity {
		return 0, ErrInvalid
	}
	if p.Paused || p.Draining {
		return 0, nil
	}
	free := p.Capacity - active
	if free > p.BatchMax {
		free = p.BatchMax
	}
	return free, nil
}
