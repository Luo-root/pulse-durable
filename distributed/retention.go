package distributed

import "time"

// RetentionPolicy is opt-in. Nil TTLs mean unlimited retention, even when enabled.
// Guards, run identities and tombstones are not subject to these TTLs.
// Terminal payload age starts at finished_at, history age at revision commit,
// and event/receipt age at occurred_at. Pins still take precedence over expiry.
type RetentionPolicy struct {
	Enabled            bool
	TerminalPayloadTTL *time.Duration
	ReplayHistoryTTL   *time.Duration
	EventTTL           *time.Duration
	ReceiptTTL         *time.Duration
}

// Validate returns an isolated copy, including every duration pointer. Duration
// is already int64 nanoseconds; adapters parsing larger values must reject
// overflow before conversion. This method never computes a deadline or deletes.
func (p RetentionPolicy) Validate() (RetentionPolicy, error) {
	out := p
	for _, field := range []**time.Duration{&out.TerminalPayloadTTL, &out.ReplayHistoryTTL, &out.EventTTL, &out.ReceiptTTL} {
		if *field == nil {
			continue
		}
		v := **field
		if v < time.Hour || v%time.Second != 0 {
			return RetentionPolicy{}, ErrInvalid
		}
		*field = &v
	}
	return out, nil
}

// RequiresDryRun reports a retention contraction, not permission to apply it.
// Even changes to an inactive policy are checked conservatively for shorter TTLs.
func (p RetentionPolicy) RequiresDryRun(next RetentionPolicy) (bool, error) {
	if _, err := p.Validate(); err != nil {
		return false, err
	}
	if _, err := next.Validate(); err != nil {
		return false, err
	}
	if !p.Enabled && next.Enabled {
		return true, nil
	}
	before := []*time.Duration{p.TerminalPayloadTTL, p.ReplayHistoryTTL, p.EventTTL, p.ReceiptTTL}
	after := []*time.Duration{next.TerminalPayloadTTL, next.ReplayHistoryTTL, next.EventTTL, next.ReceiptTTL}
	for i, v := range after {
		if v != nil && (before[i] == nil || *v < *before[i]) {
			return true, nil
		}
	}
	return false, nil
}

// RetentionPolicyUpdate is a namespace-scoped CAS request. DryRunID references a
// separate read-only preview, not a boolean bypass or deletion authorization.
// The adapter must authenticate the caller and verify that the preview is genuine
// and bound to this namespace, expected version and exact proposed policy.
type RetentionPolicyUpdate struct {
	Namespace       string
	ExpectedVersion uint64
	Policy          RetentionPolicy
	DryRunID        string
}

// Validate checks shape and the observed version only. The adapter must repeat
// the CAS atomically against authority; this pure check neither applies a policy
// nor verifies a dry-run credential. Version zero is never an implicit upsert.
func (q RetentionPolicyUpdate) Validate(currentVersion uint64, current RetentionPolicy) (RetentionPolicy, error) {
	if !text(q.Namespace) || q.ExpectedVersion == 0 || currentVersion == 0 || (q.DryRunID != "" && !text(q.DryRunID)) {
		return RetentionPolicy{}, ErrInvalid
	}
	if q.ExpectedVersion != currentVersion {
		return RetentionPolicy{}, ErrRevisionConflict
	}
	shrinks, err := current.RequiresDryRun(q.Policy)
	if err != nil {
		return RetentionPolicy{}, err
	}
	if shrinks && q.DryRunID == "" {
		return RetentionPolicy{}, ErrInvalid
	}
	return q.Policy.Validate()
}

// MutationWindow is a closed interval of instants. Fixed-zone timestamps are
// accepted; comparisons are by instant, never by time.Location pointer identity.
type MutationWindow struct {
	From    time.Time
	Through time.Time
}

func (w MutationWindow) valid() bool {
	return !w.From.IsZero() && !w.Through.IsZero() && !w.Through.Before(w.From)
}

// MutationQuery does not change Mutation or the existing Coordinator interface.
// Nil epoch/revision means unknown; non-nil requires an exact match, including
// zero. Known values (for example Initialize/Apply) should be supplied explicitly.
// Cancel(identity, mutation) may leave them nil and read the committed values
// from the receipt; do not guess them from a later Get. Epoch may not be negative.
type MutationQuery struct {
	Identity         Identity
	Mutation         Mutation
	Operation        string
	ExpectedEpoch    *int64
	ExpectedRevision *uint64
	RequestedWindow  MutationWindow
}

func (q MutationQuery) Validate() error {
	if !text(q.Identity.Namespace) || !text(q.Identity.RunID) || !text(q.Mutation.ID) || !fingerprint(q.Mutation.Digest) || (q.ExpectedEpoch != nil && *q.ExpectedEpoch < 0) || !q.RequestedWindow.valid() {
		return ErrInvalid
	}
	switch q.Operation {
	case "initialize", "apply", "finish", "release", "block", "cancel":
		return nil
	default:
		return ErrInvalid
	}
}

type MutationQueryState string

const (
	Found              MutationQueryState = "found"
	AbsentWithinWindow MutationQueryState = "absent_within_window"
	Expired            MutationQueryState = "expired"
)

// MutationReceipt describes only a committed coordinator mutation, not external
// side effects. It is separate from the existing backend-specific receipt type.
type MutationReceipt struct {
	Identity   Identity
	Mutation   Mutation
	Operation  string
	Epoch      int64
	Revision   uint64
	OccurredAt time.Time
}

// MutationQueryResult carries an authoritative, contiguous receipt coverage
// window. The adapter must establish it from database time and a persisted
// retention watermark in the same consistent read, never from the client clock.
// Coverage.From bounds continuous completeness, not all retained receipts: a
// Found receipt may be older (for example, pinned or not yet cleaned up).
// Coverage.Through is the authoritative observation upper bound for this read;
// no returned receipt may be later. AbsentWithinWindow requires full requested
// coverage; Expired means some requested history is no longer covered.
// Neither absence nor expiry proves non-commit or authorizes business replay.
// Even Found resolves only the exact mutation, not external effects or ownership.
type MutationQueryResult struct {
	State    MutationQueryState
	Coverage MutationWindow
	Receipt  *MutationReceipt
}

// Validate checks internal consistency, not the authority of coverage/receipts.
// No result from this method is execution, lease or replay authorization.
func (r MutationQueryResult) Validate(q MutationQuery) error {
	if q.Validate() != nil || !r.Coverage.valid() {
		return ErrInvalid
	}
	covered := !q.RequestedWindow.From.Before(r.Coverage.From) && !q.RequestedWindow.Through.After(r.Coverage.Through)
	switch r.State {
	case Found:
		v := r.Receipt
		if v == nil || v.Identity != q.Identity || v.Mutation != q.Mutation || v.Operation != q.Operation || v.Epoch < 0 || (q.ExpectedEpoch != nil && v.Epoch != *q.ExpectedEpoch) || (q.ExpectedRevision != nil && v.Revision != *q.ExpectedRevision) || v.OccurredAt.IsZero() || v.OccurredAt.Before(q.RequestedWindow.From) || v.OccurredAt.After(q.RequestedWindow.Through) || v.OccurredAt.After(r.Coverage.Through) {
			return ErrInvalid
		}
	case AbsentWithinWindow:
		if r.Receipt != nil || !covered {
			return ErrInvalid
		}
	case Expired:
		// A future request is not expired history. Only loss at the lower
		// coverage boundary can establish expiry, and the read must have
		// reached the requested upper boundary.
		if r.Receipt != nil || !q.RequestedWindow.From.Before(r.Coverage.From) || q.RequestedWindow.Through.After(r.Coverage.Through) {
			return ErrInvalid
		}
	default:
		return ErrInvalid
	}
	return nil
}
