package durable

import (
	"context"
	"errors"
	"fmt"

	"github.com/Luo-root/pulse"
)

// ClaimedSession is a caller-owned, already acquired Session with an explicit
// immutable RunID binding. RunID must be nonempty valid UTF-8 and match RunOptions.
// The caller must prevent concurrent executions on the same session, retain
// ownership until execution returns and any coordinated Finish/Release completes,
// then Close it. A distributed implementation must fence Load, Initialize and Apply itself;
// this interface does not implement leases or distributed coordination.
type ClaimedSession interface {
	Session
	RunID() string
}

// Outcome is the runtime's execution conclusion, not a coordinator's persisted
// task status. Only Completed and Failed permit a business-terminal Finish;
// neither implies that Finish has been performed. The accompanying error retains
// the existing Runtime error contract.
type Outcome string

const (
	// OutcomeCompleted means Wait succeeded and every root node is confirmed.
	OutcomeCompleted Outcome = "completed"
	// OutcomeFailed means a final root business failure is authoritatively saved,
	// including recovery rejected before Start because that failure already exists.
	OutcomeFailed Outcome = "failed"
	// OutcomeInterrupted means no business terminal verdict was confirmed. Host
	// cancellation and BeforeStart failures can be retried through recovery.
	OutcomeInterrupted Outcome = "interrupted"
	// OutcomeInfrastructure includes checkpoint failures and uncertain write results.
	// Reload authority before deciding whether any attempted write committed.
	OutcomeInfrastructure Outcome = "infrastructure"
	// OutcomeRejected means invalid or incompatible invocation; no business started.
	OutcomeRejected Outcome = "rejected"
)

// RunClaimed runs a new RunID using an already acquired session. It performs the
// same validation and checkpoints as Run, but never calls Acquire or Close.
// The caller retains the session after return for coordinated Finish/Release.
func (r *Runtime) RunClaimed(ctx context.Context, session ClaimedSession, g *pulse.Graph, in pulse.Inputs, opts RunOptions) (*pulse.Execution, Outcome, error) {
	var outcome Outcome
	e, err := r.executeSession(ctx, g, in, opts, false, session, &outcome)
	return e, outcome, err
}

// ResumeClaimed restores through the same validation and recovery path as Resume,
// without acquiring or closing session. Completed checkpoints are reused; final
// business failure does not receive new retry budget. Inspect Outcome, not a
// pending Snapshot or errors.Is(err, context.Canceled), to decide on Finish.
func (r *Runtime) ResumeClaimed(ctx context.Context, session ClaimedSession, g *pulse.Graph, in pulse.Inputs, opts RunOptions) (*pulse.Execution, Outcome, error) {
	var outcome Outcome
	e, err := r.executeSession(ctx, g, in, opts, true, session, &outcome)
	return e, outcome, err
}

func claimedRunID(session ClaimedSession) (id string, err error) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			id, err = "", fmt.Errorf("durable: claimed session RunID panicked")
		}
	}()
	id = session.RunID()
	returned = true
	return id, nil
}

// checkpointOrInspectionFailure treats diagnostic uncertainty like a reliable
// failure, without changing the original error returned to the caller. Track
// normal return so panic(nil) is also contained in compatibility mode.
func checkpointOrInspectionFailure(err error) (failed bool) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			failed = true
		}
	}()
	var checkpoint *pulse.CheckpointError
	failed = errors.As(err, &checkpoint)
	returned = true
	return failed
}

// Called only after Wait has joined business and reliable work. Checkpoint
// failures take precedence even when a sibling's final failure was saved.
func (s *runState) executionOutcome(e *pulse.Execution, err error) Outcome {
	// Error inspection may invoke arbitrary user As/Unwrap methods. Never run
	// those under the state lock, and never interpret an inspection panic as a
	// confirmed business verdict.
	checkpoint := checkpointOrInspectionFailure(err)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped != nil || checkpoint {
		return OutcomeInfrastructure
	}
	if s.scopeFailure(s.record.RootScope) != nil {
		return OutcomeFailed
	}
	if err == nil && len(s.record.Scopes[s.record.RootScope].Nodes) == len(s.def.graphs[s.def.root].nodes) {
		return OutcomeCompleted
	}
	if e.Snapshot().Graph.Status == pulse.GraphStatusCanceled {
		return OutcomeInterrupted
	}
	// No confirmed terminal fact: do not promote an unexplained core failure to a
	// business final failure, even if its error resembles cancellation.
	return OutcomeInfrastructure
}
