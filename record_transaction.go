package durable

import (
	"context"
	"fmt"
)

// recordTransaction stages only the objects changed by its callers. Construction,
// staging and commit require runState.mu; this is not a concurrent Store protocol.
// The legacy Session path still materializes and saves a complete format2 record.
type recordTransaction struct {
	baseRevision uint64
	attempts     map[string]putAttempt
	err          error
}

type putAttempt struct {
	expected AttemptRecord
	absent   bool
	value    AttemptRecord
}

func newRecordTransaction(baseRevision uint64) *recordTransaction {
	return &recordTransaction{baseRevision: baseRevision}
}

// putAttempt copies both values; nil expected means the key must be absent.
// Repeated puts with the same precondition coalesce to the final value. A
// conflicting precondition invalidates the whole transaction, even if ignored.
func (tx *recordTransaction) putAttempt(id string, expected *AttemptRecord, value AttemptRecord) error {
	if tx.err != nil {
		return tx.err
	}
	op := putAttempt{absent: expected == nil, value: value}
	if expected != nil {
		op.expected = *expected
	}
	if previous, ok := tx.attempts[id]; ok && (previous.absent != op.absent || previous.expected != op.expected) {
		tx.err = fmt.Errorf("%w: conflicting attempt precondition for %q", ErrConflict, id)
		return tx.err
	}
	if tx.attempts == nil {
		tx.attempts = make(map[string]putAttempt)
	}
	tx.attempts[id] = op
	return nil
}

// commitRecordTransaction shares save's confirmed-before-publish and failure
// sealing semantics. Preconditions are checked before allocating the full legacy
// proposal; no staged change can modify the live record on failure.
func (s *runState) commitRecordTransaction(ctx context.Context, tx *recordTransaction) error {
	if s.stopped != nil {
		return s.stopped
	}
	if err := checkCheckpointContext(ctx); err != nil {
		return err
	}
	if tx.err != nil {
		return tx.err
	}
	if tx.baseRevision != s.record.Revision {
		return fmt.Errorf("%w: stale record transaction revision", ErrConflict)
	}
	for id, op := range tx.attempts {
		current, exists := s.record.Attempts[id]
		if exists == op.absent || (exists && current != op.expected) {
			return fmt.Errorf("%w: attempt precondition for %q", ErrConflict, id)
		}
	}
	if len(tx.attempts) == 0 {
		return nil
	}
	proposed := cloneRecord(s.record)
	if proposed.Attempts == nil {
		proposed.Attempts = make(map[string]AttemptRecord, len(tx.attempts))
	}
	for id, op := range tx.attempts {
		proposed.Attempts[id] = op.value
	}
	return s.save(ctx, proposed)
}
