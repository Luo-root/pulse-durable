package durable

import (
	"context"
	"fmt"
	"math"
)

// ChangeHeader binds a batch to one immutable run and one revision transition.
// Namespace/ownership fencing belongs to the session, not caller-supplied fields.
type ChangeHeader struct {
	BatchFormat, LogicalFormat                                        int
	RunID, BusinessVersion, DefinitionDigest, InputsDigest, RootScope string
	ExpectedRevision, Revision                                        uint64
}

// CommitAck must exactly match the submitted header before runtime publication.
// It is confirmation for this exclusive Session, not a distributed receipt.
type CommitAck struct{ Header ChangeHeader }

// ChangeBatch is an immutable, process-local, versioned typed operation envelope.
// Its private tables contain put-attempt, create-scope, put-node, stream-header,
// pending put/remove, message, managed-header and managed-entry put/remove ops.
// Each captures Absent or an exact old typed value (including nil versus empty).
// There is intentionally no JSON patch or mutable map escape hatch. Backend code
// uses ApplyTo for the single shared precondition/apply algorithm. A future wire
// format/exporter can be added without exposing runtime transaction aliases.
// The zero value is invalid. Copies can be retained and applied independently.
type ChangeBatch struct {
	header ChangeHeader
	tx     *recordTransaction
}

func (b ChangeBatch) Header() ChangeHeader { return b.header }

// OperationCount excludes unchanged history and counts coalesced object keys.
func (b ChangeBatch) OperationCount() int {
	if b.tx == nil {
		return 0
	}
	t := b.tx
	return len(t.attempts) + len(t.scopes) + len(t.nodes) + len(t.streams) + len(t.pending) + len(t.messages) + len(t.managed) + len(t.managedEntries)
}

func changeHeader(r RunRecord) ChangeHeader {
	return ChangeHeader{BatchFormat: 1, LogicalFormat: RecordFormat, RunID: r.RunID,
		BusinessVersion: r.Version, DefinitionDigest: r.Definition, InputsDigest: r.Inputs,
		RootScope: r.RootScope, ExpectedRevision: r.Revision, Revision: r.Revision + 1}
}

// ApplyTo checks all preconditions before changing record in place. The caller
// owns record exclusively and must provide atomic persistence around this call.
// No callback/user code executes during application. Returned data never aliases
// the batch, so a backend may retain/mutate its record without touching Runtime.
// This validates the trusted runtime's typed mutations, not graph/codec schemas;
// full graph validation remains on Runtime load and existing checkpoint hooks.
func (b ChangeBatch) ApplyTo(record *RunRecord) (CommitAck, error) {
	if record == nil || b.tx == nil || record.Revision == math.MaxUint64 || b.header != changeHeader(*record) || b.header.ExpectedRevision != b.tx.baseRevision {
		return CommitAck{}, fmt.Errorf("%w: invalid delta header", ErrConflict)
	}
	if err := b.tx.check(*record); err != nil {
		return CommitAck{}, err
	}
	b.tx.apply(record)
	record.Format, record.Revision = b.header.LogicalFormat, b.header.Revision
	return CommitAck{Header: b.header}, nil
}

// copy isolates every mutable staged value, not the unchanged RunRecord history.
func (tx *recordTransaction) copy() *recordTransaction {
	out := newRecordTransaction(tx.baseRevision)
	out.err = tx.err
	for k, op := range tx.attempts {
		var e *AttemptRecord
		if !op.absent {
			e = &op.expected
		}
		_ = out.putAttempt(k, e, op.value)
	}
	for k, op := range tx.scopes {
		_ = out.createScope(k, op)
	}
	for k, op := range tx.nodes {
		var e *NodeRecord
		if !op.absent {
			e = &op.expected
		}
		_ = out.putNode(k.scope, k.node, e, op.value)
	}
	for k, op := range tx.streams {
		var e *streamHeader
		if !op.absent {
			e = &op.expected
		}
		_ = out.putStream(k, e, op.value)
	}
	for k, op := range tx.pending {
		var e, v *StreamMessageRecord
		if !op.absent {
			e = &op.expected
		}
		if !op.remove {
			v = &op.value
		}
		_ = out.putPending(k.stream, k.sequence, e, v)
	}
	for k, op := range tx.messages {
		var e *MessageAttemptRecord
		if !op.absent {
			e = &op.expected
		}
		_ = out.putMessage(k, e, op.value)
	}
	for k, op := range tx.managed {
		var e *managedHeader
		if !op.absent {
			e = &op.expected
		}
		_ = out.putManaged(k, e, op.value)
	}
	for k, op := range tx.managedEntries {
		var e, v *ValueRecord
		if !op.absent {
			e = &op.expected
		}
		if !op.remove {
			v = &op.value
		}
		_ = out.putManagedEntry(k, e, v)
	}
	return out
}

func (s *runState) applyDelta(ctx context.Context, tx *recordTransaction) (err error) {
	if s.stopped != nil {
		return s.stopped
	}
	if err := checkCheckpointContext(ctx); err != nil {
		return err
	}
	if s.record.Revision == math.MaxUint64 {
		return s.stop(fmt.Errorf("%w: revision exhausted", ErrConflict))
	}
	batch := ChangeBatch{header: changeHeader(s.record), tx: tx.copy()}
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			err = s.stop(fmt.Errorf("durable: store delta apply panicked"))
		}
	}()
	ack, err := s.session.Apply(ctx, batch)
	returned = true
	if err != nil {
		return s.stop(&operationError{op: "store delta apply", cause: err})
	}
	if ack.Header != batch.header {
		return s.stop(fmt.Errorf("%w: delta acknowledgement mismatch", ErrConflict))
	}
	// All Initialize and Load boundary values are isolated, and batch owns a separate
	// transaction copy. Only the private record is mutated after confirmation.
	tx.apply(&s.record)
	s.record.Format, s.record.Revision = RecordFormat, batch.header.Revision
	return nil
}
