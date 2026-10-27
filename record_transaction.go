package durable

import (
	"context"
	"fmt"
	"reflect"
)

// recordTransaction stages only the objects changed by its callers. Construction,
// staging and commit require runState.mu; this is not a concurrent Store protocol.
// The legacy Session path materializes a complete format2 record; the explicitly
// selected DeltaSession path submits only these staged typed operations.
type recordTransaction struct {
	baseRevision   uint64
	attempts       map[string]putAttempt
	scopes         map[string]ScopeRecord // creation only; Nodes are always empty
	nodes          map[nodeRecordKey]putNode
	streams        map[string]putStreamHeader
	pending        map[pendingKey]pendingOperation
	messages       map[string]putMessage
	managed        map[string]putManagedHeader
	managedEntries map[managedEntryKey]managedEntryOperation
	err            error
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

type nodeRecordKey struct{ scope, node string }

type putNode struct {
	expected NodeRecord
	absent   bool
	value    NodeRecord
}

// createScope has an Absent precondition. It stages only a scope header, never
// a caller's Nodes map. Even empty nil/non-nil maps are not retained. Nodes for
// the new scope must be staged separately with putNode.
func (tx *recordTransaction) createScope(id string, value ScopeRecord) error {
	if tx.err != nil {
		return tx.err
	}
	if len(value.Nodes) != 0 {
		tx.err = fmt.Errorf("%w: new scope %q contains nodes", ErrConflict, id)
		return tx.err
	}
	value.Nodes = nil
	if previous, ok := tx.scopes[id]; ok && !reflect.DeepEqual(previous, value) {
		tx.err = fmt.Errorf("%w: conflicting scope creation for %q", ErrConflict, id)
		return tx.err
	}
	if tx.scopes == nil {
		tx.scopes = make(map[string]ScopeRecord)
	}
	tx.scopes[id] = value
	return nil
}

// copyTransactionNode preserves nil versus empty slices for exact old-value
// preconditions. cloneValues intentionally normalizes empty slices on the
// existing full-record Save path; that must not weaken transaction comparison.
func copyTransactionNode(node NodeRecord) NodeRecord {
	if node.Outputs != nil {
		outputs := make([]ValueRecord, len(node.Outputs))
		copy(outputs, node.Outputs)
		for i := range outputs {
			if data := outputs[i].Data; data != nil {
				outputs[i].Data = make([]byte, len(data))
				copy(outputs[i].Data, data)
			}
			if stream := outputs[i].Stream; stream != nil {
				value := *stream
				outputs[i].Stream = &value
			}
		}
		node.Outputs = outputs
	}
	return node
}

// putNode captures an exact old value, or Absent when expected is nil. Repeated
// writes coalesce only when their original preconditions agree. Scope and node
// form the key, so equal node IDs in different scopes remain independent.
func (tx *recordTransaction) putNode(scope, id string, expected *NodeRecord, value NodeRecord) error {
	if tx.err != nil {
		return tx.err
	}
	key := nodeRecordKey{scope, id}
	op := putNode{absent: expected == nil, value: copyTransactionNode(value)}
	if expected != nil {
		op.expected = copyTransactionNode(*expected)
	}
	if previous, ok := tx.nodes[key]; ok && (previous.absent != op.absent || !reflect.DeepEqual(previous.expected, op.expected)) {
		tx.err = fmt.Errorf("%w: conflicting node precondition for %q/%q", ErrConflict, scope, id)
		return tx.err
	}
	if tx.nodes == nil {
		tx.nodes = make(map[nodeRecordKey]putNode)
	}
	tx.nodes[key] = op
	return nil
}

// commitRecordTransaction shares save's confirmed-before-publish and failure
// sealing semantics. Preconditions are checked before allocating the full legacy
// proposal; no staged change can modify the live record on failure.
func (s *runState) commitRecordTransaction(ctx context.Context, tx *recordTransaction) error {
	return s.commitRecordTransactionPrepared(ctx, tx, nil)
}

// prepare retains commit's existing stream validation/finish rules on the
// isolated legacy proposal. It runs only after every overlay precondition passes.
func (s *runState) commitRecordTransactionPrepared(ctx context.Context, tx *recordTransaction, prepare func(*RunRecord) error) error {
	if s.stopped != nil {
		return s.stopped
	}
	if err := checkCheckpointContext(ctx); err != nil {
		return err
	}
	if err := tx.check(s.record); err != nil {
		return err
	}
	return s.commitCheckedTransaction(ctx, tx, prepare)
}

func (tx *recordTransaction) check(record RunRecord) error {
	if tx.err != nil {
		return tx.err
	}
	if tx.baseRevision != record.Revision {
		return fmt.Errorf("%w: stale record transaction revision", ErrConflict)
	}
	for id, op := range tx.attempts {
		current, exists := record.Attempts[id]
		if exists == op.absent || (exists && current != op.expected) {
			return fmt.Errorf("%w: attempt precondition for %q", ErrConflict, id)
		}
	}
	for id := range tx.scopes {
		if _, exists := record.Scopes[id]; exists {
			return fmt.Errorf("%w: scope creation precondition for %q", ErrConflict, id)
		}
	}
	for key, op := range tx.nodes {
		scope, exists := record.Scopes[key.scope]
		_, created := tx.scopes[key.scope]
		if !exists && !created {
			return fmt.Errorf("%w: missing node scope %q", ErrConflict, key.scope)
		}
		current, exists := scope.Nodes[key.node]
		if exists == op.absent || (exists && !reflect.DeepEqual(current, op.expected)) {
			return fmt.Errorf("%w: node precondition for %q/%q", ErrConflict, key.scope, key.node)
		}
	}
	if err := tx.checkStreams(record); err != nil {
		return err
	}
	if err := tx.checkMessages(record); err != nil {
		return err
	}
	return nil
}

func (s *runState) commitCheckedTransaction(ctx context.Context, tx *recordTransaction, prepare func(*RunRecord) error) error {
	if len(tx.attempts)+len(tx.scopes)+len(tx.nodes)+len(tx.streams)+len(tx.pending)+len(tx.messages)+len(tx.managed)+len(tx.managedEntries) == 0 {
		return nil
	}
	if s.delta != nil {
		if prepare != nil {
			// Node-finalization retains its complete proposal validation. Ordinary
			// stream/message checkpoints never materialize the full history.
			proposal := cloneRecord(s.record)
			tx.apply(&proposal)
			if err := prepare(&proposal); err != nil {
				return err
			}
		}
		return s.applyDelta(ctx, tx)
	}
	proposed := cloneRecord(s.record)
	tx.apply(&proposed)
	if prepare != nil {
		if err := prepare(&proposed); err != nil {
			return err
		}
	}
	return s.save(ctx, proposed)
}

// apply is shared by legacy materialization, confirmed runtime publication and
// ChangeBatch.ApplyTo. The caller must have checked all preconditions first.
func (tx *recordTransaction) apply(proposed *RunRecord) {
	if len(tx.scopes) > 0 && proposed.Scopes == nil {
		proposed.Scopes = make(map[string]ScopeRecord)
	}
	for id, scope := range tx.scopes {
		scope.Nodes = make(map[string]NodeRecord)
		proposed.Scopes[id] = scope
	}
	for key, op := range tx.nodes {
		scope := proposed.Scopes[key.scope]
		if scope.Nodes == nil {
			scope.Nodes = make(map[string]NodeRecord)
		}
		scope.Nodes[key.node] = copyTransactionNode(op.value)
		proposed.Scopes[key.scope] = scope
	}
	if len(tx.attempts) > 0 && proposed.Attempts == nil {
		proposed.Attempts = make(map[string]AttemptRecord, len(tx.attempts))
	}
	for id, op := range tx.attempts {
		proposed.Attempts[id] = op.value
	}
	tx.applyStreams(proposed)
	tx.applyMessages(proposed)
}
