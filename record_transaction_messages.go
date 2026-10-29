package durable

import (
	"fmt"
	"reflect"
	"sort"
)

type putMessage struct {
	expected, value MessageAttemptRecord
	absent          bool
}
type managedHeader struct{ Scope, Parent, NodeID string }
type putManagedHeader struct {
	expected, value managedHeader
	absent          bool
}
type managedEntryKey struct {
	managed, name string
	state         bool
}
type managedEntryOperation struct {
	expected, value ValueRecord
	absent, remove  bool
}

func (tx *recordTransaction) putMessage(id string, expected *MessageAttemptRecord, value MessageAttemptRecord) error {
	if tx.err != nil {
		return tx.err
	}
	op := putMessage{absent: expected == nil, value: value}
	if expected != nil {
		op.expected = *expected
	}
	if old, ok := tx.messages[id]; ok && (old.absent != op.absent || old.expected != op.expected) {
		tx.err = fmt.Errorf("%w: conflicting message precondition for %q", ErrConflict, id)
		return tx.err
	}
	if tx.messages == nil {
		tx.messages = make(map[string]putMessage)
	}
	tx.messages[id] = op
	return nil
}
func managedHeaderOf(m ManagedRecord) managedHeader {
	return managedHeader{m.Scope, m.Parent, m.NodeID}
}
func (tx *recordTransaction) putManaged(key string, expected *managedHeader, value managedHeader) error {
	if tx.err != nil {
		return tx.err
	}
	op := putManagedHeader{absent: expected == nil, value: value}
	if expected != nil {
		op.expected = *expected
	}
	if old, ok := tx.managed[key]; ok && (old.absent != op.absent || old.expected != op.expected) {
		tx.err = fmt.Errorf("%w: conflicting managed header precondition", ErrConflict)
		return tx.err
	}
	if tx.managed == nil {
		tx.managed = make(map[string]putManagedHeader)
	}
	tx.managed[key] = op
	return nil
}
func (tx *recordTransaction) putManagedEntry(key managedEntryKey, expected, value *ValueRecord) error {
	if tx.err != nil {
		return tx.err
	}
	op := managedEntryOperation{absent: expected == nil, remove: value == nil}
	if expected != nil {
		op.expected = copyTransactionValue(*expected)
	}
	if value != nil {
		op.value = copyTransactionValue(*value)
	}
	if (expected == nil && value == nil) || (expected != nil && expected.Name != key.name) || (value != nil && value.Name != key.name) {
		tx.err = fmt.Errorf("%w: invalid managed entry", ErrConflict)
		return tx.err
	}
	if old, ok := tx.managedEntries[key]; ok && (old.absent != op.absent || !reflect.DeepEqual(old.expected, op.expected)) {
		tx.err = fmt.Errorf("%w: conflicting managed entry precondition", ErrConflict)
		return tx.err
	}
	if tx.managedEntries == nil {
		tx.managedEntries = make(map[managedEntryKey]managedEntryOperation)
	}
	tx.managedEntries[key] = op
	return nil
}
func managedEntry(values []ValueRecord, name string) *ValueRecord {
	// Format2 验证允许导入条目未排序。
	for i := range values {
		if values[i].Name == name {
			return &values[i]
		}
	}
	return nil
}
func (tx *recordTransaction) checkMessages(record RunRecord) error {
	for id, op := range tx.messages {
		current, exists := record.Messages[id]
		if exists == op.absent || (exists && current != op.expected) {
			return fmt.Errorf("%w: message precondition for %q", ErrConflict, id)
		}
	}
	for key, op := range tx.managed {
		current, exists := record.Managed[key]
		if exists == op.absent || (exists && managedHeaderOf(current) != op.expected) {
			return fmt.Errorf("%w: managed header precondition", ErrConflict)
		}
	}
	for key, op := range tx.managedEntries {
		m, exists := record.Managed[key.managed]
		created, staged := tx.managed[key.managed]
		if !exists && (!staged || !created.absent) {
			return fmt.Errorf("%w: missing managed header", ErrConflict)
		}
		values := m.Values
		if key.state {
			values = m.State
		}
		current := managedEntry(values, key.name)
		if (current != nil) == op.absent || (current != nil && !reflect.DeepEqual(*current, op.expected)) {
			return fmt.Errorf("%w: managed entry precondition", ErrConflict)
		}
	}
	return nil
}
func (tx *recordTransaction) applyMessages(record *RunRecord) {
	if len(tx.messages) > 0 && record.Messages == nil {
		record.Messages = make(map[string]MessageAttemptRecord)
	}
	for id, op := range tx.messages {
		record.Messages[id] = op.value
	}
	if len(tx.managed) > 0 && record.Managed == nil {
		record.Managed = make(map[string]ManagedRecord)
	}
	for key, op := range tx.managed {
		m := record.Managed[key]
		m.Scope, m.Parent, m.NodeID = op.value.Scope, op.value.Parent, op.value.NodeID
		sort.Slice(m.State, func(i, j int) bool { return m.State[i].Name < m.State[j].Name })
		sort.Slice(m.Values, func(i, j int) bool { return m.Values[i].Name < m.Values[j].Name })
		record.Managed[key] = m
	}
	for key, op := range tx.managedEntries {
		m := record.Managed[key.managed]
		values := &m.Values
		if key.state {
			values = &m.State
		}
		i := sort.Search(len(*values), func(i int) bool { return (*values)[i].Name >= key.name })
		if op.remove {
			*values = append((*values)[:i:i], (*values)[i+1:]...)
			if len(*values) == 0 {
				*values = nil
			}
		} else if i < len(*values) && (*values)[i].Name == key.name {
			(*values)[i] = copyTransactionValue(op.value)
		} else {
			*values = append(*values, ValueRecord{})
			copy((*values)[i+1:], (*values)[i:])
			(*values)[i] = copyTransactionValue(op.value)
		}
		record.Managed[key.managed] = m
	}
}
