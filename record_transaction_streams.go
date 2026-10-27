package durable

import (
	"fmt"
	"reflect"
	"sort"
)

// streamHeader deliberately excludes Pending: messages have independent keys.
type streamHeader struct {
	Stream                     StreamDescriptorRecord
	LastSequence, AckedThrough uint64
	Cursor                     *ValueRecord
}
type putStreamHeader struct {
	expected, value streamHeader
	absent          bool
}
type pendingKey struct {
	stream   string
	sequence uint64
}
type pendingOperation struct {
	expected, value StreamMessageRecord
	absent, remove  bool
}

func copyTransactionValue(value ValueRecord) ValueRecord {
	return copyTransactionNode(NodeRecord{Outputs: []ValueRecord{value}}).Outputs[0]
}
func headerOf(stream StreamRecord) streamHeader {
	h := streamHeader{Stream: stream.Stream, LastSequence: stream.LastSequence, AckedThrough: stream.AckedThrough}
	if stream.Cursor != nil {
		v := copyTransactionValue(*stream.Cursor)
		h.Cursor = &v
	}
	return h
}
func copyStreamHeader(h streamHeader) streamHeader {
	if h.Cursor != nil {
		v := copyTransactionValue(*h.Cursor)
		h.Cursor = &v
	}
	return h
}
func (tx *recordTransaction) putStream(key string, expected *streamHeader, value streamHeader) error {
	if tx.err != nil {
		return tx.err
	}
	op := putStreamHeader{absent: expected == nil, value: copyStreamHeader(value)}
	if expected != nil {
		op.expected = copyStreamHeader(*expected)
	}
	if old, ok := tx.streams[key]; ok && (old.absent != op.absent || !reflect.DeepEqual(old.expected, op.expected)) {
		tx.err = fmt.Errorf("%w: conflicting stream precondition for %q", ErrConflict, key)
		return tx.err
	}
	if tx.streams == nil {
		tx.streams = make(map[string]putStreamHeader)
	}
	tx.streams[key] = op
	return nil
}

// A nil expected message is Absent; nil value removes precisely that sequence.
func (tx *recordTransaction) putPending(stream string, sequence uint64, expected, value *StreamMessageRecord) error {
	if tx.err != nil {
		return tx.err
	}
	key := pendingKey{stream, sequence}
	op := pendingOperation{absent: expected == nil, remove: value == nil}
	if expected != nil {
		op.expected = *expected
		op.expected.Value = copyTransactionValue(expected.Value)
	}
	if value != nil {
		op.value = *value
		op.value.Value = copyTransactionValue(value.Value)
	}
	if (expected != nil && expected.Sequence != sequence) || (value != nil && value.Sequence != sequence) || (op.absent && op.remove) {
		tx.err = fmt.Errorf("%w: invalid pending operation", ErrConflict)
		return tx.err
	}
	if old, ok := tx.pending[key]; ok && (old.absent != op.absent || !reflect.DeepEqual(old.expected, op.expected)) {
		tx.err = fmt.Errorf("%w: conflicting pending precondition", ErrConflict)
		return tx.err
	}
	if tx.pending == nil {
		tx.pending = make(map[pendingKey]pendingOperation)
	}
	tx.pending[key] = op
	return nil
}
func pendingIndex(pending []StreamMessageRecord, sequence uint64) int {
	return sort.Search(len(pending), func(i int) bool { return pending[i].Sequence >= sequence })
}
func (tx *recordTransaction) checkStreams(record RunRecord) error {
	for key, op := range tx.streams {
		stream, exists := record.Streams[key]
		if exists == op.absent || (exists && !reflect.DeepEqual(headerOf(stream), op.expected)) {
			return fmt.Errorf("%w: stream precondition for %q", ErrConflict, key)
		}
	}
	for key, op := range tx.pending {
		stream, exists := record.Streams[key.stream]
		created, staged := tx.streams[key.stream]
		if !exists && (!staged || !created.absent) {
			return fmt.Errorf("%w: missing pending stream", ErrConflict)
		}
		index := pendingIndex(stream.Pending, key.sequence)
		found := index < len(stream.Pending) && stream.Pending[index].Sequence == key.sequence
		if found == op.absent || (found && !reflect.DeepEqual(stream.Pending[index], op.expected)) {
			return fmt.Errorf("%w: pending precondition", ErrConflict)
		}
	}
	return nil
}

// applyStreams is only used on an isolated legacy proposal, never live state.
func (tx *recordTransaction) applyStreams(record *RunRecord) {
	if len(tx.streams) > 0 && record.Streams == nil {
		record.Streams = make(map[string]StreamRecord)
	}
	for key, op := range tx.streams {
		stream := record.Streams[key]
		h := copyStreamHeader(op.value)
		stream.Stream, stream.LastSequence, stream.AckedThrough, stream.Cursor = h.Stream, h.LastSequence, h.AckedThrough, h.Cursor
		record.Streams[key] = stream
	}
	for key, op := range tx.pending {
		stream := record.Streams[key.stream]
		index := pendingIndex(stream.Pending, key.sequence)
		found := index < len(stream.Pending) && stream.Pending[index].Sequence == key.sequence
		if op.remove {
			stream.Pending = append(stream.Pending[:index:index], stream.Pending[index+1:]...)
		} else {
			value := op.value
			value.Value = copyTransactionValue(value.Value)
			if found {
				stream.Pending[index] = value
			} else {
				stream.Pending = append(stream.Pending, StreamMessageRecord{})
				copy(stream.Pending[index+1:], stream.Pending[index:])
				stream.Pending[index] = value
			}
		}
		record.Streams[key.stream] = stream
	}
}

func (tx *recordTransaction) failStreamOwner(record RunRecord, id string) error {
	if err := checkFailedStreamOwner(record, id); err != nil {
		return err
	}
	root := record.Attempts[id]
	failed := root
	failed.State = AttemptFailed
	if root != failed {
		if err := tx.putAttempt(id, &root, failed); err != nil {
			return err
		}
	}
	for key, stream := range record.Streams {
		if stream.Stream.Attempt == id && stream.Stream.Terminal == "" {
			old := headerOf(stream)
			value := old
			value.Stream.Terminal = "failed"
			if err := tx.putStream(key, &old, value); err != nil {
				return err
			}
		}
	}
	return nil
}
