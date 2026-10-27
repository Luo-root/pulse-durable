package durable

import (
	"fmt"
	"reflect"
	"sort"

	"github.com/Luo-root/pulse"
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

// streamView exposes headers and individual pending messages without cloning a
// RunRecord or Pending slice. All writes keep their original base precondition.
type streamView struct {
	tx   *recordTransaction
	base RunRecord
	key  string
}

func (v streamView) header() (streamHeader, bool) {
	if op, ok := v.tx.streams[v.key]; ok {
		return copyStreamHeader(op.value), true
	}
	stream, ok := v.base.Streams[v.key]
	return headerOf(stream), ok
}
func (v streamView) putHeader(value streamHeader) error {
	stream, ok := v.base.Streams[v.key]
	var expected *streamHeader
	if ok {
		h := headerOf(stream)
		expected = &h
	}
	return v.tx.putStream(v.key, expected, value)
}
func (v streamView) pending(sequence uint64) *StreamMessageRecord {
	if op, ok := v.tx.pending[pendingKey{v.key, sequence}]; ok {
		if op.remove {
			return nil
		}
		value := op.value
		return &value
	}
	pending := v.base.Streams[v.key].Pending
	i := pendingIndex(pending, sequence)
	if i < len(pending) && pending[i].Sequence == sequence {
		return &pending[i]
	}
	return nil
}
func (v streamView) putPending(sequence uint64, value *StreamMessageRecord) error {
	pending := v.base.Streams[v.key].Pending
	i := pendingIndex(pending, sequence)
	var expected *StreamMessageRecord
	if i < len(pending) && pending[i].Sequence == sequence {
		expected = &pending[i]
	}
	// An append followed by removal cancels the new object entirely.
	if expected == nil && value == nil {
		delete(v.tx.pending, pendingKey{v.key, sequence})
		return nil
	}
	return v.tx.putPending(v.key, sequence, expected, value)
}
func (v streamView) count() int {
	count := len(v.base.Streams[v.key].Pending)
	for key, op := range v.tx.pending {
		if key.stream != v.key {
			continue
		}
		if op.absent && !op.remove {
			count++
		}
		if !op.absent && op.remove {
			count--
		}
	}
	return count
}
func (v streamView) firstPending() *StreamMessageRecord {
	var first *StreamMessageRecord
	for _, message := range v.base.Streams[v.key].Pending {
		if current := v.pending(message.Sequence); current != nil {
			first = current
			break
		}
	}
	for key, op := range v.tx.pending {
		if key.stream == v.key && !op.remove && (first == nil || key.sequence < first.Sequence) {
			value := op.value
			first = &value
		}
	}
	return first
}
func (s *runState) currentStreamView(tx *recordTransaction, record RunRecord, id pulse.CheckpointStreamID, active bool) (streamView, streamHeader, error) {
	v := streamView{tx: tx, base: record, key: streamKey(streamID(id))}
	h, exists := v.header()
	if err := s.checkCurrentStream(h.Stream, exists, id, active); err != nil {
		return v, h, err
	}
	return v, h, nil
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
