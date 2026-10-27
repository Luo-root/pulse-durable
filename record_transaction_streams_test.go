package durable

import (
	"errors"
	"reflect"
	"testing"

	"github.com/Luo-root/pulse"
)

func transactionStreamState() (*runState, *transactionSession) {
	s, session := transactionState()
	s.record.Streams = map[string]StreamRecord{"stream": {Stream: StreamDescriptorRecord{Attempt: "old", Capacity: 8}, LastSequence: 2,
		Cursor: &ValueRecord{Data: []byte("cursor")}, Pending: []StreamMessageRecord{{Sequence: 1, Value: ValueRecord{Data: []byte("one")}}, {Sequence: 2, Value: ValueRecord{Data: []byte("two")}}}}}
	return s, session
}

func TestRecordTransactionStreamPreconditions(t *testing.T) {
	for _, mode := range []string{"header changed", "header missing", "header absent", "pending changed", "pending missing", "pending absent", "missing stream"} {
		t.Run(mode, func(t *testing.T) {
			s, session := transactionStreamState()
			tx := newRecordTransaction(s.record.Revision)
			stream := s.record.Streams["stream"]
			old := headerOf(stream)
			next := old
			next.Stream.Terminal = "failed"
			expected := &old
			if mode == "header absent" {
				expected = nil
			}
			if err := tx.putStream("stream", expected, next); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "header changed":
				stream.LastSequence++
			case "header missing":
				delete(s.record.Streams, "stream")
			case "pending changed", "pending missing", "pending absent":
				expected := &stream.Pending[0]
				value := stream.Pending[0]
				if mode == "pending absent" {
					expected = nil
				}
				if err := tx.putPending("stream", 1, expected, &value); err != nil {
					t.Fatal(err)
				}
				if mode == "pending changed" {
					stream.Pending[0].Value.Data[0] = 'X'
				}
				if mode == "pending missing" {
					stream.Pending = stream.Pending[1:]
				}
			case "missing stream":
				message := StreamMessageRecord{Sequence: 1}
				if err := tx.putPending("missing", 1, nil, &message); err != nil {
					t.Fatal(err)
				}
			}
			if mode != "header missing" {
				s.record.Streams["stream"] = stream
			}
			before := cloneRecord(s.record)
			if err := s.commitRecordTransaction(t.Context(), tx); !errors.Is(err, ErrConflict) {
				t.Fatalf("want conflict: %v", err)
			}
			if session.calls != 0 || !reflect.DeepEqual(before, s.record) {
				t.Fatal("precondition failure published")
			}
		})
	}
}

func TestRecordTransactionStreamCopiesAndOrders(t *testing.T) {
	s, session := transactionStreamState()
	tx := newRecordTransaction(s.record.Revision)
	old := headerOf(s.record.Streams["stream"])
	next := copyStreamHeader(old)
	next.LastSequence = 4
	if err := tx.putStream("stream", &old, next); err != nil {
		t.Fatal(err)
	}
	old.Cursor.Data[0], next.Cursor.Data[0] = 'X', 'Y'
	for _, sequence := range []uint64{4, 3} {
		message := StreamMessageRecord{Sequence: sequence, Value: ValueRecord{Data: []byte("value"), Stream: &StreamDescriptorRecord{SourceID: "original"}}}
		if err := tx.putPending("stream", sequence, nil, &message); err != nil {
			t.Fatal(err)
		}
		message.Value.Data[0] = 'X'
		message.Value.Stream.SourceID = "changed"
	}
	removed := s.record.Streams["stream"].Pending[0]
	removed.Value = copyTransactionValue(removed.Value)
	if err := tx.putPending("stream", 1, &removed, nil); err != nil {
		t.Fatal(err)
	}
	removed.Value.Data[0] = 'Z'
	message := StreamMessageRecord{Sequence: 1, Value: ValueRecord{Data: []byte("new")}}
	if err := tx.putPending("new", 1, nil, &message); err != nil {
		t.Fatal(err)
	}
	if err := tx.putStream("new", nil, streamHeader{LastSequence: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.commitRecordTransaction(t.Context(), tx); err != nil {
		t.Fatal(err)
	}
	got := s.record.Streams["stream"]
	if session.calls != 1 || string(got.Cursor.Data) != "cursor" || len(got.Pending) != 3 {
		t.Fatalf("bad materialization: %+v", got)
	}
	for i, sequence := range []uint64{2, 3, 4} {
		if got.Pending[i].Sequence != sequence {
			t.Fatal("unordered pending")
		}
		if i > 0 && (string(got.Pending[i].Value.Data) != "value" || got.Pending[i].Value.Stream.SourceID != "original") {
			t.Fatal("retained caller alias")
		}
	}
	if len(s.record.Streams["new"].Pending) != 1 {
		t.Fatal("new stream lost pending")
	}
}

func TestRecordTransactionStreamDuplicateKeys(t *testing.T) {
	for _, kind := range []string{"header", "pending"} {
		for _, conflict := range []bool{false, true} {
			t.Run(kind+map[bool]string{true: " conflict", false: " coalesce"}[conflict], func(t *testing.T) {
				s, session := transactionStreamState()
				tx := newRecordTransaction(s.record.Revision)
				var err error
				if kind == "header" {
					old := headerOf(s.record.Streams["stream"])
					next := copyStreamHeader(old)
					if err = tx.putStream("stream", &old, next); err != nil {
						t.Fatal(err)
					}
					if conflict {
						old.Cursor.Data = []byte{}
					}
					next.Stream.Terminal = "failed"
					err = tx.putStream("stream", &old, next)
					if len(tx.streams) != 1 {
						t.Fatal("did not coalesce header")
					}
				} else {
					message := StreamMessageRecord{Sequence: 3, Value: ValueRecord{Data: []byte{}}}
					if err = tx.putPending("stream", 3, nil, &message); err != nil {
						t.Fatal(err)
					}
					var expected *StreamMessageRecord
					if conflict {
						expected = &message
					}
					message.Value.Data = []byte("last")
					err = tx.putPending("stream", 3, expected, &message)
					if len(tx.pending) != 1 {
						t.Fatal("did not coalesce pending")
					}
				}
				if conflict {
					if !errors.Is(err, ErrConflict) || !errors.Is(s.commitRecordTransaction(t.Context(), tx), ErrConflict) || session.calls != 0 {
						t.Fatal("conflict was not sticky")
					}
				} else if err != nil {
					t.Fatal(err)
				} else if err := s.commitRecordTransaction(t.Context(), tx); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestRecordTransactionStreamExactNilEmpty(t *testing.T) {
	for _, kind := range []string{"cursor", "pending"} {
		for _, empty := range []bool{false, true} {
			s, session := transactionStreamState()
			stream := s.record.Streams["stream"]
			stream.Cursor.Data = nil
			stream.Pending[0].Value.Data = nil
			s.record.Streams["stream"] = stream
			tx := newRecordTransaction(s.record.Revision)
			old := headerOf(stream)
			pending := stream.Pending[0]
			if empty {
				if kind == "cursor" {
					old.Cursor.Data = []byte{}
				} else {
					pending.Value.Data = []byte{}
				}
			}
			if err := tx.putStream("stream", &old, old); err != nil {
				t.Fatal(err)
			}
			if err := tx.putPending("stream", 1, &pending, nil); err != nil {
				t.Fatal(err)
			}
			err := s.commitRecordTransaction(t.Context(), tx)
			if empty && (!errors.Is(err, ErrConflict) || session.calls != 0) {
				t.Fatal("nil equaled empty")
			}
			if !empty && err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestRecordTransactionStreamSaveFailure(t *testing.T) {
	for _, mode := range []string{"error", "panic", "panic nil"} {
		t.Run(mode, func(t *testing.T) {
			s, session := transactionStreamState()
			before := cloneRecord(s.record)
			tx := newRecordTransaction(s.record.Revision)
			old := headerOf(s.record.Streams["stream"])
			next := old
			next.Stream.Terminal = "failed"
			if err := tx.putStream("stream", &old, next); err != nil {
				t.Fatal(err)
			}
			message := StreamMessageRecord{Sequence: 3, Value: ValueRecord{Data: []byte("three")}}
			if err := tx.putPending("stream", 3, nil, &message); err != nil {
				t.Fatal(err)
			}
			session.after = func(r RunRecord) error {
				stream := r.Streams["stream"]
				stream.Pending[0].Value.Data[0] = 'X'
				stream.Cursor.Data[0] = 'Y'
				switch mode {
				case "panic":
					panic("save")
				case "panic nil":
					panic(nil)
				}
				return errors.New("save")
			}
			if err := s.commitRecordTransaction(t.Context(), tx); err == nil || s.stopped == nil {
				t.Fatal("save failure not sealed")
			}
			if !reflect.DeepEqual(s.record, before) {
				t.Fatal("failed Save published")
			}
		})
	}
}

func TestRecordTransactionStreamHelperSharesLegacyRules(t *testing.T) {
	p := newStreamProtocol(t)
	for _, sequence := range []uint64{1, 2, 3} {
		if err := p.append(sequence); err != nil {
			t.Fatal(err)
		}
	}
	for _, sequence := range []uint64{2, 1, 3} {
		if err := p.s.commitMessage(t.Context(), pulse.CheckpointMessageCommit{Scope: p.s.record.RootScope, Inputs: []pulse.CheckpointStreamAcknowledgement{{Stream: p.input.Stream.ID, Sequence: sequence}}}); err != nil {
			t.Fatal(err)
		}
		stream := p.s.record.Streams[streamKey(streamID(p.input.Stream.ID))]
		want := map[uint64]uint64{2: 0, 1: 2, 3: 3}[sequence]
		if stream.AckedThrough != want {
			t.Fatalf("frontier after %d = %d", sequence, stream.AckedThrough)
		}
	}
	before := cloneRecord(p.s.record)
	if err := p.append(3); err == nil || !reflect.DeepEqual(before, p.s.record) {
		t.Fatal("duplicate append changed record")
	}
	stale := p.input.Stream.ID
	stale.Generation++
	if err := p.s.appendStream(t.Context(), pulse.CheckpointStreamAppend{Site: p.site, Attempt: p.source.Scope, Stream: stale, Message: pulse.CheckpointStreamMessage{Sequence: 4, Value: 4}}); err == nil {
		t.Fatal("stale generation accepted")
	}
}
