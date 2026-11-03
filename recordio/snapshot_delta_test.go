package recordio_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
)

type snapshotCountingStore struct {
	t      *testing.T
	record durable.RunRecord
	checks int
}
type snapshotCountingSession struct{ s *snapshotCountingStore }

func (s *snapshotCountingStore) Acquire(context.Context, string) (durable.Session, error) {
	return &snapshotCountingSession{s}, nil
}
func (s *snapshotCountingSession) Close() error { return nil }
func (s *snapshotCountingSession) Load(context.Context) (durable.RunRecord, error) {
	if s.s.record.Revision == 0 {
		return durable.RunRecord{}, durable.ErrNotFound
	}
	return durable.CloneRecord(s.s.record), nil
}
func (s *snapshotCountingSession) Initialize(_ context.Context, r durable.RunRecord) error {
	s.s.record = durable.CloneRecord(r)
	return nil
}
func (s *snapshotCountingSession) Apply(_ context.Context, b durable.ChangeBatch) (durable.CommitAck, error) {
	t := s.s.t
	t.Helper()
	before := durable.CloneRecord(s.s.record)
	delta, err := b.SnapshotSizeDelta(s.s.record)
	if err != nil {
		return durable.CommitAck{}, err
	}
	if !reflect.DeepEqual(before, s.s.record) {
		t.Fatal("query mutated before state")
	}
	old, err := recordio.EncodeSnapshot(before)
	if err != nil {
		return durable.CommitAck{}, err
	}
	ack, err := b.ApplyTo(&s.s.record)
	if err != nil {
		return durable.CommitAck{}, err
	}
	next, err := recordio.EncodeSnapshot(s.s.record)
	if err != nil {
		return durable.CommitAck{}, err
	}
	if int64(len(old))+delta != int64(len(next)) {
		return durable.CommitAck{}, fmt.Errorf("revision %d: old=%d delta=%d next=%d", s.s.record.Revision, len(old), delta, len(next))
	}
	s.s.checks++
	return ack, nil
}

func TestSnapshotDeltaMatchesRuntimeEveryRevision(t *testing.T) {
	g, err := pulse.New("snapshot-count", "")
	if err != nil {
		t.Fatal(err)
	}
	key := pulse.NewKey[pulse.Stream[int]]("输入<&>中文")
	const n = 128
	if err = pulse.ProduceStream(g, "source", "", key, func(_ *pulse.RunCtx, w *pulse.StreamWriter[int]) error {
		for i := int(w.Position().Sequence) + 1; i <= n; i++ {
			if err := w.SendAt(i, i); err != nil {
				return err
			}
		}
		return nil
	}, pulse.WithBuffer(3), pulse.WithStreamSource("size/source", true)); err != nil {
		t.Fatal(err)
	}
	total := 0
	if err = pulse.ConsumeStream(g, "sum", "", key, func(rc *pulse.RunCtx, v int) error {
		previous, _, err := pulse.GetMessageState[int](rc, "sum")
		if err != nil {
			return err
		}
		total = previous + v
		return pulse.SetMessageState(rc, "sum", total)
	}, pulse.WithMessageHandling(pulse.WithMessageRetry(2, 0))); err != nil {
		t.Fatal(err)
	}
	store := &snapshotCountingStore{t: t}
	r, err := durable.New(store, durable.NewCodecs())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.Run(t.Context(), g, pulse.Inputs{}, durable.RunOptions{RunID: "snapshot-delta", Version: "1"}); err != nil {
		t.Fatal(err)
	}
	if total != n*(n+1)/2 || store.checks < 3*n || len(store.record.Messages) != n {
		t.Fatalf("sum=%d revisions=%d messages=%d", total, store.checks, len(store.record.Messages))
	}
	if _, err = r.Resume(t.Context(), g, pulse.Inputs{}, durable.RunOptions{RunID: "snapshot-delta", Version: "1"}); err != nil {
		t.Fatal(err)
	}
}
