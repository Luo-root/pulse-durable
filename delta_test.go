package durable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/Luo-root/pulse"
)

// Test-only authority. ApplyTo owns the precondition/state application; this
// backend adds no business state machine and is not a production persistence claim.
type deltaTestStore struct {
	record    RunRecord
	seeds     []evolutionSave
	batches   []ChangeBatch
	calls     int
	after     func(ChangeBatch) error
	oracle    bool
	benchmark bool
}
type deltaTestSession struct{ owner *deltaTestStore }

func (s *deltaTestStore) Acquire(context.Context, string) (Session, error) {
	return &deltaTestSession{s}, nil
}
func (s *deltaTestSession) Close() error { return nil }
func (s *deltaTestSession) Load(context.Context) (RunRecord, error) {
	if s.owner.record.Revision == 0 {
		return RunRecord{}, ErrNotFound
	}
	return cloneRecord(s.owner.record), nil
}
func (s *deltaTestSession) Initialize(_ context.Context, r RunRecord) error {
	o := s.owner
	if o.record.Revision != 0 || r.Revision != 1 {
		return ErrConflict
	}
	o.record = cloneRecord(r)
	if !o.benchmark {
		data, _ := json.Marshal(r)
		o.seeds = append(o.seeds, evolutionSave{r, data})
	}
	return nil
}
func (s *deltaTestSession) Apply(ctx context.Context, b ChangeBatch) (CommitAck, error) {
	o := s.owner
	o.calls++
	var legacy *runState
	if o.oracle {
		// Drive the actual legacy transaction consumer at every revision using the
		// same captured typed changes and same persistent IDs, not a full-record diff.
		legacy = &runState{record: cloneRecord(o.record)}
		// Test-only full-record materialization oracle, never a product backend.
		if err := b.tx.check(legacy.record); err != nil {
			return CommitAck{}, err
		}
		b.tx.apply(&legacy.record)
		legacy.record.Format, legacy.record.Revision = RecordFormat, b.Header().Revision
	}
	ack, err := b.ApplyTo(&o.record)
	if err != nil {
		return CommitAck{}, err
	}
	if legacy != nil {
		// Legacy clone normalizes empty slices; compare persisted projections.
		a, _ := json.Marshal(legacy.record)
		b, _ := json.Marshal(cloneRecord(o.record))
		if string(a) != string(b) {
			return CommitAck{}, fmt.Errorf("legacy/delta projection mismatch at %d", o.record.Revision)
		}
	}
	if !o.benchmark {
		o.batches = append(o.batches, b)
	}
	if o.after != nil {
		if err := o.after(b); err != nil {
			return CommitAck{}, err
		}
	}
	return ack, nil
}

func deltaManagedGraph(t testing.TB, n int, total *int) *pulse.Graph {
	t.Helper()
	g, err := pulse.New("delta-managed", "")
	if err != nil {
		t.Fatal(err)
	}
	key := pulse.NewKey[pulse.Stream[int]]("numbers")
	err = pulse.ProduceStream(g, "source", "", key, func(_ *pulse.RunCtx, w *pulse.StreamWriter[int]) error {
		for i := int(w.Position().Sequence) + 1; i <= n; i++ {
			if err := w.SendAt(i, i); err != nil {
				return err
			}
		}
		return nil
	}, pulse.WithBuffer(1), pulse.WithStreamSource("delta/integers", true))
	if err != nil {
		t.Fatal(err)
	}
	err = pulse.ConsumeStream(g, "sum", "", key, func(rc *pulse.RunCtx, v int) error {
		old, _, err := pulse.GetMessageState[int](rc, "sum")
		if err != nil {
			return err
		}
		*total = old + v
		return pulse.SetMessageState(rc, "sum", *total)
	}, pulse.WithMessageHandling(pulse.WithMessageRetry(2, 0)))
	if err != nil {
		t.Fatal(err)
	}
	return g
}
func TestDeltaRuntimeManaged500RevisionsAndResume(t *testing.T) {
	o := &deltaTestStore{oracle: true}
	r, _ := New(o, NewCodecs())
	total := 0
	g := deltaManagedGraph(t, 500, &total)
	opts := testOptions("delta-500")
	if _, err := r.Run(t.Context(), g, pulse.Inputs{}, opts); err != nil {
		t.Fatal(err)
	}
	if total != 125250 || len(o.seeds) != 1 || len(o.batches) < 1500 {
		t.Fatalf("total=%d seeds=%d batches=%d", total, len(o.seeds), len(o.batches))
	}
	for _, seed := range o.seeds {
		data, _ := json.Marshal(seed.record)
		if string(data) != string(seed.bytes) {
			t.Fatal("old Save argument aliased runtime")
		}
	}
	// Retained immutable batches can rebuild exact revision history independently.
	replay := cloneRecord(o.seeds[0].record)
	for _, b := range o.batches {
		if _, err := b.ApplyTo(&replay); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(replay, o.record) {
		t.Fatal("retained batch changed")
	}
	if _, err := r.Resume(t.Context(), g, pulse.Inputs{}, opts); err != nil {
		t.Fatal(err)
	}
	if total != 125250 {
		t.Fatal("Resume changed confirmed managed state")
	}
}
func TestDeltaRuntimeUncertainApplyRequiresAuthoritativeResume(t *testing.T) {
	for _, mode := range []string{"error", "panic", "panic nil"} {
		t.Run(mode, func(t *testing.T) {
			o := &deltaTestStore{}
			fault := errors.New("lost acknowledgement")
			o.after = func(b ChangeBatch) error {
				if len(b.tx.managedEntries) > 0 {
					switch mode {
					case "panic":
						panic("apply")
					case "panic nil":
						panic(nil)
					}
					return fault
				}
				return nil
			}
			r, _ := New(o, NewCodecs())
			total := 0
			g := deltaManagedGraph(t, 10, &total)
			opts := testOptions("uncertain")
			if _, err := r.Run(t.Context(), g, pulse.Inputs{}, opts); err == nil {
				t.Fatal("uncertain Apply succeeded")
			}
			calls := o.calls
			o.after = nil
			if _, err := r.Resume(t.Context(), g, pulse.Inputs{}, opts); err != nil {
				t.Fatal(err)
			}
			if total != 55 || o.calls <= calls {
				t.Fatalf("authority not resumed: %d", total)
			}
		})
	}
}
func TestMemorySupportsRequiredDelta(t *testing.T) {
	total := 0
	g := deltaManagedGraph(t, 1, &total)
	r, _ := New(NewMemoryStore(), NewCodecs())
	opts := testOptions("unsupported")
	if e, err := r.Run(t.Context(), g, pulse.Inputs{}, opts); e == nil || err != nil || total != 1 {
		t.Fatalf("%v %v total=%d", e, err, total)
	}
}
func TestDeltaFailureSealsPrivateRecord(t *testing.T) {
	for _, mode := range []string{"error", "panic", "panic nil", "ack"} {
		t.Run(mode, func(t *testing.T) {
			s, _ := transactionState()
			before := cloneRecord(s.record)
			o := &deltaTestStore{record: cloneRecord(before)}
			d := &deltaTestSession{o}
			s.session = d
			o.after = func(ChangeBatch) error {
				switch mode {
				case "panic":
					panic("apply")
				case "panic nil":
					panic(nil)
				case "ack":
					return nil
				}
				return errors.New("unknown")
			}
			if mode == "ack" {
				s.session = badAckSession{d}
			}
			tx := stageTransactionFailure(t, s)
			err := s.commitRecordTransaction(t.Context(), tx)
			if err == nil || s.stopped != err || !reflect.DeepEqual(before, s.record) {
				t.Fatal("failure published")
			}
			if again := s.commitRecordTransaction(t.Context(), tx); again != err || o.calls != 1 {
				t.Fatal("unknown retried")
			}
		})
	}
}

type badAckSession struct{ *deltaTestSession }

func (s badAckSession) Apply(ctx context.Context, b ChangeBatch) (CommitAck, error) {
	ack, err := s.deltaTestSession.Apply(ctx, b)
	ack.Header.Revision++
	return ack, err
}
func TestDeltaContextOverflowAndExactPreconditions(t *testing.T) {
	s, _ := transactionState()
	o := &deltaTestStore{record: cloneRecord(s.record)}
	s.session = &deltaTestSession{o}
	tx := stageTransactionFailure(t, s)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := s.commitRecordTransaction(ctx, tx); err == nil || o.calls != 0 || s.stopped != nil {
		t.Fatal("canceled checkpoint attempted")
	}
	s.record.Revision = math.MaxUint64
	tx = stageTransactionFailure(t, s)
	if err := s.commitRecordTransaction(t.Context(), tx); !errors.Is(err, ErrConflict) || o.calls != 0 || s.stopped == nil {
		t.Fatal("overflow attempted")
	}
	s, _ = transactionState()
	old := NodeRecord{Outputs: []ValueRecord{}}
	scope := s.record.Scopes["root"]
	scope.Nodes["empty"] = old
	tx = newRecordTransaction(s.record.Revision)
	_ = tx.putNode("root", "empty", &old, NodeRecord{})
	b := ChangeBatch{changeHeader(s.record), tx.copy()}
	scope.Nodes["empty"] = NodeRecord{Outputs: nil}
	if _, err := b.ApplyTo(&s.record); !errors.Is(err, ErrConflict) {
		t.Fatal("nil/empty precondition weakened")
	}
}

func BenchmarkDeltaRuntimeManaged(b *testing.B) {
	for _, n := range []int{10, 100, 500} {
		b.Run(fmt.Sprintf("rounds=%d", n), func(b *testing.B) {
			total := 0
			g := deltaManagedGraph(b, n, &total)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				o := &deltaTestStore{benchmark: true}
				r, _ := New(o, NewCodecs())
				_, err := r.Run(context.Background(), g, pulse.Inputs{}, RunOptions{RunID: "bench", Version: "1"})
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
