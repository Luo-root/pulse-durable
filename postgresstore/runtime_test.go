package postgresstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)

type captureStore struct {
	durable.Store
	initial durable.RunRecord
	batches []durable.ChangeBatch
}
type captureSession struct {
	durable.Session
	owner *captureStore
}

func (c *captureStore) Acquire(ctx context.Context, id string) (durable.Session, error) {
	s, e := c.Store.Acquire(ctx, id)
	if e != nil {
		return nil, e
	}
	return &captureSession{s, c}, nil
}
func (c *captureSession) Initialize(ctx context.Context, r durable.RunRecord) error {
	e := c.Session.Initialize(ctx, r)
	if e == nil {
		c.owner.initial = durable.CloneRecord(r)
	}
	return e
}
func (c *captureSession) Apply(ctx context.Context, b durable.ChangeBatch) (durable.CommitAck, error) {
	a, e := c.Session.Apply(ctx, b)
	if e == nil {
		c.owner.batches = append(c.owner.batches, b)
	}
	return a, e
}

// Real Runtime produces all identities, headers, ACKs, and two-output batches.
func atomicFixture(t *testing.T) (durable.RunRecord, []durable.ChangeBatch, int) {
	t.Helper()
	g, e := pulse.New("atomic", "")
	if e != nil {
		t.Fatal(e)
	}
	input := pulse.NewKey[pulse.Stream[int]]("input")
	left := pulse.NewKey[pulse.Stream[int]]("left")
	right := pulse.NewKey[pulse.Stream[int]]("right")
	finished := make(chan struct{})
	if e = g.Add(pulse.NewNode("source", "", nil, pulse.Provides(input), func(rc *pulse.RunCtx) error {
		w, e := pulse.OpenStream(rc, input, pulse.WithBuffer(4), pulse.WithStreamSource("atomic-input", true))
		if e != nil {
			return e
		}
		for v := 1; v <= 2; v++ {
			if e = w.SendAt(v, v); e != nil {
				return e
			}
		}
		return nil
	})); e != nil {
		t.Fatal(e)
	}
	if e = g.Add(pulse.NewNode("processor", "", pulse.Requires(input), pulse.Deps(pulse.Provides(left), pulse.Provides(right)), func(rc *pulse.RunCtx) error {
		defer close(finished)
		l, e := pulse.OpenStream(rc, left, pulse.WithBuffer(4), pulse.WithStreamSource("atomic-left", true))
		if e != nil {
			return e
		}
		r, e := pulse.OpenStream(rc, right, pulse.WithBuffer(4), pulse.WithStreamSource("atomic-right", true))
		if e != nil {
			return e
		}
		in, e := pulse.Get(rc, input)
		if e != nil {
			return e
		}
		for {
			d, e := in.Receive(rc)
			if e == io.EOF {
				return nil
			}
			if e != nil {
				return e
			}
			if e = pulse.HandleMessage(rc, d, func(rc *pulse.RunCtx, v int) error {
				if e := pulse.SetMessageState(rc, "remove", v); e != nil {
					return e
				}
				if e := l.Send(v * 10); e != nil {
					return e
				}
				return r.Send(v * 100)
			}); e != nil {
				return e
			}
		}
	})); e != nil {
		t.Fatal(e)
	}
	for i, key := range []pulse.Key[pulse.Stream[int]]{left, right} {
		key := key
		if e = g.Add(pulse.NewNode(fmt.Sprint("sink-", i), "", pulse.Requires(key), nil, func(rc *pulse.RunCtx) error {
			<-finished
			in, e := pulse.Get(rc, key)
			if e != nil {
				return e
			}
			for {
				d, e := in.Receive(rc)
				if e == io.EOF {
					return nil
				}
				if e != nil {
					return e
				}
				if e = d.Ack(); e != nil {
					return e
				}
			}
		})); e != nil {
			t.Fatal(e)
		}
	}
	capture := &captureStore{Store: durable.NewMemoryStore()}
	rt, e := durable.New(capture, durable.NewCodecs())
	if e != nil {
		t.Fatal(e)
	}
	if _, e = rt.Run(context.Background(), g, pulse.Inputs{}, durable.RunOptions{RunID: "atomic", Version: "1"}); e != nil {
		t.Fatal(e)
	}
	for i, b := range capture.batches {
		wire, e := durable.MarshalChangeBatch(b)
		if e != nil {
			t.Fatal(e)
		}
		var w atomicWire
		if e = json.Unmarshal(wire, &w); e != nil {
			t.Fatal(e)
		}
		for _, op := range w.Ops {
			if op.Kind == "message" {
				var v struct{ Sequence, State string }
				if e = json.Unmarshal(op.Value, &v); e != nil {
					t.Fatal(e)
				}
				if v.Sequence == "2" && v.State == "confirmed" {
					return capture.initial, capture.batches, i
				}
			}
		}
	}
	t.Fatal("no second message confirmation")
	return durable.RunRecord{}, nil, 0
}

type atomicOp struct {
	Kind     string          `json:"kind"`
	Key      string          `json:"key"`
	Scope    string          `json:"scope,omitempty"`
	Name     string          `json:"name,omitempty"`
	Sequence string          `json:"sequence,omitempty"`
	State    bool            `json:"state,omitempty"`
	Expected json.RawMessage `json:"expected"`
	Value    json.RawMessage `json:"value"`
}
type atomicWire struct {
	Version int             `json:"version"`
	Header  json.RawMessage `json:"header"`
	Ops     []atomicOp      `json:"ops"`
}

func editAtomic(t *testing.T, b durable.ChangeBatch, bad bool) durable.ChangeBatch {
	t.Helper()
	wire, e := durable.MarshalChangeBatch(b)
	if e != nil {
		t.Fatal(e)
	}
	var w atomicWire
	if e = json.Unmarshal(wire, &w); e != nil {
		t.Fatal(e)
	}
	removed, outputs := false, 0
	for i := range w.Ops {
		op := &w.Ops[i]
		if op.Kind == "managed_entry" && op.State && op.Name == "remove" {
			if bytes.Equal(op.Expected, []byte("null")) {
				t.Fatal("no old state")
			}
			op.Value = json.RawMessage("null")
			removed = true
		}
		if op.Kind == "pending" && !bytes.Equal(op.Value, []byte("null")) {
			outputs++
			if bad && outputs == 2 {
				op.Expected = append(json.RawMessage(nil), op.Value...)
			}
		}
	}
	if !removed || outputs != 2 {
		t.Fatalf("state removal=%t outputs=%d", removed, outputs)
	}
	wire, e = json.Marshal(w)
	if e != nil {
		t.Fatal(e)
	}
	out, e := durable.UnmarshalChangeBatch(wire)
	if e != nil {
		t.Fatal(e)
	}
	return out
}
func TestAtomicFixture(t *testing.T) {
	r, batches, index := atomicFixture(t)
	for _, b := range batches[:index] {
		if _, e := b.ApplyTo(&r); e != nil {
			t.Fatal(e)
		}
	}
	before := durable.CloneRecord(r)
	if _, e := editAtomic(t, batches[index], true).ApplyTo(&r); !errors.Is(e, durable.ErrConflict) {
		t.Fatal("bad output precondition", e)
	}
	if !reflect.DeepEqual(before, r) {
		t.Fatal("precondition failure mutated projection")
	}
	if _, e := editAtomic(t, batches[index], false).ApplyTo(&r); e != nil {
		t.Fatal(e)
	}
	assertAtomic(t, before, r)
}
func assertAtomic(t *testing.T, before, got durable.RunRecord) {
	t.Helper()
	if got.Revision != before.Revision+1 {
		t.Fatal("wrong revision")
	}
	removed := false
	for id, old := range before.Managed {
		for _, entry := range old.State {
			if entry.Name == "remove" {
				removed = true
				for _, v := range got.Managed[id].State {
					if v.Name == "remove" {
						t.Fatal("state not removed")
					}
				}
			}
		}
	}
	outputs := map[string]string{}
	acked, confirmed := false, false
	for _, m := range got.Messages {
		if m.NodeID == "processor" && m.Sequence == 2 {
			confirmed = m.State == durable.AttemptConfirmed
		}
	}
	for _, s := range got.Streams {
		switch s.Stream.ID.Key {
		case "input":
			acked = s.AckedThrough == 2 && len(s.Pending) == 0
		case "left", "right":
			if s.LastSequence != 2 || len(s.Pending) != 2 {
				t.Fatal("partial output")
			}
			outputs[s.Stream.ID.Key] = string(s.Pending[1].Value.Data)
		}
	}
	if !removed || !acked || !confirmed || outputs["left"] != "20" || outputs["right"] != "200" {
		t.Fatalf("atomic effects remove=%t ack=%t confirm=%t outputs=%v", removed, acked, confirmed, outputs)
	}
}
func TestPGAtomicStateDeleteAndTwoOutputs(t *testing.T) {
	dsn := os.Getenv("PULSE_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("opt-in isolated database required")
	}
	ctx := context.Background()
	initial, batches, index := atomicFixture(t)
	for _, mode := range []string{"success", "second-output-precondition", "insert-rollback"} {
		t.Run(mode, func(t *testing.T) {
			resetPGSchema(t, dsn)
			defer resetPGSchema(t, dsn)
			s, e := New(ctx, dsn)
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			a, e := s.Acquire(ctx, initial.RunID)
			if e != nil {
				t.Fatal(e)
			}
			if e = a.Initialize(ctx, initial); e != nil {
				t.Fatal(e)
			}
			for _, b := range batches[:index] {
				if _, e = a.Apply(ctx, b); e != nil {
					t.Fatal(e)
				}
			}
			before, e := a.Load(ctx)
			if e != nil {
				t.Fatal(e)
			}
			if mode == "insert-rollback" {
				_, e = s.pool.Exec(ctx, `CREATE FUNCTION pulse_durable.atomic_abort() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected'; END $$; CREATE TRIGGER atomic_abort BEFORE INSERT ON pulse_durable.batches FOR EACH ROW EXECUTE FUNCTION pulse_durable.atomic_abort()`)
				if e != nil {
					t.Fatal(e)
				}
			}
			b := editAtomic(t, batches[index], mode == "second-output-precondition")
			ack, e := a.Apply(ctx, b)
			if mode == "success" {
				if e != nil || ack.Header != b.Header() {
					t.Fatal("ack", e)
				}
			} else if e == nil {
				t.Fatal("fault accepted")
			}
			if mode == "second-output-precondition" && !errors.Is(e, durable.ErrConflict) {
				t.Fatal(e)
			}
			a.Close()
			s.Close()
			s, e = New(ctx, dsn)
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			a, e = s.Acquire(ctx, initial.RunID)
			if e != nil {
				t.Fatal(e)
			}
			defer a.Close()
			got, e := a.Load(ctx)
			if e != nil {
				t.Fatal(e)
			}
			if mode == "success" {
				assertAtomic(t, before, got)
			} else if !reflect.DeepEqual(before, got) {
				t.Fatal("failed batch partially committed")
			}
		})
	}
}
func TestPGRuntimeSum(t *testing.T) {
	dsn := os.Getenv("PULSE_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("opt-in isolated database required")
	}
	for _, n := range []int{10, 100, 500} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			ctx := context.Background()
			resetPGSchema(t, dsn)
			defer resetPGSchema(t, dsn)
			s, e := New(ctx, dsn)
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			rt, e := durable.New(s, durable.NewCodecs())
			if e != nil {
				t.Fatal(e)
			}
			g, e := pulse.New("sum", "")
			if e != nil {
				t.Fatal(e)
			}
			key := pulse.NewKey[pulse.Stream[int]]("input")
			calls := 0
			if e = g.Add(pulse.NewNode("source", "", nil, pulse.Provides(key), func(rc *pulse.RunCtx) error {
				w, e := pulse.OpenStream(rc, key, pulse.WithBuffer(4), pulse.WithStreamSource("integers", true))
				if e != nil {
					return e
				}
				for v := int(w.Position().Sequence) + 1; v <= n; v++ {
					if e = w.SendAt(v, v); e != nil {
						return e
					}
				}
				return nil
			})); e != nil {
				t.Fatal(e)
			}
			if e = g.Add(pulse.NewNode("consumer", "", pulse.Requires(key), nil, func(rc *pulse.RunCtx) error {
				in, e := pulse.Get(rc, key)
				if e != nil {
					return e
				}
				for {
					d, e := in.Receive(rc)
					if e == io.EOF {
						return nil
					}
					if e != nil {
						return e
					}
					if e = pulse.HandleMessage(rc, d, func(rc *pulse.RunCtx, v int) error {
						calls++
						sum, _, e := pulse.GetMessageState[int](rc, "sum")
						if e != nil {
							return e
						}
						return pulse.SetMessageState(rc, "sum", sum+v)
					}); e != nil {
						return e
					}
				}
			})); e != nil {
				t.Fatal(e)
			}
			opts := durable.RunOptions{RunID: "sum", Version: "1"}
			if _, e = rt.Run(ctx, g, pulse.Inputs{}, opts); e != nil {
				t.Fatal(e)
			}
			a, e := s.Acquire(ctx, "sum")
			if e != nil {
				t.Fatal(e)
			}
			r, e := a.Load(ctx)
			if e != nil {
				t.Fatal(e)
			}
			found := false
			for _, m := range r.Managed {
				for _, v := range m.State {
					if v.Name == "sum" {
						found = true
						if string(v.Data) != fmt.Sprint(n*(n+1)/2) {
							t.Fatal("wrong sum", string(v.Data))
						}
					}
				}
			}
			if !found || calls != n {
				t.Fatalf("sum found=%t calls=%d", found, calls)
			}
			var count, total int
			var snapshotBytes int
			if e = s.pool.QueryRow(ctx, `SELECT count(*),COALESCE(sum(octet_length(wire)),0) FROM pulse_durable.batches WHERE run_id='sum'`).Scan(&count, &total); e != nil {
				t.Fatal(e)
			}
			if e = s.pool.QueryRow(ctx, `SELECT octet_length(record) FROM pulse_durable.snapshots WHERE run_id='sum'`).Scan(&snapshotBytes); e != nil {
				t.Fatal(e)
			}
			usage := a.(*session).Usage()
			if usage.Batches != count || usage.WireBytes != total {
				t.Fatal("usage mismatch")
			}
			t.Logf("messages=%d batches=%d wire_bytes=%d initial_snapshot_bytes=%d (not WAL measurement)", n, count, total, snapshotBytes)
			a.Close()
			s.Close()
			s, e = New(ctx, dsn)
			if e != nil {
				t.Fatal(e)
			}
			defer s.Close()
			rt, e = durable.New(s, durable.NewCodecs())
			if e != nil {
				t.Fatal(e)
			}
			if _, e = rt.Resume(ctx, g, pulse.Inputs{}, opts); e != nil {
				t.Fatal(e)
			}
			if calls != n {
				t.Fatal("Resume repeated confirmed messages")
			}
		})
	}
}
