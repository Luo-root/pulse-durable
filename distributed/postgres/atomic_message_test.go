package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
)

type batchCaptureStore struct {
	durable.Store
	initial durable.RunRecord
	batches []durable.ChangeBatch
}
type batchCaptureSession struct {
	durable.Session
	owner *batchCaptureStore
}

func (s *batchCaptureStore) Acquire(ctx context.Context, id string) (durable.Session, error) {
	session, e := s.Store.Acquire(ctx, id)
	return &batchCaptureSession{Session: session, owner: s}, e
}
func (s *batchCaptureSession) Initialize(ctx context.Context, r durable.RunRecord) error {
	if e := s.Session.Initialize(ctx, r); e != nil {
		return e
	}
	s.owner.initial = durable.CloneRecord(r)
	return nil
}
func (s *batchCaptureSession) Apply(ctx context.Context, b durable.ChangeBatch) (durable.CommitAck, error) {
	ack, e := s.Session.Apply(ctx, b)
	if e == nil {
		s.owner.batches = append(s.owner.batches, b)
	}
	return ack, e
}

type editOp struct {
	Kind     string          `json:"kind"`
	Key      string          `json:"key"`
	Scope    string          `json:"scope,omitempty"`
	Name     string          `json:"name,omitempty"`
	Sequence string          `json:"sequence,omitempty"`
	State    bool            `json:"state,omitempty"`
	Expected json.RawMessage `json:"expected"`
	Value    json.RawMessage `json:"value"`
}
type editEnvelope struct {
	Version int             `json:"version"`
	Header  json.RawMessage `json:"header"`
	Ops     []editOp        `json:"ops"`
}

func TestPostgresAtomicStateDeleteAndTwoOutputs(t *testing.T) {
	c, admin, ns := integration(t)
	ctx := context.Background()
	g, e := pulse.New("atomic-pg", "")
	if e != nil {
		t.Fatal(e)
	}
	finished := make(chan struct{})
	input := pulse.NewKey[pulse.Stream[int]]("input")
	left := pulse.NewKey[pulse.Stream[int]]("left")
	right := pulse.NewKey[pulse.Stream[int]]("right")
	e = pulse.ProduceStream(g, "source", "", input, func(_ *pulse.RunCtx, w *pulse.StreamWriter[int]) error {
		for i := 1; i <= 2; i++ {
			if e := w.SendAt(i, i); e != nil {
				return e
			}
		}
		return nil
	}, pulse.WithBuffer(4), pulse.WithStreamSource("atomic/input", true))
	if e != nil {
		t.Fatal(e)
	}
	e = g.Add(pulse.NewNode("process", "", pulse.Requires(input), pulse.Deps(pulse.Provides(left), pulse.Provides(right)), func(rc *pulse.RunCtx) error {
		defer close(finished)
		l, e := pulse.OpenStream(rc, left, pulse.WithBuffer(4), pulse.WithStreamSource("atomic/left", true))
		if e != nil {
			return e
		}
		r, e := pulse.OpenStream(rc, right, pulse.WithBuffer(4), pulse.WithStreamSource("atomic/right", true))
		if e != nil {
			return e
		}
		stream, e := pulse.Get(rc, input)
		if e != nil {
			return e
		}
		for {
			delivery, e := stream.Receive(rc)
			if e == io.EOF {
				return nil
			}
			if e != nil {
				return e
			}
			if e = pulse.HandleMessage(rc, delivery, func(rc *pulse.RunCtx, v int) error {
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
	}))
	if e != nil {
		t.Fatal(e)
	}
	for i, key := range []pulse.Key[pulse.Stream[int]]{left, right} {
		key := key
		e = g.Add(pulse.NewNode(fmt.Sprint("sink", i), "", pulse.Requires(key), nil, func(rc *pulse.RunCtx) error {
			<-finished
			stream, e := pulse.Get(rc, key)
			if e != nil {
				return e
			}
			for {
				delivery, e := stream.Receive(rc)
				if e == io.EOF {
					return nil
				}
				if e != nil {
					return e
				}
				if e = delivery.Ack(); e != nil {
					return e
				}
			}
		}))
		if e != nil {
			t.Fatal(e)
		}
	}
	task, e := d.NewTask(ctx, d.Identity{Namespace: ns, RunID: "atomic"}, "atomic", "v1", g, pulse.Inputs{}, durable.NewCodecs())
	if e != nil {
		t.Fatal(e)
	}
	capture := &batchCaptureStore{Store: durable.NewMemoryStore()}
	rt, e := durable.New(capture, durable.NewCodecs())
	if e != nil {
		t.Fatal(e)
	}
	if _, e = rt.Run(ctx, g, pulse.Inputs{}, durable.RunOptions{RunID: "atomic", Version: "v1"}); e != nil {
		t.Fatal(e)
	}
	index := -1
	var good, bad durable.ChangeBatch
	for i, b := range capture.batches {
		wire, e := durable.MarshalChangeBatch(b)
		if e != nil {
			t.Fatal(e)
		}
		var envelope editEnvelope
		if e = json.Unmarshal(wire, &envelope); e != nil {
			t.Fatal(e)
		}
		removed, outputs, confirmed := false, 0, false
		for j := range envelope.Ops {
			op := &envelope.Ops[j]
			if op.Kind == "message" {
				var value struct {
					Sequence string `json:"sequence"`
					State    string `json:"state"`
				}
				_ = json.Unmarshal(op.Value, &value)
				if value.Sequence == "2" && value.State == "confirmed" {
					confirmed = true
				}
			}
			if op.Kind == "managed_entry" && op.State && op.Name == "remove" && string(op.Expected) != "null" {
				op.Value = json.RawMessage("null")
				removed = true
			}
			if op.Kind == "pending" && string(op.Value) != "null" {
				outputs++
			}
		}
		if !confirmed || !removed || outputs != 2 {
			continue
		}
		wire, e = json.Marshal(envelope)
		if e != nil {
			t.Fatal(e)
		}
		good, e = durable.UnmarshalChangeBatch(wire)
		if e != nil {
			t.Fatal(e)
		}
		n := 0
		for j := range envelope.Ops {
			op := &envelope.Ops[j]
			if op.Kind == "pending" && string(op.Value) != "null" {
				n++
				if n == 2 {
					op.Expected = append(json.RawMessage(nil), op.Value...)
				}
			}
		}
		wire, e = json.Marshal(envelope)
		if e != nil {
			t.Fatal(e)
		}
		bad, e = durable.UnmarshalChangeBatch(wire)
		if e != nil {
			t.Fatal(e)
		}
		index = i
		break
	}
	if index < 0 {
		t.Fatal("real Runtime did not emit State+ACK+two-output atomic batch")
	}
	if _, e = c.Submit(ctx, task); e != nil {
		t.Fatal(e)
	}
	claim := claimOne(t, c, task, "atomic", time.Minute)
	if e = c.InitializeOwned(ctx, d.InitializeRequest{Token: claim.Token, Record: capture.initial, Mutation: mut("atomic-init")}); e != nil {
		t.Fatal(e)
	}
	apply := func(b durable.ChangeBatch, id string) error {
		digest, e := d.DeltaMutationDigest(claim.Token, b)
		if e != nil {
			return e
		}
		_, e = c.ApplyOwned(ctx, claim.Token, d.Mutation{ID: id, Digest: digest}, b)
		return e
	}
	for i, b := range capture.batches[:index] {
		if e = apply(b, fmt.Sprint("prefix", i)); e != nil {
			t.Fatal(e)
		}
	}
	before, e := c.LoadOwned(ctx, claim.Token)
	if e != nil {
		t.Fatal(e)
	}
	if e = apply(bad, "bad-output"); e == nil {
		t.Fatal("second-output precondition accepted")
	}
	after, e := c.LoadOwned(ctx, claim.Token)
	if e != nil || after.Revision != before.Revision {
		t.Fatal("failed atomic batch advanced", e)
	}
	var badReceipts int
	if e = admin.QueryRow(ctx, `SELECT count(*) FROM pulse_durable_distributed.events WHERE namespace=$1 AND run_id=$2 AND mutation_id='bad-output'`, ns, "atomic").Scan(&badReceipts); e != nil || badReceipts != 0 {
		t.Fatal("failed batch has receipt", e)
	}
	if e = apply(good, "good-output"); e != nil {
		t.Fatal(e)
	}
	after, e = c.LoadOwned(ctx, claim.Token)
	if e != nil {
		t.Fatal(e)
	}
	removed := false
	for _, m := range after.Managed {
		found := false
		for _, v := range m.State {
			if v.Name == "remove" {
				found = true
			}
		}
		if !found {
			removed = true
		}
	}
	if !removed {
		t.Fatal("State removal not replayed")
	}
	appended := 0
	for _, s := range after.Streams {
		for _, p := range s.Pending {
			if p.Sequence == 2 {
				appended++
			}
		}
	}
	if appended < 2 {
		t.Fatalf("both outputs not replayed atomically: %d", appended)
	}
	confirmed := false
	for _, m := range after.Messages {
		if m.Sequence == 2 && string(m.State) == "confirmed" {
			confirmed = true
		}
	}
	if !confirmed {
		t.Fatal("input ACK/confirmation not committed with outputs")
	}
}
