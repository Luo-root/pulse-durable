package sqlitestore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)

// 夹具是真实双输出 Runtime 图。仅修改公开类型化线格式：
// 第二次提交删除 State 条目，而非替换。
// 不同输出键和源身份不是同一组的两个名称。
func atomicMessageFixture(t *testing.T) (durable.RunRecord, []durable.ChangeBatch, int) {
	t.Helper()
	g, e := pulse.New("atomic-two-output", "")
	if e != nil {
		t.Fatal(e)
	}
	finished := make(chan struct{})
	input := pulse.NewKey[pulse.Stream[int]]("input")
	left := pulse.NewKey[pulse.Stream[int]]("left")
	right := pulse.NewKey[pulse.Stream[int]]("right")
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
			delivery, e := in.Receive(rc)
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
	})); e != nil {
		t.Fatal(e)
	}
	for _, key := range []pulse.Key[pulse.Stream[int]]{left, right} {
		key := key
		if e = g.Add(pulse.NewNode(fmt.Sprint("sink-", key), "", pulse.Requires(key), nil, func(rc *pulse.RunCtx) error {
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
		})); e != nil {
			t.Fatal(e)
		}
	}
	capture := &captureStore{Store: durable.NewMemoryStore()}
	rt, e := durable.New(capture, durable.NewCodecs())
	if e != nil {
		t.Fatal(e)
	}
	if _, e = rt.Run(ctx, g, pulse.Inputs{}, durable.RunOptions{RunID: "atomic", Version: "atomic/1"}); e != nil {
		t.Fatal(e)
	}
	for i, b := range capture.batches {
		wire, e := durable.MarshalChangeBatch(b)
		if e != nil {
			t.Fatal(e)
		}
		var envelope atomicWire
		if e = json.Unmarshal(wire, &envelope); e != nil {
			t.Fatal(e)
		}
		for _, op := range envelope.Ops {
			if op.Kind == "message" {
				var value struct {
					Sequence string `json:"sequence"`
					State    string `json:"state"`
				}
				if e = json.Unmarshal(op.Value, &value); e != nil {
					t.Fatal(e)
				}
				if value.Sequence == "2" && value.State == "confirmed" {
					return capture.initial, capture.batches, i
				}
			}
		}
	}
	t.Fatal("Runtime did not emit second confirmed message")
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

func atomicEditedBatch(t *testing.T, original durable.ChangeBatch, badOutput bool) durable.ChangeBatch {
	t.Helper()
	wire, e := durable.MarshalChangeBatch(original)
	if e != nil {
		t.Fatal(e)
	}
	var envelope atomicWire
	if e = json.Unmarshal(wire, &envelope); e != nil {
		t.Fatal(e)
	}
	removed := false
	outputs := 0
	for i := range envelope.Ops {
		op := &envelope.Ops[i]
		if op.Kind == "managed_entry" && op.State && op.Name == "remove" {
			if bytes.Equal(op.Expected, []byte("null")) {
				t.Fatal("State removal lacks exact old value")
			}
			op.Value = json.RawMessage("null")
			removed = true
		}
		if op.Kind == "pending" && !bytes.Equal(op.Value, []byte("null")) {
			outputs++
			if badOutput && outputs == 2 {
				// 两个输出都是新条目。给第二个指定精确但不存在的
				// 期望值；解码合法，但类型化前置条件必须拒绝它。
				op.Expected = append(json.RawMessage(nil), op.Value...)
			}
		}
	}
	if !removed || outputs != 2 {
		t.Fatalf("fixture remove=%t output appends=%d", removed, outputs)
	}
	wire, e = json.Marshal(envelope)
	if e != nil {
		t.Fatal(e)
	}
	b, e := durable.UnmarshalChangeBatch(wire)
	if e != nil {
		t.Fatal(e)
	}
	canonical, e := durable.MarshalChangeBatch(b)
	if e != nil {
		t.Fatal(e)
	}
	b, e = durable.UnmarshalChangeBatch(canonical)
	if e != nil {
		t.Fatal(e)
	}
	again, e := durable.MarshalChangeBatch(b)
	if e != nil || !bytes.Equal(canonical, again) {
		t.Fatal("typed canonical roundtrip", e)
	}
	if b.Header() != original.Header() {
		t.Fatal("wire editing changed real identity/header")
	}
	return b
}

func TestSQLiteAtomicMessageStateDeleteAndTwoOutputs(t *testing.T) {
	initial, batches, index := atomicMessageFixture(t)
	good := atomicEditedBatch(t, batches[index], false)
	bad := atomicEditedBatch(t, batches[index], true)
	for _, mode := range []string{"success", "second-output-precondition", "insert-rollback"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "atomic.sqlite")
			s := openStore(t, path)
			ss := acquire(t, s, initial.RunID)
			if e := ss.Initialize(ctx, initial); e != nil {
				t.Fatal(e)
			}
			for _, b := range batches[:index] {
				if _, e := ss.Apply(ctx, b); e != nil {
					t.Fatal(e)
				}
			}
			before, e := ss.Load(ctx)
			if e != nil {
				t.Fatal(e)
			}
			if mode == "insert-rollback" {
				if _, e = s.db.Exec(`CREATE TRIGGER atomic_abort BEFORE INSERT ON durable_batches BEGIN SELECT RAISE(ABORT,'atomic rollback'); END`); e != nil {
					t.Fatal(e)
				}
			}
			candidate := good
			if mode == "second-output-precondition" {
				candidate = bad
			}
			ack, applyErr := ss.Apply(ctx, candidate)
			if mode == "success" {
				if applyErr != nil || ack.Header != good.Header() {
					t.Fatal("ack", applyErr)
				}
			} else {
				if applyErr == nil {
					t.Fatal("invalid batch succeeded")
				}
				if mode == "second-output-precondition" && !errors.Is(applyErr, durable.ErrConflict) {
					t.Fatal("wrong precondition error", applyErr)
				}
			}
			if mode == "insert-rollback" {
				if _, e = s.db.Exec(`DROP TRIGGER atomic_abort`); e != nil {
					t.Fatal(e)
				}
			}
			ss.Close()
			s.Close()
			reopened := openStore(t, path)
			got, e := acquire(t, reopened, initial.RunID).Load(ctx)
			if e != nil {
				t.Fatal(e)
			}
			if mode != "success" {
				if !reflect.DeepEqual(got, before) {
					t.Fatal("failed atomic batch changed reopened projection")
				}
				return
			}
			if got.Revision != before.Revision+1 {
				t.Fatal("wrong committed revision")
			}
			removed := false
			for id, old := range before.Managed {
				for _, entry := range old.State {
					if entry.Name == "remove" {
						removed = true
						for _, entry := range got.Managed[id].State {
							if entry.Name == "remove" {
								t.Fatal("State entry not deleted")
							}
						}
					}
				}
			}
			if !removed {
				t.Fatal("missing old State")
			}
			confirmed := false
			for _, m := range got.Messages {
				if m.NodeID == "processor" && m.Sequence == 2 {
					confirmed = true
					if m.State != durable.AttemptConfirmed {
						t.Fatal("message not confirmed")
					}
				}
			}
			if !confirmed {
				t.Fatal("no second message")
			}
			outputs := map[string]string{}
			acked := false
			for _, stream := range got.Streams {
				switch stream.Stream.ID.Key {
				case "input":
					acked = true
					if stream.AckedThrough != 2 || len(stream.Pending) != 0 {
						t.Fatal("input ACK not atomic")
					}
				case "left", "right":
					if stream.LastSequence != 2 || len(stream.Pending) != 2 {
						t.Fatal("output append missing")
					}
					outputs[stream.Stream.ID.Key] = string(stream.Pending[1].Value.Data)
				}
			}
			if !acked || outputs["left"] != "20" || outputs["right"] != "200" {
				t.Fatal("two distinct outputs", fmt.Sprint(outputs))
			}
		})
	}
}
