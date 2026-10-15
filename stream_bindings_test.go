package durable

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
)

func TestChildStreamBindingRestoresWhileAnotherInputWasPending(t *testing.T) {
	var calls atomic.Int32
	build := func() *pulse.Graph {
		g := testGraph(t, "stream-bindings-parent")
		input, later := pulse.NewKey[pulse.Stream[int]]("input"), pulse.NewKey[int]("later")
		childInput, childLater := pulse.NewKey[pulse.Stream[int]]("child-input"), pulse.NewKey[int]("child-later")
		gate := make(chan struct{})
		var once sync.Once
		testAdd(t, g, pulse.NewNode("source", "", nil, pulse.Provides(input), func(rc *pulse.RunCtx) error {
			writer, err := pulse.OpenStream(rc, input, pulse.WithBuffer(4), pulse.WithStreamSource("binding-source", true))
			if err != nil {
				return err
			}
			for i := int(writer.Position().Sequence) + 1; i <= 3; i++ {
				if err := writer.SendAt(i, i); err != nil {
					return err
				}
			}
			return nil
		}))
		testAdd(t, g, pulse.NewNode("later", "", nil, pulse.Provides(later), func(rc *pulse.RunCtx) error {
			select {
			case <-gate:
			case <-rc.Context().Done():
				return rc.Context().Err()
			}
			return pulse.Set(rc, later, 7)
		}))
		child := testGraph(t, "stream-bindings-child")
		if err := child.DeclareInput(pulse.NewPort(childInput)); err != nil {
			t.Fatal(err)
		}
		if err := child.DeclareInput(pulse.NewPort(childLater)); err != nil {
			t.Fatal(err)
		}
		testAdd(t, child, pulse.NewNode("consume", "", pulse.Requires(childInput), nil, func(rc *pulse.RunCtx) error {
			stream, err := pulse.Get(rc, childInput)
			if err != nil {
				return err
			}
			// 第一轮在 late 仍 pending 时必须能进入这里，否则 gate 永远打不开。
			for {
				delivery, err := stream.Receive(rc)
				if err == io.EOF {
					return nil
				}
				if err != nil {
					return err
				}
				if err := pulse.HandleMessage(rc, delivery, func(rc *pulse.RunCtx, value int) error {
					calls.Add(1)
					once.Do(func() { close(gate) })
					sum, _, err := pulse.GetMessageState[int](rc, "sum")
					if err != nil {
						return err
					}
					return pulse.SetMessageState(rc, "sum", sum+value)
				}); err != nil {
					return err
				}
			}
		}))
		testAdd(t, child, pulse.NewNode("after", "", pulse.Requires(childLater), nil, func(*pulse.RunCtx) error { return nil }))
		if err := pulse.Sub(g, "mount", "", []pulse.SubBind{pulse.In(input, childInput), pulse.In(later, childLater)}, child); err != nil {
			t.Fatal(err)
		}
		return g
	}
	store := &faultStore{Store: NewMemoryStore(), after: true, failure: errors.New("uncertain child message"), match: func(record RunRecord) bool {
		for _, message := range record.Messages {
			if message.NodeID == "consume" && message.State == AttemptConfirmed {
				return true
			}
		}
		return false
	}}
	rt := testRuntime(t, store, NewCodecs())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := rt.Run(ctx, build(), pulse.Inputs{}, testOptions("run")); err == nil {
		t.Fatal("fault ignored")
	} else {
		t.Logf("first interruption: %v", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if _, err := rt.Resume(ctx2, build(), pulse.Inputs{}, testOptions("run")); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatalf("child message calls=%d, want 3", calls.Load())
	}
}
