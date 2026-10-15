package durable

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
)

// 第二条输入的持久消息尝试已打开、两个分支的容量仍由第一条占据之后，
// 才让两个消费者 Ack 第一条并 Stop。Tee 的提交预检必须正常收尾，不能确认
// 已无分支接纳的第二条，也不能把正常 Stop 锁存为可靠失败。
func TestTeeAllStopMessageAttemptRunAndResume(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	sourceConfirmed, secondOpened := make(chan struct{}), make(chan struct{})
	var sourceOnce, secondOnce sync.Once
	var openedSecond atomic.Bool
	store := &streamObservationStore{Store: NewMemoryStore(), afterSave: func(record RunRecord) {
		if _, ok := record.Scopes[record.RootScope].Nodes["source"]; ok {
			sourceOnce.Do(func() { close(sourceConfirmed) })
		}
		for _, message := range record.Messages {
			if message.NodeID == "tee" && message.Sequence == 2 && message.State == AttemptActive {
				openedSecond.Store(true)
				secondOnce.Do(func() { close(secondOpened) })
			}
		}
	}}
	codecs := NewCodecs()
	runtime := testRuntime(t, store, codecs)
	g := testGraph(t, "tee-all-stop", pulse.WithMaxRunning(1))
	input := pulse.NewKey[pulse.Stream[int]]("input")
	branches := pulse.Keys(pulse.NewKey[pulse.Stream[int]]("left"), pulse.NewKey[pulse.Stream[int]]("right"))
	var sourceCalls, teeCalls atomic.Int32
	var sinkCalls [2]atomic.Int32
	var source pulse.Stream[int]
	var endpoints [2]pulse.Stream[int]
	wait := func(rc *pulse.RunCtx, signal <-chan struct{}) error {
		return rc.WaitStream(func(ctx context.Context) error {
			select {
			case <-signal:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}
	if err := pulse.ProduceStream(g, "source", "先完整接纳两条", input, func(rc *pulse.RunCtx, writer *pulse.StreamWriter[int]) error {
		sourceCalls.Add(1)
		var err error
		source, err = pulse.Get(rc, input)
		if err != nil {
			return err
		}
		for value := 1; value <= 2; value++ {
			if err := writer.SendAt(value, value); err != nil {
				return err
			}
		}
		return nil
	}, pulse.WithBuffer(2), pulse.WithStreamSource("tee-all-stop/integers", true)); err != nil {
		t.Fatal(err)
	}
	if err := pulse.TeeStream(g, "tee", "全支停止不确认未转发消息", input, branches, pulse.WithBuffer(1),
		pulse.WithStreamAspects(func(rc *pulse.RunCtx, next func(*pulse.RunCtx) error) error {
			teeCalls.Add(1)
			if err := wait(rc, sourceConfirmed); err != nil {
				return err
			}
			return next(rc)
		})); err != nil {
		t.Fatal(err)
	}
	for i, key := range branches {
		testAdd(t, g, pulse.NewNode(key.Name(), "只确认第一条后停止", pulse.Requires(key), nil, func(rc *pulse.RunCtx) error {
			sinkCalls[i].Add(1)
			var err error
			endpoints[i], err = pulse.Get(rc, key)
			if err != nil {
				return err
			}
			first, err := endpoints[i].Receive(rc)
			if err != nil {
				return err
			}
			if first.Sequence != 1 || first.Value != 1 {
				return fmt.Errorf("%s received %+v, want only the first input", key.Name(), first)
			}
			if err := wait(rc, secondOpened); err != nil {
				return err
			}
			if snapshot := source.Snapshot(); snapshot.LastSequence != 2 || snapshot.AckedThrough != 1 {
				return fmt.Errorf("second message was not pending before Stop: %+v", snapshot)
			}
			// 两支与 Tee 共享唯一业务额度。此处 Ack 唤醒 Tee 后，它必须等本
			// 消费者 Stop 并退出才能恢复；最后一支退出后预检必定看到全 Stop。
			if err := first.Ack(); err != nil {
				return err
			}
			return endpoints[i].Stop(rc)
		}))
	}
	options := testOptions("tee-all-stop-run")
	execution, err := runtime.Run(ctx, g, pulse.Inputs{}, options)
	if err != nil {
		t.Fatalf("normal all-branch Stop became a run failure: %v", err)
	}
	if !openedSecond.Load() {
		t.Fatal("did not exercise Stop after the second durable message attempt opened")
	}
	if snapshot := source.Snapshot(); snapshot.LastSequence != 2 || snapshot.AckedThrough != 1 || snapshot.InFlight != 0 || !snapshot.Stopped || snapshot.Terminal != pulse.StreamCompleted || snapshot.Err != nil {
		t.Fatalf("input Stop fabricated an acknowledgement or source terminal: %+v", snapshot)
	}
	for i, stream := range endpoints {
		if snapshot := stream.Snapshot(); snapshot.LastSequence != 1 || snapshot.AckedThrough != 1 || snapshot.InFlight != 0 || !snapshot.Stopped || snapshot.Terminal != pulse.StreamStopped || snapshot.Err != nil {
			t.Fatalf("branch %d received or confirmed extra work: %+v", i, snapshot)
		}
	}
	before := readRecord(t, store, options.RunID)
	assertTeeAllStopRecord(t, before)
	definition, err := inspectDefinition(g, codecs.snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if err := (&runState{def: definition, record: cloneRecord(before)}).validateLoaded(); err != nil {
		t.Fatalf("normal Stop left an invalid Messages/Attempts record: %v", err)
	}
	var restored atomic.Int32
	options.OnEvent = func(event Event) error {
		if event.Kind != EventRestored {
			return fmt.Errorf("unexpected resume event: %+v", event)
		}
		restored.Add(1)
		return nil
	}
	// 新 Runtime 从 MemoryStore 重新 Load，不能依靠前一次 runState 的内存账。
	resumed, err := testRuntime(t, store, codecs).Resume(ctx, g, pulse.Inputs{}, options)
	if err != nil {
		t.Fatalf("all-branch Stop could not Resume: %v", err)
	}
	if resumed.ID() == execution.ID() || restored.Load() != 4 || sourceCalls.Load() != 1 || teeCalls.Load() != 1 || sinkCalls[0].Load() != 1 || sinkCalls[1].Load() != 1 {
		t.Fatalf("Resume reentered completed business: restored=%d source=%d tee=%d sinks=[%d,%d]", restored.Load(), sourceCalls.Load(), teeCalls.Load(), sinkCalls[0].Load(), sinkCalls[1].Load())
	}
	after := readRecord(t, store, options.RunID)
	assertTeeAllStopRecord(t, after)
	if !reflect.DeepEqual(before.Messages, after.Messages) || !reflect.DeepEqual(before.Attempts, after.Attempts) || !reflect.DeepEqual(before.Managed, after.Managed) || !reflect.DeepEqual(before.Scopes, after.Scopes) {
		t.Fatal("Resume changed historical messages, attempts, state or completed nodes")
	}
	if err := (&runState{def: definition, record: cloneRecord(after)}).validateLoaded(); err != nil {
		t.Fatalf("Resume left invalid persisted relationships: %v", err)
	}
	t.Logf("Run/Resume preserved input last=2 acked=1 stopped=true; Tee message 1 confirmed, message 2 active on stopped input; source/tee/sinks called once; revisions=%d->%d", before.Revision, after.Revision)
}

func assertTeeAllStopRecord(t *testing.T, record RunRecord) {
	t.Helper()
	if len(record.Streams) != 3 || len(record.Messages) != 2 || len(record.Attempts) != 4 || len(record.Scopes[record.RootScope].Nodes) != 4 {
		t.Fatalf("unexpected stream/message/attempt/node counts: %d/%d/%d/%d", len(record.Streams), len(record.Messages), len(record.Attempts), len(record.Scopes[record.RootScope].Nodes))
	}
	for _, stream := range record.Streams {
		last, terminal := uint64(1), string(pulse.StreamStopped)
		if stream.Stream.ID.Key == "input" {
			last, terminal = 2, string(pulse.StreamCompleted)
		}
		if stream.LastSequence != last || stream.AckedThrough != 1 || len(stream.Pending) != 0 || !stream.Stream.Stopped || stream.Stream.Terminal != terminal {
			t.Fatalf("invalid all-stop stream record: %+v", stream)
		}
	}
	for id, attempt := range record.Attempts {
		node, exists := record.Scopes[record.RootScope].Nodes[attempt.NodeID]
		if !exists || node.Attempt != id || node.Reason != string(pulse.NodeCompleted) || attempt.State != AttemptConfirmed || attempt.Parent != record.RootScope || attempt.Boundary != "" || attempt.Index != 1 || attempt.Limit != 1 {
			t.Fatalf("normal Stop left an unfinished/failed node attempt: id=%s attempt=%+v node=%+v", id, attempt, node)
		}
	}
	seen := make(map[uint64]bool)
	for id, message := range record.Messages {
		parent, exists := record.Attempts[message.Parent]
		stream := record.Streams[streamKey(message.Input)]
		wantState := AttemptConfirmed
		if message.Sequence == 2 {
			wantState = AttemptActive // Stop 消除读取义务，不虚构确认或失败。
		}
		if !exists || parent.NodeID != "tee" || parent.State != AttemptConfirmed || message.NodeID != "tee" || message.Scope != record.RootScope || message.Input.Key != "input" || message.Sequence < 1 || message.Sequence > 2 || seen[message.Sequence] || message.State != wantState || message.Index != 1 || message.Limit != 1 || !stream.Stream.Stopped {
			t.Fatalf("invalid message/parent/stopped-input relation: id=%s message=%+v parent=%+v", id, message, parent)
		}
		seen[message.Sequence] = true
	}
}
