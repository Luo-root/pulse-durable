package integration_test

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/filestore"
)

func TestCompletedSourceHostTailAcrossProcessExit(t *testing.T) {
	dir := t.TempDir()
	run := func(phase string) error {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestHostTailProcessHelper$", "-test.count=1")
		cmd.Env = append(os.Environ(), "PULSE_HOST_TAIL_DIR="+dir, "PULSE_HOST_TAIL_PHASE="+phase)
		output, err := cmd.CombinedOutput()
		if len(output) > 0 {
			t.Logf("%s: %s", phase, output)
		}
		if ctx.Err() != nil {
			t.Fatalf("host process deadline: %v", ctx.Err())
		}
		return err
	}
	var exit *exec.ExitError
	if err := run("run"); !errors.As(err, &exit) || exit.ExitCode() != crashExit {
		t.Fatalf("first process did not exit after failed host Wait: %v", err)
	}
	before := loadStreamRecord(t, dir)
	source, ok := before.Scopes[before.RootScope].Nodes["source"]
	if !ok || before.Attempts[source.Attempt].State != durable.AttemptConfirmed {
		t.Fatal("source was not confirmed before host interruption")
	}
	for _, stream := range before.Streams {
		if len(stream.Pending) != 2 || stream.Stream.Terminal != "completed" {
			t.Fatal("interrupted host lost its completed-source tail")
		}
	}
	if err := run("resume"); err != nil {
		t.Fatalf("host resume: %v", err)
	}
	after := loadStreamRecord(t, dir)
	for id, stream := range after.Streams {
		if len(stream.Pending) != 0 || stream.AckedThrough != 2 || stream.Stream.ID.Generation <= before.Streams[id].Stream.ID.Generation {
			t.Fatal("host did not reclaim and confirm its tail")
		}
	}
	if calls := callCount(dir, "source"); calls != 1 {
		t.Fatalf("completed source ran %d times", calls)
	}
	t.Logf("real exit/restart: confirmed source calls=1, host tail 2→0, revision=%d→%d", before.Revision, after.Revision)
}

func TestHostTailProcessHelper(t *testing.T) {
	dir := os.Getenv("PULSE_HOST_TAIL_DIR")
	if dir == "" {
		return
	}
	phase := os.Getenv("PULSE_HOST_TAIL_PHASE")
	store, err := filestore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	confirmed := make(chan struct{})
	backend := &hostTailObservationStore{Store: store, confirmed: confirmed}
	runtime, err := durable.New(backend, durable.NewCodecs())
	if err != nil {
		t.Fatal(err)
	}
	graph, err := pulse.New("host-tail-process", "来源完成后宿主中断并跨进程确认尾部")
	if err != nil {
		t.Fatal(err)
	}
	key := pulse.NewKey[pulse.Stream[int]]("numbers")
	if err := graph.DeclareOutput(pulse.NewPort(key)); err != nil {
		t.Fatal(err)
	}
	if err := pulse.ProduceStream(graph, "source", "不可续读但可重建完成尾部", key, func(_ *pulse.RunCtx, writer *pulse.StreamWriter[int]) error {
		logCall(dir, "source")
		if err := writer.Send(1); err != nil {
			return err
		}
		return writer.Send(2)
	}, pulse.WithBuffer(2), pulse.WithStreamSource("host-tail/integers", false)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	reader := make(chan error, 1)
	opts := durable.RunOptions{RunID: "stream-run", Version: "host-tail/1", BeforeStart: func(e *pulse.Execution) error {
		subscription, err := pulse.SubscribeStream(e, key)
		if err != nil {
			return err
		}
		go func() {
			if phase == "run" {
				select {
				case <-confirmed:
				case <-ctx.Done():
					reader <- ctx.Err()
					return
				}
				_, err := receiveHostTail(ctx, subscription)
				cancel() // 已领取但不 Ack：Wait 必须失败，不能把来源 completed 当作读取完成。
				reader <- err
				return
			}
			for {
				delivery, err := receiveHostTail(ctx, subscription)
				if err == io.EOF {
					reader <- nil
					return
				}
				if err != nil {
					reader <- err
					return
				}
				if err := delivery.Ack(); err != nil {
					reader <- err
					return
				}
			}
		}()
		return nil
	}}
	if phase == "run" {
		if _, err := runtime.Run(ctx, graph, pulse.Inputs{}, opts); err == nil {
			t.Fatal("unacknowledged host Wait succeeded")
		}
	} else if _, err := runtime.Resume(ctx, graph, pulse.Inputs{}, opts); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-reader:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("host reader did not finish")
	}
	if phase == "run" {
		os.Exit(crashExit)
	}
}

func receiveHostTail(ctx context.Context, subscription *pulse.StreamSubscription[int]) (pulse.Delivery[int], error) {
	for {
		delivery, err := subscription.Receive(ctx)
		if !errors.Is(err, pulse.ErrGraphNotStarted) {
			return delivery, err
		}
		select {
		case <-ctx.Done():
			return delivery, ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
}

type hostTailObservationStore struct {
	durable.Store
	confirmed chan struct{}
	once      sync.Once
}

func (s *hostTailObservationStore) Acquire(ctx context.Context, id string) (durable.Session, error) {
	session, err := s.Store.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	return &hostTailObservationSession{Session: session, owner: s}, nil
}

type hostTailObservationSession struct {
	durable.Session
	owner *hostTailObservationStore
}

func (s *hostTailObservationSession) Save(ctx context.Context, record durable.RunRecord, expected uint64) error {
	if err := s.Session.Save(ctx, record, expected); err != nil {
		return err
	}
	if _, complete := record.Scopes[record.RootScope].Nodes["source"]; complete {
		s.owner.once.Do(func() { close(s.owner.confirmed) })
	}
	return nil
}
