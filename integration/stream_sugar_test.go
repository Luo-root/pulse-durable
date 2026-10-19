package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/filestore"
)

func TestManagedSugarRecoveryAcrossProcessExit(t *testing.T) {
	dir := t.TempDir()
	for _, phase := range []string{"run", "resume"} {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestManagedSugarProcessHelper$", "-test.count=1")
		cmd.Env = append(os.Environ(), "PULSE_SUGAR_DIR="+dir, "PULSE_SUGAR_PHASE="+phase)
		output, err := cmd.CombinedOutput()
		cancel()
		if len(output) > 0 {
			t.Logf("%s: %s", phase, output)
		}
		if phase == "run" {
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != crashExit {
				t.Fatalf("missing real exit: %v", err)
			}
			before := loadStreamRecord(t, dir)
			stopped := false
			for _, stream := range before.Streams {
				if stream.Stream.ID.Key == "left" && stream.Stream.Stopped {
					stopped = true
				}
			}
			if !stopped {
				t.Fatal("crash occurred before the explicit branch Stop was persisted")
			}
		} else if err != nil {
			t.Fatalf("resume: %v", err)
		}
	}
	record := loadStreamRecord(t, dir)
	for _, stream := range record.Streams {
		if stream.Stream.ID.Key == "left" {
			if !stream.Stream.Stopped || len(stream.Pending) != 0 || stream.Stream.Terminal != "stopped" {
				t.Fatal("stopped branch was reopened or failed")
			}
		} else if stream.LastSequence != 3 || stream.AckedThrough != 3 || len(stream.Pending) != 0 || stream.Stream.Terminal != "completed" {
			t.Fatalf("continuing branch lost data: %+v", stream)
		}
	}
	for _, node := range []string{"left", "right"} {
		want := 6
		if node == "left" {
			want = 1
		}
		found := false
		for _, managed := range record.Managed {
			if managed.NodeID == node {
				for _, state := range managed.State {
					if state.Name == "sum" {
						var got int
						if err := json.Unmarshal(state.Data, &got); err != nil {
							t.Fatal(err)
						}
						if got != want {
							t.Fatalf("%s sum=%d, want %d", node, got, want)
						}
						found = true
					}
				}
			}
		}
		if !found {
			t.Fatalf("missing state for %s", node)
		}
	}
	t.Logf("real exit/restart with ProduceStream→TeeStream→ConsumeStream: left stopped at 1, right sum=6, revision=%d", record.Revision)
}

func TestMessageBatchClearsSendAtCursorAcrossProcessExit(t *testing.T) {
	dir := t.TempDir()
	for _, phase := range []string{"run", "resume"} {
		dependencyRecoveryProcess(t, dir, "cursor", phase)
		r := loadRecord(t, dir)
		found := false
		for _, stream := range r.Streams {
			if stream.Stream.ID.NodeID == "transform" {
				found = true
				if stream.LastSequence != 2 || stream.Cursor != nil || stream.AckedThrough != 2 || len(stream.Pending) != 0 || stream.Stream.Terminal != "completed" {
					t.Fatalf("%s: confirmed batch and source finish disagree: %+v", phase, stream)
				}
			}
		}
		if !found || callCount(dir, "prefix") != 1 || callCount(dir, "value-10") != 1 || callCount(dir, "transform") != 1 {
			t.Fatalf("%s: output lost/replayed or completed transform reentered", phase)
		}
	}
}

func dependencyCursorGraph(t *testing.T, dir string) *pulse.Graph {
	t.Helper()
	g, err := pulse.New("dependency-cursor", "")
	if err != nil {
		t.Fatal(err)
	}
	input, output := pulse.NewKey[pulse.Stream[int]]("input"), pulse.NewKey[pulse.Stream[int]]("output")
	if err := pulse.ProduceStream(g, "source", "", input, func(_ *pulse.RunCtx, writer *pulse.StreamWriter[int]) error {
		if writer.Position().Sequence == 0 {
			return writer.SendAt(1, 1)
		}
		return nil
	}, pulse.WithStreamSource("input/1", true)); err != nil {
		t.Fatal(err)
	}
	if err := g.Add(pulse.NewNode("transform", "", pulse.Requires(input), pulse.Provides(output), func(rc *pulse.RunCtx) error {
		logCall(dir, "transform")
		writer, err := pulse.OpenStream(rc, output, pulse.WithBuffer(2), pulse.WithStreamSource("output/1", true))
		if err != nil {
			return err
		}
		if writer.Position().Sequence == 0 {
			if err := writer.SendAt(-1, "prefix"); err != nil {
				return err
			}
		}
		stream, err := pulse.Get(rc, input)
		if err != nil {
			return err
		}
		for {
			delivery, err := stream.Receive(rc)
			if err == io.EOF {
				if position := writer.Position(); position.Sequence != 2 || position.Cursor != nil {
					return fmt.Errorf("batch left stale local cursor: %+v", position)
				}
				return nil
			}
			if err != nil {
				return err
			}
			if err := pulse.HandleMessage(rc, delivery, func(_ *pulse.RunCtx, value int) error { return writer.Send(value * 10) }); err != nil {
				return err
			}
		}
	})); err != nil {
		t.Fatal(err)
	}
	if err := pulse.ConsumeStream(g, "sink", "", output, func(_ *pulse.RunCtx, value int) error {
		name := fmt.Sprintf("value-%d", value)
		if value == -1 {
			name = "prefix"
		}
		logCall(dir, name)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return g
}

func TestManagedSugarProcessHelper(t *testing.T) {
	dir := os.Getenv("PULSE_SUGAR_DIR")
	if dir == "" {
		return
	}
	phase := os.Getenv("PULSE_SUGAR_PHASE")
	store, err := filestore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	backend := &streamInjectedStore{Store: store, scenario: "sugar-stop", first: phase == "run"}
	rt, err := durable.New(backend, durable.NewCodecs())
	if err != nil {
		t.Fatal(err)
	}
	g, err := pulse.New("sugar-recovery", "单支停止后其它支继续并真实恢复")
	if err != nil {
		t.Fatal(err)
	}
	numbers, left, right := pulse.NewKey[pulse.Stream[int]]("numbers"), pulse.NewKey[pulse.Stream[int]]("left"), pulse.NewKey[pulse.Stream[int]]("right")
	if err := pulse.ProduceStream(g, "source", "可续读来源", numbers, func(_ *pulse.RunCtx, writer *pulse.StreamWriter[int]) error {
		logCall(dir, "source")
		for value := int(writer.Position().Sequence) + 1; value <= 3; value++ {
			if err := writer.SendAt(value, value); err != nil {
				return err
			}
		}
		return nil
	}, pulse.WithBuffer(4), pulse.WithStreamSource("sugar/integers", true)); err != nil {
		t.Fatal(err)
	}
	if err := pulse.TeeStream(g, "tee", "复制两路", numbers, pulse.Keys(left, right), pulse.WithBuffer(1)); err != nil {
		t.Fatal(err)
	}
	consume := func(key pulse.Key[pulse.Stream[int]], id string, stop bool) {
		if err := pulse.ConsumeStream(g, id, "保存显式状态", key, func(rc *pulse.RunCtx, value int) error {
			sum, _, err := pulse.GetMessageState[int](rc, "sum")
			if err != nil {
				return err
			}
			if err := pulse.SetMessageState(rc, "sum", sum+value); err != nil {
				return err
			}
			if stop {
				return pulse.ErrStreamStopped
			}
			return nil
		}, pulse.WithMessageHandling(pulse.WithMessageRetry(2, 0))); err != nil {
			t.Fatal(err)
		}
	}
	consume(left, "left", true)
	consume(right, "right", false)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	opts := durable.RunOptions{RunID: "stream-run", Version: "sugar/1"}
	if phase == "run" {
		_, err = rt.Run(ctx, g, pulse.Inputs{}, opts)
	} else {
		_, err = rt.Resume(ctx, g, pulse.Inputs{}, opts)
	}
	if err != nil {
		t.Fatal(err)
	}
}
