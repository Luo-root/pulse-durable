package filestore

import (
	"context"
	"os"
	"os/exec"
	"testing"

	pulse "github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)

type exitStore struct {
	durable.Store
	stage string
}
type exitSession struct {
	durable.Session
	stage string
}

func (s exitStore) Acquire(ctx context.Context, id string) (durable.Session, error) {
	ss, err := s.Store.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	return &exitSession{ss, s.stage}, nil
}

type exitContext struct {
	context.Context
	checks, target int
}

func (c *exitContext) Err() error {
	c.checks++
	if c.checks == c.target {
		os.Exit(73)
	}
	return c.Context.Err()
}
func (s *exitSession) Apply(ctx context.Context, b durable.ChangeBatch) (durable.CommitAck, error) {
	switch s.stage {
	case "before-batch":
		ctx = &exitContext{Context: ctx, target: 2}
	case "before-head":
		ctx = &exitContext{Context: ctx, target: 3}
	}
	ack, err := s.Session.Apply(ctx, b)
	if err == nil && s.stage == "after-head" {
		os.Exit(73)
	}
	return ack, err
}
func processGraph(t *testing.T, total *int) *pulse.Graph {
	t.Helper()
	g, err := pulse.New("exit-stream", "")
	if err != nil {
		t.Fatal(err)
	}
	k := pulse.NewKey[pulse.Stream[int]]("values")
	err = pulse.ProduceStream(g, "source", "", k, func(_ *pulse.RunCtx, w *pulse.StreamWriter[int]) error {
		for i := int(w.Position().Sequence) + 1; i <= 10; i++ {
			if err := w.SendAt(i, i); err != nil {
				return err
			}
		}
		return nil
	}, pulse.WithBuffer(1), pulse.WithStreamSource("exit/values", true))
	if err != nil {
		t.Fatal(err)
	}
	err = pulse.ConsumeStream(g, "sum", "", k, func(rc *pulse.RunCtx, v int) error {
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
func TestIncrementalProcessPublication(t *testing.T) {
	if dir := os.Getenv("PULSE_INCREMENTAL_CHILD_DIR"); dir != "" {
		st, err := New(dir)
		if err != nil {
			t.Fatal(err)
		}
		total := 0
		rt, err := durable.New(exitStore{Store: st, stage: os.Getenv("PULSE_INCREMENTAL_CHILD_STAGE")}, durable.NewCodecs())
		if err != nil {
			t.Fatal(err)
		}
		_, err = rt.Run(t.Context(), processGraph(t, &total), pulse.Inputs{}, durable.RunOptions{RunID: "exit", Version: "1"})
		t.Fatalf("child did not exit at publication boundary: %v", err)
	}
	for _, stage := range []string{"before-batch", "before-head", "after-head"} {
		t.Run(stage, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestIncrementalProcessPublication$")
			cmd.Env = append(os.Environ(), "PULSE_INCREMENTAL_CHILD_DIR="+dir, "PULSE_INCREMENTAL_CHILD_STAGE="+stage)
			out, err := cmd.CombinedOutput()
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != 73 {
				t.Fatalf("child exit=%v output=%s", err, out)
			}
			st, err := New(dir)
			if err != nil {
				t.Fatal(err)
			}
			ss, err := st.Acquire(t.Context(), "exit")
			if err != nil {
				t.Fatal(err)
			}
			r, err := ss.Load(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			want := uint64(1)
			if stage == "after-head" {
				want = 2
			}
			if r.Revision != want {
				t.Fatalf("revision=%d want=%d", r.Revision, want)
			}
			if err = ss.Close(); err != nil {
				t.Fatal(err)
			}
			total := 0
			rt, err := durable.New(st, durable.NewCodecs())
			if err != nil {
				t.Fatal(err)
			}
			if _, err = rt.Resume(t.Context(), processGraph(t, &total), pulse.Inputs{}, durable.RunOptions{RunID: "exit", Version: "1"}); err != nil {
				t.Fatal(err)
			}
			if total != 55 {
				t.Fatalf("recovered sum=%d want=55", total)
			}
		})
	}
}
