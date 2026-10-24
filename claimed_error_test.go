package durable

import (
	"context"
	"testing"

	"github.com/Luo-root/pulse"
)

// Inspection is safe during node failure handling, then becomes hostile only
// after the core has joined work and published its final graph observation.
type claimedInspectionError struct {
	method    string
	armed     bool
	execution *pulse.Execution
}

func (*claimedInspectionError) Error() string { return "inspection failure" }
func (e *claimedInspectionError) inspect(method string) {
	if !e.armed {
		return
	}
	_ = e.execution.Err()
	_ = e.execution.Snapshot()
	if e.method == method {
		panic("late diagnostic panic")
	}
}
func (e *claimedInspectionError) As(any) bool   { e.inspect("As"); return false }
func (e *claimedInspectionError) Unwrap() error { e.inspect("Unwrap"); return nil }

func TestClaimedGuardedCheckpointInspection(t *testing.T) {
	for _, method := range []string{"As", "Unwrap"} {
		cause := &claimedInspectionError{method: method, armed: true}
		// Use a real pending Execution so hostile methods can synchronously reenter
		// public inspection, just as they can from a Start error path.
		g := testGraph(t, "pending")
		e, err := g.NewExecution(context.Background(), pulse.Inputs{})
		if err != nil {
			t.Fatal(err)
		}
		defer e.Cancel()
		cause.execution = e
		if !checkpointOrInspectionFailure(cause) {
			t.Fatal("diagnostic panic was treated as business failure")
		}
	}
	if checkpointOrInspectionFailure(nil) {
		t.Fatal("nil error is not a checkpoint failure")
	}
	if !checkpointOrInspectionFailure(&pulse.CheckpointError{Operation: "test", Err: context.Canceled}) {
		t.Fatal("checkpoint classification lost")
	}
}

func TestClaimedLateErrorInspectionPanic(t *testing.T) {
	for _, action := range []string{"Run", "Resume"} {
		for _, method := range []string{"As", "Unwrap"} {
			t.Run(action+"/"+method, func(t *testing.T) {
				s := &countedStore{Store: NewMemoryStore()}
				rt := testRuntime(t, s, NewCodecs())
				session := claimForTest(t, s, "run")
				defer session.Close()
				g := testGraph(t, "graph")
				cause := &claimedInspectionError{method: method}
				testAdd(t, g, pulse.NewNode("work", "", nil, nil, func(*pulse.RunCtx) error { return cause }))
				opts := testOptions("run")
				if action == "Resume" {
					opts.BeforeStart = func(*pulse.Execution) error { return context.Canceled }
					if _, outcome, err := rt.RunClaimed(context.Background(), session, g, pulse.Inputs{}, opts); err == nil || outcome != OutcomeInterrupted {
						t.Fatal("prepare active run")
					}
				}
				opts.BeforeStart = func(e *pulse.Execution) error { cause.execution = e; return nil }
				opts.ExecOptions = []pulse.ExecOption{pulse.WithExecObserver(pulse.ObserverFunc{GraphFinished: func(_ pulse.Site, _ pulse.NodeFinishReason, _ error) { cause.armed = true }})}
				var e *pulse.Execution
				var outcome Outcome
				var err error
				if action == "Run" {
					e, outcome, err = rt.RunClaimed(context.Background(), session, g, pulse.Inputs{}, opts)
				} else {
					e, outcome, err = rt.ResumeClaimed(context.Background(), session, g, pulse.Inputs{}, opts)
				}
				if e == nil || err != cause || outcome != OutcomeInfrastructure {
					t.Fatalf("late inspection must be contained conservatively: execution=%t outcome=%s", e != nil, outcome)
				}
				record, loadErr := session.Load(context.Background())
				if loadErr != nil {
					t.Fatal(loadErr)
				}
				final := false
				for _, a := range record.Attempts {
					final = final || a.Parent == record.RootScope && a.State == AttemptFailed
				}
				if !final {
					t.Fatal("test did not confirm business failure before hostile diagnostic")
				}
			})
		}
	}
}
