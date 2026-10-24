package durable_test

import (
	"context"
	"fmt"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)

// A coordinator's real session implements RunID plus fenced Load/Save. This
// example only adds the identity binding to a local MemoryStore session.
type boundSession struct {
	durable.Session
	id string
}

func (s boundSession) RunID() string { return s.id }

func ExampleRuntime_RunClaimed() {
	ctx := context.Background()
	store := durable.NewMemoryStore()
	runtime, _ := durable.New(store, durable.NewCodecs())
	session, _ := store.Acquire(ctx, "invoice-42")
	defer session.Close() // after the caller's terminal decision, not inside Runtime
	graph, _ := pulse.New("invoice", "")
	_, outcome, err := runtime.RunClaimed(ctx, boundSession{session, "invoice-42"}, graph, pulse.Inputs{}, durable.RunOptions{RunID: "invoice-42", Version: "v1"})
	if err != nil {
		panic(err)
	}
	// A distributed caller now performs its fenced Finish while still owning
	// session. Runtime does not declare or persist a coordinator task status.
	fmt.Println(outcome)
	// Output: completed
}
