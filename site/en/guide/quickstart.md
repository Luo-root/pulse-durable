# Quick start

Start with a new Go module, complete one Durable `Run`, and read a declared outlet. This one-node example uses `MemoryStore` to demonstrate the minimal calling sequence. It does not retain checkpoints after process exit.

## 1. Prepare a module

You need Go 1.25 or later and access to Go module downloads. Run:

```sh
mkdir durable-quickstart
cd durable-quickstart
go mod init example.com/durable-quickstart
go get github.com/Luo-root/pulse-durable@9fda63212a51fc04e44e771cb09e06a9a06b1c7b
```

If you already created a module in [Installation](installation.md), continue in that module directory. Durable has no formal release yet; this tutorial pins a public development revision.

## 2. Create main.go

Save this complete program as `main.go`:

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	runtime, err := durable.New(durable.NewMemoryStore(), durable.NewCodecs())
	if err != nil {
		return err
	}
	input := pulse.NewKey[int]("input")
	output := pulse.NewKey[int]("output")
	graph, err := pulse.New("quickstart", "Add one")
	if err != nil {
		return err
	}
	if err := graph.DeclareInput(pulse.NewPort(input)); err != nil {
		return err
	}
	if err := graph.DeclareOutput(pulse.NewPort(output)); err != nil {
		return err
	}
	if err := graph.Add(pulse.NewNode("add", "Add one",
		pulse.Requires(input), pulse.Provides(output),
		func(rc *pulse.RunCtx) error {
			value, err := pulse.Get(rc, input)
			if err != nil {
				return err
			}
			return pulse.Set(rc, output, value+1)
		})); err != nil {
		return err
	}
	var inputs pulse.Inputs
	if err := pulse.Seed(&inputs, input, 41); err != nil {
		return err
	}
	execution, err := runtime.Run(context.Background(), graph, inputs,
		durable.RunOptions{RunID: "quickstart-001", Version: "increment/1"})
	if err != nil {
		return err
	}
	value, state, err := pulse.ReadOutput(execution, output)
	if err != nil {
		return err
	}
	if state != pulse.SlotStateReady {
		return fmt.Errorf("unexpected output state: %s", state)
	}
	fmt.Println("result:", value, state)
	return nil
}
```

The program performs these steps:

1. Create a `Runtime` with a Store and a codec registry. `NewCodecs` already includes the `int` used here.
2. Declare the graph input `input` and outlet `output`. The node declares what it reads and produces using `Requires` and `Provides`.
3. Supply external input `41` with `Seed`. The node reads it with `Get` and supplies output `42` with `Set`.
4. Create a run with a nonempty `RunID` and business `Version`. `Run` blocks until this execution finishes.
5. Check the run error, then read this execution's declared outlet with `ReadOutput`.

## 3. Run and check the result

From the module directory containing `main.go`, run:

```sh
go mod tidy
go run .
```

Expected output:

```text
result: 42 ready
```

`ready` means a business value exists. A zero value, empty string, or valid nil value can also be `ready`; do not infer presence from whether the value is zero. A normal skip uses state `skipped`. Check `err` and `state` separately.

This example requires `ready` because the graph is expected to produce an incremented result. In a general graph, an outlet may legitimately be `skipped`, or `pending` for an optional pass-through outlet without a supplied input. Those states do not by themselves mean the run failed. See the [Execution contract](../reference/execution.md) for details.

## 4. Understand the boundary

Calling `Set` inside a function does not mean the complete node has been durably confirmed. Durable saves the checkpoint after the full node chain, including Aspects, finishes. It publishes results downstream after that save is confirmed.

Every launch of this example creates a new `MemoryStore`, so running `go run .` again executes the node again. Within the same Store, you cannot call `Run` again for an existing RunID. Open an existing record with `Resume`, and allocate a new RunID for new business work.

## When something fails

- `no registered codec`: check the types of all node keys and declared ports. Business structs and named types need [explicit codec registration](codecs.md).
- Outlet undeclared or type mismatch: check the keys used in `DeclareOutput` and `ReadOutput`. An internal key cannot be read as a graph outlet.
- The run returns an error: handle that error first. `ReadOutput` on a failed execution returns the failure cause; do not continue business calculations using a zero value.
- RunID conflict: decide whether you are creating new business work or recovering existing work, then choose a new RunID or use `Resume`.

## Next steps

Continue with [Recovery after process exit](recovery.md): switch to `filestore`, actually exit the first process, and complete the run in a second process. See the [Execution contract](../reference/execution.md) for the complete identity, version, and event constraints.
