# Recovery after process exit

This tutorial uses two real processes. The first completes an upstream node and exits after entering a downstream node. The second rebuilds the same graph, restores file checkpoints, and produces `42 ready`. Use it to validate local restart recovery and understand which business functions run again.

## 1. Prepare a separate module

You need Go 1.26 or later and a writable local directory. Run both processes from the same working directory so that `./checkpoints` identifies the same storage.

```sh
mkdir durable-recovery
cd durable-recovery
go mod init example.com/durable-recovery
go get github.com/Luo-root/pulse-durable@v0.0.0-20261023040750-1b397e91c878
```

Durable has no formal release yet; the command pins a public development revision. Use a new module directory where this example has not run before. If checkpoints already exist, read “Repeat runs and new work” below first.

## 2. Create main.go

Save the following complete program as `main.go`. Both modes use the same graph ID, node IDs, ports, inputs, RunID, Version, and codecs. `mode` only controls the exit fault injected by this demonstration.

```go
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/filestore"
)

func main() {
	mode := flag.String("mode", "run", "run or resume")
	flag.Parse()
	if err := execute(*mode); err != nil {
		log.Fatal(err)
	}
}

func execute(mode string) error {
	if mode != "run" && mode != "resume" {
		return fmt.Errorf("invalid mode %q: use run or resume", mode)
	}
	store, err := filestore.New("./checkpoints")
	if err != nil {
		return err
	}
	runtime, err := durable.New(store, durable.NewCodecs())
	if err != nil {
		return err
	}
	input := pulse.NewKey[int]("input")
	prepared := pulse.NewKey[int]("prepared")
	output := pulse.NewKey[int]("output")
	graph, err := pulse.New("process-recovery", "Recover after process exit")
	if err != nil {
		return err
	}
	if err := graph.DeclareInput(pulse.NewPort(input)); err != nil {
		return err
	}
	if err := graph.DeclareOutput(pulse.NewPort(output)); err != nil {
		return err
	}
	if err := graph.Add(pulse.NewNode("upstream", "Prepare input",
		pulse.Requires(input), pulse.Provides(prepared),
		func(rc *pulse.RunCtx) error {
			value, err := pulse.Get(rc, input)
			if err != nil {
				return err
			}
			fmt.Println("enter upstream:", value)
			return pulse.Set(rc, prepared, value)
		})); err != nil {
		return err
	}
	if err := graph.Add(pulse.NewNode("downstream", "Add one",
		pulse.Requires(prepared), pulse.Provides(output),
		func(rc *pulse.RunCtx) error {
			value, err := pulse.Get(rc, prepared)
			if err != nil {
				return err
			}
			fmt.Println("enter downstream:", value)
			if mode == "run" {
				fmt.Println("simulate process exit: 75")
				os.Exit(75)
			}
			return pulse.Set(rc, output, value+1)
		})); err != nil {
		return err
	}
	var inputs pulse.Inputs
	if err := pulse.Seed(&inputs, input, 41); err != nil {
		return err
	}
	options := durable.RunOptions{RunID: "recovery-demo-001", Version: "recovery/1"}
	var execution *pulse.Execution
	if mode == "run" {
		execution, err = runtime.Run(context.Background(), graph, inputs, options)
	} else {
		execution, err = runtime.Resume(context.Background(), graph, inputs, options)
	}
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

The downstream node calls `os.Exit(75)` only after reading `prepared`. Durable publishes that value after confirming the complete upstream checkpoint. The exit therefore occurs after the upstream result is saved and before the downstream node finishes. No delay or scheduling coincidence is needed.

## 3. Start the first process

From the module directory, run:

```sh
go mod tidy
go run . -mode=run
```

Expected application output:

```text
enter upstream: 41
enter downstream: 41
simulate process exit: 75
```

This command intentionally fails. The program itself calls `os.Exit(75)`. When launched through `go run`, the Go tool usually also prints `exit status 75`, but the shell may receive `1` as the Go tool's exit status. Do not treat the wrapper command's status as the application's `75`.

`os.Exit` does not run Go deferred functions. The operating system releases the file lock when the process exits, while saved checkpoints remain in `./checkpoints`. Do not delete checkpoints or `.lock` files.

## 4. Start the second process

Stay in the same module directory and run:

```sh
go run . -mode=resume
```

Expected output:

```text
enter downstream: 41
result: 42 ready
```

There is no `enter upstream` in the second process. Across both transcripts, the upstream body ran once and the downstream body ran twice. The downstream function restarts at its beginning; it does not continue at the line after `os.Exit`.

To inspect the application's exit code directly, build and launch the executable. In PowerShell:

```powershell
go build -o durable-recovery.exe .
.\durable-recovery.exe -mode=run
$LASTEXITCODE
.\durable-recovery.exe -mode=resume
```

In a POSIX shell:

```sh
go build -o durable-recovery .
./durable-recovery -mode=run
echo $?
./durable-recovery -mode=resume
```

The first executable invocation exits with `75`. These commands are an alternative to steps 3 and 4. Use a new directory without a record for this RunID, or assign a new RunID to a new demonstration first.

## 5. Repeat runs and new work

Once `recovery-demo-001` has a record in this Store, another `-mode=run` returns `ErrConflict`. `Run` creates a record; `Resume` opens an existing one. The methods do not switch between these behaviors automatically.

Another `-mode=resume` reads the completed results and prints `result: 42 ready` without entering either node. To start different business work or demonstrate the exit again, assign it a new RunID in the code, then use `run` followed by `resume`. This tutorial never deletes or overwrites old checkpoints automatically.

For the same business run, keep input `41`, the business version, the graph definition, and the encoding contract unchanged. Changes to the graph, nodes, ports, Codec IDs/versions, or inputs may cause recovery conflicts. Changes to functions, closures, or Aspect semantics require an explicit business `Version` change, with old records handled as new work or by an application migration policy.

## Handle errors

| Symptom | Cause and action |
| --- | --- |
| `ErrNotFound` | No record exists. Confirm that `run` was executed first and that the working directory and RunID match |
| `ErrBusy` | Another session owns the same RunID. Check whether the original process is still alive; do not delete lock files |
| `ErrConflict` | A record exists, or recovery identity, version, definition, or inputs differ. Check the method and original conditions |
| `CheckpointError` | Saving, loading, encoding, or the recovery protocol failed. Resolve the underlying cause, then recover with the same conditions and reload the authoritative record |

A failed save may already have committed. Do not manually roll the record back or blindly repeat the old save operation. A new `Resume` reloads the actual record. See [Error handling](../reference/errors.md) for classification details.

## Rules for application integration

The printed lines only show how often functions are entered. Real database writes, network requests, and file operations may occur before checkpoint confirmation and may repeat. Use business idempotency keys, transactions, deduplication, or an outbox. Durable does not promise exactly-once effects.

`filestore` targets one local host and requires file locks and atomic replacement within a directory. It does not guarantee durability during power loss or distributed run management. Recovery executes the current code of unconfirmed functions; it is not deterministic event replay.

## Next steps

For workflows with multiple child stages, continue with [Subgraphs and retries](subgraphs.md) to distinguish active parent attempts from failed ones. For business structs or another backend, read [Value codecs](codecs.md) and [Storage](storage.md).
