# Subgraphs and retries

Use this page when composing business stages into a static child graph and reusing its internal results after interruption. Complete [Quick start](quickstart.md) and [Recovery after process exit](recovery.md) before running this example.

## Two kinds of child recovery

Think of a parent mount attempt as a work order. Child results belong to that specific order:

- **The parent attempt is still active**: for example, after host cancellation, process exit, or a reliable checkpoint failure. Recovery reclaims the same attempt and child scope, reusing confirmed nodes inside it.
- **The parent attempt has failed**: for example, a real business error in the child failed this Retry attempt. Results in the old scope remain in the record but cannot be used by the next parent attempt. If retry budget remains, the next attempt gets a new scope and runs the child again.

Suppose a child has “prepare” and “finish” stages. Host cancellation after entering “finish” allows recovery to reuse “prepare.” If a real business error has already failed that parent attempt, the next parent attempt cannot borrow “prepare” from the previous work order.

Managed-stream pass-through outlets have an independent completion relationship. An empty child can pass its inlet stream directly to its outlet and save the Sub result while the actual source continues producing. That result stores a reference and does not publish the writer's EOF. After process exit, the confirmed Sub does not rerun; a resumable source continues from its reliable Position, sharing one endpoint, generation, pending tail, and acknowledgement ledger with the restored outlet. Recovery rejects active sources that have not declared resumability. Completed sources only rebuild their unacknowledged tail.

Loading still validates retained history from failed attempts, checking the source, generation, and known state. It neither requires those invalidated sources to remain resumable nor treats their later real failure as record corruption. Resumability and valid-ancestor requirements apply when preparing actual recovery for a valid scope. Nodes from failed old scopes do not run again.

## 1. Create an example module

Requires Go 1.25 or later and a writable local directory.

```sh
mkdir durable-subgraphs
cd durable-subgraphs
go mod init example.com/durable-subgraphs
go get github.com/Luo-root/pulse-durable@b7a8c6746439
```

The example uses a fixed RunID and `./checkpoints`. Start in a new directory without a record for this example. Assign a new RunID for a new demonstration rather than automatically deleting old records.

## 2. Create main.go

This complete program cancels the first execution, then rebuilds the file Store, Runtime, and an equivalent graph in the same process. It demonstrates child result reuse beneath an active parent attempt. See the [recovery tutorial](recovery.md) for actual process exit.

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/filestore"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func newRuntime() (*durable.Runtime, error) {
	store, err := filestore.New("./checkpoints")
	if err != nil {
		return nil, err
	}
	return durable.New(store, durable.NewCodecs())
}

func buildGraph(interrupt context.CancelFunc, calls *[2]atomic.Int32) (
	graph *pulse.Graph, inputs pulse.Inputs, output pulse.Key[int], err error,
) {
	input := pulse.NewKey[int]("input")
	output = pulse.NewKey[int]("output")
	source := pulse.NewKey[int]("source")
	prepared := pulse.NewKey[int]("prepared")
	result := pulse.NewKey[int]("result")
	child, err := pulse.New("child", "Prepare and finish")
	if err != nil {
		return
	}
	if err = child.DeclareInput(pulse.NewPort(source)); err != nil {
		return
	}
	if err = child.DeclareOutput(pulse.NewPort(result)); err != nil {
		return
	}
	if err = child.Add(pulse.NewNode("prepare", "Prepare",
		pulse.Requires(source), pulse.Provides(prepared),
		func(rc *pulse.RunCtx) error {
			calls[0].Add(1)
			value, err := pulse.Get(rc, source)
			if err != nil {
				return err
			}
			return pulse.Set(rc, prepared, value)
		})); err != nil {
		return
	}
	if err = child.Add(pulse.NewNode("finish", "Finish",
		pulse.Requires(prepared), pulse.Provides(result),
		func(rc *pulse.RunCtx) error {
			calls[1].Add(1)
			value, err := pulse.Get(rc, prepared)
			if err != nil {
				return err
			}
			if interrupt != nil {
				interrupt()
				return rc.Context().Err()
			}
			return pulse.Set(rc, result, value+1)
		})); err != nil {
		return
	}
	graph, err = pulse.New("parent", "Mount a child graph")
	if err != nil {
		return
	}
	if err = graph.DeclareInput(pulse.NewPort(input)); err != nil {
		return
	}
	if err = graph.DeclareOutput(pulse.NewPort(output)); err != nil {
		return
	}
	if err = pulse.Sub(graph, "process", "Process input", []pulse.SubBind{
		pulse.In(input, source),
		pulse.Out(result, output),
	}, child, pulse.Retry(3, 0)); err != nil {
		return
	}
	err = pulse.Seed(&inputs, input, 41)
	return
}

func run() error {
	var calls [2]atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	graph, inputs, _, err := buildGraph(cancel, &calls)
	if err != nil {
		return err
	}
	runtime, err := newRuntime()
	if err != nil {
		return err
	}
	options := durable.RunOptions{RunID: "subgraph-demo-001", Version: "subgraph/1"}
	_, err = runtime.Run(ctx, graph, inputs, options)
	var checkpoint *pulse.CheckpointError
	if errors.As(err, &checkpoint) {
		return err
	}
	if !errors.Is(err, context.Canceled) {
		return fmt.Errorf("expected host cancellation, got %v", err)
	}
	before := [2]int32{calls[0].Load(), calls[1].Load()}
	fmt.Printf("Run body calls: prepare=%d finish=%d\n", before[0], before[1])
	runtime, err = newRuntime()
	if err != nil {
		return err
	}
	graph, inputs, output, err := buildGraph(nil, &calls)
	if err != nil {
		return err
	}
	execution, err := runtime.Resume(context.Background(), graph, inputs, options)
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
	fmt.Printf("Resume body calls: prepare=%d finish=%d\n",
		calls[0].Load()-before[0], calls[1].Load()-before[1])
	fmt.Println("result:", value, state)
	return nil
}
```

`pulse.In` binds the parent input to the child input. `pulse.Out` binds the child outlet to the parent outlet. `Retry(3, 0)` is installed on the parent mount node and allows at most three attempts, not an initial attempt plus three retries.

## 3. Run and check the result

```sh
go mod tidy
go run .
```

Expected output:

```text
Run body calls: prepare=1 finish=1
Resume body calls: prepare=0 finish=1
result: 42 ready
```

The first execution cancels the host context after `finish` reads upstream data. `prepare` is confirmed, `finish` is unconfirmed, and the parent Retry attempt remains active. Recovery reclaims the same scope and only re-enters `finish`. Counters are observations, not recovery state. The file record is the recovery source.

## Resume does not increase the Retry budget

| Persistent state | Recovery behavior |
| --- | --- |
| Current parent Retry attempt is `active` | Continue the same attempt and reuse valid child results |
| Current parent Retry attempt is `failed`, with budget remaining | Create the next attempt and a new child scope; do not reuse old child results |
| Retry is exhausted and the complete node root attempt has failed | This RunID's node budget is finished; Resume does not allocate more attempts |
| The child finished but the complete parent chain is unconfirmed | Reuse child results and finish the parent chain while its attempt remains active |
| The complete parent chain is confirmed | Restore the parent result without entering its chain or child graph again |

Host cancellation and reliable checkpoint failures preserve active attempts. A node that cancels its own context and then returns a real business error does not automatically count as a host interruption. Real failures still consume budget. Recovery is not a way to bypass the Retry limit.

Change the business `Version` when changing custom Aspect semantics, ordering, or Retry parameters. Durable persists the built-in Retry boundaries; it does not automatically turn an arbitrary custom loop into a persistent retry.

## Handle errors

- Child inputs or mount structure changed: recovery checks the definition and actual bound inputs. Recover with the original conditions; allocate a new RunID for new business conditions.
- Real business failure: Retry consumes the remaining budget. External effects still need idempotency or transactions. Child results beneath a failed parent attempt are not reused.
- `CheckpointError`: stop this execution, resolve the storage, codec, or callback issue, then recover. A parent Retry, normal skip, or error-swallowing Aspect cannot turn a reliable failure into success.
- Retry budget exhausted: resolve the business cause and create new work according to application policy. Repeated `Resume` calls do not add attempts.

## Next steps

Continue with [Value codecs](codecs.md) to persist business types. See the [Data model](../reference/data-model.md) for exact scope and attempt associations, and [Error handling](../reference/errors.md) for classification and reliable failure rules.
