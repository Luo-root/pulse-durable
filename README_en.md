[English](README_en.md) | [中文](README.md)

<p align="center">
  <img src=".github/assets/logo.svg" alt="Pulse Durable" width="360">
</p>

<p align="center">Checkpoint storage and recovery for Pulse workflows</p>

<p align="center">
  <a href="https://go.dev/"><img src="https://img.shields.io/badge/Go-1.25%2B-2563eb" alt="Go 1.25+"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-MIT-2563eb" alt="MIT license"></a>
  <a href="https://luo-root.github.io/pulse-durable/en/"><img src="https://img.shields.io/badge/docs-中文%20%7C%20English-2563eb" alt="Bilingual documentation"></a>
</p>

Pulse Durable adds checkpoint storage and recovery to [Pulse](https://github.com/Luo-root/pulse) workflows. You continue to define graphs, nodes, and inputs and outputs with Pulse. Use Durable's `Run` to create a run and `Resume` to recover an existing run.

Outputs are published to downstream nodes after storage confirms the checkpoint for the complete node chain. Recovery reuses confirmed results in valid scopes and restarts unconfirmed nodes from the beginning of their functions. Durable does not save the Go call stack or automatically undo external effects such as database writes and HTTP requests.

## Install

Requires **Go 1.25 or later**. Durable is currently a development version with no formal release. Pin the public development revision for reproducible installation:

```sh
go get github.com/Luo-root/pulse-durable@53e6f7fa88e2
```

That revision's `go.mod` pins Pulse `v0.4.2-0.20261015103043-d21714d1af84`. See [Installation](site/en/guide/installation.md) for the complete steps to create a new module.

## Minimal example

This `main.go` declares an input and an outlet, runs an “add one” node, and reads the outlet from this execution. `MemoryStore` is used only for this minimal demonstration; use persistent storage for recovery across processes.

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

Run it in a module with the dependency installed:

```sh
go run .
```

Expected output:

```text
result: 42 ready
```

[Quick start](site/en/guide/quickstart.md) includes every step from an empty directory. Continue with [Recovery after process exit](site/en/guide/recovery.md) to observe a real exit and restart using file storage and two separate processes.

## Documentation map

Online documentation: [English](https://luo-root.github.io/pulse-durable/en/) · [中文](https://luo-root.github.io/pulse-durable/). The links below point to the source files in this repository.

| Task | Documentation |
| --- | --- |
| Understand the purpose and reading order | [Guide overview](site/en/guide/index.md) |
| Install the development version and inspect dependencies | [Installation](site/en/guide/installation.md) |
| Complete the first run and read an outlet | [Quick start](site/en/guide/quickstart.md) |
| Keep checkpoints and recover in a new process | [Recovery after process exit](site/en/guide/recovery.md) |
| Compose child graphs and understand Retry recovery | [Subgraphs and retries](site/en/guide/subgraphs.md) |
| Persist business structs and custom types | [Value codecs](site/en/guide/codecs.md) |
| Use file storage or implement a Store | [Storage](site/en/guide/storage.md) |
| Check API, record, and error rules | [Reference overview](site/en/reference/index.md), [Execution contract](site/en/reference/execution.md), [Data model](site/en/reference/data-model.md), [Error handling](site/en/reference/errors.md) |

## Current boundaries

- Supports ordinary values, managed `pulse.Stream[T]`, static child graphs, and persistent node Retry and message retry budgets. `Resume` continues remaining budgets; child results beneath a failed parent attempt are not reused.
- Messages are durably appended before delivery. `HandleMessage` confirms its input, explicit `SetMessageState` snapshot, and outputs in one Save. Recovery reclaims ordinary message values into complete-node staging; publication still waits for complete node confirmation without replaying acknowledged inputs. Active sources must declare `WithStreamSource(id, true)` and continue through `StreamWriter.Position()` and `SendAt`. Completed sources rebuild unacknowledged tails without rerunning the source body.
- `RunID` and business `Version` are required. Recovery needs the same definition, inputs, and encoding contract. Change the business version when changing functions, closures, or Aspect semantics.
- `filestore` targets one local host and depends on exclusive file locking and atomic replacement within a directory. It does not provide distributed leases, dispatch across machines, or a durability guarantee for power loss. `MemoryStore` validates only the in-process protocol.
- External effects from unconfirmed nodes may repeat. Applications need idempotency keys, transactions, deduplication, or an outbox. Durable does not promise exactly-once effects, deterministic replay, or durable waits. Channel, function, and `unsafe.Pointer` values are unsupported.
- New records use format `2`. Ordinary format `1` records remain recoverable and upgrade on their next Save. Old Runtime versions reject the new format. Codecs encode message elements, cursors, and explicit state, never stream endpoints; nested channel values remain unsupported.
- `RunOptions.BeforeStart` lets the host register `pulse.SubscribeStream` and start its reader before Start. Run/Resume still block in Wait and own the Session; readers must acknowledge messages or explicitly stop.
- Checkpoints contain encoded business outputs and are not encrypted containers. The host manages storage access, backups, and retention.

## Validate the repository

From the repository root, with local workspace overrides disabled, run:

```sh
go build ./...
go vet ./...
go test -race -count=1 ./...
```

Runnable examples are in [example_test.go](example_test.go) and [subgraph_example_test.go](subgraph_example_test.go). Actual process exit and restart tests are in [integration/recovery_test.go](integration/recovery_test.go).

## Contributing

Read the [contribution guide](CONTRIBUTING.md) before submitting changes and follow the [Code of Conduct](CODE_OF_CONDUCT.md) in community discussions. Report vulnerabilities privately according to the [security policy](SECURITY.md).

## License

This project is licensed under the [MIT License](LICENSE), Copyright © 2026 Luo-root. Font-derived logo assets retain their [third-party font license](.github/assets/FONT-LICENSE.txt).
