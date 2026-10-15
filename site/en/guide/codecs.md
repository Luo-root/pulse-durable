# Value codecs

Use this page to persist business structs, named types, or values needing a special encoding. Durable resolves codecs by the actual Go type. Similar-looking types, or types with the same underlying representation, do not necessarily share a registration.

## 1. Identify types that need registration

`durable.NewCodecs()` supplies these built-in JSON codecs, all at version `1`:

| Go type | Codec ID |
| --- | --- |
| `bool`, `string` | `go/bool`, `go/string` |
| `int`, `int8`, `int16`, `int32`, `int64` | `go/` followed by the type name |
| `uint`, `uint8`, `uint16`, `uint32`, `uint64`, `uintptr` | `go/` followed by the type name |
| `float32`, `float64` | `go/float32`, `go/float64` |
| `[]byte` | `go/bytes` |

Structs, slices, maps, pointers, and named business types need explicit registration. For example, `type OrderID string` is a distinct actual type requiring its own Codec. Cover all node Requires/Provides and graph input/output ports, including declarations unused on the current business path.

Use `RegisterJSON[T]` for concrete types suitable for standard JSON. Implement `Codec` for types containing interfaces or requiring a special representation.

## 2. Run a business struct example

Requires Go 1.25 or later. Create a new module:

```sh
mkdir durable-codecs
cd durable-codecs
go mod init example.com/durable-codecs
go get github.com/Luo-root/pulse-durable@443f0cfedbfb
```

Save this complete program as `main.go`. It registers `Document`, runs once, then restores from the same in-process Store and reads a struct outlet.

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)

type Document struct {
	ID   string `json:"id"`
	Text string `json:"text"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	codecs := durable.NewCodecs()
	if err := durable.RegisterJSON[Document](codecs, "example/document", "1"); err != nil {
		return err
	}
	runtime, err := durable.New(durable.NewMemoryStore(), codecs)
	if err != nil {
		return err
	}
	input := pulse.NewKey[Document]("document")
	output := pulse.NewKey[Document]("summary")
	graph, err := pulse.New("document-codecs", "Persist a document")
	if err != nil {
		return err
	}
	if err := graph.DeclareInput(pulse.NewPort(input)); err != nil {
		return err
	}
	if err := graph.DeclareOutput(pulse.NewPort(output)); err != nil {
		return err
	}
	if err := graph.Add(pulse.NewNode("summarize", "Summarize",
		pulse.Requires(input), pulse.Provides(output),
		func(rc *pulse.RunCtx) error {
			value, err := pulse.Get(rc, input)
			if err != nil {
				return err
			}
			fmt.Println("enter summarize")
			return pulse.Set(rc, output, Document{
				ID: value.ID, Text: "summary(" + value.Text + ")",
			})
		})); err != nil {
		return err
	}
	var inputs pulse.Inputs
	if err := pulse.Seed(&inputs, input, Document{ID: "doc-001", Text: "Hello"}); err != nil {
		return err
	}
	options := durable.RunOptions{RunID: "document-codecs-001", Version: "document/1"}
	if _, err := runtime.Run(context.Background(), graph, inputs, options); err != nil {
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
	fmt.Println("result:", value.ID, value.Text, state)
	return nil
}
```

Run:

```sh
go mod tidy
go run .
```

Expected output:

```text
enter summarize
result: doc-001 summary(Hello) ready
```

`enter summarize` appears only once. Recovery reads a `Document` decoded from the checkpoint. `MemoryStore` demonstrates the type round trip; persistence across processes still needs a [persistent Store](storage.md).

## 3. Choose stable IDs and versions

The Codec ID identifies a business type's encoding contract. Choose a stable name such as `example/document`. Codec `Version` identifies the byte format and decoding semantics; change it when either changes.

Registrations cannot replace existing entries. A duplicate actual type or duplicate ID returns `ErrConflict`, even if the new Codec uses another version. Create a new registry and Runtime for an upgrade, and explicitly handle old run compatibility. Each `Run`/`Resume` snapshots the registry at entry, so later registrations do not change an execution already in progress.

Codec IDs and versions participate in the definition fingerprint. You cannot directly restore an old record with a different codec contract. Do not keep a version number whose meaning has changed just to make recovery pass. Business function changes also require a separate business `Version` change.

## 4. Understand JSON limitations

- `RegisterJSON` recursively rejects interface declarations, including `any`, `[]any`, `map[string]any`, and interface fields in structs. JSON does not retain dynamic Go types; use a custom Codec to restore them explicitly.
- Every string, including nested fields and map keys, must be valid UTF-8. Use `[]byte` or a custom Codec for arbitrary bytes instead of silently replacing invalid strings with standard JSON.
- Standard library JSON rules still apply: unexported fields are not saved, `json:"-"` fields are ignored, and object cycles, NaN/Inf, complex numbers, or unsupported map keys cannot be encoded normally. Registration does not guarantee that every runtime value can be encoded.
- JSON `null` for pointers, slices, and maps preserves typed nil with state `ready`. Scalar `null` is rejected. `skipped` separately represents the absence of a business value.
- Decoding rejects unknown struct fields, trailing JSON content, and invalid raw UTF-8. Use a custom Codec when standard JSON cannot preserve the state you need.

Channels, functions, `unsafe.Pointer`, and declarations containing them are outside the persistent value protocol. Interface dynamic values are checked too. A custom Codec does not remove this restriction.

## 5. Implement a custom Codec

The following local type and Codec declarations can be added to your own Go package. They need imports for `fmt`, `reflect`, and `durable`. The named string is stored as raw bytes rather than JSON.

```go
type RawText string

type rawTextCodec struct{}

func (rawTextCodec) Type() reflect.Type { return reflect.TypeFor[RawText]() }
func (rawTextCodec) ID() string         { return "example/raw-text" }
func (rawTextCodec) Version() string    { return "1" }
func (rawTextCodec) Encode(value any) ([]byte, error) {
	text, ok := value.(RawText)
	if !ok {
		return nil, fmt.Errorf("unexpected value type")
	}
	return []byte(text), nil
}
func (rawTextCodec) Decode(data []byte) (any, error) {
	return RawText(string(data)), nil
}

func newRawCodecs() (*durable.Codecs, error) {
	codecs := durable.NewCodecs()
	if err := codecs.Register(rawTextCodec{}); err != nil {
		return nil, err
	}
	return codecs, nil
}
```

`Encode` and `Decode` must complete synchronously, be concurrency safe, and not mutate business values. Equal values must produce equal bytes because input fingerprints compare encodings. `Decode` must return a value assignable to `Type()`.

Custom encodings may be zero bytes long. With state `ready` and a valid checksum, zero bytes do not mean a missing value. For a declared interface type, a nil interface can be a valid `ready` value; custom decoding must preserve that meaning too.

## Handle errors

Missing registration parameters, unsupported types, and duplicate registrations are configuration or contract problems to fix before execution. Runtime encode/decode errors and panics stop the execution as reliable checkpoint failures. Public error text does not include business values or encoded bytes; original error causes remain available through `errors.Is/As`. `panic(error)` also preserves its cause; non-error panic text is not exposed.

Do not turn encoding failure into skip or publish a zero value. Resolve the cause and recover using the original compatible contract. If the contract actually changed, follow your version and migration policy. See [Error handling](../reference/errors.md).

## Next steps

Continue with [Storage](storage.md) to choose a directory or implement a backend. See the [Data model](../reference/data-model.md) for exact value record, checksum, and state definitions.
