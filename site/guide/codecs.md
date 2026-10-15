# 值编码

本页适用于在检查点中保存业务结构体、命名类型或需要特殊编码的值。Durable 按真实 Go 类型查找 Codec：类型看起来相似，或底层类型相同，不代表它们使用同一个注册项。

## 1. 确定哪些类型需要注册

`durable.NewCodecs()` 提供以下内置 JSON 编码器，版本均为 `1`：

| Go 类型 | Codec ID |
| --- | --- |
| `bool`、`string` | `go/bool`、`go/string` |
| `int`、`int8`、`int16`、`int32`、`int64` | `go/` 加类型名 |
| `uint`、`uint8`、`uint16`、`uint32`、`uint64`、`uintptr` | `go/` 加类型名 |
| `float32`、`float64` | `go/float32`、`go/float64` |
| `[]byte` | `go/bytes` |

结构体、切片、map、指针和命名业务类型需要显式注册。例如 `type OrderID string` 是独立的真实类型，需要自己的 Codec。必须覆盖所有节点的 Requires/Provides 及图输入输出端口，包括没有在当前业务路径使用的声明。

适合标准 JSON 的具体类型可使用 `RegisterJSON[T]`；包含接口或特殊表示的类型应实现 `Codec`。

## 2. 运行一个业务结构体示例

需要 Go 1.25 或更高版本。创建新模块：

```sh
mkdir durable-codecs
cd durable-codecs
go mod init example.com/durable-codecs
go get github.com/Luo-root/pulse-durable@6c96a0afb793
```

将以下完整程序保存为 `main.go`。它注册 `Document`，运行一次，再从同一进程内的存储恢复并读取结构体出口。

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

执行：

```sh
go mod tidy
go run .
```

预期输出：

```text
enter summarize
result: doc-001 summary(Hello) ready
```

`enter summarize` 只出现一次，恢复取得的是检查点中解码后的 `Document`。这里使用 `MemoryStore` 演示类型往返；跨进程保存仍需要[持久 Store](storage.md)。

## 3. 选择稳定的 ID 与版本

Codec ID 标识一种业务类型的编码契约，应使用稳定名称，例如 `example/document`。Codec `Version` 标识字节格式及解码语义；格式或语义改变时需要升级它。

注册表不允许替换现有项：重复真实类型或重复 ID 返回 `ErrConflict`，即使新 Codec 使用另一个版本，也不能覆盖旧项。升级时创建新的注册表和 Runtime，并明确处理旧运行的兼容性。每次 `Run`/`Resume` 入口会复制注册表，后续注册不会改变已经开始的执行。

Codec ID/版本参与定义指纹。旧记录不能直接使用不同 Codec 契约恢复；不要为了让恢复通过而保留一个已经改变含义的旧版本号。业务函数的改变还需要独立升级业务 `Version`。

## 4. 理解 JSON 的限制

- `RegisterJSON` 递归拒绝接口声明，包括 `any`、`[]any`、`map[string]any` 及结构体中的接口字段。JSON 不保存动态 Go 类型，需要自定义 Codec 明确恢复。
- 所有字符串，包括嵌套字段和 map 键，必须是有效 UTF-8。任意字节用 `[]byte` 或自定义 Codec；不要用标准 JSON 静默替换非法字符串。
- 仍须满足标准库 JSON 的约束：未导出字段不保存，`json:"-"` 字段被忽略，对象环、NaN/Inf、复杂数及不受支持的 map 键等无法正常 JSON 编码。注册成功不意味着任意运行值都能编码。
- 指针、slice、map 的 JSON `null` 保留为 typed nil，并保持 `ready`。标量的 `null` 会被拒绝。`skipped` 单独表示没有业务值。
- 解码拒绝结构体未知字段、尾随 JSON 内容和非法原始 UTF-8。需要完整保留标准 JSON 无法表达的状态时，应使用自定义 Codec。

channel、函数、`unsafe.Pointer` 及包含它们的声明不属于持久值协议；接口的动态值也会检查这些限制。自定义 Codec 不会解除这条约束。

## 5. 实现自定义 Codec

下面是类型与 Codec 的局部声明，添加到自己的 Go 包中使用；需要导入 `fmt`、`reflect` 和 `durable`。它将命名字符串作为原始字节保存，不使用 JSON。

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

`Encode` 与 `Decode` 必须同步完成、并发安全，并且不修改业务值。相同值必须生成相同字节，因为输入指纹比较编码结果。`Decode` 必须返回可赋给 `Type()` 的值。

自定义编码可以是零字节。只要状态为 `ready` 且校验和有效，零字节并不代表缺失。声明为接口类型时，nil 接口可以是合法 `ready` 值；自定义解码也必须保留这种语义。

## 错误如何处理

注册阶段的缺失参数、不支持类型和重复注册属于配置或契约问题，应在运行前修正。运行中的编码、解码错误或 panic 会停止本次执行，并按可靠检查点错误处理；错误正文不会拼入业务值或编码字节，原错误原因可通过 `errors.Is/As` 检查。`panic(error)` 也保留其原因，非 error 的 panic 文本不会暴露。

不要将编码失败转成 skip 或继续发布零值。修正原因后，用原兼容契约恢复；实际契约已经改变时，需要按版本与迁移策略处理。详见[错误处理](../reference/errors.md)。

## 下一步

继续[存储](storage.md)，选择文件目录或实现自己的后端。值记录、校验和和状态的精确定义见[数据模型](../reference/data-model.md)。
