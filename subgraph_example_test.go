package durable_test

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/filestore"
)

func ExampleRuntime_Resume_subgraph() {
	// 文件记录放在当前目录的独占目录中；本例结束后清理，不使用系统临时目录。
	dir, err := os.MkdirTemp(".", ".pulse-durable-subgraph-example-")
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			panic(err)
		}
	}()

	// 计数只观察业务体是否进入，不参与恢复；下载、提取、摘要用字符串变换模拟。
	var calls [3]int
	buildGraph := func(interrupt context.CancelFunc) (*pulse.Graph, pulse.Inputs, pulse.Key[string]) {
		document := pulse.NewKey[string]("document")
		result := pulse.NewKey[string]("result")
		source := pulse.NewKey[string]("source")
		downloaded := pulse.NewKey[string]("downloaded")
		extracted := pulse.NewKey[string]("extracted")
		summary := pulse.NewKey[string]("summary")

		child, err := pulse.New("document-processing", "下载、提取、摘要")
		if err != nil {
			panic(err)
		}
		if err := child.DeclareInput(pulse.NewPort(source)); err != nil {
			panic(err)
		}
		if err := child.DeclareOutput(pulse.NewPort(summary)); err != nil {
			panic(err)
		}
		if err := child.Add(pulse.NewNode("C1", "下载", pulse.Requires(source), pulse.Provides(downloaded), func(rc *pulse.RunCtx) error {
			calls[0]++
			name, err := pulse.Get(rc, source)
			if err != nil {
				return err
			}
			return pulse.Set(rc, downloaded, "download("+name+")")
		})); err != nil {
			panic(err)
		}
		if err := child.Add(pulse.NewNode("C2", "提取", pulse.Requires(downloaded), pulse.Provides(extracted), func(rc *pulse.RunCtx) error {
			calls[1]++
			data, err := pulse.Get(rc, downloaded)
			if err != nil {
				return err
			}
			return pulse.Set(rc, extracted, "text("+data+")")
		})); err != nil {
			panic(err)
		}
		if err := child.Add(pulse.NewNode("C3", "摘要", pulse.Requires(extracted), pulse.Provides(summary), func(rc *pulse.RunCtx) error {
			calls[2]++
			text, err := pulse.Get(rc, extracted)
			if err != nil {
				return err
			}
			if interrupt != nil {
				// C3 已进体并读到 C2；检查点确认后才发布，故 C1/C2 已持久保存。
				interrupt() // 确定性模拟宿主取消，不依赖 sleep 或 goroutine 调度。
				return rc.Context().Err()
			}
			return pulse.Set(rc, summary, "summary("+text+")")
		})); err != nil {
			panic(err)
		}

		parent, err := pulse.New("document-workflow", "父图挂载文档处理子图")
		if err != nil {
			panic(err)
		}
		if err := parent.DeclareInput(pulse.NewPort(document)); err != nil {
			panic(err)
		}
		if err := parent.DeclareOutput(pulse.NewPort(result)); err != nil {
			panic(err)
		}
		if err := pulse.Sub(parent, "process", "处理文档", []pulse.SubBind{
			pulse.In(document, source),
			pulse.Out(summary, result),
		}, child, pulse.Retry(3, 0)); err != nil {
			panic(err)
		}
		var in pulse.Inputs
		if err := pulse.Seed(&in, document, "document.pdf"); err != nil {
			panic(err)
		}
		return parent, in, result
	}

	store, err := filestore.New(dir)
	if err != nil {
		panic(err)
	}
	runtime, err := durable.New(store, durable.NewCodecs())
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, in, _ := buildGraph(cancel)
	options := durable.RunOptions{RunID: "document-123", Version: "document-workflow/1"}
	_, err = runtime.Run(ctx, g, in, options)
	if !errors.Is(err, context.Canceled) {
		panic(fmt.Sprintf("Run: expected host cancellation, got %v", err))
	}
	fmt.Println("Run canceled:", errors.Is(err, context.Canceled))
	fmt.Printf("Run body calls: C1=%d C2=%d C3=%d\n", calls[0], calls[1], calls[2])
	before := calls

	// 同进程重建对象；真正退出/重启的证据见 integration/recovery_test.go。
	store, err = filestore.New(dir)
	if err != nil {
		panic(err)
	}
	runtime, err = durable.New(store, durable.NewCodecs())
	if err != nil {
		panic(err)
	}
	g, in, result := buildGraph(nil) // 等价定义，重新提供相同外部输入。
	// 宿主取消保留父 Retry 的 active 尝试；Resume 认回其作用域，复用 C1/C2。
	e, err := runtime.Resume(context.Background(), g, in, options)
	if err != nil {
		panic(err)
	}
	value, state, err := pulse.ReadOutput(e, result) // 读取父图声明出口。
	if err != nil {
		panic(err)
	}
	fmt.Printf("Resume body calls: C1=%d C2=%d C3=%d\n", calls[0]-before[0], calls[1]-before[1], calls[2]-before[2])
	fmt.Println("summary:", value, state)
	// Output:
	// Run canceled: true
	// Run body calls: C1=1 C2=1 C3=1
	// Resume body calls: C1=0 C2=0 C3=1
	// summary: summary(text(download(document.pdf))) ready
}
