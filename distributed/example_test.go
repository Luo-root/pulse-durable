package distributed_test

import (
	"context"
	"fmt"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/distributed"
)

// 每个进程安装相同版本的 assembly，而不是序列化的 closure。
func portableAssembly() (distributed.Assembly, error) {
	g, err := pulse.New("portable-example", "finite input")
	if err != nil {
		return distributed.Assembly{}, err
	}
	key := pulse.NewKey[string]("document")
	if err = g.DeclareInput(pulse.NewPort(key)); err != nil {
		return distributed.Assembly{}, err
	}
	if err = g.DeclareOutput(pulse.NewPort(key)); err != nil {
		return distributed.Assembly{}, err
	}
	keys := pulse.NewRegistry()
	if err = pulse.RegisterKey(keys, key); err != nil {
		return distributed.Assembly{}, err
	}
	return distributed.Assembly{Graph: g, Codecs: durable.NewCodecs(), Keys: keys}, nil
}

func ExampleRegistry_Resolve() {
	ctx := context.Background()
	producer, err := portableAssembly()
	if err != nil {
		panic(err)
	}
	var in pulse.Inputs
	if err = pulse.Seed(&in, pulse.NewKey[string]("document"), "finite document"); err != nil {
		panic(err)
	}
	task, err := distributed.NewTask(ctx, distributed.Identity{Namespace: "documents", RunID: "document-1"}, "summarize", "business/1", producer.Graph, in, producer.Codecs)
	if err != nil {
		panic(err)
	}
	// 通过传输层发送这些有界字节。新的 worker 独立组装自己的 graph、带类型的 key 和 codec；
	// 不发送进程内指针。
	wire := task.Bytes()
	worker := &distributed.Registry{}
	if _, err = worker.Register("summarize", "business/1", portableAssembly); err != nil {
		panic(err)
	}
	received, err := distributed.ParseTask(wire)
	if err != nil {
		panic(err)
	}
	assembly, decoded, err := worker.Resolve(ctx, received)
	if err != nil {
		panic(err)
	}
	prepared, err := durable.Prepare(ctx, assembly.Graph, decoded, assembly.Codecs)
	if err != nil {
		panic(err)
	}
	fmt.Println(received.Identity().RunID, prepared.Inputs == received.InputsFingerprint())
	// 未获取 lease，也未发生业务执行或数据库验证。
	// Output: document-1 true
}
