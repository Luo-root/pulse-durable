package distributed_test

import (
	"context"
	"fmt"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/distributed"
)

// Each process installs the same versioned assembly, not a serialized closure.
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
	// Send these bounded bytes over your transport. A fresh worker independently
	// assembles its graph, typed keys and codecs; no in-process pointers are sent.
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
	// No lease was acquired and no business execution or database validation occurred.
	// Output: document-1 true
}
