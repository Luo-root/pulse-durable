package durable_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse/observe"
)

// #region runIDSink
// runIDSink 属于宿主，一次装配给该 RunID 的全部记录补充稳定身份。
// Attrs 有引用语义：先复制四种标量，再补充字段，不能修改收到的 Record。
type runIDSink struct {
	runID string
	next  observe.Sink
}

func (s runIDSink) Write(record observe.Record) {
	var attrs observe.Attrs
	record.Attrs.Range(func(key string, value any) {
		switch value := value.(type) {
		case string:
			observe.Set(&attrs, key, value)
		case int64:
			observe.Set(&attrs, key, value)
		case float64:
			observe.Set(&attrs, key, value)
		case bool:
			observe.Set(&attrs, key, value)
		}
	})
	observe.Set(&attrs, durable.AttrRunID, s.runID)
	record.Attrs = attrs
	s.next.Write(record)
}

// #endregion runIDSink

// #region observation
func ExampleRuntime_Run_observation() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	runtime, err := durable.New(durable.NewMemoryStore(), durable.NewCodecs())
	if err != nil {
		panic(err)
	}
	g, err := pulse.New("observed-retry", "自动记录真实重试")
	if err != nil {
		panic(err)
	}
	calls := 0
	if err := g.Add(pulse.NewNode("work", "失败一次后成功", nil, nil, func(*pulse.RunCtx) error {
		calls++
		if calls == 1 {
			return errors.New("temporary business failure")
		}
		return nil
	}, pulse.Retry(3, 0))); err != nil {
		panic(err)
	}
	const runID = "observation-example"
	sink := &observe.MemorySink{}
	observer, err := observe.NewRecordObserver(observe.ObserveConfig{
		Sink: runIDSink{runID: runID, next: sink}, HostID: "worker-1", TraceID: "request-1",
	})
	if err != nil {
		panic(err)
	}
	_, err = runtime.Run(ctx, g, pulse.Inputs{}, durable.RunOptions{
		RunID: runID, Version: "business/1", ExecOptions: []pulse.ExecOption{pulse.WithExecObserver(observer)},
	})
	if err != nil {
		panic(err)
	}
	for _, record := range sink.Snapshot() {
		phase, _ := observe.Get[string](record.Attrs, pulse.AttrAspectPhase)
		if record.Event == observe.EventAspect && phase == string(pulse.AspectAttemptStarted) {
			id, _ := observe.Get[string](record.Attrs, durable.AttrRunID)
			attempt, _ := observe.Get[int64](record.Attrs, pulse.AttrAttempt)
			origin, _ := observe.Get[string](record.Attrs, pulse.AttrAttemptOrigin)
			fmt.Printf("run=%s attempt=%d origin=%s\n", id, attempt, origin)
		}
	}
	// Output:
	// run=observation-example attempt=1 origin=new
	// run=observation-example attempt=2 origin=new
}

// #endregion observation

func TestRunIDSinkCopiesAllScalarsWithoutMutatingInput(t *testing.T) {
	var input observe.Record
	observe.Set(&input.Attrs, "app.name", "document")
	observe.Set(&input.Attrs, "app.count", int64(3))
	observe.Set(&input.Attrs, "app.ratio", 0.5)
	observe.Set(&input.Attrs, "app.cached", true)
	observe.Set(&input.Attrs, durable.AttrRunID, "previous-owner")
	before, err := json.Marshal(input.Attrs)
	if err != nil {
		t.Fatal(err)
	}
	enriched, untouched := &observe.MemorySink{}, &observe.MemorySink{}
	observe.MultiSink{runIDSink{runID: "stable-run", next: enriched}, untouched}.Write(input)
	after, err := json.Marshal(untouched.Snapshot()[0].Attrs)
	if err != nil || string(after) != string(before) {
		t.Fatalf("enrichment mutated its caller/neighbour: before=%s after=%s err=%v", before, after, err)
	}
	r := enriched.Snapshot()[0]
	id, _ := observe.Get[string](r.Attrs, durable.AttrRunID)
	name, _ := observe.Get[string](r.Attrs, "app.name")
	count, _ := observe.Get[int64](r.Attrs, "app.count")
	ratio, _ := observe.Get[float64](r.Attrs, "app.ratio")
	cached, _ := observe.Get[bool](r.Attrs, "app.cached")
	if id != "stable-run" || name != "document" || count != 3 || ratio != 0.5 || !cached {
		t.Fatalf("scalar copy lost facts: %s", mustObservationJSON(t, r.Attrs))
	}
}

func mustObservationJSON(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
