package postgres

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/Luo-root/pulse-durable/distributed/worker"
)

// 数据库真实提交后，此传输边界包装器才丢弃一次
// 响应。这不是未提交的模拟 Apply 实现。
type lostApplyResponse struct {
	d.Coordinator
	injected     atomic.Bool
	finishes     atomic.Int32
	releases     atomic.Int32
	lost         d.Mutation
	lostRevision uint64
}

func (c *lostApplyResponse) ApplyOwned(ctx context.Context, t d.Token, m d.Mutation, b durable.ChangeBatch) (durable.CommitAck, error) {
	ack, e := c.Coordinator.ApplyOwned(ctx, t, m, b)
	if e == nil && b.Header().Revision >= 40 && c.injected.CompareAndSwap(false, true) {
		c.lost = m
		c.lostRevision = b.Header().Revision
		return durable.CommitAck{}, d.ErrUnknown
	}
	return ack, e
}
func (c *lostApplyResponse) Finish(ctx context.Context, r d.FinishRequest) (d.Snapshot, error) {
	c.finishes.Add(1)
	return c.Coordinator.Finish(ctx, r)
}
func (c *lostApplyResponse) Release(ctx context.Context, t d.Token, m d.Mutation) error {
	c.releases.Add(1)
	return c.Coordinator.Release(ctx, t, m)
}

func TestPostgresWorker500MessagesUnknownResponseRecovery(t *testing.T) {
	c, admin, ns := integration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	var traceMu sync.Mutex
	trace := make([]string, 0, 256)
	significant := make([]string, 0, 64)
	failures := make([]string, 0, 64)
	started := time.Now()
	c.clockDiagnostic = func(value clockDiagnostic) {
		line := fmt.Sprintf("%s clock phase=%s delta_us=%d step_us=%d", time.Since(started).Round(time.Millisecond), value.Phase, value.DeltaUS, value.StepUS)
		traceMu.Lock()
		defer traceMu.Unlock()
		if len(failures) == 64 {
			copy(failures, failures[1:])
			failures = failures[:63]
		}
		failures = append(failures, line)
	}
	recordEvent := func(e worker.Event) {
		// 有意省略令牌 ID、变更摘要、任务及载荷数据。
		line := fmt.Sprintf("%s kind=%s reason=%s epoch=%d rev=%d duration=%s", time.Since(started).Round(time.Millisecond), e.Kind, e.Reason, e.Token.Epoch, e.Revision, e.Duration.Round(time.Millisecond))
		traceMu.Lock()
		defer traceMu.Unlock()
		if len(trace) == 256 {
			copy(trace, trace[1:])
			trace = trace[:255]
		}
		trace = append(trace, line)
		// 分别保留生命周期与错误：成功的空轮询、
		// 续租和 Apply 流量不能挤掉更早的所有权丢失记录。
		if e.Kind == "owned" || e.Kind == "execution" || e.Kind == "finish" {
			if len(significant) == 64 {
				copy(significant, significant[1:])
				significant = significant[:63]
			}
			significant = append(significant, line)
		}
		if e.Reason != "" && e.Reason != "ok" {
			if len(failures) == 64 {
				copy(failures, failures[1:])
				failures = failures[:63]
			}
			failures = append(failures, line)
		}
	}
	defer func() {
		if t.Failed() {
			traceMu.Lock()
			defer traceMu.Unlock()
			for _, line := range significant {
				t.Log("lifecycle", line)
			}
			for _, line := range failures {
				t.Log("non-ok", line)
			}
			for _, line := range trace {
				t.Log("tail", line)
			}
		}
	}()
	var calls atomic.Int32
	factory := func() (d.Assembly, error) {
		g, e := pulse.New("pg-worker-500", "")
		if e != nil {
			return d.Assembly{}, e
		}
		key := pulse.NewKey[pulse.Stream[int]]("numbers")
		e = pulse.ProduceStream(g, "source", "", key, func(_ *pulse.RunCtx, w *pulse.StreamWriter[int]) error {
			for i := int(w.Position().Sequence) + 1; i <= 500; i++ {
				if e := w.SendAt(i, i); e != nil {
					return e
				}
			}
			return nil
		}, pulse.WithBuffer(1), pulse.WithStreamSource("pg-worker-500/source", true))
		if e != nil {
			return d.Assembly{}, e
		}
		e = pulse.ConsumeStream(g, "sink", "", key, func(rc *pulse.RunCtx, v int) error { calls.Add(1); return pulse.SetMessageState(rc, "last", v) }, pulse.WithMessageHandling(pulse.WithMessageRetry(1, 0)))
		return d.Assembly{Graph: g, Codecs: durable.NewCodecs(), Keys: pulse.NewRegistry()}, e
	}
	registry := &d.Registry{}
	if _, e := registry.Register("worker500", "v1", factory); e != nil {
		t.Fatal(e)
	}
	assembly, e := factory()
	if e != nil {
		t.Fatal(e)
	}
	task, e := d.NewTask(ctx, d.Identity{Namespace: ns, RunID: "worker500"}, "worker500", "v1", assembly.Graph, pulse.Inputs{}, assembly.Codecs)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = c.Submit(ctx, task); e != nil {
		t.Fatal(e)
	}
	wrapped := &lostApplyResponse{Coordinator: c}
	run := func(backend d.Coordinator, id string, kind string) worker.Event {
		events := make(chan worker.Event, 4)
		cfg := worker.Config{Coordinator: backend, Renewer: c, Registry: registry, Namespace: ns, WorkerID: id, Capacity: 1, BatchMax: 1, TTL: 15 * time.Second, RenewInterval: time.Second, RenewTimeout: 2 * time.Second, OperationTimeout: 5 * time.Second, PollMin: 10 * time.Millisecond, PollMax: 20 * time.Millisecond, Observe: func(e worker.Event) {
			recordEvent(e)
			if e.Kind == kind {
				select {
				case events <- e:
				default:
				}
			}
		}}
		w, e := worker.New(cfg)
		if e != nil {
			t.Fatal(e)
		}
		local, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- w.Run(local) }()
		defer func() {
			stop()
			select {
			case <-done:
			case <-ctx.Done():
				t.Error("worker failed to join")
			}
		}()
		select {
		case event := <-events:
			w.Pause()
			if e := w.Drain(ctx); e != nil {
				t.Fatal(e)
			}
			return event
		case <-ctx.Done():
			t.Fatal("worker event timeout", ctx.Err())
		}
		return worker.Event{}
	}
	first := run(wrapped, "old", "execution")
	if !wrapped.injected.Load() || wrapped.finishes.Load() != 0 || wrapped.releases.Load() != 0 {
		t.Fatalf("unknown response incorrectly finished/released: event=%+v", first)
	}
	s, e := c.Get(ctx, task.Identity())
	if e != nil || s.Status != d.Running || s.Revision < 40 {
		t.Fatal("unknown write lost authority", e)
	}
	receipts, e := c.FindMutation(ctx, receiptQuery(t, admin, task.Identity(), wrapped.lost, "apply", &first.Token.Epoch, &wrapped.lostRevision))
	if e != nil || receipts.State != d.Found || receipts.Receipt.Operation != "apply" {
		t.Fatal("lost response missing exact mutation receipt", e)
	}
	if _, e = admin.Exec(ctx, `UPDATE pulse_durable_distributed.runs SET lease_until=clock_timestamp()-interval '1 second' WHERE namespace=$1 AND run_id=$2`, ns, task.Identity().RunID); e != nil {
		t.Fatal(scrub(e, false))
	}
	finish := run(c, "new", "finish")
	if finish.Reason != "ok" || finish.Token.Epoch != 2 {
		t.Fatalf("recovery finish: %+v", finish)
	}
	s, e = c.Get(ctx, task.Identity())
	if e != nil || s.Status != d.Succeeded {
		t.Fatal("not succeeded", e)
	}
	var count, receiptsCount int
	var snapshotRevision int
	if e = admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM pulse_durable_distributed.batches WHERE namespace=$1 AND run_id=$2),(SELECT count(*) FROM pulse_durable_distributed.mutation_receipts m JOIN pulse_durable_distributed.mutation_guards g USING(namespace,run_id,mutation_id) WHERE namespace=$1 AND run_id=$2 AND g.operation='apply'),(convert_from(record,'UTF8')::jsonb->>'revision')::int FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2`, ns, task.Identity().RunID).Scan(&count, &receiptsCount, &snapshotRevision); e != nil {
		t.Fatal(scrub(e, false))
	}
	if count < 1500 || count != receiptsCount || snapshotRevision != 1 {
		t.Fatalf("not incremental: batches=%d receipts=%d snapshot=%d", count, receiptsCount, snapshotRevision)
	}
	// 提交前被中断的回调可能再次运行。已确认消息
	// 必须恰为 500 个逻辑输入；无法在 Finish 前用新所有者
	// 检查重放，因此通过 SQL 验证不可变线格式证据。
	var confirmed int
	if e = admin.QueryRow(ctx, `SELECT count(*) FROM pulse_durable_distributed.batches b CROSS JOIN LATERAL jsonb_array_elements(convert_from(b.wire,'UTF8')::jsonb->'ops') op WHERE namespace=$1 AND run_id=$2 AND op->>'kind'='message' AND op->'value'->>'state'='confirmed'`, ns, task.Identity().RunID).Scan(&confirmed); e != nil {
		t.Fatal(scrub(e, false))
	}
	if confirmed != 500 || calls.Load() < 500 {
		t.Fatalf("business coverage confirmed=%d calls=%d", confirmed, calls.Load())
	}
	t.Logf("500 confirmed messages, %d batches/receipts, %d callbacks, unknown commit recovered at epoch 2", count, calls.Load())
}
