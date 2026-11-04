package systemtest

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/Luo-root/pulse-durable/distributed/postgres"
	"github.com/Luo-root/pulse-durable/distributed/worker"
)

// 真实协调器集成测试，仅由授权管理员有意破坏任务记录。
// 不改变 SQL 允许列表或服务器时钟。
func TestWorkerRejectionBlocks(t *testing.T) {
	if os.Getenv("PULSE_DISTRIBUTED_TEST_DSN") == "" || os.Getenv("PULSE_DISTRIBUTED_TEST_ALLOW") != "yes" {
		t.Skip("dedicated DSN and ALLOW=yes required")
	}
	for _, kind := range []string{"assembly", "record"} {
		t.Run(kind, func(t *testing.T) {
			f := provision(t)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			var business atomic.Int32
			var fail atomic.Bool
			factory := func() (d.Assembly, error) {
				if fail.Load() {
					return d.Assembly{}, errors.New("assembly unavailable")
				}
				g, err := pulse.New("rejection", "validation before execution")
				if err != nil {
					return d.Assembly{}, err
				}
				err = g.Add(pulse.NewNode("body", "", nil, nil, func(*pulse.RunCtx) error { business.Add(1); return nil }))
				return d.Assembly{Graph: g, Codecs: durable.NewCodecs(), Keys: pulse.NewRegistry()}, err
			}
			reg := &d.Registry{}
			route, err := reg.Register("rejection", "v1", factory)
			if err != nil {
				t.Fatal(err)
			}
			a, err := factory()
			if err != nil {
				t.Fatal(err)
			}
			id := d.Identity{Namespace: f.ns, RunID: "rejection-" + kind}
			task, err := d.NewTask(ctx, id, "rejection", "v1", a.Graph, pulse.Inputs{}, a.Codecs)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = f.c.Submit(ctx, task); err != nil {
				t.Fatal(err)
			}
			expected := "invalid_payload"
			if kind == "assembly" {
				fail.Store(true)
			} else {
				expected = "invalid_record"
				data, _ := json.Marshal(durable.RunRecord{RunID: id.RunID, Revision: 1, Format: durable.RecordFormat, Version: "v1", Definition: route.Definition, Inputs: task.InputsFingerprint(), RootScope: "missing-root", Scopes: map[string]durable.ScopeRecord{}, Attempts: map[string]durable.AttemptRecord{}})
				if _, err = f.admin.Exec(ctx, "UPDATE pulse_durable_distributed.runs SET record=$1,revision=1 WHERE namespace=$2 AND run_id=$3", data, id.Namespace, id.RunID); err != nil {
					t.Fatal("record injection failed")
				}
			}
			renew, err := postgres.New(ctx, f.dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer renew.Close()
			events := make(chan worker.Event, 8)
			w, err := worker.New(worker.Config{Coordinator: f.c, Renewer: renew, Registry: reg, Namespace: f.ns, WorkerID: "reject", Capacity: 1, BatchMax: 1, TTL: time.Second, RenewInterval: 200 * time.Millisecond, RenewTimeout: 100 * time.Millisecond, OperationTimeout: 200 * time.Millisecond, PollMin: 10 * time.Millisecond, PollMax: 20 * time.Millisecond, Observe: func(e worker.Event) {
				if e.Kind == "block" {
					events <- e
				}
			}})
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			runCtx, stop := context.WithCancel(ctx)
			defer stop()
			go func() { done <- w.Run(runCtx) }()
			select {
			case e := <-events:
				if e.Reason != "ok" {
					stop()
					<-done
					t.Fatalf("Block rejected by real coordinator: %s", e.Reason)
				}
			case <-ctx.Done():
				t.Fatal("no block result")
			}
			w.Pause()
			snapshot, err := f.c.Get(ctx, id)
			if err != nil || snapshot.Status != d.Blocked || snapshot.Reason != expected {
				t.Fatalf("authority status=%s reason=%s err=%v", snapshot.Status, snapshot.Reason, err)
			}
			stop()
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal("worker did not stop")
			}
			// 等待数据库时间，而非固定主机休眠，直到阻塞后已过一个完整原始 TTL。
			// 随后以精确路由显式尝试新的 Claim。
			if _, err = f.admin.Exec(ctx, "SELECT pg_sleep(GREATEST(0, EXTRACT(EPOCH FROM ($1::timestamptz + interval '1 second' - clock_timestamp()))))", snapshot.UpdatedAt); err != nil {
				t.Fatal("database time barrier failed")
			}
			claims, err := f.c.Claim(ctx, d.ClaimRequest{Namespace: f.ns, Worker: d.Worker{ID: "after-rejection", BootID: "0123456789abcdef0123456789abcdef"}, Capabilities: []d.Route{route}, Limit: 1, TTL: time.Second, LeaseIDs: []string{"fedcba9876543210fedcba9876543210"}})
			if err != nil || len(claims) != 0 {
				t.Fatalf("blocked task reclaimed: count=%d err=%v", len(claims), err)
			}
			snapshot, err = f.c.Get(ctx, id)
			if err != nil || snapshot.Status != d.Blocked || business.Load() != 0 {
				t.Fatalf("rejection not durable: status=%s business=%d err=%v", snapshot.Status, business.Load(), err)
			}
		})
	}
}
