package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
)

func TestRegistryRejectionUsesAllowedReason(t *testing.T) {
	for _, kind := range []string{"invalid_payload", "incompatible", "invalid_task"} {
		t.Run(kind, func(t *testing.T) {
			w, f := fixture(t, func(*pulse.RunCtx) error { t.Error("rejected task executed business"); return nil })
			token := d.Token{Identity: f.task.Identity(), Worker: w.identity, LeaseID: "0123456789abcdef0123456789abcdef", Epoch: 1}
			switch kind {
			case "invalid_payload":
				fail := false
				factory := func() (d.Assembly, error) {
					if fail {
						return d.Assembly{}, errors.New("assembly failed")
					}
					g, err := pulse.New("work", "test")
					if err != nil {
						return d.Assembly{}, err
					}
					err = g.Add(pulse.NewNode("node", "", nil, nil, func(*pulse.RunCtx) error { t.Error("business ran"); return nil }))
					return d.Assembly{Graph: g, Codecs: durable.NewCodecs(), Keys: pulse.NewRegistry()}, err
				}
				reg := &d.Registry{}
				if _, err := reg.Register("task", "v1", factory); err != nil {
					t.Fatal(err)
				}
				fail = true
				w.cfg.Registry = reg
			case "incompatible":
				w.cfg.Registry = &d.Registry{}
			case "invalid_task":
				f.task = d.Task{}
			}
			w.reserve()
			w.execute(context.Background(), d.Claim{Token: token, Snapshot: d.Snapshot{Task: f.task, Status: d.Running}})
			select {
			case got := <-f.block:
				if got != kind {
					t.Fatalf("reason=%s want=%s", got, kind)
				}
			default:
				t.Fatal("Block rejected or absent")
			}
		})
	}
}
func TestFakeBlockRejectsUnknownReason(t *testing.T) {
	_, f := fixture(t, func(*pulse.RunCtx) error { return nil })
	if err := f.Block(context.Background(), d.Token{}, "rejected", d.Mutation{}); !errors.Is(err, d.ErrInvalid) {
		t.Fatal("fake accepted SQL-invalid reason")
	}
}
