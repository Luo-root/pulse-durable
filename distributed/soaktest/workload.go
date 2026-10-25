// Package soaktest implements an opt-in local, process-based load driver.
package soaktest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
)

func checksum(id string, rounds int) string {
	h := sha256.Sum256([]byte(id))
	for i := 0; i < rounds; i++ {
		h = sha256.Sum256(h[:])
	}
	return hex.EncodeToString(h[:])
}
func assembly(rounds int, delay time.Duration) (d.Assembly, error) {
	g, e := pulse.New("soak-v1", "fixed SHA256 CPU work and bounded wait; checkpoints provide database IO")
	if e != nil {
		return d.Assembly{}, e
	}
	id := pulse.NewKey[string]("task_id")
	result := pulse.NewKey[string]("checksum")
	if e = g.DeclareInput(pulse.NewPort(id)); e != nil {
		return d.Assembly{}, e
	}
	e = g.Add(pulse.NewNode("work", "", pulse.Requires(id), pulse.Provides(result), func(rc *pulse.RunCtx) error {
		v, e := pulse.Get(rc, id)
		if e != nil {
			return e
		}
		// Bounded chunks keep cancellation cooperative even with large configured work.
		h := sha256.Sum256([]byte(v))
		for i := 0; i < rounds; i++ {
			if i%1024 == 0 {
				if e := rc.Context().Err(); e != nil {
					return e
				}
			}
			h = sha256.Sum256(h[:])
		}
		if delay > 0 {
			t := time.NewTimer(delay)
			defer t.Stop()
			select {
			case <-t.C:
			case <-rc.Context().Done():
				return rc.Context().Err()
			}
		}
		return pulse.Set(rc, result, hex.EncodeToString(h[:]))
	}))
	if e != nil {
		return d.Assembly{}, e
	}
	if e = g.DeclareOutput(pulse.NewPort(result)); e != nil {
		return d.Assembly{}, e
	}
	keys := pulse.NewRegistry()
	if e = pulse.RegisterKey(keys, id); e != nil {
		return d.Assembly{}, e
	}
	return d.Assembly{Graph: g, Codecs: durable.NewCodecs(), Keys: keys}, nil
}
func task(ctx context.Context, c Config, id string) (d.Task, error) {
	a, e := assembly(c.CPURounds, c.Wait)
	if e != nil {
		return d.Task{}, e
	}
	var in pulse.Inputs
	if e = pulse.Seed(&in, pulse.NewKey[string]("task_id"), id); e != nil {
		return d.Task{}, e
	}
	return d.NewTask(ctx, d.Identity{Namespace: c.Namespace, RunID: id}, "soak", c.version(), a.Graph, in, a.Codecs)
}
func validRecord(raw []byte, id string, rounds int) bool {
	var r durable.RunRecord
	if json.Unmarshal(raw, &r) != nil || r.RunID != id {
		return false
	}
	scope, ok := r.Scopes[r.RootScope]
	if !ok {
		return false
	}
	node, ok := scope.Nodes["work"]
	if !ok || node.Reason != "completed" {
		return false
	}
	for _, v := range node.Outputs {
		if v.Name == "checksum" && v.State == "ready" {
			var got string
			return json.Unmarshal(v.Data, &got) == nil && got == checksum(id, rounds)
		}
	}
	return false
}
func (c Config) version() string { return fmt.Sprintf("v1-cpu%d-wait%d", c.CPURounds, c.Wait) }
