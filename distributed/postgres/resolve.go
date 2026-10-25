package postgres

import (
	"context"
	"encoding/json"

	d "github.com/Luo-root/pulse-durable/distributed"
)

// ResolveClaim either returns a currently valid committed claim, or permanently
// closes this ID against delayed claims. Errors never imply closure. The SQL
// operation locks the durable lease ID before its run, matching Claim lock order.
func (c *Coordinator) ResolveClaim(ctx context.Context, ns string, w d.Worker, id string) (d.ClaimResolution, error) {
	if !validText(ns) || !validWorker(w) || !lease(id) {
		return d.ClaimResolution{}, d.ErrInvalid
	}
	a := args(d.Identity{Namespace: ns})
	a["worker_id"] = w.ID
	a["boot_id"] = w.BootID
	a["lease_id"] = id
	b, e := c.call(ctx, "resolve", a, true)
	if e != nil {
		return d.ClaimResolution{}, e
	}
	var wire struct {
		Claim  json.RawMessage `json:"claim"`
		Closed bool            `json:"closed"`
	}
	if json.Unmarshal(b, &wire) != nil {
		return d.ClaimResolution{}, d.ErrInvalid
	}
	if wire.Closed {
		if string(wire.Claim) != "null" {
			return d.ClaimResolution{}, d.ErrInvalid
		}
		return d.ClaimResolution{Closed: true}, nil
	}
	s, t, _, e := decodeSnapshot(wire.Claim)
	if e != nil {
		return d.ClaimResolution{}, e
	}
	if s.Status != d.Running || t.Identity.Namespace != ns || t.Worker != w || t.LeaseID != id {
		return d.ClaimResolution{}, d.ErrInvalid
	}
	return d.ClaimResolution{Claim: &d.Claim{Snapshot: s, Token: t}}, nil
}
