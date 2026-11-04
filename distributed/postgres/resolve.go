package postgres

import (
	"context"
	"encoding/json"

	d "github.com/Luo-root/pulse-durable/distributed"
)

// ResolveClaim 返回当前有效的已提交领取结果，或永久
// 关闭此 ID，防止延迟领取。错误绝不意味着关闭。SQL
// 操作先锁持久租约 ID，再锁运行，与 Claim 的锁顺序一致。
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
