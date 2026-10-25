package postgres

import (
	"context"
	"encoding/json"
	"time"

	d "github.com/Luo-root/pulse-durable/distributed"
)

// Receipt is a committed database mutation, not proof of external side effects.
// Match ID AND digest, operation, epoch and revision before resolving ErrUnknown.
type Receipt struct {
	Operation      string    `json:"operation"`
	Epoch          int64     `json:"epoch"`
	Revision       string    `json:"revision"`
	MutationID     string    `json:"mutation_id"`
	MutationDigest string    `json:"mutation_digest"`
	OccurredAt     time.Time `json:"occurred_at"`
}

// FindMutation reads retained transactional events from the authority. A missing
// receipt is not permission to replay execution or claim using fresh lease IDs.
// This backend-specific method does not change the Coordinator interface.
func (c *Coordinator) FindMutation(ctx context.Context, i d.Identity, m d.Mutation) ([]Receipt, error) {
	if !identity(i) || !mutation(m) {
		return nil, d.ErrInvalid
	}
	a := args(i)
	mutate(a, m)
	b, e := c.call(ctx, "receipt", a, false)
	if e != nil {
		return nil, e
	}
	var out []Receipt
	if json.Unmarshal(b, &out) != nil {
		return nil, d.ErrInvalid
	}
	for _, r := range out {
		if r.MutationID != m.ID || r.MutationDigest != m.Digest {
			return nil, d.ErrInvalid
		}
	}
	return out, nil
}
