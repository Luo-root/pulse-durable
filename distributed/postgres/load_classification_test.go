package postgres

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
)

// Exercise the production decoder after a successful fenced SQL observation.
// This does not substitute for the real coordinator rejection system test.
func TestOwnedLoadCorruptionClassification(t *testing.T) {
	task := taskFixture(t, "tenant", "load-classification")
	token := d.Token{Identity: task.Identity(), Worker: d.Worker{ID: "worker", BootID: strings.Repeat("a", 32)}, LeaseID: strings.Repeat("b", 32), Epoch: 1}
	now := time.Now().UTC()
	until := now.Add(time.Minute)
	initial, err := json.Marshal(record(task, 1))
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(initial)
	base := wireSnapshot{Task: task.Bytes(), Digest: task.SubmissionDigest(), Namespace: token.Identity.Namespace, RunID: token.Identity.RunID, Route: task.Route(), Status: d.Running, Epoch: token.Epoch, Revision: "1", LeaseUntil: &until, CreatedAt: now, UpdatedAt: now, LeaseID: &token.LeaseID, WorkerID: &token.Worker.ID, BootID: &token.Worker.BootID, Record: initial}
	for _, kind := range []string{"valid", "snapshot_digest", "record", "head_chain", "batch", "owner", "envelope", "revoked"} {
		t.Run(kind, func(t *testing.T) {
			w := base
			log := storedLog{Digest: chainText(hash), Chain: chainText(durable.SeedChain(hash))}
			p := &projection{}
			switch kind {
			case "snapshot_digest":
				log.Digest = strings.Repeat("f", 64)
			case "record":
				w.Record = []byte(`{}`)
				h := sha256.Sum256(w.Record)
				log.Digest = chainText(h)
			case "head_chain":
				log.Chain = strings.Repeat("f", 64)
			case "batch":
				log.Batches = []storedBatch{{Revision: "2", Parent: log.Chain, Wire: []byte("invalid")}}
			case "owner":
				w.Epoch++
			case "envelope":
				w.Digest = "wrong"
			case "revoked":
				p.revoked.Store(true)
			}
			b, e := json.Marshal(struct {
				wireSnapshot
				storedLog
			}{w, log})
			if e != nil {
				t.Fatal(e)
			}
			_, e = (&Coordinator{}).decodeProjection(token, p, b)
			switch kind {
			case "valid":
				if e != nil || !p.loaded {
					t.Fatalf("valid load: loaded=%v err=%v", p.loaded, e)
				}
			case "owner", "envelope", "revoked":
				if e == nil || errors.Is(e, d.ErrInvalidRecord) || p.loaded {
					t.Fatalf("uncertain observation classified as corrupt: %v", e)
				}
			default:
				if !errors.Is(e, d.ErrInvalidRecord) || !errors.Is(e, d.ErrInvalid) || p.loaded {
					t.Fatalf("corrupt record classification: loaded=%v err=%v", p.loaded, e)
				}
			}
		})
	}
}
