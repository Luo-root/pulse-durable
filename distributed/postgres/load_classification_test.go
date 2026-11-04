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
	"github.com/Luo-root/pulse-durable/recordio"
)

// 在成功且受栅栏保护的 SQL 观测后测试生产解码器。
// 这不能替代真实协调器拒绝请求的系统测试。
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

func TestDecodeRecordInvalidCategories(t *testing.T) {
	task := taskFixture(t, "tenant", "classification-business-value")
	base := record(task, 1)
	const businessValue = "classification-business-value"
	cases := []struct {
		name   string
		want   error
		mutate func(*durable.RunRecord)
		wire   func(durable.RunRecord) []byte
	}{
		{name: "snapshot", want: ErrInvalidRecordSnapshot, wire: func(durable.RunRecord) []byte { return []byte(`{"version":`) }},
		{name: "identity", want: ErrInvalidRecordIdentity, mutate: func(r *durable.RunRecord) { r.RunID = "other" }},
		{name: "revision", want: ErrInvalidRecordRevision, mutate: func(r *durable.RunRecord) { r.Revision = 2 }},
		{name: "format", want: ErrInvalidRecordFormat, mutate: func(r *durable.RunRecord) { r.Format++ }},
		{name: "version", want: ErrInvalidRecordVersion, mutate: func(r *durable.RunRecord) { r.Version = "other" }},
		{name: "definition", want: ErrInvalidRecordDefinition, mutate: func(r *durable.RunRecord) { r.Definition = strings.Repeat("a", 64) }},
		{name: "inputs", want: ErrInvalidRecordInputs, mutate: func(r *durable.RunRecord) { r.Inputs = strings.Repeat("b", 64) }},
		{name: "root", want: ErrInvalidRecordRoot, mutate: func(r *durable.RunRecord) { r.RootScope = "" }},
		{name: "shapes", want: ErrInvalidRecordShapes, mutate: func(r *durable.RunRecord) { r.Scopes = nil }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := base
			if tc.mutate != nil {
				tc.mutate(&r)
			}
			encode := tc.wire
			if encode == nil {
				encode = func(r durable.RunRecord) []byte {
					wire, err := recordio.Encode(r)
					if err != nil {
						t.Fatal(err)
					}
					return wire
				}
			}
			_, err := decodeRecord(task, 1, encode(r))
			if !errors.Is(err, tc.want) || !errors.Is(err, d.ErrInvalid) {
				t.Fatalf("classification: %v", err)
			}
			if strings.Contains(err.Error(), businessValue) {
				t.Fatalf("error leaks business value: %v", err)
			}
		})
	}
}
