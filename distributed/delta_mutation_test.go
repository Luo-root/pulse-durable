package distributed

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
)

func TestDeltaMutationDigestMatchesWorkerSemantics(t *testing.T) {
	batch, err := durable.UnmarshalChangeBatch([]byte(`{"version":1,"header":{"batch_format":"1","logical_format":"2","run_id":"r","business_version":"v","definition_digest":"d","inputs_digest":"i","root_scope":"root","expected_revision":"1","revision":"2"},"ops":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	token := Token{Identity: Identity{Namespace: "tenant", RunID: "r"}, Worker: Worker{ID: "worker", BootID: "boot"}, LeaseID: "lease", Epoch: 1}
	wire, err := durable.MarshalChangeBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	semantics, err := json.Marshal(struct {
		Operation string
		Token     Token
		Batch     []byte
	}{"apply_owned", token, wire})
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(semantics)
	got, err := DeltaMutationDigest(token, batch)
	if err != nil || got != hex.EncodeToString(h[:]) {
		t.Fatalf("digest differs from existing worker encoding: %v", err)
	}
	token.Epoch++
	changed, err := DeltaMutationDigest(token, batch)
	if err != nil || changed == got {
		t.Fatalf("epoch not bound: %v", err)
	}
}

func TestDeltaMutationDigestRejectsInvalidBatch(t *testing.T) {
	if _, err := DeltaMutationDigest(Token{}, durable.ChangeBatch{}); err == nil {
		t.Fatal("invalid batch accepted")
	}
}
