package distributed

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
)

func TestDeltaMutationDigestMatchesWorkerSemantics(t *testing.T) {
	// 当前批次只在 wire envelope 上带版本，header 保留业务身份与修订。
	batch, err := durable.UnmarshalChangeBatch([]byte(`{"version":2,"header":{"run_id":"r","business_version":"v","definition_digest":"d","inputs_digest":"i","root_scope":"root","expected_revision":"1","revision":"2"},"ops":[]}`))
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

// 冻结旧 wire，不沿用当前常量重编码，防止兼容拒绝测试被夹具更新抹掉。
func TestLegacyChangeBatchWireRejectedWithoutMutation(t *testing.T) {
	wire := []byte(`{"version":1,"header":{"batch_format":"1","logical_format":"2","run_id":"r","business_version":"v","definition_digest":"d","inputs_digest":"i","root_scope":"root","expected_revision":"1","revision":"2"},"ops":[]}`)
	before := append([]byte(nil), wire...)
	if _, err := durable.UnmarshalChangeBatch(wire); err == nil {
		t.Fatal("legacy batch wire accepted")
	}
	if !bytes.Equal(wire, before) {
		t.Fatal("legacy batch rejection rewrote input bytes")
	}
}

func TestDeltaMutationDigestRejectsInvalidBatch(t *testing.T) {
	if _, err := DeltaMutationDigest(Token{}, durable.ChangeBatch{}); err == nil {
		t.Fatal("invalid batch accepted")
	}
}
