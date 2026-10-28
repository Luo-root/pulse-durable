package distributed

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	durable "github.com/Luo-root/pulse-durable"
)

// DeltaMutationDigest returns the digest used by worker ApplyOwned calls.
// The JSON shape and field ordering intentionally match the historical worker
// implementation; changing either would make retries incompatible.
func DeltaMutationDigest(token Token, batch durable.ChangeBatch) (string, error) {
	wire, err := durable.MarshalChangeBatch(batch)
	if err != nil {
		return "", err
	}
	semantics, err := json.Marshal(struct {
		Operation string
		Token     Token
		Batch     []byte
	}{"apply_owned", token, wire})
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(semantics)
	return hex.EncodeToString(h[:]), nil
}
