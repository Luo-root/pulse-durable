package distributed

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	durable "github.com/Luo-root/pulse-durable"
)

// DeltaMutationDigest 返回 worker ApplyOwned 调用使用的 digest。
// JSON 结构和字段顺序刻意与历史 worker 实现保持一致；修改任一项都会使重试不兼容。
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
