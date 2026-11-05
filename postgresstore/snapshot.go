package postgresstore

import (
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
)

func encodeSnapshot(r durable.RunRecord) ([]byte, error) { return recordio.EncodeSnapshot(r) }
func decodeSnapshot(data []byte) (durable.RunRecord, error) {
	r, err := recordio.DecodeSnapshot(data)
	if err != nil {
		return durable.RunRecord{}, err
	}
	if err := durable.ValidateSemanticEvidence(r); err != nil {
		return durable.RunRecord{}, err
	}
	return r, nil
}
