package postgresstore

import (
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
)

func encodeSnapshot(r durable.RunRecord) ([]byte, error)    { return recordio.EncodeSnapshot(r) }
func decodeSnapshot(data []byte) (durable.RunRecord, error) { return recordio.DecodeSnapshot(data) }
