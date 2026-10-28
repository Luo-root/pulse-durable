package postgresstore

import (
	"encoding/json"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
)

// Only empty execution states are snapshots in layout v2. Explicit top-level
// maps preserve empty versus nil despite RunRecord's optional JSON fields.
type snapshot struct {
	Record   durable.RunRecord                       `json:"record"`
	Streams  map[string]durable.StreamRecord         `json:"streams"`
	Messages map[string]durable.MessageAttemptRecord `json:"messages"`
	Managed  map[string]durable.ManagedRecord        `json:"managed"`
}

func encodeSnapshot(r durable.RunRecord) ([]byte, error) {
	if _, err := recordio.Encode(r); err != nil {
		return nil, err
	}
	return json.Marshal(snapshot{r, r.Streams, r.Messages, r.Managed})
}
func decodeSnapshot(data []byte) (durable.RunRecord, error) {
	// recordio rejects invalid UTF-8 and lone surrogate escapes before JSON can
	// normalize them, even though this envelope is not itself a RunRecord.
	if _, err := recordio.Decode(data); err != nil {
		return durable.RunRecord{}, err
	}
	var v snapshot
	if err := json.Unmarshal(data, &v); err != nil {
		return durable.RunRecord{}, errCorrupt
	}
	raw, err := recordio.Encode(v.Record)
	if err != nil {
		return durable.RunRecord{}, err
	}
	if _, err = recordio.Decode(raw); err != nil {
		return durable.RunRecord{}, err
	}
	v.Record.Streams = v.Streams
	v.Record.Messages = v.Messages
	v.Record.Managed = v.Managed
	return v.Record, nil
}
