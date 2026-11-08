package durable

import (
	"fmt"
)

const historyEvidenceSummaryKind = "history_evidence"

type historyEvidenceSummaryPayload struct {
	Schema string `json:"schema"`
	Source string `json:"source"`
}

type historyEvidenceSummaryHandler struct{}

func (historyEvidenceSummaryHandler) decode(data []byte) (decodedHistorySummary, error) {
	var payload historyEvidenceSummaryPayload
	if err := unmarshalRecord(data, &payload); err != nil || payload.Schema != "history-evidence/v1" || payload.Source != "committed-chain" {
		return nil, fmt.Errorf("%w: invalid history evidence summary payload", ErrInvalidRecord)
	}
	return historyEvidenceSummaryPayloadValue{payload}, nil
}

type historyEvidenceSummaryPayloadValue struct{ payload historyEvidenceSummaryPayload }

func (historyEvidenceSummaryPayloadValue) applicationRevision() uint64 { return 0 }
func (historyEvidenceSummaryPayloadValue) withApplication(uint64) ([]byte, error) {
	return nil, fmt.Errorf("%w: history evidence summary is not a compression candidate", ErrUnsupportedCapability)
}
func (historyEvidenceSummaryPayloadValue) validateRecovery() error { return nil }
func (historyEvidenceSummaryPayloadValue) candidates(RunRecord, *compressionCandidateIndex) ([]CompressionObjectRef, error) {
	return nil, fmt.Errorf("%w: history evidence summary has no deletion semantics", ErrUnsupportedCapability)
}
func (historyEvidenceSummaryPayloadValue) restore(record RunRecord, index *recoverySummaryIndex) error {
	if record.Revision == 0 || index == nil {
		return fmt.Errorf("%w: invalid history evidence restore context", ErrInvalidRecord)
	}
	return nil
}
