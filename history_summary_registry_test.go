// 本文件相当于注册表的契约测试：第三种测试算法只新增实现并注册，
// 验证五种分派能力共用该实现；另外区分 opaque 证据保存与执行修改权限。
package durable

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

type registryProbeHandler struct{}
type registryProbePayload struct{ revision uint64 }

func (*registryProbeHandler) decode(data []byte) (decodedHistorySummary, error) {
	if !bytes.Equal(data, []byte(`{"probe":true}`)) {
		return nil, ErrInvalidRecord
	}
	return registryProbePayload{}, nil
}
func (p registryProbePayload) applicationRevision() uint64 { return p.revision }
func (p registryProbePayload) withApplication(revision uint64) ([]byte, error) {
	return []byte(u64(revision)), nil
}
func (registryProbePayload) validateRecovery() error { return nil }
func (registryProbePayload) candidates(RunRecord, *compressionCandidateIndex) ([]CompressionObjectRef, error) {
	return []CompressionObjectRef{{Kind: "probe", ID: "candidate"}}, nil
}
func (registryProbePayload) restore(_ RunRecord, index *recoverySummaryIndex) error {
	index.reserved["probe"] = true
	return nil
}

func TestHistorySummaryRegistryComposesAdditionalKind(t *testing.T) {
	registrations := []historySummaryRegistration{{"probe", &registryProbeHandler{}}}
	registry, err := newHistorySummaryRegistry(registrations...)
	if err != nil {
		t.Fatal(err)
	}
	// 修改调用方注册切片不能改变已创建的路由表。
	registrations[0].kind, registrations[0].handler = "replacement", nil
	payload, err := registry.decode(HistorySummary{Kind: "probe", Payload: []byte(`{"probe":true}`)})
	if err != nil || payload.applicationRevision() != 0 || payload.validateRecovery() != nil {
		t.Fatalf("additional Kind not dispatched: %v", err)
	}
	rows, err := payload.candidates(RunRecord{}, nil)
	if err != nil || !reflect.DeepEqual(rows, []CompressionObjectRef{{Kind: "probe", ID: "candidate"}}) {
		t.Fatalf("candidate implementation bypassed: %v", err)
	}
	index := &recoverySummaryIndex{reserved: make(map[string]bool)}
	if err := payload.restore(RunRecord{}, index); err != nil || !index.reserved["probe"] {
		t.Fatalf("recovery implementation bypassed: %v", err)
	}
	applied, err := payload.withApplication(9)
	if err != nil || string(applied) != "9" || payload.applicationRevision() != 0 {
		t.Fatal("application encoding mutated the original decoded payload")
	}
	if _, err := registry.decode(HistorySummary{Kind: "replacement"}); !errors.Is(err, ErrUnsupportedCapability) {
		t.Fatalf("caller alias changed registry: %v", err)
	}
}

func TestHistorySummaryRegistryRejectsInvalidRegistrations(t *testing.T) {
	var typedNil *registryProbeHandler
	for _, registrations := range [][]historySummaryRegistration{
		{{"", &registryProbeHandler{}}},
		{{"probe", nil}},
		{{"probe", typedNil}},
		{{"probe", &registryProbeHandler{}}, {"probe", &registryProbeHandler{}}},
	} {
		if _, err := newHistorySummaryRegistry(registrations...); err == nil {
			t.Fatal("invalid summary registry accepted")
		}
	}
}

func TestUnknownSummaryRejectsMutationBeforePublishing(t *testing.T) {
	record, _ := format3Fixture(t)
	record.HistorySummaries[0].Kind = "future_kind"
	record.HistorySummaries[0].Payload = []byte{0, 255, 'x'}
	record.HistorySummaries[0].Digest = digest(record.HistorySummaries[0].Payload)
	before := CloneRecord(record)
	// 通用证据校验只检查容器、来源与摘要，可用于只读保存未知字节。
	if err := ValidateSemanticEvidence(record); err != nil {
		t.Fatal(err)
	}
	if _, err := buildRecoverySummaryIndex(record); !errors.Is(err, ErrUnsupportedCapability) {
		t.Fatalf("unknown recovery Kind accepted: %v", err)
	}
	tx := newRecordTransaction(record.Revision)
	if err := tx.putAttempt("ordinary", nil, AttemptRecord{Parent: "root", NodeID: "node", Index: 1, Limit: 1, State: AttemptActive}); err != nil {
		t.Fatal(err)
	}
	if err := tx.ensureSemanticCommitIndex(record); err != nil {
		t.Fatal(err)
	}
	batch := ChangeBatch{header: changeHeaderFor(record, tx), tx: tx}
	if _, err := batch.SnapshotSizeDelta(record); !errors.Is(err, ErrUnsupportedCapability) {
		t.Fatalf("unknown summary size mutation accepted: %v", err)
	}
	if _, err := batch.ApplyTo(&record); !errors.Is(err, ErrUnsupportedCapability) {
		t.Fatalf("unknown summary mutation accepted: %v", err)
	}
	if err := checkOrdinarySummaryOperation(putHistorySummary{absent: true, value: record.HistorySummaries[0]}); !errors.Is(err, ErrUnsupportedCapability) {
		t.Fatalf("unknown summary insert accepted: %v", err)
	}
	if !reflect.DeepEqual(record, before) {
		t.Fatal("unknown summary rejection changed the original evidence")
	}
}
