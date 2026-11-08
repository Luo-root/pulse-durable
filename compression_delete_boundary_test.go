// 本文件是历史删除边界的回归验收，相当于对公开事务入口的负控测试。
// 它验证普通变更不能遗忘 Attempt/Message，以及压缩不能发布缺少恢复
// parent/witness 的结果；所有失败都必须在修改来源、revision 或链之前发生。
package durable

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestOrdinaryBatchRejectsHistoryDeletes(t *testing.T) {
	for _, evidence := range []bool{false, true} {
		for _, kind := range []string{"attempt", "message"} {
			name := kind + "/ordinary"
			if evidence {
				name = kind + "/evidence"
			}
			t.Run(name, func(t *testing.T) {
				p := compressionMessageCommitRecord(t)
				record := CloneRecord(p.s.record)
				if !evidence {
					record.HistorySummaries, record.CommitIndex, record.Provenance = nil, nil, nil
				}
				before := CloneRecord(record)
				tx := newRecordTransaction(record.Revision)
				if kind == "attempt" {
					value := record.Attempts[p.processor.Scope]
					if err := tx.deleteAttempt(p.processor.Scope, &value); err != nil {
						t.Fatal(err)
					}
				} else {
					for id, value := range record.Messages {
						if err := tx.deleteMessage(id, &value); err != nil {
							t.Fatal(err)
						}
						break
					}
				}
				batch := ChangeBatch{header: changeHeader(record), tx: tx}
				// 精确 CAS 命中仍不能授予删除权限；直接检查共同入口的错误，
				// 排除“缺索引”等无关拒绝让负控假通过。
				if err := tx.check(record); !errors.Is(err, ErrConflict) || !strings.Contains(err.Error(), "dedicated compression transaction") {
					t.Fatalf("ordinary delete missed its shared guard: %v", err)
				}
				if _, err := batch.ApplyTo(&record); !errors.Is(err, ErrConflict) {
					t.Fatalf("ordinary in-memory delete accepted: %v", err)
				}
				if _, err := batch.SnapshotSizeDelta(record); !errors.Is(err, ErrConflict) {
					t.Fatalf("ordinary size query accepted delete: %v", err)
				}
				// 绕过 Marshal 的自检构造攻击字节，验证公开 decoder 自身有守卫。
				ops, err := batch.wireOps(true)
				if err != nil {
					t.Fatal(err)
				}
				wire, err := json.Marshal(wireBatch{Version: ChangeBatchWireVersion, Header: headerWire(batch.header), Ops: ops})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := UnmarshalChangeBatch(wire); err == nil {
					t.Fatal("ordinary public wire accepted history delete")
				}
				if _, err := MarshalChangeBatch(batch); err == nil {
					t.Fatal("ordinary encoder accepted history delete")
				}
				if !reflect.DeepEqual(record, before) {
					t.Fatal("rejected delete changed source record")
				}
			})
		}
	}
}

func TestCompressionBuilderRejectsMissingRecoveryParent(t *testing.T) {
	p, request := compressionRetryCommitRecord(t)
	record := CloneRecord(p.s.record)
	delete(record.Attempts, request.Parent)
	// 重签候选事实使规划可以通过；这只证明自洽，不能补造恢复 parent。
	record.HistorySummaries[0].SourceDigest = compressionSourceDigest(record)
	before := CloneRecord(record)
	plan, err := PlanHistoryCompression(record, HistoryCompressionRequest{RunID: record.RunID, ThroughRevision: record.Revision, MaxObjects: 100, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("probe failed before reaching recovery closure: %v", err)
	}
	if _, err := BuildHistoryCompressionBatch(record, plan); !errors.Is(err, ErrInvalidRecord) || !strings.Contains(err.Error(), "parent changed") {
		t.Fatalf("missing retained Retry parent accepted: %v", err)
	}
	if !reflect.DeepEqual(record, before) {
		t.Fatal("failed compression builder changed its source")
	}
}

func TestCompressionBuilderRejectsMissingManagedWitness(t *testing.T) {
	p := compressionMessageCommitRecord(t)
	record := CloneRecord(p.s.record)
	key := managedKey(record.RootScope, p.processor.Scope, "processor")
	// 保留 Managed 身份但破坏候选绑定值，且不再保留任何后续确认。
	managed := record.Managed[key]
	managed.State[0].Data = []byte("forged")
	managed.State[0].Digest = digest(managed.State[0].Data)
	record.Managed[key] = managed
	record.HistorySummaries[0].SourceDigest = compressionSourceDigest(record)
	before := CloneRecord(record)
	plan := HistoryCompressionPlan{Request: HistoryCompressionRequest{RunID: record.RunID, ThroughRevision: record.Revision, MaxObjects: 100, MaxBytes: 1 << 20}}
	// 不能用可重算摘要覆盖既有 witness；规划本身就应拒绝事实不符。
	if _, err := PlanHistoryCompression(record, plan.Request); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed Managed witness accepted: %v", err)
	}
	if !reflect.DeepEqual(record, before) {
		t.Fatal("rejected witness changed source")
	}
}

func TestCompressionCommitRetryKeepsRuntimeBudget(t *testing.T) {
	p, request := compressionRetryCommitRecord(t)
	before := CloneRecord(p.s.record)
	plan, err := PlanHistoryCompression(before, HistoryCompressionRequest{RunID: before.RunID, ThroughRevision: before.Revision, MaxObjects: 100, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := BuildHistoryCompressionBatch(before, plan)
	if err != nil {
		t.Fatal(err)
	}
	after := CloneRecord(before)
	if _, err := batch.ApplyTo(&after); err != nil {
		t.Fatal(err)
	}
	replaceMemoryFixture(p.s.session, after)
	p.s.record, p.s.summaryIndex = after, nil
	if err := p.s.validateLoaded(); err != nil {
		t.Fatalf("successful compression cannot restore: %v", err)
	}
	attempt, err := p.s.openAttempt(t.Context(), request)
	if err != nil || attempt.Index != 3 || attempt.Exhausted {
		t.Fatalf("compressed Retry reset or lost budget: %+v, %v", attempt, err)
	}
	if err := p.s.failAttempt(t.Context(), attempt.Scope); err != nil {
		t.Fatal(err)
	}
	revision := p.s.record.Revision
	exhausted, err := p.s.openAttempt(t.Context(), request)
	if err != nil || !exhausted.Exhausted || exhausted.Index != 3 || p.s.record.Revision != revision {
		t.Fatalf("exhausted compressed Retry reopened: %+v, %v", exhausted, err)
	}
}
