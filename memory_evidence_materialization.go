package durable

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

// HistoryEvidenceMaterializeRequest 绑定一次物化发布到观察到的物理代次。
// Bounds 只控制锁外构建工作量；它不是 retention 策略，也不能把不完整
// 的来源链裁成“可恢复”结果。
type HistoryEvidenceMaterializeRequest struct {
	RunID              string
	ExpectedGeneration uint64
	Bounds             HistoryEvidenceBounds
}

// HistoryEvidenceMaterializer 是后端的原子发布边界。实现必须在发布锁内
// 重新验证当前 head 与 Generation；调用方不能把 Verified 结果直接写入
// Store。它不要求所有后端都支持，但支持时不能退化成普通 Apply 首次升级。
type HistoryEvidenceMaterializer interface {
	MaterializeHistoryEvidence(context.Context, HistoryEvidenceMaterializeRequest) (StorageState, error)
}

// memoryHistoryEvidenceSource 将 MemoryStore 中已经提交的 origin 快照和批
// 转成只读来源视图。它使用 Store 的互斥保护每次读取；Materialize 方法
// 在最终发布处重新比较状态，因此中途写入只会导致冲突而不会丢后缀。
type memoryHistoryEvidenceSource struct{ store *MemoryStore }

func (s *MemoryStore) HistoryEvidenceState(ctx context.Context, id string) (StorageState, error) {
	if err := maintenanceContext(ctx, s, id); err != nil {
		return StorageState{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.storageLocked(id)
}

func (s *MemoryStore) HistoryEvidenceOrigin(ctx context.Context, id string) (RunRecord, [32]byte, uint64, error) {
	var zero [32]byte
	if err := maintenanceContext(ctx, s, id); err != nil {
		return RunRecord{}, zero, 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.history[id]
	if h == nil {
		return RunRecord{}, zero, 0, ErrNotFound
	}
	image, ok := h.snapshots[1]
	if !ok || image.Record.Revision != 1 || image.Record.RunID != id {
		return RunRecord{}, zero, 0, fmt.Errorf("%w: missing memory origin snapshot", ErrInvalidRecord)
	}
	data, err := json.Marshal(image.Record)
	if err != nil {
		return RunRecord{}, zero, 0, fmt.Errorf("%w: encode memory origin: %v", ErrInvalidRecord, err)
	}
	digest := sha256.Sum256(data)
	return CloneRecord(image.Record), digest, uint64(len(data)), nil
}

func (s *MemoryStore) HistoryEvidenceBatch(ctx context.Context, id string, revision uint64) ([]byte, [32]byte, [32]byte, [32]byte, error) {
	var zero [32]byte
	if err := maintenanceContext(ctx, s, id); err != nil {
		return nil, zero, zero, zero, err
	}
	if revision < 2 {
		return nil, zero, zero, zero, fmt.Errorf("%w: invalid history evidence batch revision", ErrConflict)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.history[id]
	if h == nil || revision > uint64(len(h.batches)+1) {
		return nil, zero, zero, zero, ErrNotFound
	}
	batch := h.batches[revision-2]
	wire, err := MarshalChangeBatch(batch)
	if err != nil {
		return nil, zero, zero, zero, fmt.Errorf("%w: encode memory history batch: %v", ErrInvalidRecord, err)
	}
	wireDigest := sha256.Sum256(wire)
	parent, ok := h.anchors[revision-1]
	if !ok {
		return nil, zero, zero, zero, fmt.Errorf("%w: missing memory parent anchor", ErrInvalidRecord)
	}
	next, ok := h.anchors[revision]
	if !ok {
		return nil, zero, zero, zero, fmt.Errorf("%w: missing memory next anchor", ErrInvalidRecord)
	}
	return append([]byte(nil), wire...), wireDigest, parent, next, nil
}

var _ HistoryEvidenceSource = (*MemoryStore)(nil)
var _ HistoryEvidenceMaterializer = (*MemoryStore)(nil)

// MaterializeHistoryEvidence 在无活动 Session 时构建并原子发布新的同修订
// 快照。业务 revision/head chain 不变，只推进物理表示 generation；因此
// 旧 revision 仍可按原普通快照读取，当前 head 读取到闭合证据组件。
func (s *MemoryStore) MaterializeHistoryEvidence(ctx context.Context, req HistoryEvidenceMaterializeRequest) (StorageState, error) {
	if err := maintenanceContext(ctx, s, req.RunID); err != nil {
		return StorageState{}, err
	}
	if req.ExpectedGeneration == 0 {
		return StorageState{}, ErrConflict
	}
	s.mu.Lock()
	if _, active := s.active[req.RunID]; active {
		s.mu.Unlock()
		return StorageState{}, ErrBusy
	}
	observedRecord, exists := s.records[req.RunID]
	if !exists {
		s.mu.Unlock()
		return StorageState{}, ErrNotFound
	}
	observedDigest := recordDigest(observedRecord)
	s.mu.Unlock()
	state, err := MaterializeHistoryEvidence(ctx, s, req.RunID, req.Bounds)
	if err != nil {
		return StorageState{}, err
	}
	proposal, err := MaterializeHistoryEvidenceProposal(ctx, state)
	if err != nil {
		return StorageState{}, err
	}
	if proposal.State.Generation != req.ExpectedGeneration {
		return StorageState{}, ErrConflict
	}
	current := proposal.Record
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, active := s.active[req.RunID]; active {
		return StorageState{}, ErrBusy
	}
	currentState, err := s.storageLocked(req.RunID)
	if err != nil {
		return StorageState{}, err
	}
	if currentState != proposal.State || observedDigest != recordDigest(state.record) {
		return StorageState{}, ErrConflict
	}
	h := s.history[req.RunID]
	if h == nil || h.generation != req.ExpectedGeneration {
		return StorageState{}, ErrConflict
	}
	h.generation++
	h.base = currentState.HeadRevision
	h.snapshots[h.base] = RevisionImage{Record: CloneRecord(current), Chain: currentState.HeadChain}
	s.records[req.RunID] = CloneRecord(current)
	return s.storageLocked(req.RunID)
}
