package filestore

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
)

// fileHistoryEvidenceSource 把同一个独占 session 已选中的 manifest 暴露给
// 根包来源验证器。它只读取受 manifest 引用的 origin/batch，不扫描目录，
// 也不采纳孤儿文件。session 的 OS 锁由外层 Materialize 持有。
type fileHistoryEvidenceSource struct {
	session *session
	head    manifest
}

func (s *fileHistoryEvidenceSource) HistoryEvidenceState(context.Context, string) (durable.StorageState, error) {
	return storageState(s.session.runID, s.head), nil
}

func (s *fileHistoryEvidenceSource) HistoryEvidenceOrigin(ctx context.Context, id string) (durable.RunRecord, [32]byte, uint64, error) {
	var zero [32]byte
	if id != s.session.runID {
		return durable.RunRecord{}, zero, 0, durable.ErrConflict
	}
	image := originSnapshot(s.head)
	data, err := os.ReadFile(s.session.snapshotPath(image.ID))
	if err != nil {
		return durable.RunRecord{}, zero, 0, invalidRecord(err)
	}
	digest := sha256.Sum256(data)
	if digest != image.Digest {
		return durable.RunRecord{}, zero, 0, indexError("history evidence origin digest mismatch")
	}
	record, err := recordio.DecodeSnapshot(data)
	if err != nil {
		return durable.RunRecord{}, zero, 0, invalidRecord(err)
	}
	if err := durable.ValidateInitialRecord(record, id); err != nil {
		return durable.RunRecord{}, zero, 0, invalidRecord(err)
	}
	return record, digest, uint64(len(data)), nil
}

func (s *fileHistoryEvidenceSource) HistoryEvidenceBatch(ctx context.Context, id string, revision uint64) ([]byte, [32]byte, [32]byte, [32]byte, error) {
	var zero [32]byte
	if id != s.session.runID || revision < 2 || revision > s.head.HeadRevision {
		return nil, zero, zero, zero, durable.ErrConflict
	}
	ref, err := s.session.locateBatch(ctx, s.head, headBatch(s.head), revision)
	if err != nil {
		return nil, zero, zero, zero, err
	}
	index, err := s.session.readBatchIndex(ref, originBatch(s.head))
	if err != nil {
		return nil, zero, zero, zero, err
	}
	data, err := os.ReadFile(s.session.batchPath(ref.ID))
	if err != nil {
		return nil, zero, zero, zero, invalidRecord(err)
	}
	if uint64(len(data)) != batchIndexSize+index.WireSize {
		return nil, zero, zero, zero, indexError("history evidence batch size mismatch")
	}
	wire := append([]byte(nil), data[batchIndexSize:]...)
	if sha256.Sum256(wire) != index.WireDigest {
		return nil, zero, zero, zero, indexError("history evidence batch digest mismatch")
	}
	return wire, index.WireDigest, index.Parent.Chain, ref.Chain, nil
}

var _ durable.HistoryEvidenceSource = (*fileHistoryEvidenceSource)(nil)
var _ durable.HistoryEvidenceMaterializer = (*Store)(nil)

// MaterializeHistoryEvidence verifies the committed origin and every batch under
// the existing exclusive OS run lock, then publishes one new snapshot/index/head
// representation. It never writes a ChangeBatch and never advances business head.
func (s *Store) MaterializeHistoryEvidence(ctx context.Context, req durable.HistoryEvidenceMaterializeRequest) (state durable.StorageState, err error) {
	if req.RunID == "" || req.ExpectedGeneration == 0 {
		return state, durable.ErrConflict
	}
	ss, err := s.maintenanceSession(ctx, req.RunID)
	if err != nil {
		return state, err
	}
	defer func() { err = errors.Join(err, ss.Close()) }()
	h, err := ss.readHead()
	if err != nil {
		return state, err
	}
	if h.Generation != req.ExpectedGeneration || h.HeadRevision == 0 {
		return state, durable.ErrConflict
	}
	if h.Generation == ^uint64(0) {
		return state, durable.ErrLimitExceeded
	}
	// The selected head is read before verification so the proposal and the
	// final publication compare the same ordinary projection.
	snapshot, err := ss.selectSnapshot(ctx, h, h.HeadRevision)
	if err != nil {
		return state, err
	}
	ordinary, err := ss.replay(ctx, h, snapshot, headBatch(h))
	if err != nil {
		return state, err
	}
	source := &fileHistoryEvidenceSource{session: ss, head: h}
	verified, err := durable.MaterializeHistoryEvidence(ctx, source, req.RunID, req.Bounds)
	if err != nil {
		return state, err
	}
	if verified.Record().Revision != ordinary.Record.Revision || durableRecordDigest(verified.Record()) != durableRecordDigest(ordinary.Record) {
		return state, indexError("history evidence head changed during verification")
	}
	proposal, err := durable.MaterializeHistoryEvidenceProposal(ctx, verified)
	if err != nil {
		return state, err
	}
	return ss.publishHistoryEvidence(ctx, h, proposal)
}

func durableRecordDigest(record durable.RunRecord) [32]byte {
	data, err := recordio.EncodeSnapshot(record)
	if err != nil {
		return [32]byte{}
	}
	return sha256.Sum256(data)
}

func (s *session) publishHistoryEvidence(ctx context.Context, h manifest, proposal durable.HistoryEvidenceMaterialization) (durable.StorageState, error) {
	if proposal.State.Generation != h.Generation || proposal.State.HeadRevision != h.HeadRevision || proposal.State.HeadChain != h.HeadChain {
		return durable.StorageState{}, durable.ErrConflict
	}
	if proposal.Record.Revision != h.HeadRevision {
		return durable.StorageState{}, durable.ErrConflict
	}
	data, err := recordio.EncodeSnapshot(proposal.Record)
	if err != nil {
		return durable.StorageState{}, err
	}
	decoded, err := recordio.DecodeSnapshot(data)
	if err != nil || durableRecordDigest(decoded) != durableRecordDigest(proposal.Record) {
		return durable.StorageState{}, invalidRecord(fmt.Errorf("history evidence snapshot roundtrip: %w", err))
	}
	id, err := newID()
	if err != nil {
		return durable.StorageState{}, err
	}
	if err = s.writeImmutable(ctx, s.snapshotPath(id), data); err != nil {
		return durable.StorageState{}, err
	}
	target := headBatch(h)
	previous := currentSnapshotRef(h)
	skip, err := s.locateSnapshotGeneration(ctx, h, h.Generation+1-lowbit(h.Generation))
	if err != nil {
		return durable.StorageState{}, err
	}
	indexID, err := newID()
	if err != nil {
		return durable.StorageState{}, err
	}
	index := snapshotIndex{ID: indexID, Generation: h.Generation + 1,
		Image: snapshotImage{ID: id, Digest: sha256.Sum256(data), Base: target}, Previous: previous, Skip: skip}
	indexData, err := jsonMarshalSnapshotIndex(index)
	if err != nil {
		return durable.StorageState{}, err
	}
	if err = s.writeImmutable(ctx, s.snapshotIndexPath(indexID), indexData); err != nil {
		return durable.StorageState{}, err
	}
	current, err := s.readHead()
	if err != nil {
		return durable.StorageState{}, err
	}
	if current != h {
		return durable.StorageState{}, durable.ErrConflict
	}
	h.Generation++
	h.BaseRevision, h.BaseID, h.ChainAnchor, h.BaseIndexDigest = target.Revision, target.ID, target.Chain, target.Digest
	h.SnapshotID, h.SnapshotDigest, h.BatchBytes = id, index.Image.Digest, h.HistoryBytes-target.Bytes
	h.SnapshotIndexID, h.SnapshotIndexDigest = indexID, sha256.Sum256(indexData)
	if err = s.publish(ctx, h); err != nil {
		return durable.StorageState{}, err
	}
	s.head, s.record, s.loaded = h, proposal.Record, true
	return storageState(s.runID, h), nil
}

// 保持 JSON 编码细节集中在这个文件，避免物化路径自己构造不同的 index 字节。
func jsonMarshalSnapshotIndex(index snapshotIndex) ([]byte, error) { return json.Marshal(index) }
