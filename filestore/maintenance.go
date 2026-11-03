package filestore

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
)

var _ durable.Maintenance = (*Store)(nil)

// 维护取得相同的独占 OS RunID 锁，活跃 Session 返回 ErrBusy。
// 不枚举目录清理暂存文件，不与业务写入并行，也不回收历史或孤立对象。
func (s *Store) maintenanceSession(ctx context.Context, id string) (*session, error) {
	ss, err := s.acquire(ctx, id, false)
	if err != nil {
		return nil, err
	}
	return ss.(*session), nil
}
func storageState(id string, h manifest) durable.StorageState {
	return durable.StorageState{RunID: id, Generation: h.Generation, BaseRevision: h.BaseRevision, BaseChain: h.ChainAnchor, HeadRevision: h.HeadRevision, HeadChain: h.HeadChain}
}
func (s *Store) InspectStorage(ctx context.Context, id string) (state durable.StorageState, err error) {
	ss, err := s.maintenanceSession(ctx, id)
	if err != nil {
		return state, err
	}
	defer func() { err = errors.Join(err, ss.Close()) }()
	h, err := ss.readHead()
	if err != nil {
		return state, err
	}
	if err = checkContext(ctx); err != nil {
		return state, err
	}
	return storageState(id, h), nil
}
func (s *Store) LoadRevision(ctx context.Context, id string, revision uint64) (image durable.RevisionImage, err error) {
	ss, err := s.maintenanceSession(ctx, id)
	if err != nil {
		return image, err
	}
	defer func() { err = errors.Join(err, ss.Close()) }()
	h, err := ss.readHead()
	if err != nil {
		return image, err
	}
	if revision == 0 || revision > h.HeadRevision {
		return image, durable.ErrConflict
	}
	snapshot, err := ss.selectSnapshot(ctx, h, revision)
	if err != nil {
		return image, err
	}
	target := snapshot.Base
	if revision != target.Revision {
		target, err = ss.locateBatch(ctx, h, headBatch(h), revision)
		if err != nil {
			return image, err
		}
	}
	return ss.replay(ctx, h, snapshot, target)
}

func (s *Store) CompactStorage(ctx context.Context, req durable.CompactStorageRequest) (state durable.StorageState, err error) {
	if req.ThroughRevision == 0 || req.ExpectedGeneration == 0 {
		return state, durable.ErrConflict
	}
	ss, err := s.maintenanceSession(ctx, req.RunID)
	if err != nil {
		return state, err
	}
	defer func() { err = errors.Join(err, ss.Close()) }()
	return ss.compact(ctx, req)
}

func (s *session) compact(ctx context.Context, req durable.CompactStorageRequest) (durable.StorageState, error) {
	if req.ThroughRevision == 0 || req.ExpectedGeneration == 0 {
		return durable.StorageState{}, durable.ErrConflict
	}
	h, err := s.readHead()
	if err != nil {
		return durable.StorageState{}, err
	}
	if h.Generation != req.ExpectedGeneration || req.ThroughRevision < h.BaseRevision || req.ThroughRevision > h.HeadRevision {
		return durable.StorageState{}, durable.ErrConflict
	}
	if req.ThroughRevision > h.BaseRevision && h.Generation == ^uint64(0) {
		return durable.StorageState{}, durable.ErrLimitExceeded
	}
	snapshot, err := s.selectSnapshot(ctx, h, h.BaseRevision)
	if err != nil {
		return durable.StorageState{}, err
	}
	target := snapshot.Base
	if req.ThroughRevision != h.BaseRevision {
		target, err = s.locateBatch(ctx, h, headBatch(h), req.ThroughRevision)
		if err != nil {
			return durable.StorageState{}, err
		}
	}
	image, err := s.replay(ctx, h, snapshot, target)
	if err != nil {
		return durable.StorageState{}, err
	}
	// 同基线只验证权威头、当前快照及其本代索引，不读批或旧归档。
	// 不写文件、不消耗 generation；即使 generation 耗尽也允许此 no-op。
	if req.ThroughRevision == h.BaseRevision {
		return storageState(req.RunID, h), nil
	}
	if target.Bytes > h.HistoryBytes {
		return durable.StorageState{}, indexError("compaction target byte accounting mismatch")
	}
	data, err := recordio.EncodeSnapshot(image.Record)
	if err != nil {
		return durable.StorageState{}, err
	}
	id, err := newID()
	if err != nil {
		return durable.StorageState{}, err
	}
	if err = s.writeImmutable(ctx, s.snapshotPath(id), data); err != nil {
		return durable.StorageState{}, err
	}
	previous := currentSnapshotRef(h)
	skip, err := s.locateSnapshotGeneration(ctx, h, h.Generation+1-lowbit(h.Generation))
	if err != nil {
		return durable.StorageState{}, err
	}
	indexID, err := newID()
	if err != nil {
		return durable.StorageState{}, err
	}
	index := snapshotIndex{ID: indexID, Generation: h.Generation + 1, Image: snapshotImage{ID: id, Digest: sha256.Sum256(data), Base: target}, Previous: previous, Skip: skip}
	indexData, err := json.Marshal(index)
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
	h.BaseRevision, h.BaseID, h.ChainAnchor, h.BaseIndexDigest = req.ThroughRevision, target.ID, image.Chain, target.Digest
	h.SnapshotID, h.SnapshotDigest, h.BatchBytes = id, index.Image.Digest, h.HistoryBytes-target.Bytes
	h.SnapshotIndexID, h.SnapshotIndexDigest = indexID, sha256.Sum256(indexData)
	if err = s.publish(ctx, h); err != nil {
		return durable.StorageState{}, err
	}
	return storageState(req.RunID, h), nil
}
