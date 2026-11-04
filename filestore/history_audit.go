package filestore

import (
	"context"
	"crypto/sha256"
	"errors"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
)

var _ durable.HistoryAuditor = (*Store)(nil)

type auditedBatch struct {
	ref   batchRef
	index batchIndex
	batch durable.ChangeBatch
}

// AuditHistory 完整验证权威头引用的原始链、全部快照及两类索引。
// 与精确读取不同，此操作的工作量随完整保留历史增长，且不接受孤儿。
func (s *Store) AuditHistory(ctx context.Context, req durable.AuditHistoryRequest) (result durable.HistoryAuditResult, err error) {
	ss, err := s.maintenanceSession(ctx, req.RunID)
	if err != nil {
		return result, err
	}
	defer func() { err = errors.Join(err, ss.Close()) }()
	h, err := ss.readHead()
	if err != nil {
		return result, err
	}
	images := map[uint64]snapshotImage{1: originSnapshot(h)}
	refs := map[uint64]snapshotRef{1: initialSnapshotRef()}
	var indexes []snapshotIndex
	ref := currentSnapshotRef(h)
	for ref.Generation > 1 {
		if err = checkContext(ctx); err != nil {
			return result, err
		}
		index, e := ss.readSnapshotIndex(ref, h)
		if e != nil {
			return result, e
		}
		if ref == currentSnapshotRef(h) && index.Image != baseSnapshot(h) {
			return result, indexError("current snapshot index mismatch")
		}
		images[ref.Revision], refs[ref.Generation] = index.Image, ref
		indexes = append(indexes, index)
		ref = index.Previous
	}
	if ref != initialSnapshotRef() {
		return result, indexError("incomplete retained snapshot index")
	}
	for _, index := range indexes {
		if index.Skip != refs[index.Skip.Generation] {
			return result, indexError("retained snapshot skip mismatch")
		}
	}
	origin := originBatch(h)
	var reverse []auditedBatch
	batch := headBatch(h)
	for batch.Revision > 1 {
		if err = checkContext(ctx); err != nil {
			return result, err
		}
		b, index, e := ss.readBatch(batch, origin)
		if e != nil {
			return result, e
		}
		reverse = append(reverse, auditedBatch{ref: batch, index: index, batch: b})
		batch = index.Parent
	}
	if batch != origin {
		return result, indexError("incomplete retained original chain")
	}
	for _, b := range reverse {
		want := origin
		if b.index.Skip.Revision > 1 {
			want = reverse[h.HeadRevision-b.index.Skip.Revision].ref
		}
		if b.index.Skip != want {
			return result, indexError("retained batch skip mismatch")
		}
	}
	r, err := ss.readSnapshot(originSnapshot(h))
	if err != nil {
		return result, err
	}
	result.Snapshots = 1
	for i := len(reverse) - 1; i >= 0; i-- {
		if err = checkContext(ctx); err != nil {
			return result, err
		}
		b := reverse[i]
		if _, err = b.batch.ApplyTo(&r); err != nil {
			return result, invalidRecord(err)
		}
		if image, ok := images[r.Revision]; ok {
			if image.Base != b.ref {
				return result, indexError("retained snapshot chain anchor mismatch")
			}
			if _, err = ss.readSnapshot(image); err != nil {
				return result, err
			}
			data, e := recordio.EncodeSnapshot(r)
			if e != nil || sha256.Sum256(data) != image.Digest {
				return result, indexError("retained snapshot projection mismatch")
			}
			result.Snapshots++
		}
	}
	if result.Snapshots != h.Generation || r.Revision != h.HeadRevision {
		return result, indexError("incomplete retained snapshots")
	}
	if err = checkContext(ctx); err != nil {
		return result, err
	}
	result.State = storageState(req.RunID, h)
	result.Batches, result.BatchBytes = h.HeadRevision-1, h.HistoryBytes
	return result, nil
}
