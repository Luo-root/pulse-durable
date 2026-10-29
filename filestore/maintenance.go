package filestore

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
)

var _ durable.Maintenance = (*Store)(nil)

// All local maintenance operations acquire the same exclusive OS RunID lock as
// business sessions. An active Session causes ErrBusy; local synthesis does not
// run concurrently with business writes. No history or orphan is reclaimed.
func (s *Store) maintenanceSession(ctx context.Context, id string) (*session, error) {
	ss, err := s.Acquire(ctx, id)
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
	image, _, _, err = ss.history(ctx, h, revision)
	return image, err
}

// history authenticates the complete retained original chain against the current
// head, including the current synthesized base anchor. It never selects files by
// directory ordering, and missing retained components are corruption, not expiry.
func (s *session) history(ctx context.Context, h manifest, target uint64) (image durable.RevisionImage, targetID string, suffixBytes uint64, err error) {
	if target == 0 || target > h.HeadRevision {
		return image, "", 0, durable.ErrConflict
	}
	invalid := func(e error) error { return errors.Join(durable.ErrInvalidRecord, e) }
	data, err := os.ReadFile(s.snapshotPath(h.OriginID))
	if err != nil {
		return image, "", 0, invalid(err)
	}
	if sha256.Sum256(data) != h.OriginDigest {
		return image, "", 0, invalid(errors.New("filestore: original snapshot digest mismatch"))
	}
	r, err := recordio.DecodeSnapshot(data)
	if err != nil {
		return image, "", 0, invalid(err)
	}
	if err = durable.ValidateInitialRecord(r, s.runID); err != nil {
		return image, "", 0, invalid(err)
	}
	var reverse []durable.ChangeBatch
	chain, id := h.HeadChain, h.HeadID
	var total uint64
	var targetChain [32]byte
	for rev := h.HeadRevision; rev > 1; rev-- {
		if err = checkContext(ctx); err != nil {
			return image, "", 0, err
		}
		if !validID(id) {
			return image, "", 0, invalid(errors.New("filestore: invalid history identity"))
		}
		wire, e := os.ReadFile(s.batchPath(id))
		if e != nil {
			return image, "", 0, invalid(e)
		}
		if uint64(len(wire)) > ^uint64(0)-total {
			return image, "", 0, durable.ErrLimitExceeded
		}
		total += uint64(len(wire))
		if rev > target {
			suffixBytes += uint64(len(wire))
		}
		var env envelope
		if e = json.Unmarshal(wire, &env); e != nil {
			return image, "", 0, invalid(e)
		}
		digest := sha256.Sum256(env.Wire)
		next, e := durable.NextChain(env.ParentChain, digest, rev)
		if e != nil || env.ParentRevision != rev-1 || env.Digest != digest || env.Chain != chain || next != chain {
			return image, "", 0, invalid(errors.New("filestore: invalid retained chain"))
		}
		b, e := durable.UnmarshalChangeBatch(env.Wire)
		if e != nil {
			return image, "", 0, invalid(e)
		}
		canonical, e := durable.MarshalChangeBatch(b)
		if e != nil || string(canonical) != string(env.Wire) || b.Header().Revision != rev {
			return image, "", 0, invalid(errors.New("filestore: noncanonical retained batch"))
		}
		if rev == h.BaseRevision && (chain != h.ChainAnchor || id != h.BaseID) {
			return image, "", 0, invalid(errors.New("filestore: base history mismatch"))
		}
		if rev == target {
			targetID, targetChain = id, chain
		}
		reverse = append(reverse, b)
		chain, id = env.ParentChain, env.ParentID
	}
	if id != "" || chain != durable.SeedChain(h.OriginDigest) || total != h.HistoryBytes {
		return image, "", 0, invalid(errors.New("filestore: incomplete retained history"))
	}
	if target == 1 {
		image = durable.RevisionImage{Record: durable.CloneRecord(r), Chain: chain}
	}
	for i := len(reverse) - 1; i >= 0; i-- {
		if err = checkContext(ctx); err != nil {
			return image, "", 0, err
		}
		if _, err = reverse[i].ApplyTo(&r); err != nil {
			return image, "", 0, invalid(err)
		}
		if r.Revision == h.BaseRevision {
			base, e := recordio.EncodeSnapshot(r)
			if e != nil || sha256.Sum256(base) != h.SnapshotDigest {
				return image, "", 0, invalid(errors.New("filestore: synthesized projection mismatch"))
			}
		}
		if r.Revision == target {
			image = durable.RevisionImage{Record: durable.CloneRecord(r), Chain: targetChain}
		}
	}
	if err = checkContext(ctx); err != nil {
		return image, "", 0, err
	}
	return image, targetID, suffixBytes, nil
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
	h, err := ss.readHead()
	if err != nil {
		return state, err
	}
	if h.Generation != req.ExpectedGeneration || req.ThroughRevision < h.BaseRevision || req.ThroughRevision > h.HeadRevision {
		return state, durable.ErrConflict
	}
	if h.Generation == ^uint64(0) {
		return state, durable.ErrLimitExceeded
	}
	image, baseID, suffix, err := ss.history(ctx, h, req.ThroughRevision)
	if err != nil {
		return state, err
	}
	if req.ThroughRevision == h.BaseRevision {
		return storageState(req.RunID, h), nil
	}
	data, err := recordio.EncodeSnapshot(image.Record)
	if err != nil {
		return state, err
	}
	id, err := newID()
	if err != nil {
		return state, err
	}
	if err = ss.writeImmutable(ctx, ss.snapshotPath(id), data); err != nil {
		return state, err
	}
	current, err := ss.readHead()
	if err != nil {
		return state, err
	}
	if current != h {
		return state, durable.ErrConflict
	}
	h.Generation++
	h.BaseRevision, h.BaseID, h.ChainAnchor = req.ThroughRevision, baseID, image.Chain
	h.SnapshotID, h.SnapshotDigest, h.BatchBytes = id, sha256.Sum256(data), suffix
	if err = ss.publish(ctx, h); err != nil {
		return state, err
	}
	return storageState(req.RunID, h), nil
}
