package durable

import (
	"context"
	"fmt"
	"math"
	"unicode/utf8"
)

type memoryHistory struct {
	generation uint64
	first      uint64 // explicit oldest retained revision; never inferred from missing data
	base       uint64
	anchors    map[uint64][32]byte
	snapshots  map[uint64]RevisionImage
	batches    []ChangeBatch
}

var _ Maintenance = (*MemoryStore)(nil)

func (s *MemoryStore) storageLocked(id string) (StorageState, error) {
	r, ok := s.records[id]
	if !ok {
		return StorageState{}, ErrNotFound
	}
	h := s.history[id]
	if h == nil || h.generation == 0 || h.first == 0 || h.base < h.first || h.base > r.Revision ||
		r.RunID != id || r.Revision < h.first || r.Revision-h.first != uint64(len(h.batches)) {
		return StorageState{}, fmt.Errorf("%w: invalid memory history head", ErrInvalidRecord)
	}
	chain, anchored := s.chains[id]
	head, headOK := h.anchors[r.Revision]
	base, ok := h.snapshots[h.base]
	baseChain, baseOK := h.anchors[h.base]
	if !anchored || !headOK || head != chain || !ok || !baseOK ||
		base.Record.RunID != id || base.Record.Revision != h.base || base.Chain != baseChain {
		return StorageState{}, fmt.Errorf("%w: invalid memory anchor", ErrInvalidRecord)
	}
	return StorageState{RunID: id, Generation: h.generation, BaseRevision: h.base,
		BaseChain: base.Chain, HeadRevision: r.Revision, HeadChain: s.chains[id]}, nil
}

func maintenanceContext(ctx context.Context, s *MemoryStore, id string) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	if s == nil || id == "" || !utf8.ValidString(id) {
		return fmt.Errorf("durable: maintenance requires store and UTF-8 run ID")
	}
	return nil
}

func (s *MemoryStore) InspectStorage(ctx context.Context, id string) (StorageState, error) {
	if err := maintenanceContext(ctx, s, id); err != nil {
		return StorageState{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return StorageState{}, err
	}
	return s.storageLocked(id)
}

// LoadRevision captures immutable batches and an isolated nearest snapshot under
// the store lock. Replaying runs outside that lock, independently of writers.
func (s *MemoryStore) LoadRevision(ctx context.Context, id string, revision uint64) (RevisionImage, error) {
	if err := maintenanceContext(ctx, s, id); err != nil {
		return RevisionImage{}, err
	}
	if revision == 0 {
		return RevisionImage{}, ErrConflict
	}
	s.mu.Lock()
	state, err := s.storageLocked(id)
	if err != nil {
		s.mu.Unlock()
		return RevisionImage{}, err
	}
	if revision > state.HeadRevision {
		s.mu.Unlock()
		return RevisionImage{}, ErrConflict
	}
	h := s.history[id]
	if revision < h.first {
		s.mu.Unlock()
		return RevisionImage{}, ErrHistoryExpired
	}
	expected, anchored := h.anchors[revision]
	var base RevisionImage
	for rev, image := range h.snapshots {
		anchor, ok := h.anchors[rev]
		if rev < h.first || rev > state.HeadRevision || image.Record.RunID != id || image.Record.Revision != rev || !ok || image.Chain != anchor {
			s.mu.Unlock()
			return RevisionImage{}, fmt.Errorf("%w: invalid retained snapshot", ErrInvalidRecord)
		}
		if rev <= revision && rev > base.Record.Revision {
			base = image
		}
	}
	if !anchored || base.Record.Revision < h.first || base.Record.Revision > revision ||
		revision-h.first > uint64(len(h.batches)) {
		s.mu.Unlock()
		return RevisionImage{}, fmt.Errorf("%w: incomplete memory history", ErrInvalidRecord)
	}
	base.Record = CloneRecord(base.Record)
	batches := append([]ChangeBatch(nil), h.batches[base.Record.Revision-h.first:revision-h.first]...)
	s.mu.Unlock()
	for _, batch := range batches {
		if err := ctx.Err(); err != nil {
			return RevisionImage{}, err
		}
		digest, err := ChangeBatchDigest(batch)
		if err != nil {
			return RevisionImage{}, fmt.Errorf("%w: replay: %v", ErrInvalidRecord, err)
		}
		chain, err := NextChain(base.Chain, digest, batch.Header().Revision)
		if err != nil {
			return RevisionImage{}, fmt.Errorf("%w: replay: %v", ErrInvalidRecord, err)
		}
		if _, err := batch.ApplyTo(&base.Record); err != nil {
			return RevisionImage{}, fmt.Errorf("%w: replay: %v", ErrInvalidRecord, err)
		}
		base.Chain = chain
	}
	if err := ctx.Err(); err != nil {
		return RevisionImage{}, err
	}
	if base.Record.RunID != id || base.Record.Revision != revision || base.Chain != expected {
		return RevisionImage{}, fmt.Errorf("%w: memory history chain mismatch", ErrInvalidRecord)
	}
	return base, nil
}

func (s *MemoryStore) CompactStorage(ctx context.Context, request CompactStorageRequest) (StorageState, error) {
	state, err := s.InspectStorage(ctx, request.RunID)
	if err != nil {
		return StorageState{}, err
	}
	if request.ExpectedGeneration == 0 || request.ExpectedGeneration != state.Generation ||
		request.ThroughRevision < state.BaseRevision || request.ThroughRevision > state.HeadRevision {
		return StorageState{}, ErrConflict
	}
	if state.Generation == math.MaxUint64 {
		return StorageState{}, ErrLimitExceeded
	}
	image, err := s.LoadRevision(ctx, request.RunID, request.ThroughRevision)
	if err != nil {
		return StorageState{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return StorageState{}, err
	}
	current, err := s.storageLocked(request.RunID)
	if err != nil {
		return StorageState{}, err
	}
	if current.Generation != state.Generation {
		return StorageState{}, ErrConflict
	}
	h := s.history[request.RunID]
	h.snapshots[request.ThroughRevision] = image
	h.base = request.ThroughRevision
	h.generation++
	return s.storageLocked(request.RunID)
}
