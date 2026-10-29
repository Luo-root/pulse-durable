package durable

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"
)

// MemoryStore 以进程内互斥和 CAS 验证 Store 协议，不保证进程退出后的恢复。
// 零值可用；Initialize 复制一次，Apply 只更新变更对象，Load 返回隔离副本。
type MemoryStore struct {
	mu      sync.Mutex
	records map[string]RunRecord
	active  map[string]*memorySession
	chains  map[string][32]byte
	history map[string]*memoryHistory
}

// NewMemoryStore 创建进程内存储。
func NewMemoryStore() *MemoryStore { return &MemoryStore{} }

// Acquire 立即获得 RunID 独占权，已被占用时返回 ErrBusy。
func (s *MemoryStore) Acquire(ctx context.Context, runID string) (Session, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if s == nil || runID == "" {
		return nil, fmt.Errorf("durable: store and run ID are required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.active == nil {
		s.active = make(map[string]*memorySession)
	}
	if _, ok := s.active[runID]; ok {
		return nil, ErrBusy
	}
	session := &memorySession{store: s, runID: runID}
	s.active[runID] = session
	return session, nil
}

type memorySession struct {
	store    *MemoryStore
	runID    string
	closed   bool // 受 store.mu 保护
	revision uint64
	chain    [32]byte
	loaded   bool
}

func (s *memorySession) Load(ctx context.Context) (RunRecord, error) {
	if err := checkContext(ctx); err != nil {
		return RunRecord{}, err
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return RunRecord{}, err
	}
	if s.closed {
		return RunRecord{}, fmt.Errorf("durable: session closed")
	}
	r, ok := s.store.records[s.runID]
	if !ok {
		return RunRecord{}, ErrNotFound
	}
	chain, anchored := s.store.chains[s.runID]
	if !anchored || r.RunID != s.runID || r.Revision == 0 {
		return RunRecord{}, fmt.Errorf("%w: invalid memory checkpoint", ErrInvalidRecord)
	}
	s.revision, s.chain, s.loaded = r.Revision, chain, true
	return cloneRecord(r), nil
}

func (s *memorySession) Initialize(ctx context.Context, r RunRecord) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return fmt.Errorf("durable: session closed")
	}
	if _, exists := s.store.records[s.runID]; exists {
		return ErrConflict
	}
	if err := ValidateInitialRecord(r, s.runID); err != nil {
		return err
	}
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	chain := SeedChain(sha256.Sum256(data))
	if s.store.records == nil {
		s.store.records = make(map[string]RunRecord)
	}
	if s.store.chains == nil {
		s.store.chains = make(map[string][32]byte)
	}
	if s.store.history == nil {
		s.store.history = make(map[string]*memoryHistory)
	}
	s.store.history[s.runID] = &memoryHistory{generation: 1, first: 1, base: 1,
		anchors:   map[uint64][32]byte{1: chain},
		snapshots: map[uint64]RevisionImage{1: {Record: cloneRecord(r), Chain: chain}}}
	s.store.records[s.runID] = cloneRecord(r)
	s.store.chains[s.runID] = chain
	s.revision, s.chain, s.loaded = 1, chain, true
	return nil
}

func (s *memorySession) Apply(ctx context.Context, batch ChangeBatch) (CommitAck, error) {
	if err := checkContext(ctx); err != nil {
		return CommitAck{}, err
	}
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return CommitAck{}, err
	}
	if s.closed {
		return CommitAck{}, fmt.Errorf("durable: session closed")
	}
	r, exists := s.store.records[s.runID]
	if !exists {
		return CommitAck{}, ErrConflict
	}
	// Validate retained history before ApplyTo can mutate shared record maps.
	if _, err := s.store.storageLocked(s.runID); err != nil {
		return CommitAck{}, err
	}
	if !s.loaded || s.revision != r.Revision || s.chain != s.store.chains[s.runID] || batch.Header().RunID != s.runID {
		return CommitAck{}, ErrConflict
	}
	digest, err := ChangeBatchDigest(batch)
	if err != nil {
		return CommitAck{}, err
	}
	next, err := NextChain(s.chain, digest, batch.Header().Revision)
	if err != nil {
		return CommitAck{}, err
	}
	// ApplyTo validates every precondition before mutating only changed objects.
	ack, err := batch.ApplyTo(&r)
	if err != nil {
		return CommitAck{}, err
	}
	h := s.store.history[s.runID]
	h.batches = append(h.batches, batch)
	h.anchors[r.Revision] = next
	s.store.records[s.runID], s.store.chains[s.runID] = r, next
	s.revision, s.chain = r.Revision, next
	return ack, nil
}

func (s *memorySession) Close() error {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()
	if !s.closed {
		s.closed = true
		delete(s.store.active, s.runID)
	}
	return nil
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("durable: nil context")
	}
	return ctx.Err()
}
