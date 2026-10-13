package durable

import (
	"context"
	"fmt"
	"math"
	"sync"
)

// MemoryStore 以进程内互斥和 CAS 验证 Store 协议，不保证进程退出后的恢复。
// 零值可用；Load 与 Save 均复制记录，调用方修改副本不会污染存储。
type MemoryStore struct {
	mu      sync.Mutex
	records map[string]RunRecord
	active  map[string]*memorySession
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
	store  *MemoryStore
	runID  string
	closed bool // 受 store.mu 保护
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
	return cloneRecord(r), nil
}

func (s *memorySession) Save(ctx context.Context, r RunRecord, expected uint64) error {
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
	old, exists := s.store.records[s.runID]
	if r.RunID != s.runID || expected == math.MaxUint64 || r.Revision != expected+1 ||
		(!exists && expected != 0) || (exists && old.Revision != expected) {
		return ErrConflict
	}
	if s.store.records == nil {
		s.store.records = make(map[string]RunRecord)
	}
	s.store.records[s.runID] = cloneRecord(r)
	return nil
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
