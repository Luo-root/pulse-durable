package durable

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"sync"

	"github.com/Luo-root/pulse"
)

type runState struct {
	mu      sync.Mutex
	session Session
	def     *definition
	record  RunRecord
	stopped error
	event   func(Event) error
	opened  map[string]bool
	// 同一 Resume 的各层恢复计划共用首次核验的权威描述符，后续 Open 升代际
	// 不得为晚装配的子图别名制造另一份本地消息账。
	restoreStreams map[string]StreamDescriptorRecord
}

func (s *runState) hooks() pulse.CheckpointHooks {
	return pulse.CheckpointHooks{OpenAttempt: s.openAttempt, FailAttempt: s.failAttempt, OpenChild: s.openChild, Commit: s.commit, StreamHooks: s.streamHooks()}
}

// save 的调用方串行持有 mu，首次保存发生在业务启动前。
// proposed 与当前内存记录隔离，只有 Save 成功才推进内存；不确定错误锁存并停止。
func (s *runState) save(ctx context.Context, proposed RunRecord) (err error) {
	if s.stopped != nil {
		return s.stopped
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	if s.record.Revision == math.MaxUint64 {
		return s.stop(fmt.Errorf("%w: revision exhausted", ErrConflict))
	}
	proposed.Format = RecordFormat
	proposed.Revision = s.record.Revision + 1
	// panic(nil) 在兼容模式下 recover 为 nil，须按调用是否正常返回判定。
	// Store panic 沿原契约不保留原值；Save 异常仍须锁存 stopped。
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			err = s.stop(fmt.Errorf("durable: store save panicked"))
		}
	}()
	err = s.session.Save(ctx, cloneRecord(proposed), s.record.Revision)
	returned = true
	if err != nil {
		return s.stop(&operationError{op: "store save", cause: err})
	}
	s.record = cloneRecord(proposed)
	return nil
}

func (s *runState) stop(err error) error {
	if s.stopped == nil {
		s.stopped = err
	}
	return s.stopped
}

func (s *runState) openAttempt(ctx context.Context, req pulse.CheckpointAttemptRequest) (pulse.CheckpointAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped != nil {
		return pulse.CheckpointAttempt{}, s.stopped
	}
	if err := checkContext(ctx); err != nil {
		return pulse.CheckpointAttempt{}, err
	}
	loc := location{req.Site.Graph, req.Site.Path}
	gs, ok := s.def.graphs[loc]
	if !ok {
		return pulse.CheckpointAttempt{}, fmt.Errorf("durable: attempt has an unknown graph site")
	}
	if _, ok := gs.nodes[req.NodeID]; !ok {
		return pulse.CheckpointAttempt{}, fmt.Errorf("durable: attempt has an unknown node")
	}
	if req.Boundary == "" {
		parent, ok := s.record.Scopes[req.Parent]
		if !ok || req.Limit != 1 || (location{parent.Graph, parent.Path}) != loc {
			return pulse.CheckpointAttempt{}, fmt.Errorf("durable: invalid root attempt parent or limit")
		}
		if err := s.activeScope(req.Parent); err != nil {
			return pulse.CheckpointAttempt{}, err
		}
	} else {
		parent, ok := s.record.Attempts[req.Parent]
		if !ok || req.Limit <= 0 || parent.NodeID != req.NodeID {
			return pulse.CheckpointAttempt{}, fmt.Errorf("durable: invalid Retry attempt parent or limit")
		}
		root, _, err := s.rootAttempt(req.Parent)
		if err != nil {
			return pulse.CheckpointAttempt{}, err
		}
		scope := s.record.Scopes[root.Parent]
		if (location{scope.Graph, scope.Path}) != loc {
			return pulse.CheckpointAttempt{}, fmt.Errorf("durable: Retry attempt site mismatch")
		}
		if err := s.activeAttempt(req.Parent); err != nil {
			return pulse.CheckpointAttempt{}, err
		}
	}
	var latest AttemptRecord
	var latestID string
	for id, a := range s.record.Attempts {
		if a.Parent == req.Parent && a.NodeID == req.NodeID && a.Boundary == req.Boundary {
			if a.Limit != req.Limit {
				return pulse.CheckpointAttempt{}, fmt.Errorf("%w: attempt limit mismatch", ErrConflict)
			}
			if a.Index > latest.Index {
				latestID, latest = id, a
			}
		}
	}
	if latestID != "" {
		switch latest.State {
		case AttemptActive:
			attempt := pulse.CheckpointAttempt{Scope: latestID, Index: latest.Index}
			if req.Boundary == "" {
				if err := s.activeAttempt(latestID); err != nil {
					return pulse.CheckpointAttempt{}, err
				}
				values, err := s.restoreAttemptValues(req.Parent, latestID, req.NodeID)
				if err != nil {
					return pulse.CheckpointAttempt{}, err
				}
				attempt.Values = values
			}
			return attempt, nil
		case AttemptFailed:
			if latest.Index >= req.Limit {
				return pulse.CheckpointAttempt{Scope: latestID, Index: latest.Index, Exhausted: true}, nil
			}
		case AttemptConfirmed:
			return pulse.CheckpointAttempt{}, fmt.Errorf("durable: confirmed node attempt cannot be reopened")
		default:
			return pulse.CheckpointAttempt{}, fmt.Errorf("durable: invalid attempt state")
		}
	}
	id, err := s.newID()
	if err != nil {
		return pulse.CheckpointAttempt{}, err
	}
	proposed := cloneRecord(s.record)
	a := AttemptRecord{Parent: req.Parent, NodeID: req.NodeID, Boundary: req.Boundary,
		Index: latest.Index + 1, Limit: req.Limit, State: AttemptActive}
	proposed.Attempts[id] = a
	if err := s.save(ctx, proposed); err != nil {
		return pulse.CheckpointAttempt{}, err
	}
	return pulse.CheckpointAttempt{Scope: id, Index: a.Index}, nil
}

func (s *runState) failAttempt(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped != nil {
		return s.stopped
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	a, ok := s.record.Attempts[id]
	if !ok {
		return fmt.Errorf("durable: unknown failed attempt")
	}
	if a.State == AttemptFailed {
		return nil
	}
	if err := s.activeAttempt(id); err != nil {
		return err
	}
	proposed := cloneRecord(s.record)
	a.State = AttemptFailed
	proposed.Attempts[id] = a
	return s.save(ctx, proposed)
}

func (s *runState) rootAttempt(id string) (AttemptRecord, string, error) {
	seen := make(map[string]bool)
	for {
		if seen[id] {
			return AttemptRecord{}, "", fmt.Errorf("durable: attempt ancestry cycle")
		}
		seen[id] = true
		a, ok := s.record.Attempts[id]
		if !ok {
			return AttemptRecord{}, "", fmt.Errorf("durable: missing attempt ancestor")
		}
		if a.Boundary == "" {
			return a, id, nil
		}
		parent, ok := s.record.Attempts[a.Parent]
		if !ok || parent.NodeID != a.NodeID {
			return AttemptRecord{}, "", fmt.Errorf("durable: Retry ancestor node mismatch")
		}
		id = a.Parent
	}
}

func (s *runState) activeScope(id string) error {
	if id == s.record.RootScope {
		return nil
	}
	return s.activeAttempt(id)
}

// ancestry 沿 Retry → 节点根 → 子图 Scope → 父 Attempt 检查整条链。
// activeOnly 在变更时要求全部仍活跃；读取时允许已确认祖先，但失败永不有效。
func (s *runState) ancestry(id string, activeOnly bool) error {
	seen := make(map[string]bool)
	for id != s.record.RootScope {
		if seen[id] {
			return fmt.Errorf("durable: persistent ancestry cycle")
		}
		seen[id] = true
		a, ok := s.record.Attempts[id]
		if !ok || a.State == AttemptFailed || (activeOnly && a.State != AttemptActive) {
			return fmt.Errorf("durable: inactive attempt ancestry")
		}
		if a.Boundary == "" {
			if _, ok := s.record.Scopes[a.Parent]; !ok {
				return fmt.Errorf("durable: missing graph scope ancestor")
			}
		}
		id = a.Parent
	}
	return nil
}

func (s *runState) activeAttempt(id string) error { return s.ancestry(id, true) }

func randomID() (string, error) {
	var data [16]byte
	if _, err := rand.Read(data[:]); err != nil {
		return "", fmt.Errorf("durable: random identity generation failed")
	}
	return hex.EncodeToString(data[:]), nil
}

func (s *runState) newID() (string, error) {
	for range 3 {
		id, err := randomID()
		if err != nil {
			return "", err
		}
		_, scopeExists := s.record.Scopes[id]
		_, attemptExists := s.record.Attempts[id]
		_, messageExists := s.record.Messages[id]
		if !scopeExists && !attemptExists && !messageExists {
			return id, nil
		}
	}
	return "", fmt.Errorf("durable: persistent identity collision")
}
