package durable

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/Luo-root/pulse"
)

func (s *runState) openChild(ctx context.Context, req pulse.CheckpointChild) ([]pulse.NodeCheckpoint, error) {
	s.mu.Lock()
	records, err := s.openChildLocked(ctx, req)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if err := s.restored(req.Attempt, records); err != nil {
		s.mu.Lock()
		s.stop(err)
		s.mu.Unlock()
		return nil, err
	}
	return records, nil
}

func (s *runState) openChildLocked(ctx context.Context, req pulse.CheckpointChild) ([]pulse.NodeCheckpoint, error) {
	if s.stopped != nil {
		return nil, s.stopped
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	root, _, err := s.rootAttempt(req.Attempt)
	if err != nil {
		return nil, err
	}
	parent, ok := s.record.Scopes[root.Parent]
	parentLoc := location{parent.Graph, parent.Path}
	if !ok || root.NodeID != req.NodeID {
		return nil, fmt.Errorf("durable: child attempt or parent site mismatch")
	}
	if err := s.activeAttempt(req.Attempt); err != nil {
		return nil, err
	}
	childLoc, ok := s.def.graphs[parentLoc].children[req.NodeID]
	if !ok || childLoc.graph != req.Graph || childLoc != (location{req.Site.Graph, req.Site.Path}) {
		return nil, fmt.Errorf("durable: child graph does not match static mount")
	}
	inputHash, err := s.def.inputsFingerprint(childLoc, req.Inputs)
	if err != nil {
		return nil, err
	}
	if scope, exists := s.record.Scopes[req.Attempt]; exists {
		if scope.Graph != childLoc.graph || scope.Path != childLoc.path || scope.Inputs != inputHash {
			return nil, fmt.Errorf("%w: child scope identity or input mismatch", ErrConflict)
		}
	} else {
		proposed := cloneRecord(s.record)
		proposed.Scopes[req.Attempt] = ScopeRecord{Graph: childLoc.graph, Path: childLoc.path, Inputs: inputHash, Nodes: make(map[string]NodeRecord)}
		if err := s.save(ctx, proposed); err != nil {
			return nil, err
		}
	}
	return s.decodeScope(req.Attempt)
}

func (s *runState) commit(ctx context.Context, req pulse.CheckpointCommit) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped != nil {
		return s.stopped
	}
	if err := checkContext(ctx); err != nil {
		return err
	}
	scope, ok := s.record.Scopes[req.Scope]
	if !ok || (location{scope.Graph, scope.Path}) != (location{req.Site.Graph, req.Site.Path}) {
		return fmt.Errorf("durable: commit graph scope mismatch")
	}
	a, ok := s.record.Attempts[req.Attempt]
	if !ok || a.Boundary != "" || a.Parent != req.Scope || a.NodeID != req.Node.NodeID || a.Limit != 1 {
		return fmt.Errorf("durable: commit root attempt or node mismatch")
	}
	if err := s.activeAttempt(req.Attempt); err != nil {
		return err
	}
	if _, exists := scope.Nodes[req.Node.NodeID]; exists {
		return fmt.Errorf("durable: node already confirmed")
	}
	values, err := s.def.encodeOutputs(req.Node.Outputs)
	if err != nil {
		return err
	}
	node := NodeRecord{NodeID: req.Node.NodeID, Attempt: req.Attempt, Reason: string(req.Node.Reason), Outputs: values}
	loc := location{scope.Graph, scope.Path}
	if _, err := s.def.decodeNode(loc, node); err != nil {
		return err
	}
	if err := s.checkDependencies(loc, scope.Nodes, req.Node.NodeID); err != nil {
		return err
	}
	proposed := cloneRecord(s.record)
	ps := proposed.Scopes[req.Scope]
	ps.Nodes[node.NodeID] = node
	proposed.Scopes[req.Scope] = ps
	a.State = AttemptConfirmed
	proposed.Attempts[req.Attempt] = a
	return s.save(ctx, proposed)
}

func (s *runState) decodeScope(id string) ([]pulse.NodeCheckpoint, error) {
	scope, ok := s.record.Scopes[id]
	if !ok {
		return nil, fmt.Errorf("durable: unknown checkpoint scope")
	}
	loc := location{scope.Graph, scope.Path}
	gs, ok := s.def.graphs[loc]
	if !ok {
		return nil, fmt.Errorf("durable: record has an unknown graph site")
	}
	out := make([]pulse.NodeCheckpoint, 0, len(scope.Nodes))
	for _, id := range gs.order {
		if node, exists := scope.Nodes[id]; exists {
			if err := s.checkDependencies(loc, scope.Nodes, id); err != nil {
				return nil, err
			}
			decoded, err := s.def.decodeNode(loc, node)
			if err != nil {
				return nil, err
			}
			out = append(out, decoded)
		}
	}
	if len(out) != len(scope.Nodes) {
		return nil, fmt.Errorf("durable: scope contains unknown node records")
	}
	return out, nil
}

func (s *runState) checkDependencies(loc location, nodes map[string]NodeRecord, nodeID string) error {
	gs := s.def.graphs[loc]
	n, ok := gs.nodes[nodeID]
	if !ok {
		return fmt.Errorf("durable: checkpoint node is unknown")
	}
	for _, k := range n.Requires {
		if producer, internal := gs.producers[k.Name]; internal {
			if _, exists := nodes[producer]; !exists {
				return fmt.Errorf("durable: checkpoint set lacks a required producer")
			}
		}
	}
	return nil
}

func (s *runState) validateLoaded() error {
	r := s.record
	root, ok := r.Scopes[r.RootScope]
	if r.Format != RecordFormat || r.Revision == 0 || !validIdentity(r.RootScope) || !ok ||
		(location{root.Graph, root.Path}) != s.def.root || root.Inputs != r.Inputs || !validDigest(r.Inputs) ||
		r.Scopes == nil || r.Attempts == nil {
		return fmt.Errorf("durable: invalid run record header or root scope")
	}
	if _, collision := r.Attempts[r.RootScope]; collision {
		return fmt.Errorf("durable: root scope collides with an attempt")
	}
	type groupKey struct{ parent, node, boundary string }
	groups := make(map[groupKey]map[int]AttemptRecord)
	for _, id := range sortedKeys(r.Attempts) {
		a := r.Attempts[id]
		if !validIdentity(id) || a.Index < 1 || a.Limit < 1 || a.Index > a.Limit || a.NodeID == "" ||
			(a.State != AttemptActive && a.State != AttemptFailed && a.State != AttemptConfirmed) {
			return fmt.Errorf("durable: invalid persisted attempt")
		}
		if a.Boundary == "" {
			parent, ok := r.Scopes[a.Parent]
			gs := s.def.graphs[location{parent.Graph, parent.Path}]
			if !ok || gs == nil || a.Limit != 1 {
				return fmt.Errorf("durable: invalid persisted root attempt")
			}
			if _, ok := gs.nodes[a.NodeID]; !ok {
				return fmt.Errorf("durable: persisted attempt refers to an unknown node")
			}
			if a.State == AttemptConfirmed {
				n, ok := parent.Nodes[a.NodeID]
				if !ok || n.Attempt != id {
					return fmt.Errorf("durable: confirmed attempt has no complete node record")
				}
			}
		} else {
			p, ok := r.Attempts[a.Parent]
			if !ok || p.NodeID != a.NodeID || a.State == AttemptConfirmed {
				return fmt.Errorf("durable: invalid persisted Retry attempt")
			}
		}
		key := groupKey{a.Parent, a.NodeID, a.Boundary}
		if groups[key] == nil {
			groups[key] = make(map[int]AttemptRecord)
		}
		if _, duplicate := groups[key][a.Index]; duplicate {
			return fmt.Errorf("durable: duplicate persisted attempt index")
		}
		groups[key][a.Index] = a
		if err := s.validateAncestry(id); err != nil {
			return err
		}
	}
	for _, attempts := range groups {
		for i := 1; i <= len(attempts); i++ {
			a, ok := attempts[i]
			if !ok || a.Limit != attempts[1].Limit || (i < len(attempts) && a.State != AttemptFailed) {
				return fmt.Errorf("durable: invalid persisted attempt sequence")
			}
		}
	}
	for _, id := range sortedKeys(r.Scopes) {
		scope := r.Scopes[id]
		loc := location{scope.Graph, scope.Path}
		if !validIdentity(id) || scope.Nodes == nil || !validDigest(scope.Inputs) || s.def.graphs[loc] == nil {
			return fmt.Errorf("durable: invalid persisted graph scope")
		}
		if id != r.RootScope {
			ra, _, err := s.rootAttempt(id)
			if err != nil {
				return err
			}
			parent := r.Scopes[ra.Parent]
			gs := s.def.graphs[location{parent.Graph, parent.Path}]
			if gs == nil || gs.children[ra.NodeID] != loc {
				return fmt.Errorf("durable: persisted child scope does not match its mount")
			}
		}
		for key, n := range scope.Nodes {
			a, ok := r.Attempts[n.Attempt]
			if key != n.NodeID || !ok || a.Parent != id || a.NodeID != key || a.Boundary != "" || a.State != AttemptConfirmed {
				return fmt.Errorf("durable: persisted node root association mismatch")
			}
		}
		// 失败祖先下的旧成果也必须可解码且依赖闭合，但运行时永不复用它们。
		if _, err := s.decodeScope(id); err != nil {
			return err
		}
	}
	return nil
}

// validateAncestry 校验结构（包括失败祖先后面的部分），不把逻辑失效误报成格式损坏。
func (s *runState) validateAncestry(id string) error {
	seen := make(map[string]bool)
	for id != s.record.RootScope {
		if seen[id] {
			return fmt.Errorf("durable: persisted ancestry cycle")
		}
		seen[id] = true
		a, ok := s.record.Attempts[id]
		if !ok {
			return fmt.Errorf("durable: missing persisted attempt ancestor")
		}
		if a.Boundary == "" {
			if _, ok := s.record.Scopes[a.Parent]; !ok {
				return fmt.Errorf("durable: missing persisted graph ancestor")
			}
		} else {
			p, ok := s.record.Attempts[a.Parent]
			if !ok || p.NodeID != a.NodeID {
				return fmt.Errorf("durable: invalid persisted Retry ancestor")
			}
		}
		id = a.Parent
	}
	return nil
}

func (s *runState) restored(scopeID string, records []pulse.NodeCheckpoint) (err error) {
	if s.event == nil || len(records) == 0 {
		return nil
	}
	s.mu.Lock()
	scope := s.record.Scopes[scopeID]
	runID := s.record.RunID
	s.mu.Unlock()
	defer func() {
		if rec := recover(); rec != nil {
			err = operationPanic("recovery event callback", rec)
		}
	}()
	for _, n := range records {
		if err := s.event(Event{Kind: EventRestored, RunID: runID, Scope: scopeID, Graph: scope.Graph, Path: scope.Path, NodeID: n.NodeID}); err != nil {
			return &operationError{op: "recovery event callback", cause: err}
		}
	}
	return nil
}

func validIdentity(id string) bool {
	bytes, err := hex.DecodeString(id)
	return err == nil && len(bytes) == 16
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
