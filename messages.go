package durable

import (
	"context"
	"fmt"
	"reflect"
	"sort"

	"github.com/Luo-root/pulse"
)

func (s *runState) processingAttempt(scope, parent, node string, site pulse.Site) error {
	root, _, err := s.rootAttempt(parent)
	if err != nil {
		return err
	}
	graph, ok := s.record.Scopes[scope]
	if !ok || root.Parent != scope || root.NodeID != node || (location{graph.Graph, graph.Path}) != (location{site.Graph, site.Path}) {
		return fmt.Errorf("durable: message processing attempt or graph mismatch")
	}
	return s.activeAttempt(parent)
}

func sameMessage(a, b MessageAttemptRecord) bool {
	return a.Scope == b.Scope && a.Parent == b.Parent && a.NodeID == b.NodeID && streamKey(a.Input) == streamKey(b.Input) && a.Sequence == b.Sequence
}

func (s *runState) openMessage(ctx context.Context, req pulse.CheckpointMessageAttemptRequest) (pulse.CheckpointMessageAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.streamReady(ctx); err != nil {
		return pulse.CheckpointMessageAttempt{}, err
	}
	if err := s.processingAttempt(req.Scope, req.Attempt, req.NodeID, req.Site); err != nil {
		return pulse.CheckpointMessageAttempt{}, err
	}
	if !s.nodeReadsStream(req.Scope, req.NodeID, streamID(req.Input.Stream)) {
		return pulse.CheckpointMessageAttempt{}, fmt.Errorf("durable: message source is not a declared input")
	}
	if req.Limit < 1 {
		return pulse.CheckpointMessageAttempt{}, fmt.Errorf("durable: message retry limit must be positive")
	}
	// 在副本上验证当前输入；Open 不确认它。
	probe := cloneRecord(s.record)
	if err := s.acknowledge(&probe, req.Input); err != nil {
		return pulse.CheckpointMessageAttempt{}, err
	}
	candidate := MessageAttemptRecord{Scope: req.Scope, Parent: req.Attempt, NodeID: req.NodeID, Input: streamID(req.Input.Stream), Sequence: req.Input.Sequence, Limit: req.Limit, State: AttemptActive}
	var latest MessageAttemptRecord
	var latestID string
	for id, existing := range s.record.Messages {
		if sameMessage(existing, candidate) {
			if existing.Limit != req.Limit {
				return pulse.CheckpointMessageAttempt{}, fmt.Errorf("%w: message retry limit mismatch", ErrConflict)
			}
			if existing.Index > latest.Index {
				latestID, latest = id, existing
			}
		}
	}
	state, err := s.decodeManagedState(s.record.Managed[managedKey(req.Scope, req.Attempt, req.NodeID)].State)
	if err != nil {
		return pulse.CheckpointMessageAttempt{}, err
	}
	if latestID != "" {
		if latest.State == AttemptConfirmed {
			return pulse.CheckpointMessageAttempt{}, fmt.Errorf("durable: confirmed message cannot reopen")
		}
		if latest.State == AttemptFailed && latest.Index == latest.Limit {
			return pulse.CheckpointMessageAttempt{Attempt: pulse.CheckpointAttempt{Scope: latestID, Index: latest.Index, Exhausted: true}, State: state}, nil
		}
		if latest.State == AttemptActive {
			if latest.Input != candidate.Input {
				proposed := cloneRecord(s.record)
				latest.Input = candidate.Input
				proposed.Messages[latestID] = latest
				if err := s.save(ctx, proposed); err != nil {
					return pulse.CheckpointMessageAttempt{}, err
				}
			}
			return pulse.CheckpointMessageAttempt{Attempt: pulse.CheckpointAttempt{Scope: latestID, Index: latest.Index}, State: state}, nil
		}
	}
	id, err := s.newID()
	if err != nil {
		return pulse.CheckpointMessageAttempt{}, err
	}
	candidate.Index = latest.Index + 1
	proposed := cloneRecord(s.record)
	if proposed.Messages == nil {
		proposed.Messages = make(map[string]MessageAttemptRecord)
	}
	proposed.Messages[id] = candidate
	if err := s.save(ctx, proposed); err != nil {
		return pulse.CheckpointMessageAttempt{}, err
	}
	return pulse.CheckpointMessageAttempt{Attempt: pulse.CheckpointAttempt{Scope: id, Index: candidate.Index}, State: state}, nil
}

func (s *runState) decodeManagedState(values []ValueRecord) ([]pulse.CheckpointState, error) {
	out := make([]pulse.CheckpointState, 0, len(values))
	seen := make(map[string]bool)
	for _, value := range values {
		if value.Name == "" || seen[value.Name] {
			return nil, fmt.Errorf("durable: duplicate or empty state name")
		}
		seen[value.Name] = true
		decoded, err := s.def.decodePayload(value)
		if err != nil {
			return nil, err
		}
		out = append(out, decoded)
	}
	return out, nil
}

func (s *runState) failMessage(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.streamReady(ctx); err != nil {
		return err
	}
	message, ok := s.record.Messages[id]
	if !ok {
		return fmt.Errorf("durable: unknown message attempt")
	}
	if err := s.activeAttempt(message.Parent); err != nil {
		return err
	}
	if _, _, err := s.currentStream(s.record, message.Input.checkpoint(), false); err != nil {
		return err
	}
	if message.State == AttemptFailed {
		return nil
	}
	if message.State != AttemptActive {
		return fmt.Errorf("durable: confirmed message cannot fail")
	}
	proposed := cloneRecord(s.record)
	message.State = AttemptFailed
	proposed.Messages[id] = message
	return s.save(ctx, proposed)
}

func (s *runState) commitMessage(ctx context.Context, req pulse.CheckpointMessageCommit) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.streamReady(ctx); err != nil {
		return err
	}
	proposed := cloneRecord(s.record)
	if req.Attempt == "" {
		for _, input := range req.Inputs {
			if input.Stream.Scope != req.Scope {
				return fmt.Errorf("durable: manual Ack scope mismatch")
			}
		}
		for _, stop := range req.Stops {
			if stop.Scope != req.Scope {
				return fmt.Errorf("durable: manual Stop scope mismatch")
			}
		}
		if len(req.State)+len(req.Values)+len(req.Outputs) != 0 || len(req.Inputs)+len(req.Stops) == 0 {
			return fmt.Errorf("durable: manual acknowledgement carries processing results or no input")
		}
	} else {
		message, ok := s.record.Messages[req.Attempt]
		if !ok || message.State != AttemptActive || message.Scope != req.Scope {
			return fmt.Errorf("durable: invalid message commit attempt")
		}
		if err := s.processingAttempt(req.Scope, message.Parent, message.NodeID, req.Site); err != nil {
			return err
		}
		if len(req.Inputs) != 1 || len(req.Stops) != 0 || streamID(req.Inputs[0].Stream) != message.Input || req.Inputs[0].Sequence != message.Sequence {
			return fmt.Errorf("durable: message commit does not confirm its input")
		}
		key := managedKey(req.Scope, message.Parent, message.NodeID)
		managed := proposed.Managed[key]
		managed.Scope, managed.Parent, managed.NodeID = req.Scope, message.Parent, message.NodeID
		oldTypes := make(map[string]reflect.Type)
		previous, err := s.decodeManagedState(managed.State)
		if err != nil {
			return err
		}
		for _, state := range previous {
			oldTypes[state.Name] = state.Type
		}
		managed.State = nil
		seen := make(map[string]bool)
		for _, state := range req.State {
			if state.Name == "" || seen[state.Name] || oldTypes[state.Name] != nil && oldTypes[state.Name] != state.Type {
				return fmt.Errorf("durable: invalid state name or type change")
			}
			seen[state.Name] = true
			value, err := s.def.encodePayload(state.Name, state.Type, state.Value)
			if err != nil {
				return err
			}
			managed.State = append(managed.State, value)
		}
		sort.Slice(managed.State, func(i, j int) bool { return managed.State[i].Name < managed.State[j].Name })
		node := s.def.graphs[location{req.Site.Graph, req.Site.Path}].nodes[message.NodeID]
		declared := make(map[string]pulse.CheckpointKey)
		for _, key := range node.Provides {
			declared[key.Name] = key
		}
		for _, value := range managed.Values {
			delete(declared, value.Name)
		}
		for _, value := range req.Values {
			if key, ok := declared[value.Key.Name]; !ok || key != value.Key || key.Element != nil {
				return fmt.Errorf("durable: duplicate, undeclared or stream message value")
			}
			delete(declared, value.Key.Name)
		}
		values, err := s.def.encodeOutputs(req.Values)
		if err != nil {
			return err
		}
		managed.Values = append(managed.Values, values...)
		sort.Slice(managed.Values, func(i, j int) bool { return managed.Values[i].Name < managed.Values[j].Name })
		for _, output := range req.Outputs {
			root, _, err := s.rootAttempt(output.Attempt)
			owner, _, ownerErr := s.rootAttempt(message.Parent)
			if err != nil || ownerErr != nil || root != owner || output.Stream.Scope != req.Scope || output.Stream.NodeID != message.NodeID || streamKey(streamID(output.Stream)) == streamKey(message.Input) {
				return fmt.Errorf("durable: message output has a different processing owner")
			}
		}
		if proposed.Managed == nil {
			proposed.Managed = make(map[string]ManagedRecord)
		}
		proposed.Managed[key] = managed
		message.State = AttemptConfirmed
		proposed.Messages[req.Attempt] = message
	}
	for _, input := range req.Inputs {
		if err := s.acknowledge(&proposed, input); err != nil {
			return err
		}
	}
	for _, output := range req.Outputs {
		if err := s.appendTo(&proposed, output); err != nil {
			return err
		}
	}
	for _, stop := range req.Stops {
		key, stream, err := s.currentStream(proposed, stop, false)
		if err != nil {
			return err
		}
		stream.Stream.Stopped, stream.Pending = true, nil
		proposed.Streams[key] = stream
	}
	return s.save(ctx, proposed)
}
