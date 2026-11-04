package durable

import (
	"context"
	"fmt"
	"reflect"

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

type messageGroupKey struct {
	scope, parent, node string
	stream              [4]string
	sequence            uint64
}

type messageIndexEntry struct {
	id    string
	index int
}

func messageGroup(message MessageAttemptRecord) messageGroupKey {
	input := message.Input
	// 与 streamKey 使用相同四字段；Generation 不参与消息 Retry 预算分组。
	return messageGroupKey{scope: message.Scope, parent: message.Parent, node: message.NodeID,
		stream: [4]string{input.Scope, input.NodeID, input.Key, input.Group}, sequence: message.Sequence}
}

// rebuildMessageIndex 在权威记录加载校验或初始化确认后调用，要求持有 s.mu。
func (s *runState) rebuildMessageIndex() error {
	index := make(map[messageGroupKey]messageIndexEntry, len(s.record.Messages))
	for id, message := range s.record.Messages {
		key := messageGroup(message)
		current, exists := index[key]
		if exists && s.record.Messages[current.id].Limit != message.Limit {
			return fmt.Errorf("%w: message retry limit mismatch", ErrConflict)
		}
		if !exists || message.Index > current.index || (message.Index == current.index && id > current.id) {
			index[key] = messageIndexEntry{id: id, index: message.Index}
		}
	}
	s.messageIndex, s.messageIndexReady = index, true
	return nil
}

// updateMessageIndex 只消费已确认批次；消息归属和 Index 不会被检查点钩子改变。
// 直接构造的 runState 可保持未初始化，由首次消息查询统一重建。
func (s *runState) updateMessageIndex(tx *recordTransaction) {
	if !s.messageIndexReady {
		return
	}
	for id, op := range tx.messages {
		key := messageGroup(op.value)
		current, exists := s.messageIndex[key]
		if !exists || op.value.Index > current.index || (op.value.Index == current.index && id > current.id) {
			s.messageIndex[key] = messageIndexEntry{id: id, index: op.value.Index}
		}
	}
}

func (s *runState) indexedMessage(candidate MessageAttemptRecord) (string, MessageAttemptRecord, error) {
	if s.stopped != nil {
		return "", MessageAttemptRecord{}, s.stopped
	}
	if !s.messageIndexReady {
		// 兼容根包测试直接构造的状态；生产 Run/Resume 在启动前已完成初始化。
		if err := s.rebuildMessageIndex(); err != nil {
			return "", MessageAttemptRecord{}, s.stop(err)
		}
	}
	if s.messageIndex == nil {
		return "", MessageAttemptRecord{}, s.stop(fmt.Errorf("%w: missing message index", ErrConflict))
	}
	key := messageGroup(candidate)
	entry, ok := s.messageIndex[key]
	if !ok {
		return "", MessageAttemptRecord{}, nil
	}
	current, exists := s.record.Messages[entry.id]
	if !exists || current.Index != entry.index || messageGroup(current) != key || current.Index < 1 || current.Limit < current.Index ||
		(current.State != AttemptActive && current.State != AttemptFailed && current.State != AttemptConfirmed) {
		return "", MessageAttemptRecord{}, s.stop(fmt.Errorf("%w: inconsistent message index", ErrConflict))
	}
	return entry.id, current, nil
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
	// 通过共享暂存规则验证；Open 不提交 ACK。
	probe := newRecordTransaction(s.record.Revision)
	if err := s.stageAcknowledgement(probe, s.record, req.Input); err != nil {
		return pulse.CheckpointMessageAttempt{}, err
	}
	candidate := MessageAttemptRecord{Scope: req.Scope, Parent: req.Attempt, NodeID: req.NodeID, Input: streamID(req.Input.Stream), Sequence: req.Input.Sequence, Limit: req.Limit, State: AttemptActive}
	latestID, latest, err := s.indexedMessage(candidate)
	if err != nil {
		return pulse.CheckpointMessageAttempt{}, err
	}
	if latestID != "" && latest.Limit != req.Limit {
		return pulse.CheckpointMessageAttempt{}, fmt.Errorf("%w: message retry limit mismatch", ErrConflict)
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
				tx := newRecordTransaction(s.record.Revision)
				old := latest
				latest.Input = candidate.Input
				if err := tx.putMessage(latestID, &old, latest); err != nil {
					return pulse.CheckpointMessageAttempt{}, err
				}
				if err := s.commitRecordTransaction(ctx, tx); err != nil {
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
	tx := newRecordTransaction(s.record.Revision)
	if err := tx.putMessage(id, nil, candidate); err != nil {
		return pulse.CheckpointMessageAttempt{}, err
	}
	if err := s.commitRecordTransaction(ctx, tx); err != nil {
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
	tx := newRecordTransaction(s.record.Revision)
	old := message
	message.State = AttemptFailed
	if err := tx.putMessage(id, &old, message); err != nil {
		return err
	}
	return s.commitRecordTransaction(ctx, tx)
}

func (s *runState) commitMessage(ctx context.Context, req pulse.CheckpointMessageCommit) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.streamReady(ctx); err != nil {
		return err
	}
	tx := newRecordTransaction(s.record.Revision)
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
		managed, exists := s.record.Managed[key]
		var expected *managedHeader
		if exists {
			h := managedHeaderOf(managed)
			expected = &h
		}
		if err := tx.putManaged(key, expected, managedHeader{req.Scope, message.Parent, message.NodeID}); err != nil {
			return err
		}
		oldTypes := make(map[string]reflect.Type)
		previous, err := s.decodeManagedState(managed.State)
		if err != nil {
			return err
		}
		for _, state := range previous {
			oldTypes[state.Name] = state.Type
		}
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
			old := managedEntry(managed.State, state.Name)
			if old == nil || !reflect.DeepEqual(*old, value) {
				if err := tx.putManagedEntry(managedEntryKey{key, state.Name, true}, old, &value); err != nil {
					return err
				}
			}
		}
		for i := range managed.State {
			old := &managed.State[i]
			if !seen[old.Name] {
				if err := tx.putManagedEntry(managedEntryKey{key, old.Name, true}, old, nil); err != nil {
					return err
				}
			}
		}
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
		for i := range values {
			if err := tx.putManagedEntry(managedEntryKey{key, values[i].Name, false}, nil, &values[i]); err != nil {
				return err
			}
		}
		for _, output := range req.Outputs {
			root, _, err := s.rootAttempt(output.Attempt)
			owner, _, ownerErr := s.rootAttempt(message.Parent)
			if err != nil || ownerErr != nil || root != owner || output.Stream.Scope != req.Scope || output.Stream.NodeID != message.NodeID || streamKey(streamID(output.Stream)) == streamKey(message.Input) {
				return fmt.Errorf("durable: message output has a different processing owner")
			}
		}
		old := message
		message.State = AttemptConfirmed
		if err := tx.putMessage(req.Attempt, &old, message); err != nil {
			return err
		}
	}
	for _, input := range req.Inputs {
		if err := s.stageAcknowledgement(tx, s.record, input); err != nil {
			return err
		}
	}
	for _, output := range req.Outputs {
		if err := s.stageAppend(tx, s.record, output); err != nil {
			return err
		}
	}
	for _, stop := range req.Stops {
		if err := s.stageStop(tx, s.record, stop); err != nil {
			return err
		}
	}
	return s.commitRecordTransaction(ctx, tx)
}
