package durable

import (
	"fmt"
	"reflect"

	"github.com/Luo-root/pulse"
)

func equalValue(a, b ValueRecord) bool { return reflect.DeepEqual(a, b) }

// 仅当整条处理祖先链有效时，消息值才约束其精确完整节点根。
// 失败的 Retry 和图历史仍可解码，
// 但不能约束后续成功所有者的输出。
func (s *runState) checkManagedValues(scope string, node NodeRecord, managed ManagedRecord) error {
	if managed.Scope != scope || managed.NodeID != node.NodeID {
		return nil
	}
	_, rootID, err := s.rootAttempt(managed.Parent)
	if err != nil {
		return err
	}
	if rootID != node.Attempt || s.ancestry(managed.Parent, false) != nil {
		return nil
	}
	for _, committed := range managed.Values {
		found := false
		for _, value := range node.Outputs {
			if value.Name == committed.Name {
				found = equalValue(value, committed)
			}
		}
		if !found {
			return fmt.Errorf("durable: node commit lost a confirmed message value")
		}
	}
	return nil
}

func (s *runState) hasIncrementalSource(scope, key string, historical bool) bool {
	for _, stream := range s.record.Streams {
		if !s.streamComesFrom(scope, key, stream.Stream.ID, make(map[string]bool)) {
			continue
		}
		// 已确认消费者可以早于来源完成；来源后来失败不把历史记录变成格式损坏。
		// 新提交则仍要求来源及全部祖先有效，旧失败作用域永远不能继续推进。
		if historical || stream.Stream.Terminal != string(pulse.StreamFailed) && stream.Stream.Terminal != string(pulse.StreamCanceled) && s.ancestry(stream.Stream.Attempt, false) == nil {
			return true
		}
	}
	return false
}

func (s *runState) validateStreams() error {
	r := s.record
	if r.Format == 1 {
		if len(r.Streams)+len(r.Messages)+len(r.Managed) != 0 {
			return fmt.Errorf("durable: format 1 cannot contain incremental records")
		}
		for _, scope := range r.Scopes {
			if scope.Bindings {
				return fmt.Errorf("durable: format 1 cannot contain binding fingerprints")
			}
			for _, node := range scope.Nodes {
				for _, value := range node.Outputs {
					if value.Stream != nil {
						return fmt.Errorf("durable: format 1 cannot contain stream descriptors")
					}
				}
			}
		}
	}
	for key, record := range r.Streams {
		id := record.Stream.ID
		if key != streamKey(id) {
			return fmt.Errorf("durable: stream storage identity mismatch")
		}
		declaration, err := s.streamDeclaration(id)
		if err != nil {
			return err
		}
		if _, err := s.def.decodeDescriptor(declaration, record.Stream); err != nil {
			return err
		}
		owner, ok := r.Attempts[record.Stream.Attempt]
		scope := r.Scopes[id.Scope]
		if !ok || owner.Boundary != "" || owner.Parent != id.Scope || owner.NodeID != id.NodeID ||
			record.Stream.Graph != scope.Graph || record.Stream.Path != scope.Path {
			return fmt.Errorf("durable: stream source association mismatch")
		}
		if record.AckedThrough > record.LastSequence || len(record.Pending) > record.Stream.Capacity || record.Stream.Stopped && len(record.Pending) != 0 {
			return fmt.Errorf("durable: invalid stream acknowledgement or capacity")
		}
		previous := record.AckedThrough
		for _, message := range record.Pending {
			if message.Sequence <= previous || message.Sequence > record.LastSequence {
				return fmt.Errorf("durable: duplicate or unordered stream messages")
			}
			previous = message.Sequence
		}
		if !record.Stream.Stopped && (len(record.Pending) == 0 && record.AckedThrough != record.LastSequence || len(record.Pending) > 0 && record.Pending[0].Sequence != record.AckedThrough+1) {
			return fmt.Errorf("durable: stream continuous acknowledgement frontier is inconsistent")
		}
		if _, err := s.decodeStreamRecord(record, true); err != nil {
			return err
		}
		if record.Stream.Terminal == string(pulse.StreamCompleted) || record.Stream.Terminal == string(pulse.StreamStopped) {
			if err := s.checkStreamReference(r, record.Stream); err != nil {
				return err
			}
		} else if owner.State == AttemptConfirmed {
			return fmt.Errorf("durable: confirmed source has no successful stream terminal")
		}
		if record.Stream.Terminal == string(pulse.StreamFailed) {
			if err := checkFailedStreamOwner(r, record.Stream.Attempt); err != nil {
				return err
			}
		}
	}
	groups := make(map[string]map[int]MessageAttemptRecord)
	for id, message := range r.Messages {
		if !validIdentity(id) || !validIdentity(message.Parent) || message.NodeID == "" || message.Index < 1 || message.Limit < message.Index ||
			(message.State != AttemptActive && message.State != AttemptFailed && message.State != AttemptConfirmed) {
			return fmt.Errorf("durable: invalid persisted message attempt")
		}
		if _, collision := r.Attempts[id]; collision {
			return fmt.Errorf("durable: message identity collides with node attempt")
		}
		if _, collision := r.Scopes[id]; collision {
			return fmt.Errorf("durable: message identity collides with graph scope")
		}
		root, _, err := s.rootAttempt(message.Parent)
		if err != nil || root.Parent != message.Scope || root.NodeID != message.NodeID {
			return fmt.Errorf("durable: message processing association mismatch")
		}
		if !s.nodeReadsStream(message.Scope, message.NodeID, message.Input) {
			return fmt.Errorf("durable: message record refers to an unrelated input")
		}
		stream, ok := r.Streams[streamKey(message.Input)]
		if !ok || message.Input.Generation == 0 || message.Input.Generation > stream.Stream.ID.Generation || message.Sequence == 0 || message.Sequence > stream.LastSequence {
			return fmt.Errorf("durable: message input identity or position mismatch")
		}
		pending := false
		for _, value := range stream.Pending {
			if value.Sequence == message.Sequence {
				pending = true
			}
		}
		if message.State == AttemptConfirmed {
			if pending {
				return fmt.Errorf("durable: confirmed message still has its input pending")
			}
			// CommitMessage 始终保存此容器，即使没有状态或值。
			managed, ok := r.Managed[managedKey(message.Scope, message.Parent, message.NodeID)]
			if !ok || managed.Scope != message.Scope || managed.Parent != message.Parent || managed.NodeID != message.NodeID {
				return fmt.Errorf("durable: confirmed message has no matching managed state")
			}
		}
		key := managedKey(message.Scope, message.Parent, fmt.Sprintf("%s/%s/%d", message.NodeID, streamKey(message.Input), message.Sequence))
		if groups[key] == nil {
			groups[key] = make(map[int]MessageAttemptRecord)
		}
		if _, duplicate := groups[key][message.Index]; duplicate {
			return fmt.Errorf("durable: duplicate persisted message index")
		}
		groups[key][message.Index] = message
	}
	for _, group := range groups {
		latest := group[len(group)]
		stream := r.Streams[streamKey(latest.Input)]
		pending := false
		for _, message := range stream.Pending {
			if message.Sequence == latest.Sequence {
				pending = true
			}
		}
		if latest.State != AttemptConfirmed && !pending && !stream.Stream.Stopped {
			return fmt.Errorf("durable: latest message attempt lost its pending input")
		}
		for i := 1; i <= len(group); i++ {
			message, ok := group[i]
			if !ok || message.Limit != group[1].Limit || i < len(group) && message.State != AttemptFailed {
				return fmt.Errorf("durable: invalid persisted message budget sequence")
			}
		}
	}
	for key, managed := range r.Managed {
		root, _, err := s.rootAttempt(managed.Parent)
		if err != nil || root.Parent != managed.Scope || root.NodeID != managed.NodeID || key != managedKey(managed.Scope, managed.Parent, managed.NodeID) {
			return fmt.Errorf("durable: managed state association mismatch")
		}
		confirmed := false
		for _, message := range r.Messages {
			if message.Scope == managed.Scope && message.Parent == managed.Parent && message.NodeID == managed.NodeID && message.State == AttemptConfirmed {
				confirmed = true
			}
		}
		if !confirmed {
			return fmt.Errorf("durable: managed state has no confirmed message")
		}
		if _, err := s.decodeManagedState(managed.State); err != nil {
			return err
		}
		scope := r.Scopes[managed.Scope]
		node := s.def.graphs[location{scope.Graph, scope.Path}].nodes[managed.NodeID]
		keys := make(map[string]pulse.CheckpointKey)
		for _, key := range node.Provides {
			keys[key.Name] = key
		}
		for _, value := range managed.Values {
			key, ok := keys[value.Name]
			if !ok || key.Element != nil {
				return fmt.Errorf("durable: duplicate or undeclared managed value")
			}
			delete(keys, value.Name)
			if _, err := s.def.decodeValue(key, value); err != nil {
				return err
			}
		}
		if complete, ok := scope.Nodes[managed.NodeID]; ok {
			if err := s.checkManagedValues(managed.Scope, complete, managed); err != nil {
				return err
			}
		}
	}
	return nil
}
