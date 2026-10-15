package durable

import (
	"fmt"
	"reflect"

	"github.com/Luo-root/pulse"
)

func equalValue(a, b ValueRecord) bool { return reflect.DeepEqual(a, b) }

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
	if len(r.Messages)+len(r.Managed) != 0 {
		return fmt.Errorf("durable: message checkpoints are not enabled")
	}
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
	}
	return nil
}
