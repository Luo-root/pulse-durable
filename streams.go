package durable

import (
	"context"
	"fmt"
	"github.com/Luo-root/pulse"
	"reflect"
	"sort"
)

func (s *runState) streamDeclaration(id StreamIDRecord) (pulse.CheckpointKey, error) {
	scope, ok := s.record.Scopes[id.Scope]
	gs := s.def.graphs[location{scope.Graph, scope.Path}]
	if !ok || gs == nil || id.Group == "" {
		return pulse.CheckpointKey{}, fmt.Errorf("durable: unknown stream scope or group")
	}
	n, ok := gs.nodes[id.NodeID]
	if ok {
		for _, key := range n.Provides {
			if key.Name == id.Key && key.Element != nil {
				return key, nil
			}
		}
	}
	return pulse.CheckpointKey{}, fmt.Errorf("durable: undeclared stream source")
}

func (s *runState) streamReady(ctx context.Context) error {
	if s.stopped != nil {
		return s.stopped
	}
	return checkContext(ctx)
}

func (s *runState) openStream(ctx context.Context, req pulse.CheckpointStreamOpen) (pulse.CheckpointStreamRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.streamReady(ctx); err != nil {
		return pulse.CheckpointStreamRecord{}, err
	}
	id := StreamIDRecord{Scope: req.Scope, NodeID: req.NodeID, Key: req.Key.Name, Group: req.Group, Generation: 1}
	key, err := s.streamDeclaration(id)
	if err != nil {
		return pulse.CheckpointStreamRecord{}, err
	}
	if key != req.Key {
		return pulse.CheckpointStreamRecord{}, fmt.Errorf("durable: stream declaration mismatch")
	}
	descriptor, err := s.def.encodeDescriptor(key, pulse.CheckpointStreamDescriptor{ID: id.checkpoint(), Graph: req.Site.Graph,
		Path: req.Site.Path, Attempt: req.Attempt, Element: key.Element, Source: req.Source, Capacity: req.Capacity})
	if err != nil {
		return pulse.CheckpointStreamRecord{}, err
	}
	a, ok := s.record.Attempts[req.Attempt]
	scope := s.record.Scopes[req.Scope]
	if !ok || a.Boundary != "" || a.NodeID != req.NodeID || a.Parent != req.Scope || descriptor.Graph != scope.Graph || descriptor.Path != scope.Path {
		return pulse.CheckpointStreamRecord{}, fmt.Errorf("durable: stream owner does not match its root attempt")
	}
	storageKey := streamKey(id)
	wasResumed, openedThisRun := s.opened[storageKey]
	record, exists := s.record.Streams[storageKey]
	if exists {
		expected := record.Stream
		descriptor.ID.Generation, descriptor.Terminal, descriptor.Stopped = expected.ID.Generation, expected.Terminal, expected.Stopped
		if descriptor != expected {
			return pulse.CheckpointStreamRecord{}, fmt.Errorf("%w: stream source identity changed", ErrConflict)
		}
		if err := s.ancestry(req.Attempt, false); err != nil {
			return pulse.CheckpointStreamRecord{}, err
		}
		if expected.Terminal == string(pulse.StreamFailed) || expected.Terminal == string(pulse.StreamCanceled) {
			return pulse.CheckpointStreamRecord{}, fmt.Errorf("durable: failed stream source cannot reopen")
		}
		if req.Restore != nil {
			reference, err := s.def.encodeDescriptor(key, *req.Restore)
			if err != nil {
				return pulse.CheckpointStreamRecord{}, err
			}
			if streamKey(reference.ID) != storageKey {
				return pulse.CheckpointStreamRecord{}, fmt.Errorf("durable: restore reference names another source")
			}
			if err := s.checkStreamReference(s.record, reference); err != nil {
				return pulse.CheckpointStreamRecord{}, err
			}
		} else {
			if err := s.activeAttempt(req.Attempt); err != nil {
				return pulse.CheckpointStreamRecord{}, err
			}
			if expected.Terminal != "" {
				return pulse.CheckpointStreamRecord{}, fmt.Errorf("durable: completed source requires a confirmed node reference")
			}
		}
		if expected.Terminal == "" && !expected.Resumable && !openedThisRun {
			return pulse.CheckpointStreamRecord{}, fmt.Errorf("durable: active stream source is not resumable")
		}
	} else {
		if req.Restore != nil {
			return pulse.CheckpointStreamRecord{}, fmt.Errorf("durable: missing referenced stream")
		}
		if err := s.activeAttempt(req.Attempt); err != nil {
			return pulse.CheckpointStreamRecord{}, err
		}
		record.Stream = descriptor
	}
	if s.opened == nil {
		s.opened = make(map[string]bool)
	}
	if !openedThisRun {
		proposed := cloneRecord(s.record)
		if exists {
			if record.Stream.ID.Generation == ^uint64(0) {
				return pulse.CheckpointStreamRecord{}, fmt.Errorf("durable: stream generation exhausted")
			}
			record.Stream.ID.Generation++
		}
		if proposed.Streams == nil {
			proposed.Streams = make(map[string]StreamRecord)
		}
		proposed.Streams[storageKey] = record
		// 所有解码也须在 Save 之前完成，编解码失败不能留下半个 Open。
		result, err := s.decodeStreamRecord(record, exists)
		if err != nil {
			return result, err
		}
		if err := s.save(ctx, proposed); err != nil {
			return pulse.CheckpointStreamRecord{}, err
		}
		s.opened[storageKey] = exists
		return result, nil
	}
	return s.decodeStreamRecord(record, wasResumed)
}

func (s *runState) decodeStreamRecord(record StreamRecord, resumed bool) (pulse.CheckpointStreamRecord, error) {
	key, err := s.streamDeclaration(record.Stream.ID)
	if err != nil {
		return pulse.CheckpointStreamRecord{}, err
	}
	descriptor, err := s.def.decodeDescriptor(key, record.Stream)
	if err != nil {
		return pulse.CheckpointStreamRecord{}, err
	}
	out := pulse.CheckpointStreamRecord{Stream: descriptor, LastSequence: record.LastSequence, AckedThrough: record.AckedThrough, Resumed: resumed}
	for _, pending := range record.Pending {
		if pending.Value.State != string(pulse.SlotStateReady) {
			return out, fmt.Errorf("durable: pending message is not ready")
		}
		value, err := s.def.decodeValue(pulse.CheckpointKey{Name: "element", Type: key.Element}, pending.Value)
		if err != nil {
			return out, err
		}
		out.Pending = append(out.Pending, pulse.CheckpointStreamMessage{Sequence: pending.Sequence, Value: value.Value})
	}
	if record.Cursor != nil {
		if record.Cursor.Name != "cursor" {
			return out, fmt.Errorf("durable: invalid source cursor name")
		}
		value, err := s.def.decodePayload(*record.Cursor)
		if err != nil {
			return out, err
		}
		out.Cursor = value.Value
	}
	return out, nil
}

func (s *runState) currentStream(record RunRecord, id pulse.CheckpointStreamID, active bool) (string, StreamRecord, error) {
	key := streamKey(streamID(id))
	stream, ok := record.Streams[key]
	if !ok || stream.Stream.ID != streamID(id) {
		return "", StreamRecord{}, fmt.Errorf("durable: unknown stream or stale generation")
	}
	if err := s.ancestry(stream.Stream.Attempt, active); err != nil {
		return "", StreamRecord{}, err
	}
	if stream.Stream.Terminal == string(pulse.StreamFailed) || stream.Stream.Terminal == string(pulse.StreamCanceled) {
		return "", StreamRecord{}, fmt.Errorf("durable: failed stream cannot advance")
	}
	return key, stream, nil
}

func (s *runState) appendTo(record *RunRecord, req pulse.CheckpointStreamAppend) error {
	key, stream, err := s.currentStream(*record, req.Stream, true)
	if err != nil {
		return err
	}
	if req.Attempt != stream.Stream.Attempt || req.Site.Graph != stream.Stream.Graph || req.Site.Path != stream.Stream.Path ||
		stream.Stream.Terminal != "" || stream.Stream.Stopped || stream.LastSequence == ^uint64(0) || req.Message.Sequence != stream.LastSequence+1 ||
		len(stream.Pending) >= stream.Stream.Capacity {
		return fmt.Errorf("durable: invalid stream append owner, position or capacity")
	}
	declaration, err := s.streamDeclaration(stream.Stream.ID)
	if err != nil {
		return err
	}
	value, err := s.def.encodePayload("element", declaration.Element, req.Message.Value)
	if err != nil {
		return err
	}
	var cursor *ValueRecord
	if req.Cursor != nil {
		encoded, err := s.def.encodePayload("cursor", reflect.TypeOf(req.Cursor), req.Cursor)
		if err != nil {
			return err
		}
		cursor = &encoded
	}
	stream.LastSequence = req.Message.Sequence
	stream.Pending = append(stream.Pending, StreamMessageRecord{Sequence: req.Message.Sequence, Value: value})
	stream.Cursor = cursor
	record.Streams[key] = stream
	return nil
}

func (s *runState) appendStream(ctx context.Context, req pulse.CheckpointStreamAppend) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.streamReady(ctx); err != nil {
		return err
	}
	proposed := cloneRecord(s.record)
	if err := s.appendTo(&proposed, req); err != nil {
		return err
	}
	return s.save(ctx, proposed)
}

func (s *runState) acknowledge(record *RunRecord, input pulse.CheckpointStreamAcknowledgement) error {
	key, stream, err := s.currentStream(*record, input.Stream, false)
	if err != nil {
		return err
	}
	index := sort.Search(len(stream.Pending), func(i int) bool { return stream.Pending[i].Sequence >= input.Sequence })
	if index == len(stream.Pending) || stream.Pending[index].Sequence != input.Sequence || stream.Stream.Stopped {
		return fmt.Errorf("durable: message is not pending in this reading group")
	}
	stream.Pending = append(stream.Pending[:index:index], stream.Pending[index+1:]...)
	if len(stream.Pending) == 0 {
		stream.AckedThrough = stream.LastSequence
	} else {
		stream.AckedThrough = stream.Pending[0].Sequence - 1
	}
	record.Streams[key] = stream
	return nil
}

func (s *runState) applyFinish(record *RunRecord, req pulse.CheckpointStreamFinish, success bool) error {
	key, stream, err := s.currentStream(*record, req.Stream, true)
	if err != nil {
		return err
	}
	valid := req.Terminal == pulse.StreamFailed || req.Terminal == pulse.StreamCanceled
	if success {
		valid = req.Terminal == pulse.StreamCompleted || req.Terminal == pulse.StreamStopped
	}
	if !valid || req.Attempt != stream.Stream.Attempt || req.Site.Graph != stream.Stream.Graph || req.Site.Path != stream.Stream.Path ||
		stream.Stream.Terminal != "" || req.LastSequence != stream.LastSequence || (req.Terminal == pulse.StreamStopped) != stream.Stream.Stopped && success {
		return fmt.Errorf("durable: invalid source finish")
	}
	var cursor *ValueRecord
	if req.Cursor != nil {
		encoded, err := s.def.encodePayload("cursor", reflect.TypeOf(req.Cursor), req.Cursor)
		if err != nil {
			return err
		}
		cursor = &encoded
	}
	if !reflect.DeepEqual(cursor, stream.Cursor) {
		return fmt.Errorf("durable: source finish changed the last accepted cursor")
	}
	stream.Stream.Terminal = string(req.Terminal)
	record.Streams[key] = stream
	return nil
}

func (s *runState) finishStream(ctx context.Context, req pulse.CheckpointStreamFinish) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.streamReady(ctx); err != nil {
		return err
	}
	proposed := cloneRecord(s.record)
	if err := s.applyFinish(&proposed, req, false); err != nil {
		return err
	}
	return s.save(ctx, proposed)
}

// 旧 descriptor 可引用已提升代际的 completed 来源，但来源根成果及其确切终态必须存在。
func (s *runState) checkStreamReference(record RunRecord, ref StreamDescriptorRecord) error {
	stream, ok := record.Streams[streamKey(ref.ID)]
	if !ok || ref.ID.Generation > stream.Stream.ID.Generation || ref.ID.Generation == 0 {
		return fmt.Errorf("durable: missing stream reference or future generation")
	}
	expected := stream.Stream
	expected.ID.Generation = ref.ID.Generation
	// Stop 可以在来源已确认后发生，不倒改历史节点成果。
	if !ref.Stopped {
		expected.Stopped = false
	}
	if ref != expected || ref.Terminal != string(pulse.StreamCompleted) && ref.Terminal != string(pulse.StreamStopped) {
		return fmt.Errorf("durable: stream reference source or terminal mismatch")
	}
	root, ok := record.Attempts[ref.Attempt]
	node, present := record.Scopes[ref.ID.Scope].Nodes[ref.ID.NodeID]
	if !ok || !present || root.State != AttemptConfirmed || root.Boundary != "" || root.Parent != ref.ID.Scope ||
		root.NodeID != ref.ID.NodeID || node.Attempt != ref.Attempt {
		return fmt.Errorf("durable: stream reference lacks a confirmed source node")
	}
	found := false
	for _, value := range node.Outputs {
		if value.Name == ref.ID.Key && value.Stream != nil {
			known := *value.Stream
			known.ID.Generation, known.Stopped = ref.ID.Generation, ref.Stopped
			found = known == ref
		}
	}
	if !found {
		return fmt.Errorf("durable: stream reference is not an owned source output")
	}
	return nil
}
