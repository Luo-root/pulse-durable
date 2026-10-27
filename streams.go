package durable

import (
	"context"
	"fmt"
	"reflect"
	"sort"

	"github.com/Luo-root/pulse"
)

func (s *runState) streamHooks() *pulse.CheckpointStreamHooks {
	return &pulse.CheckpointStreamHooks{Open: s.openStream, Append: s.appendStream, Commit: s.commitMessage,
		OpenMessage: s.openMessage, FailMessage: s.failMessage, Finish: s.finishStream}
}

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
	return checkCheckpointContext(ctx)
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
		tx := newRecordTransaction(s.record.Revision)
		old := headerOf(record)
		if exists {
			if record.Stream.ID.Generation == ^uint64(0) {
				return pulse.CheckpointStreamRecord{}, fmt.Errorf("durable: stream generation exhausted")
			}
			record.Stream.ID.Generation++
		}
		var expected *streamHeader
		if exists {
			expected = &old
		}
		if err := tx.putStream(storageKey, expected, headerOf(record)); err != nil {
			return pulse.CheckpointStreamRecord{}, err
		}
		// 所有解码也须在 Save 之前完成，编解码失败不能留下半个 Open。
		result, err := s.decodeStreamRecord(record, exists)
		if err != nil {
			return result, err
		}
		if err := s.commitRecordTransaction(ctx, tx); err != nil {
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
	tx := newRecordTransaction(record.Revision)
	if err := s.stageAppend(tx, *record, req); err != nil {
		return err
	}
	tx.applyStreams(record)
	return nil
}

func (s *runState) stageAppend(tx *recordTransaction, record RunRecord, req pulse.CheckpointStreamAppend) error {
	key, stream, err := s.currentStream(record, req.Stream, true)
	if err != nil {
		return err
	}
	old := headerOf(stream)
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
	next := old
	next.LastSequence, next.Cursor = req.Message.Sequence, cursor
	if err := tx.putStream(key, &old, next); err != nil {
		return err
	}
	message := StreamMessageRecord{Sequence: req.Message.Sequence, Value: value}
	return tx.putPending(key, message.Sequence, nil, &message)
}

func (s *runState) appendStream(ctx context.Context, req pulse.CheckpointStreamAppend) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.streamReady(ctx); err != nil {
		return err
	}
	tx := newRecordTransaction(s.record.Revision)
	if err := s.stageAppend(tx, s.record, req); err != nil {
		return err
	}
	return s.commitRecordTransaction(ctx, tx)
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
	tx := newRecordTransaction(record.Revision)
	old := headerOf(stream)
	next := old
	if len(stream.Pending) == 1 {
		next.AckedThrough = stream.LastSequence
	} else if index == 0 {
		next.AckedThrough = stream.Pending[1].Sequence - 1
	} else {
		next.AckedThrough = stream.Pending[0].Sequence - 1
	}
	if err := tx.putStream(key, &old, next); err != nil {
		return err
	}
	if err := tx.putPending(key, input.Sequence, &stream.Pending[index], nil); err != nil {
		return err
	}
	tx.applyStreams(record)
	return nil
}

func (s *runState) stageFinish(tx *recordTransaction, record RunRecord, req pulse.CheckpointStreamFinish, success bool) error {
	key, stream, err := s.currentStream(record, req.Stream, true)
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
	old := headerOf(stream)
	next := old
	next.Stream.Terminal = string(req.Terminal)
	return tx.putStream(key, &old, next)
}

func (s *runState) finishStream(ctx context.Context, req pulse.CheckpointStreamFinish) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.streamReady(ctx); err != nil {
		return err
	}
	// Core still finishes each output and then fails the root. Once a failed
	// Finish has atomically closed the whole owner, only exact cleanup is valid.
	key := streamKey(streamID(req.Stream))
	stream, exists := s.record.Streams[key]
	root := s.record.Attempts[req.Attempt]
	if exists && root.State == AttemptFailed && req.Terminal == pulse.StreamFailed && stream.Stream.Terminal == string(pulse.StreamFailed) {
		if stream.Stream.ID != streamID(req.Stream) || stream.Stream.Attempt != req.Attempt || root.Boundary != "" ||
			root.Parent != req.Stream.Scope || root.NodeID != req.Stream.NodeID || stream.Stream.Graph != req.Site.Graph ||
			stream.Stream.Path != req.Site.Path || stream.LastSequence != req.LastSequence {
			return fmt.Errorf("durable: invalid repeated source finish")
		}
		if err := s.activeScope(root.Parent); err != nil {
			return err
		}
		var cursor *ValueRecord
		if req.Cursor != nil {
			value, err := s.def.encodePayload("cursor", reflect.TypeOf(req.Cursor), req.Cursor)
			if err != nil {
				return err
			}
			cursor = &value
		}
		if !reflect.DeepEqual(cursor, stream.Cursor) {
			return fmt.Errorf("durable: repeated source finish changed the last accepted cursor")
		}
		return nil
	}
	tx := newRecordTransaction(s.record.Revision)
	if err := s.stageFinish(tx, s.record, req, false); err != nil {
		return err
	}
	if req.Terminal == pulse.StreamFailed {
		if err := tx.failStreamOwner(s.record, req.Attempt); err != nil {
			return err
		}
	}
	return s.commitRecordTransaction(ctx, tx)
}

// A failed terminal is a full-chain business verdict, not a reliable operation
// error or host cancellation. All opened outputs share that owner verdict;
// closing siblings here avoids partial multi-output recovery and needs no Core
// API for sending/acknowledging an already failed source.
func checkFailedStreamOwner(record RunRecord, id string) error {
	for _, stream := range record.Streams {
		if stream.Stream.Attempt == id && stream.Stream.Terminal != "" && stream.Stream.Terminal != string(pulse.StreamFailed) {
			return fmt.Errorf("durable: failed source owner has inconsistent sibling terminals")
		}
	}
	return nil
}

// Only after complete history validation: legacy format-2 saves could persist
// one failed output before the owner failure. Reconcile live scopes in one CAS;
// confirmed/failed ancestor history stays untouched. Canceled is not evidence.
func (s *runState) reconcileFailedStreams(ctx context.Context) error {
	tx := newRecordTransaction(s.record.Revision)
	for _, stream := range s.record.Streams {
		root := s.record.Attempts[stream.Stream.Attempt]
		if stream.Stream.Terminal != string(pulse.StreamFailed) || s.activeScope(root.Parent) != nil {
			continue
		}
		if err := tx.failStreamOwner(s.record, stream.Stream.Attempt); err != nil {
			return err
		}
	}
	return s.commitRecordTransaction(ctx, tx)
}

// The caller has validated the complete record and the requested live scope.
// Only failed node roots are final scope failures; inner Retry failures and
// canceled streams do not establish this fact or consume an ancestor's budget.
func (s *runState) scopeFailure(scope string) error {
	for _, attempt := range s.record.Attempts {
		if attempt.Parent == scope && attempt.Boundary == "" && attempt.State == AttemptFailed {
			return &pulse.CheckpointScopeFailure{Scope: scope}
		}
	}
	return nil
}

// 引用可以早于实际来源的成功终态、读取组 Stop 或恢复升代际；每条历史仍须
// 匹配同一实际来源，已知终态不能倒退。active 来源有真实活跃根，成功来源有完整成果。
func (s *runState) checkStreamReference(record RunRecord, ref StreamDescriptorRecord) error {
	return s.checkStreamReferenceRecord(record, ref, false)
}

// historical 仅用于加载时的结构校验，不授权 Commit、Open 或建立恢复端点。
// 旧别名可早于后来发生的来源失败；稳定来源和已经声明的终态仍须一致。
func (s *runState) checkStreamReferenceRecord(record RunRecord, ref StreamDescriptorRecord, historical bool) error {
	// 每条历史引用先核自身状态，不能让缓存或稍后快照覆盖不可能的 stopped/false。
	if !streamTerminal(ref.Terminal, ref.Stopped) {
		return fmt.Errorf("durable: stream reference has an inconsistent terminal and reading group")
	}
	stream, ok := record.Streams[streamKey(ref.ID)]
	if !ok || ref.ID.Generation > stream.Stream.ID.Generation || ref.ID.Generation == 0 {
		return fmt.Errorf("durable: missing stream reference or future generation")
	}
	expected := stream.Stream
	if !historical && (expected.Terminal == string(pulse.StreamFailed) || expected.Terminal == string(pulse.StreamCanceled)) {
		return fmt.Errorf("durable: stream reference names a failed or canceled source")
	}
	expected.ID.Generation = ref.ID.Generation
	// 较早的 Sub 引用不知道将来的终态和 Stop，但不能抹掉已经知道的事实。
	if ref.Terminal == "" {
		expected.Terminal = ""
	}
	if !ref.Stopped {
		expected.Stopped = false
	}
	if ref != expected {
		return fmt.Errorf("durable: stream reference source or terminal mismatch")
	}
	root, ok := record.Attempts[ref.Attempt]
	scope, scoped := record.Scopes[ref.ID.Scope]
	if !ok || !scoped || root.Boundary != "" || root.Parent != ref.ID.Scope || root.NodeID != ref.ID.NodeID ||
		scope.Graph != ref.Graph || scope.Path != ref.Path {
		return fmt.Errorf("durable: stream reference has an invalid source root")
	}
	node, present := scope.Nodes[ref.ID.NodeID]
	if stream.Stream.Terminal == "" || stream.Stream.Terminal == string(pulse.StreamFailed) || stream.Stream.Terminal == string(pulse.StreamCanceled) {
		if (root.State != AttemptActive && (!historical || root.State != AttemptFailed)) || present {
			return fmt.Errorf("durable: unfinished stream reference has an invalid source node")
		}
		// Open 建立来源记录时已核过声明；恢复时 validateStreams 也逐条核声明。
		return nil
	}
	if !present || root.State != AttemptConfirmed || node.Attempt != ref.Attempt {
		return fmt.Errorf("durable: stream reference lacks a confirmed source node")
	}
	found := false
	for _, value := range node.Outputs {
		if value.Name == ref.ID.Key && value.Stream != nil {
			known := *value.Stream
			known.ID.Generation, known.Stopped = ref.ID.Generation, ref.Stopped
			if ref.Terminal == "" {
				known.Terminal = ""
			}
			found = known == ref
		}
	}
	if !found {
		return fmt.Errorf("durable: stream reference is not an owned source output")
	}
	return nil
}

// 先逐条核验历史引用，再给同一逻辑流返回一份权威引用。历史节点成果不会因
// Open 提升代际或稍后的 Stop 被倒改；差异不是两条来源，也不能靠放宽历史校验抹平。
// 调用方持 runState.mu，或处于业务尚未启动的根恢复阶段。
func (s *runState) restoreStreamReference(key pulse.CheckpointKey, historical StreamDescriptorRecord) (pulse.CheckpointStreamDescriptor, error) {
	if stream, exists := s.record.Streams[streamKey(historical.ID)]; exists && stream.Stream.Terminal == string(pulse.StreamFailed) {
		// A confirmed alias may live above the failing producer's Sub. Preserve
		// that result as a read-only failed endpoint so intermediate parent
		// chains can still catch/retry the actual producer; never reopen it.
		if err := s.checkStreamReferenceRecord(s.record, historical, true); err != nil {
			return pulse.CheckpointStreamDescriptor{}, err
		}
		if s.record.Attempts[stream.Stream.Attempt].State != AttemptFailed {
			return pulse.CheckpointStreamDescriptor{}, fmt.Errorf("durable: failed reference lacks a confirmed failed source root")
		}
		if err := s.activeScope(stream.Stream.ID.Scope); err != nil {
			return pulse.CheckpointStreamDescriptor{}, err
		}
		return s.def.decodeDescriptor(key, stream.Stream)
	}
	if err := s.checkStreamReference(s.record, historical); err != nil {
		return pulse.CheckpointStreamDescriptor{}, err
	}
	logical := streamKey(historical.ID)
	current := s.record.Streams[logical].Stream
	if current.Terminal == "" && !current.Resumable {
		return pulse.CheckpointStreamDescriptor{}, fmt.Errorf("durable: active stream source is not resumable")
	}
	descriptor, exists := s.restoreStreams[logical]
	if !exists {
		descriptor = s.record.Streams[logical].Stream
		if s.restoreStreams == nil {
			s.restoreStreams = make(map[string]StreamDescriptorRecord)
		}
		s.restoreStreams[logical] = descriptor
	}
	// 这份恢复计划身份在本次运行内固定。较早安装的端点可能已经 Open 并提升了
	// 当前记录代际；晚开的子图仍须命中 Core 的同一别名缓存，再由共享端点认回最新账。
	return s.def.decodeDescriptor(key, descriptor)
}
