// 本文件负责消息前缀事实与持久引用闭包。
// 稳定流身份不含 Generation：恢复升代际不能把旧消息预算从规划中遗漏。
package durable

import (
	"fmt"
	"sort"
)

type compressionMessageFact struct {
	ID      string               `json:"id"`
	Message MessageAttemptRecord `json:"message"`
}

func messagePrefixForCompression(record RunRecord, payload messagePrefixSummaryPayload, index *compressionCandidateIndex) ([]CompressionObjectRef, string, error) {
	stream, ok := record.Streams[streamKey(payload.Input)]
	if !ok || payload.Input != stream.Stream.ID || payload.ThroughSequence > stream.AckedThrough || stream.AckedThrough > stream.LastSequence {
		return nil, "", fmt.Errorf("%w: message prefix is not covered by current ACK", ErrConflict)
	}
	group := index.messages[messageSummaryGroup{payload.ProcessingScope, payload.ProcessingAttempt, payload.NodeID, streamKey(payload.Input)}]
	if payload.ThroughSequence > uint64(len(group)) {
		return nil, "", fmt.Errorf("%w: message prefix has a gap", ErrInvalid)
	}
	if _, ok := record.Managed[managedKey(payload.ProcessingScope, payload.ProcessingAttempt, payload.NodeID)]; !ok {
		return nil, "", fmt.Errorf("%w: message prefix lacks retained managed state", ErrInvalid)
	}
	for _, pending := range stream.Pending {
		if pending.Sequence <= payload.ThroughSequence {
			return nil, "", fmt.Errorf("%w: message prefix input remains pending", ErrConflict)
		}
	}
	groups := make(map[uint64]map[int]compressionMessageFact)
	for id, message := range group {
		if message.Scope != payload.ProcessingScope || message.Parent != payload.ProcessingAttempt || message.NodeID != payload.NodeID || streamKey(message.Input) != streamKey(payload.Input) || message.Sequence > payload.ThroughSequence {
			continue
		}
		if message.Sequence == 0 || message.Input.Generation == 0 || message.Input.Generation > stream.Stream.ID.Generation || message.Index < 1 || message.Limit < message.Index || !validAttemptState(message.State) {
			return nil, "", fmt.Errorf("%w: invalid message prefix attempt", ErrInvalid)
		}
		if groups[message.Sequence] == nil {
			groups[message.Sequence] = make(map[int]compressionMessageFact)
		}
		if _, duplicate := groups[message.Sequence][message.Index]; duplicate {
			return nil, "", fmt.Errorf("%w: duplicate message prefix budget index", ErrInvalid)
		}
		groups[message.Sequence][message.Index] = compressionMessageFact{ID: id, Message: message}
	}
	if uint64(len(groups)) != payload.ThroughSequence {
		return nil, "", fmt.Errorf("%w: message prefix is not continuous from sequence 1", ErrInvalid)
	}
	var facts []compressionMessageFact
	var rows []CompressionObjectRef
	for sequence := uint64(1); sequence <= payload.ThroughSequence; sequence++ {
		group := groups[sequence]
		latest, exists := group[len(group)]
		if !exists || latest.Message.State != AttemptConfirmed {
			return nil, "", fmt.Errorf("%w: message prefix is not settled", ErrConflict)
		}
		for attemptIndex := 1; attemptIndex <= len(group); attemptIndex++ {
			fact, exists := group[attemptIndex]
			if !exists || fact.Message.Limit != latest.Message.Limit || attemptIndex < len(group) && fact.Message.State != AttemptFailed {
				return nil, "", fmt.Errorf("%w: message prefix budget has a gap or unresolved attempt", ErrInvalid)
			}
			if index.refs[fact.ID] {
				return nil, "", fmt.Errorf("%w: message attempt remains referenced", ErrConflict)
			}
			facts = append(facts, fact)
			rows = append(rows, CompressionObjectRef{Kind: "message", ID: fact.ID})
		}
	}
	budget := historyCompressionDigest("message-prefix-facts/v1", record.RunID, record.Revision, payload.ProcessingScope, payload.ProcessingAttempt, payload.NodeID, payload.Input, payload.ThroughSequence, facts)
	return rows, budget, nil
}

func compressionReferenceClosure(record RunRecord) map[string]bool {
	refs := make(map[string]bool)
	addValues := func(values []ValueRecord) {
		for _, value := range values {
			if value.Stream != nil {
				refs[value.Stream.Attempt] = true
				refs[value.Stream.ID.Scope] = true
			}
		}
	}
	refs[record.RootScope] = true
	for _, attempt := range record.Attempts {
		refs[attempt.Parent] = true
	}
	for id, scope := range record.Scopes {
		refs[id] = true
		for _, node := range scope.Nodes {
			refs[node.Attempt] = true
			addValues(node.Outputs)
		}
	}
	for _, message := range record.Messages {
		refs[message.Parent] = true
		refs[message.Scope] = true
		refs[message.Input.Scope] = true
	}
	for _, stream := range record.Streams {
		refs[stream.Stream.Attempt] = true
		refs[stream.Stream.ID.Scope] = true
		for _, pending := range stream.Pending {
			addValues([]ValueRecord{pending.Value})
		}
		if stream.Cursor != nil {
			addValues([]ValueRecord{*stream.Cursor})
		}
	}
	for _, managed := range record.Managed {
		refs[managed.Parent] = true
		refs[managed.Scope] = true
		addValues(managed.State)
		addValues(managed.Values)
	}
	delete(refs, "")
	return refs
}

func sortCompressionObjects(objects []CompressionObjectRef) {
	sort.Slice(objects, func(i, j int) bool {
		if objects[i].Kind != objects[j].Kind {
			return objects[i].Kind < objects[j].Kind
		}
		return objects[i].ID < objects[j].ID
	})
}
