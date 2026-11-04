package durable

import (
	"context"
	"fmt"
	"reflect"
)

// recordTransaction 仅暂存调用方修改的对象。构造、
// 暂存和提交要求持有 runState.mu；这不是并发 Store 协议。
// 每个普通检查点仅提交这些暂存的类型化操作。
type recordTransaction struct {
	baseRevision   uint64
	attempts       map[string]putAttempt
	scopes         map[string]ScopeRecord // 仅用于创建；Nodes 始终为空
	nodes          map[nodeRecordKey]putNode
	streams        map[string]putStreamHeader
	pending        map[pendingKey]pendingOperation
	messages       map[string]putMessage
	managed        map[string]putManagedHeader
	managedEntries map[managedEntryKey]managedEntryOperation
	err            error
}

type putAttempt struct {
	expected AttemptRecord
	absent   bool
	value    AttemptRecord
}

func newRecordTransaction(baseRevision uint64) *recordTransaction {
	return &recordTransaction{baseRevision: baseRevision}
}

// putAttempt 复制两个值；expected 为 nil 表示键必须不存在。
// 相同前置条件的重复写入合并为最终值。
// 冲突前置条件使整个事务无效，即使被忽略。
func (tx *recordTransaction) putAttempt(id string, expected *AttemptRecord, value AttemptRecord) error {
	if tx.err != nil {
		return tx.err
	}
	op := putAttempt{absent: expected == nil, value: value}
	if expected != nil {
		op.expected = *expected
	}
	if previous, ok := tx.attempts[id]; ok && (previous.absent != op.absent || previous.expected != op.expected) {
		tx.err = fmt.Errorf("%w: conflicting attempt precondition for %q", ErrConflict, id)
		return tx.err
	}
	if tx.attempts == nil {
		tx.attempts = make(map[string]putAttempt)
	}
	tx.attempts[id] = op
	return nil
}

type nodeRecordKey struct{ scope, node string }

type putNode struct {
	expected NodeRecord
	absent   bool
	value    NodeRecord
}

// createScope 的前置条件为 Absent。它仅暂存作用域头，绝不
// 暂存调用方的 Nodes 映射。即使空的 nil 或非 nil 映射也不保留。
// 新作用域的节点必须通过 putNode 单独暂存。
func (tx *recordTransaction) createScope(id string, value ScopeRecord) error {
	if tx.err != nil {
		return tx.err
	}
	if len(value.Nodes) != 0 {
		tx.err = fmt.Errorf("%w: new scope %q contains nodes", ErrConflict, id)
		return tx.err
	}
	value.Nodes = nil
	if previous, ok := tx.scopes[id]; ok && !reflect.DeepEqual(previous, value) {
		tx.err = fmt.Errorf("%w: conflicting scope creation for %q", ErrConflict, id)
		return tx.err
	}
	if tx.scopes == nil {
		tx.scopes = make(map[string]ScopeRecord)
	}
	tx.scopes[id] = value
	return nil
}

// copyTransactionNode 保留 nil 与空切片的区别，以满足精确旧值
// 前置条件。cloneValues 在现有完整记录 Save 路径中有意归一化
// 空切片；这不得削弱事务比较。
func copyTransactionNode(node NodeRecord) NodeRecord {
	if node.Outputs != nil {
		outputs := make([]ValueRecord, len(node.Outputs))
		copy(outputs, node.Outputs)
		for i := range outputs {
			if data := outputs[i].Data; data != nil {
				outputs[i].Data = make([]byte, len(data))
				copy(outputs[i].Data, data)
			}
			if stream := outputs[i].Stream; stream != nil {
				value := *stream
				outputs[i].Stream = &value
			}
		}
		node.Outputs = outputs
	}
	return node
}

// putNode 捕获精确旧值，expected 为 nil 时则为 Absent。重复
// 写入仅在原始前置条件一致时合并。作用域和节点共同
// 构成键，因此不同作用域的相同节点 ID 保持独立。
func (tx *recordTransaction) putNode(scope, id string, expected *NodeRecord, value NodeRecord) error {
	if tx.err != nil {
		return tx.err
	}
	key := nodeRecordKey{scope, id}
	op := putNode{absent: expected == nil, value: copyTransactionNode(value)}
	if expected != nil {
		op.expected = copyTransactionNode(*expected)
	}
	if previous, ok := tx.nodes[key]; ok && (previous.absent != op.absent || !reflect.DeepEqual(previous.expected, op.expected)) {
		tx.err = fmt.Errorf("%w: conflicting node precondition for %q/%q", ErrConflict, scope, id)
		return tx.err
	}
	if tx.nodes == nil {
		tx.nodes = make(map[nodeRecordKey]putNode)
	}
	tx.nodes[key] = op
	return nil
}

// commitRecordTransaction 与 save 共享确认后发布及失败封闭
// 语义。在分配完整旧版提案前检查前置条件；
// 失败时任何暂存变更都不能修改活跃记录。
func (s *runState) commitRecordTransaction(ctx context.Context, tx *recordTransaction) error {
	return s.commitRecordTransactionPrepared(ctx, tx, nil)
}

// prepare 在隔离的旧版提案上保留 commit 现有的流验证及完成规则。
// 仅在所有覆盖层前置条件通过后运行。
func (s *runState) commitRecordTransactionPrepared(ctx context.Context, tx *recordTransaction, prepare func(*RunRecord) error) error {
	if s.stopped != nil {
		return s.stopped
	}
	if err := checkCheckpointContext(ctx); err != nil {
		return err
	}
	if err := tx.check(s.record); err != nil {
		return err
	}
	return s.commitCheckedTransaction(ctx, tx, prepare)
}

func (tx *recordTransaction) check(record RunRecord) error {
	if tx.err != nil {
		return tx.err
	}
	if tx.baseRevision != record.Revision {
		return fmt.Errorf("%w: stale record transaction revision", ErrConflict)
	}
	for id, op := range tx.attempts {
		current, exists := record.Attempts[id]
		if exists == op.absent || (exists && current != op.expected) {
			return fmt.Errorf("%w: attempt precondition for %q", ErrConflict, id)
		}
	}
	for id := range tx.scopes {
		if _, exists := record.Scopes[id]; exists {
			return fmt.Errorf("%w: scope creation precondition for %q", ErrConflict, id)
		}
	}
	for key, op := range tx.nodes {
		scope, exists := record.Scopes[key.scope]
		_, created := tx.scopes[key.scope]
		if !exists && !created {
			return fmt.Errorf("%w: missing node scope %q", ErrConflict, key.scope)
		}
		current, exists := scope.Nodes[key.node]
		if exists == op.absent || (exists && !reflect.DeepEqual(current, op.expected)) {
			return fmt.Errorf("%w: node precondition for %q/%q", ErrConflict, key.scope, key.node)
		}
	}
	if err := tx.checkStreams(record); err != nil {
		return err
	}
	if err := tx.checkMessages(record); err != nil {
		return err
	}
	return nil
}

func (s *runState) commitCheckedTransaction(ctx context.Context, tx *recordTransaction, prepare func(*RunRecord) error) error {
	if len(tx.attempts)+len(tx.scopes)+len(tx.nodes)+len(tx.streams)+len(tx.pending)+len(tx.messages)+len(tx.managed)+len(tx.managedEntries) == 0 {
		return nil
	}
	if prepare != nil {
		// 最终节点验证可能物化只读提案。绝不将其作为
		// 完整检查点提交；普通钩子保持增量。
		proposal := cloneRecord(s.record)
		tx.apply(&proposal)
		if err := prepare(&proposal); err != nil {
			return err
		}
	}
	return s.applyDelta(ctx, tx)
}

// apply 由旧版物化、已确认运行时发布及 ChangeBatch.ApplyTo 共享。
// 调用方必须预先检查所有前置条件。
func (tx *recordTransaction) apply(proposed *RunRecord) {
	if len(tx.scopes) > 0 && proposed.Scopes == nil {
		proposed.Scopes = make(map[string]ScopeRecord)
	}
	for id, scope := range tx.scopes {
		scope.Nodes = make(map[string]NodeRecord)
		proposed.Scopes[id] = scope
	}
	for key, op := range tx.nodes {
		scope := proposed.Scopes[key.scope]
		if scope.Nodes == nil {
			scope.Nodes = make(map[string]NodeRecord)
		}
		scope.Nodes[key.node] = copyTransactionNode(op.value)
		proposed.Scopes[key.scope] = scope
	}
	if len(tx.attempts) > 0 && proposed.Attempts == nil {
		proposed.Attempts = make(map[string]AttemptRecord, len(tx.attempts))
	}
	for id, op := range tx.attempts {
		proposed.Attempts[id] = op.value
	}
	tx.applyStreams(proposed)
	tx.applyMessages(proposed)
}
