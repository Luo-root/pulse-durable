package durable

import (
	"context"
	"fmt"
	"math"
)

// ChangeHeader 将批次绑定到一次不可变运行和一次 revision 转换。
// 命名空间/所有权栅栏属于 session，而不是调用方提供的字段。
type ChangeHeader struct {
	BatchFormat, LogicalFormat                                        int
	RunID, BusinessVersion, DefinitionDigest, InputsDigest, RootScope string
	ExpectedRevision, Revision                                        uint64
}

// CommitAck 在运行时发布前必须与提交的 header 完全匹配。
// 它是此独占 Session 的确认，不是分布式回执。
type CommitAck struct{ Header ChangeHeader }

// ChangeBatch 是不可变、进程内、带版本的类型化操作封装。
// 其私有表包含 put-attempt、create-scope、put-node、stream-header、
// pending put/remove、message、managed-header 和 managed-entry put/remove 操作。
// 每项记录 Absent 或精确的旧类型化值（包括 nil 与 empty 的区别）。
// 这里有意不提供 JSON patch 或可变 map 的逃逸入口。后端代码使用 ApplyTo 执行统一的前置条件检查与应用算法。
// 未来可以添加线格式/导出器，而不暴露运行时事务别名。
// 零值无效。副本可以保留并独立应用。
type ChangeBatch struct {
	header ChangeHeader
	tx     *recordTransaction
}

func (b ChangeBatch) Header() ChangeHeader { return b.header }

// OperationCount 排除未改变的历史，并统计合并后的对象键数量。
func (b ChangeBatch) OperationCount() int {
	if b.tx == nil {
		return 0
	}
	t := b.tx
	return len(t.attempts) + len(t.scopes) + len(t.nodes) + len(t.streams) + len(t.pending) + len(t.messages) + len(t.managed) + len(t.managedEntries)
}

func changeHeader(r RunRecord) ChangeHeader {
	return ChangeHeader{BatchFormat: 1, LogicalFormat: RecordFormat, RunID: r.RunID,
		BusinessVersion: r.Version, DefinitionDigest: r.Definition, InputsDigest: r.Inputs,
		RootScope: r.RootScope, ExpectedRevision: r.Revision, Revision: r.Revision + 1}
}

// ApplyTo 在原地修改 record 前检查所有前置条件。调用方独占 record，必须围绕此调用提供原子持久化。
// 应用期间不会执行回调或用户代码。返回数据不会与批次共享别名，因此后端可以保留/修改自己的 record，而不影响 Runtime。
// 这里校验受信运行时的类型化变更，而不是图/codec schema；完整图校验仍由 Runtime 加载和现有检查点钩子执行。
func (b ChangeBatch) ApplyTo(record *RunRecord) (CommitAck, error) {
	if record == nil || b.tx == nil || record.Revision == math.MaxUint64 || b.header != changeHeader(*record) || b.header.ExpectedRevision != b.tx.baseRevision {
		return CommitAck{}, fmt.Errorf("%w: invalid delta header", ErrConflict)
	}
	if err := b.tx.check(*record); err != nil {
		return CommitAck{}, err
	}
	b.tx.apply(record)
	record.Format, record.Revision = b.header.LogicalFormat, b.header.Revision
	return CommitAck{Header: b.header}, nil
}

// copy 隔离每个可变的暂存值，但不复制未改变的 RunRecord 历史。
func (tx *recordTransaction) copy() *recordTransaction {
	out := newRecordTransaction(tx.baseRevision)
	out.err = tx.err
	for k, op := range tx.attempts {
		var e *AttemptRecord
		if !op.absent {
			e = &op.expected
		}
		_ = out.putAttempt(k, e, op.value)
	}
	for k, op := range tx.scopes {
		_ = out.createScope(k, op)
	}
	for k, op := range tx.nodes {
		var e *NodeRecord
		if !op.absent {
			e = &op.expected
		}
		_ = out.putNode(k.scope, k.node, e, op.value)
	}
	for k, op := range tx.streams {
		var e *streamHeader
		if !op.absent {
			e = &op.expected
		}
		_ = out.putStream(k, e, op.value)
	}
	for k, op := range tx.pending {
		var e, v *StreamMessageRecord
		if !op.absent {
			e = &op.expected
		}
		if !op.remove {
			v = &op.value
		}
		_ = out.putPending(k.stream, k.sequence, e, v)
	}
	for k, op := range tx.messages {
		var e *MessageAttemptRecord
		if !op.absent {
			e = &op.expected
		}
		_ = out.putMessage(k, e, op.value)
	}
	for k, op := range tx.managed {
		var e *managedHeader
		if !op.absent {
			e = &op.expected
		}
		_ = out.putManaged(k, e, op.value)
	}
	for k, op := range tx.managedEntries {
		var e, v *ValueRecord
		if !op.absent {
			e = &op.expected
		}
		if !op.remove {
			v = &op.value
		}
		_ = out.putManagedEntry(k, e, v)
	}
	return out
}

func (s *runState) applyDelta(ctx context.Context, tx *recordTransaction) (err error) {
	if s.stopped != nil {
		return s.stopped
	}
	if err := checkCheckpointContext(ctx); err != nil {
		return err
	}
	if s.record.Revision == math.MaxUint64 {
		return s.stop(fmt.Errorf("%w: revision exhausted", ErrConflict))
	}
	batch := ChangeBatch{header: changeHeader(s.record), tx: tx.copy()}
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			err = s.stop(fmt.Errorf("durable: store delta apply panicked"))
		}
	}()
	ack, err := s.session.Apply(ctx, batch)
	returned = true
	if err != nil {
		return s.stop(&operationError{op: "store delta apply", cause: err})
	}
	if ack.Header != batch.header {
		return s.stop(fmt.Errorf("%w: delta acknowledgement mismatch", ErrConflict))
	}
	// 所有 Initialize 和 Load 边界值都已隔离，批次拥有独立的事务副本。
	// 确认后只修改私有 record。
	tx.apply(&s.record)
	s.record.Format, s.record.Revision = RecordFormat, batch.header.Revision
	return nil
}
