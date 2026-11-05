package durable

import (
	"fmt"
	"math"

	"github.com/Luo-root/pulse-durable/internal/snapshotfmt"
)

// SnapshotSizeDelta 返回本批应用前后无损快照格式 1 的精确有符号字节差。
// 它核对与 ApplyTo 相同的 header 和对象前置条件，不修改 before，也不克隆完整记录。
// 两种逻辑格式均只计算变化对象及其容器的大小；未变更的语义历史不参与编码。
// 调用方须独占 before，且保证其来自完整加载校验。
// 完整快照大小包括版本信封；本方法只计算差额，不能代替初始化时的完整计数。
// nil/空容器、JSON 转义、base64 和修订号位数均参与计数；结果不代表磁盘或 WAL 字节。
func (b ChangeBatch) SnapshotSizeDelta(before RunRecord) (int64, error) {
	if b.tx == nil || before.Revision == math.MaxUint64 || !validChangeHeader(before, b.header, b.tx) || b.header.ExpectedRevision != b.tx.baseRevision {
		return 0, fmt.Errorf("%w: invalid delta header", ErrConflict)
	}
	if err := b.tx.check(before); err != nil {
		return 0, err
	}
	if err := b.validateSemanticTransition(before); err != nil {
		return 0, err
	}
	c := snapshotDelta{}
	c.replace(before.Format, b.header.LogicalFormat)
	c.replace(before.Revision, b.header.Revision)
	tx := b.tx
	attempts := 0
	for key, op := range tx.attempts {
		if op.absent {
			c.insert(key, op.value)
			attempts++
		} else {
			c.replace(op.expected, op.value)
		}
	}
	c.container(before.Attempts == nil, len(before.Attempts), attempts, len(tx.attempts) != 0, false)

	for key, scope := range tx.scopes {
		// apply 创建的 Nodes 为非 nil 空 map，而捕获值的 Nodes 固定为 nil。
		c.insert(key, scope)
		c.add(-2)
	}
	c.container(before.Scopes == nil, len(before.Scopes), len(tx.scopes), len(tx.scopes) != 0, false)
	nodeAdds := make(map[string]int)
	for key, op := range tx.nodes {
		if op.absent {
			c.insert(key.node, op.value)
			nodeAdds[key.scope]++
		} else {
			c.replace(op.expected, op.value)
		}
	}
	for scope, additions := range nodeAdds {
		nodes := before.Scopes[scope].Nodes
		_, created := tx.scopes[scope]
		c.container(nodes == nil && !created, len(nodes), additions, true, false)
	}

	streams := 0
	for key, op := range tx.streams {
		value := snapshotStreamHeader(op.value)
		if op.absent {
			c.insert(key, value)
			streams++
		} else {
			c.replace(snapshotStreamHeader(op.expected), value)
		}
	}
	c.container(before.Streams == nil, len(before.Streams), streams, len(tx.streams) != 0, false)
	pendingChanges := make(map[string]int)
	for key, op := range tx.pending {
		if op.absent {
			c.value(op.value, 1)
			pendingChanges[key.stream]++
		} else if op.remove {
			c.value(op.expected, -1)
			pendingChanges[key.stream]--
		} else {
			c.replace(op.expected, op.value)
			if _, ok := pendingChanges[key.stream]; !ok {
				pendingChanges[key.stream] = 0
			}
		}
	}
	for key, additions := range pendingChanges {
		pending := before.Streams[key].Pending
		// 删除最后一个 pending 后仍保留非 nil 空切片。
		c.container(pending == nil, len(pending), additions, true, false)
	}

	messages := 0
	for key, op := range tx.messages {
		if op.absent {
			c.insert(key, op.value)
			messages++
		} else {
			c.replace(op.expected, op.value)
		}
	}
	c.container(before.Messages == nil, len(before.Messages), messages, len(tx.messages) != 0, false)

	managed := 0
	for key, op := range tx.managed {
		value := snapshotManagedHeader(op.value)
		if op.absent {
			c.insert(key, value)
			managed++
		} else {
			c.replace(snapshotManagedHeader(op.expected), value)
		}
	}
	c.container(before.Managed == nil, len(before.Managed), managed, len(tx.managed) != 0, false)
	type entriesKey struct {
		managed string
		state   bool
	}
	entryChanges := make(map[entriesKey]int)
	for key, op := range tx.managedEntries {
		group := entriesKey{key.managed, key.state}
		if op.absent {
			c.value(op.value, 1)
			entryChanges[group]++
		} else if op.remove {
			c.value(op.expected, -1)
			entryChanges[group]--
		} else {
			c.replace(op.expected, op.value)
			if _, ok := entryChanges[group]; !ok {
				entryChanges[group] = 0
			}
		}
	}
	for group, additions := range entryChanges {
		m := before.Managed[group.managed]
		values := m.Values
		if group.state {
			values = m.State
		}
		// managed 条目的最后一次删除将空切片归一化为 nil。
		c.container(values == nil, len(values), additions, true, len(values)+additions == 0)
	}
	summaries := 0
	for _, op := range tx.summaries {
		switch {
		case op.absent:
			c.value(op.value, 1)
			summaries++
		case op.remove:
			c.value(op.expected, -1)
			summaries--
		default:
			c.replace(op.expected, op.value)
		}
	}
	c.container(before.HistorySummaries == nil, len(before.HistorySummaries), summaries, len(tx.summaries) != 0, len(before.HistorySummaries)+summaries == 0)
	// 普通 Format3 提交只允许追加下一条索引，历史条目是不可变证据。
	for _, op := range tx.commitIndex {
		c.value(op.value, 1)
	}
	c.container(before.CommitIndex == nil, len(before.CommitIndex), len(tx.commitIndex), len(tx.commitIndex) != 0, false)
	if op := tx.provenance; op != nil {
		c.replace(before.Provenance, &op.value)
	}
	if c.err != nil {
		return 0, fmt.Errorf("%w: snapshot size cannot be represented", ErrInvalidRecord)
	}
	return c.delta, nil
}

// 这些空子容器只用作头的编码骨架，替换时子容器贡献抵消。
func snapshotStreamHeader(h streamHeader) StreamRecord {
	return StreamRecord{Stream: h.Stream, LastSequence: h.LastSequence, AckedThrough: h.AckedThrough, Cursor: h.Cursor}
}
func snapshotManagedHeader(h managedHeader) ManagedRecord {
	return ManagedRecord{Scope: h.Scope, Parent: h.Parent, NodeID: h.NodeID}
}

type snapshotDelta struct {
	delta int64
	err   error
}

func (c *snapshotDelta) add(n int64) {
	if c.err == nil {
		c.delta, c.err = snapshotfmt.Add(c.delta, n)
	}
}
func (c *snapshotDelta) value(v any, sign int64) {
	if c.err != nil {
		return
	}
	n, err := snapshotfmt.Size(v)
	if err != nil {
		c.err = err
		return
	}
	c.add(sign * n)
}
func (c *snapshotDelta) replace(old, next any) {
	c.value(old, -1)
	c.value(next, 1)
}
func (c *snapshotDelta) insert(key string, value any) {
	if c.err != nil {
		return
	}
	n, err := snapshotfmt.QuotedSize(key)
	if err != nil {
		c.err = err
		return
	}
	c.add(n)
	c.add(1)
	c.value(value, 1)
}
func (c *snapshotDelta) container(wasNil bool, count, additions int, changed, becomesNil bool) {
	if !changed || c.err != nil {
		return
	}
	old, err := snapshotfmt.ContainerOverhead(wasNil, count)
	if err != nil {
		c.err = err
		return
	}
	next, err := snapshotfmt.ContainerOverhead(becomesNil, count+additions)
	if err != nil {
		c.err = err
		return
	}
	c.add(next - old)
}
