// 本文件固定快照编码边界的数据形状，并显式映射领域记录。
// DTO 不嵌入 RunRecord：领域类型新增字段不会自动修改持久 schema。
// RunRecord 只表达业务事实；封装格式由 snapshot.go 的边界负责。
package recordio

import durable "github.com/Luo-root/pulse-durable"

// snapshotRecord 是当前快照的固定顶层 DTO。嵌套记录也使用专用 DTO，
// 防止领域对象的结构变更在未审查的情况下静默改变持久字节和预算。
type snapshotRecord struct {
	RunID            string                            `json:"run_id"`
	Revision         uint64                            `json:"revision"`
	Version          string                            `json:"version"`
	Definition       string                            `json:"definition"`
	Inputs           string                            `json:"inputs"`
	RootScope        string                            `json:"root_scope"`
	Scopes           map[string]snapshotScope          `json:"scopes"`
	Attempts         map[string]snapshotAttempt        `json:"attempts"`
	Streams          map[string]snapshotStream         `json:"streams"`
	Messages         map[string]snapshotMessageAttempt `json:"messages"`
	Managed          map[string]snapshotManaged        `json:"managed"`
	HistorySummaries []snapshotHistorySummary          `json:"history_summaries" snapshot:"optional"`
	CommitIndex      []snapshotCommitIndex             `json:"commit_index" snapshot:"optional"`
	Provenance       *snapshotProvenance               `json:"provenance" snapshot:"optional"`
}

// snapshotScope 只固定领域事实；旧 bindings 标记不属于当前快照形状。
// DecodeSnapshot 的精确字段校验会拒绝携带该字段的旧结构，不做迁移映射。
type snapshotScope struct {
	Graph  string                  `json:"graph"`
	Path   string                  `json:"path"`
	Inputs string                  `json:"inputs"`
	Nodes  map[string]snapshotNode `json:"nodes"`
}

type snapshotAttempt struct {
	Parent   string `json:"parent"`
	NodeID   string `json:"node_id"`
	Boundary string `json:"boundary"`
	Index    int    `json:"index"`
	Limit    int    `json:"limit"`
	State    string `json:"state"`
}

type snapshotNode struct {
	NodeID  string          `json:"node_id"`
	Attempt string          `json:"attempt"`
	Reason  string          `json:"reason"`
	Outputs []snapshotValue `json:"outputs"`
}

type snapshotValue struct {
	Name    string                    `json:"name"`
	TypeID  string                    `json:"type_id"`
	Version string                    `json:"version"`
	State   string                    `json:"state"`
	Digest  string                    `json:"digest"`
	Data    []byte                    `json:"data"`
	Stream  *snapshotStreamDescriptor `json:"stream"`
}

type snapshotStreamID struct {
	Scope      string `json:"scope"`
	NodeID     string `json:"node_id"`
	Key        string `json:"key"`
	Group      string `json:"group"`
	Generation uint64 `json:"generation"`
}

type snapshotStreamDescriptor struct {
	ID        snapshotStreamID `json:"id"`
	Graph     string           `json:"graph"`
	Path      string           `json:"path"`
	Attempt   string           `json:"attempt"`
	TypeID    string           `json:"type_id"`
	Version   string           `json:"version"`
	SourceID  string           `json:"source_id"`
	Resumable bool             `json:"resumable"`
	Capacity  int              `json:"capacity"`
	Terminal  string           `json:"terminal"`
	Stopped   bool             `json:"stopped"`
}

type snapshotStreamMessage struct {
	Sequence uint64        `json:"sequence"`
	Value    snapshotValue `json:"value"`
}

type snapshotStream struct {
	Stream       snapshotStreamDescriptor `json:"stream"`
	LastSequence uint64                   `json:"last_sequence"`
	AckedThrough uint64                   `json:"acked_through"`
	Pending      []snapshotStreamMessage  `json:"pending"`
	Cursor       *snapshotValue           `json:"cursor"`
}

type snapshotMessageAttempt struct {
	Scope    string           `json:"scope"`
	Parent   string           `json:"parent"`
	NodeID   string           `json:"node_id"`
	Input    snapshotStreamID `json:"input"`
	Sequence uint64           `json:"sequence"`
	Index    int              `json:"index"`
	Limit    int              `json:"limit"`
	State    string           `json:"state"`
}

type snapshotManaged struct {
	Scope  string          `json:"scope"`
	Parent string          `json:"parent"`
	NodeID string          `json:"node_id"`
	State  []snapshotValue `json:"state"`
	Values []snapshotValue `json:"values"`
}

type snapshotHistorySummary struct {
	Kind           string `json:"kind"`
	ID             string `json:"id"`
	SourceRevision uint64 `json:"source_revision"`
	SourceDigest   string `json:"source_digest"`
	Payload        []byte `json:"payload"`
	Digest         string `json:"digest"`
}

type snapshotCommitIndex struct {
	Revision       uint64 `json:"revision"`
	ParentRevision uint64 `json:"parent_revision"`
	BatchDigest    string `json:"batch_digest"`
}

type snapshotProvenance struct {
	OriginRunID        string `json:"origin_run_id"`
	OriginRevision     uint64 `json:"origin_revision"`
	OriginRecordDigest string `json:"origin_record_digest"`
	CompressionID      string `json:"compression_id"`
	Digest             string `json:"digest"`
}

// mapSnapshotMap/mapSnapshotSlice 只复用容器形状映射，不判断业务语义。
// nil 与非 nil 空容器必须保持不同；业务不变量由根包统一校验。
func mapSnapshotMap[A, B any](values map[string]A, convert func(A) B) map[string]B {
	if values == nil {
		return nil
	}
	out := make(map[string]B, len(values))
	for key, value := range values {
		out[key] = convert(value)
	}
	return out
}

func mapSnapshotSlice[A, B any](values []A, convert func(A) B) []B {
	if values == nil {
		return nil
	}
	out := make([]B, len(values))
	for i, value := range values {
		out[i] = convert(value)
	}
	return out
}

func mapSnapshotPointer[A, B any](value *A, convert func(A) B) *B {
	if value == nil {
		return nil
	}
	out := convert(*value)
	return &out
}

func snapshotDescriptorFromRecord(s durable.StreamDescriptorRecord) snapshotStreamDescriptor {
	return snapshotStreamDescriptor{ID: snapshotStreamID(s.ID), Graph: s.Graph, Path: s.Path, Attempt: s.Attempt,
		TypeID: s.TypeID, Version: s.Version, SourceID: s.SourceID, Resumable: s.Resumable, Capacity: s.Capacity, Terminal: s.Terminal, Stopped: s.Stopped}
}

func (s snapshotStreamDescriptor) record() durable.StreamDescriptorRecord {
	return durable.StreamDescriptorRecord{ID: durable.StreamIDRecord(s.ID), Graph: s.Graph, Path: s.Path, Attempt: s.Attempt,
		TypeID: s.TypeID, Version: s.Version, SourceID: s.SourceID, Resumable: s.Resumable, Capacity: s.Capacity, Terminal: s.Terminal, Stopped: s.Stopped}
}

func snapshotValueFromRecord(v durable.ValueRecord) snapshotValue {
	return snapshotValue{v.Name, v.TypeID, v.Version, v.State, v.Digest, v.Data, mapSnapshotPointer(v.Stream, snapshotDescriptorFromRecord)}
}

func (v snapshotValue) record() durable.ValueRecord {
	return durable.ValueRecord{Name: v.Name, TypeID: v.TypeID, Version: v.Version, State: v.State, Digest: v.Digest, Data: v.Data,
		Stream: mapSnapshotPointer(v.Stream, snapshotStreamDescriptor.record)}
}

// snapshotRecordFromDomain 建立只读编码视图；调用期间领域记录须由调用方独占。
// 视图不出此编码边界，也不作为可变状态发布，因此不复制不透明 Data 字节。
func snapshotRecordFromDomain(r durable.RunRecord) snapshotRecord {
	out := snapshotRecord{RunID: r.RunID, Revision: r.Revision, Version: r.Version, Definition: r.Definition, Inputs: r.Inputs, RootScope: r.RootScope}
	out.Scopes = mapSnapshotMap(r.Scopes, func(s durable.ScopeRecord) snapshotScope {
		return snapshotScope{s.Graph, s.Path, s.Inputs, mapSnapshotMap(s.Nodes, func(n durable.NodeRecord) snapshotNode {
			return snapshotNode{n.NodeID, n.Attempt, n.Reason, mapSnapshotSlice(n.Outputs, snapshotValueFromRecord)}
		})}
	})
	out.Attempts = mapSnapshotMap(r.Attempts, func(a durable.AttemptRecord) snapshotAttempt {
		return snapshotAttempt{a.Parent, a.NodeID, a.Boundary, a.Index, a.Limit, string(a.State)}
	})
	out.Streams = mapSnapshotMap(r.Streams, func(s durable.StreamRecord) snapshotStream {
		return snapshotStream{snapshotDescriptorFromRecord(s.Stream), s.LastSequence, s.AckedThrough,
			mapSnapshotSlice(s.Pending, func(m durable.StreamMessageRecord) snapshotStreamMessage {
				return snapshotStreamMessage{m.Sequence, snapshotValueFromRecord(m.Value)}
			}),
			mapSnapshotPointer(s.Cursor, snapshotValueFromRecord)}
	})
	out.Messages = mapSnapshotMap(r.Messages, func(m durable.MessageAttemptRecord) snapshotMessageAttempt {
		return snapshotMessageAttempt{m.Scope, m.Parent, m.NodeID, snapshotStreamID(m.Input), m.Sequence, m.Index, m.Limit, string(m.State)}
	})
	out.Managed = mapSnapshotMap(r.Managed, func(m durable.ManagedRecord) snapshotManaged {
		return snapshotManaged{m.Scope, m.Parent, m.NodeID, mapSnapshotSlice(m.State, snapshotValueFromRecord), mapSnapshotSlice(m.Values, snapshotValueFromRecord)}
	})
	out.HistorySummaries = mapSnapshotSlice(r.HistorySummaries, func(s durable.HistorySummary) snapshotHistorySummary { return snapshotHistorySummary(s) })
	out.CommitIndex = mapSnapshotSlice(r.CommitIndex, func(e durable.CommitIndexEntry) snapshotCommitIndex { return snapshotCommitIndex(e) })
	out.Provenance = mapSnapshotPointer(r.Provenance, func(p durable.Provenance) snapshotProvenance { return snapshotProvenance(p) })
	return out
}

// record 只从已经严格解码的 DTO 构造领域事实。Data/Payload 的存储由本次
// JSON 解码独占；映射返回后不再保留 DTO，不向调用方泄漏可变共享状态。
func (r snapshotRecord) record() durable.RunRecord {
	out := durable.RunRecord{RunID: r.RunID, Revision: r.Revision, Version: r.Version, Definition: r.Definition, Inputs: r.Inputs, RootScope: r.RootScope}
	out.Scopes = mapSnapshotMap(r.Scopes, func(s snapshotScope) durable.ScopeRecord {
		return durable.ScopeRecord{Graph: s.Graph, Path: s.Path, Inputs: s.Inputs, Nodes: mapSnapshotMap(s.Nodes, func(n snapshotNode) durable.NodeRecord {
			return durable.NodeRecord{NodeID: n.NodeID, Attempt: n.Attempt, Reason: n.Reason, Outputs: mapSnapshotSlice(n.Outputs, snapshotValue.record)}
		})}
	})
	out.Attempts = mapSnapshotMap(r.Attempts, func(a snapshotAttempt) durable.AttemptRecord {
		return durable.AttemptRecord{Parent: a.Parent, NodeID: a.NodeID, Boundary: a.Boundary, Index: a.Index, Limit: a.Limit, State: durable.AttemptState(a.State)}
	})
	out.Streams = mapSnapshotMap(r.Streams, func(s snapshotStream) durable.StreamRecord {
		return durable.StreamRecord{Stream: s.Stream.record(), LastSequence: s.LastSequence, AckedThrough: s.AckedThrough,
			Pending: mapSnapshotSlice(s.Pending, func(m snapshotStreamMessage) durable.StreamMessageRecord {
				return durable.StreamMessageRecord{Sequence: m.Sequence, Value: m.Value.record()}
			}),
			Cursor: mapSnapshotPointer(s.Cursor, snapshotValue.record)}
	})
	out.Messages = mapSnapshotMap(r.Messages, func(m snapshotMessageAttempt) durable.MessageAttemptRecord {
		return durable.MessageAttemptRecord{Scope: m.Scope, Parent: m.Parent, NodeID: m.NodeID, Input: durable.StreamIDRecord(m.Input), Sequence: m.Sequence, Index: m.Index, Limit: m.Limit, State: durable.AttemptState(m.State)}
	})
	out.Managed = mapSnapshotMap(r.Managed, func(m snapshotManaged) durable.ManagedRecord {
		return durable.ManagedRecord{Scope: m.Scope, Parent: m.Parent, NodeID: m.NodeID, State: mapSnapshotSlice(m.State, snapshotValue.record), Values: mapSnapshotSlice(m.Values, snapshotValue.record)}
	})
	out.HistorySummaries = mapSnapshotSlice(r.HistorySummaries, func(s snapshotHistorySummary) durable.HistorySummary { return durable.HistorySummary(s) })
	out.CommitIndex = mapSnapshotSlice(r.CommitIndex, func(e snapshotCommitIndex) durable.CommitIndexEntry { return durable.CommitIndexEntry(e) })
	out.Provenance = mapSnapshotPointer(r.Provenance, func(p snapshotProvenance) durable.Provenance { return durable.Provenance(p) })
	return out
}
