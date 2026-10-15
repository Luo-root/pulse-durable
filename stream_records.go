package durable

// StreamIDRecord 是稳定逻辑流、读取组和当前持久代际。恢复提升代际，旧交付不能确认新代际。
type StreamIDRecord struct {
	Scope      string `json:"scope"`
	NodeID     string `json:"node_id"`
	Key        string `json:"key"`
	Group      string `json:"group"`
	Generation uint64 `json:"generation"`
}

// StreamDescriptorRecord 是完整节点成果中的纯数据端点引用，不含 Go 类型、channel 或闭包。
type StreamDescriptorRecord struct {
	ID        StreamIDRecord `json:"id"`
	Graph     string         `json:"graph"`
	Path      string         `json:"path"`
	Attempt   string         `json:"attempt"`
	TypeID    string         `json:"type_id"`
	Version   string         `json:"version"`
	SourceID  string         `json:"source_id"`
	Resumable bool           `json:"resumable"`
	Capacity  int            `json:"capacity"`
	Terminal  string         `json:"terminal,omitempty"`
	Stopped   bool           `json:"stopped,omitempty"`
}

// StreamMessageRecord 保存一条仍有读取义务的消息；已 Ack 的消息移除，序号不复用。
type StreamMessageRecord struct {
	Sequence uint64      `json:"sequence"`
	Value    ValueRecord `json:"value"`
}

// StreamRecord 保存可靠消息和来源位置。Pending 有序，AckedThrough 只推进连续前沿。
// Stopped 结束读取义务而不伪造 Ack；它与写端终态分开保存。
type StreamRecord struct {
	Stream       StreamDescriptorRecord `json:"stream"`
	LastSequence uint64                 `json:"last_sequence"`
	AckedThrough uint64                 `json:"acked_through"`
	Pending      []StreamMessageRecord  `json:"pending,omitempty"`
	Cursor       *ValueRecord           `json:"cursor,omitempty"`
}

// MessageAttemptRecord 的预算不按 Execution 或流代际重置。Parent 是处理节点的有效尝试。
// 消息尝试与普通 Retry 分开存储；失败后才能打开下一次，输入确认与 confirmed 原子保存。
type MessageAttemptRecord struct {
	Scope    string         `json:"scope"`
	Parent   string         `json:"parent"`
	NodeID   string         `json:"node_id"`
	Input    StreamIDRecord `json:"input"`
	Sequence uint64         `json:"sequence"`
	Index    int            `json:"index"`
	Limit    int            `json:"limit"`
	State    AttemptState   `json:"state"`
}

// ManagedRecord 是一个处理尝试已确认的显式状态和普通值成果，不包含隐式引用改动。
// State 是完整替换快照，Values 是只到达一次的节点输出，均与输入 Ack、输出消息一起保存。
type ManagedRecord struct {
	Scope  string        `json:"scope"`
	Parent string        `json:"parent"`
	NodeID string        `json:"node_id"`
	State  []ValueRecord `json:"state,omitempty"`
	Values []ValueRecord `json:"values,omitempty"`
}

func cloneValues(values []ValueRecord) []ValueRecord {
	out := append([]ValueRecord(nil), values...)
	for i := range out {
		out[i].Data = append([]byte(nil), values[i].Data...)
		if out[i].Stream != nil {
			copy := *out[i].Stream
			out[i].Stream = &copy
		}
	}
	return out
}

func cloneStreams(streams map[string]StreamRecord) map[string]StreamRecord {
	if streams == nil {
		return nil
	}
	out := make(map[string]StreamRecord, len(streams))
	for key, record := range streams {
		record.Pending = append([]StreamMessageRecord(nil), record.Pending...)
		for i := range record.Pending {
			record.Pending[i].Value = cloneValues([]ValueRecord{record.Pending[i].Value})[0]
		}
		if record.Cursor != nil {
			copy := cloneValues([]ValueRecord{*record.Cursor})[0]
			record.Cursor = &copy
		}
		out[key] = record
	}
	return out
}
