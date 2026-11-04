// recordio 包将 Durable 记录编码为 JSON，并对 UTF-8 元数据进行无损检查。
// 业务 Data 保持为不透明字节。它不验证运行时模式、
// 运行身份或修订转换；这些仍由调用方负责。
package recordio

import (
	"encoding/json"
	"errors"
	"strconv"
	"unicode/utf8"

	durable "github.com/Luo-root/pulse-durable"
)

// Encode 在编码记录时验证元数据，并保留不透明 Data 字节。
func Encode(record durable.RunRecord) ([]byte, error) {
	if !validMetadata(record) {
		return nil, errors.New("recordio: record metadata is not UTF-8")
	}
	data, err := json.Marshal(record)
	if err != nil {
		return nil, errors.New("recordio: encode record failed")
	}
	return data, nil
}

// Decode 检查 JSON 语法及 Unicode，不在错误中暴露记录内容。
// Store 实现仍负责运行身份、修订号及 CAS 检查。
func Decode(data []byte) (durable.RunRecord, error) {
	if !json.Valid(data) {
		return durable.RunRecord{}, errors.New("recordio: invalid record JSON")
	}
	if !validJSONText(data) {
		return durable.RunRecord{}, errors.New("recordio: stored JSON contains invalid Unicode strings")
	}
	var record durable.RunRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return durable.RunRecord{}, errors.New("recordio: decode record failed")
	}
	return record, nil
}

// validMetadata 检查元数据字符串及映射键，不检查不透明业务 Data。
func validMetadata(record durable.RunRecord) bool {
	if !validStrings(record.RunID, record.Version, record.Definition, record.Inputs, record.RootScope) {
		return false
	}
	for id, scope := range record.Scopes {
		if !validStrings(id, scope.Graph, scope.Path, scope.Inputs) {
			return false
		}
		for id, node := range scope.Nodes {
			if !validStrings(id, node.NodeID, node.Attempt, node.Reason) {
				return false
			}
			for _, output := range node.Outputs {
				if !validValueMetadata(output) {
					return false
				}
			}
		}
	}
	for id, attempt := range record.Attempts {
		if !validStrings(id, attempt.Parent, attempt.NodeID, attempt.Boundary, string(attempt.State)) {
			return false
		}
	}
	for id, stream := range record.Streams {
		if !validStrings(id) || !validStreamMetadata(stream.Stream) {
			return false
		}
		for _, message := range stream.Pending {
			if !validValueMetadata(message.Value) {
				return false
			}
		}
		if stream.Cursor != nil && !validValueMetadata(*stream.Cursor) {
			return false
		}
	}
	for id, message := range record.Messages {
		if !validStrings(id, message.Scope, message.Parent, message.NodeID, string(message.State)) || !validStreamIDMetadata(message.Input) {
			return false
		}
	}
	for id, managed := range record.Managed {
		if !validStrings(id, managed.Scope, managed.Parent, managed.NodeID) {
			return false
		}
		for _, state := range managed.State {
			if !validValueMetadata(state) {
				return false
			}
		}
		for _, value := range managed.Values {
			if !validValueMetadata(value) {
				return false
			}
		}
	}
	return true
}

func validStreamIDMetadata(id durable.StreamIDRecord) bool {
	return validStrings(id.Scope, id.NodeID, id.Key, id.Group)
}

func validStreamMetadata(stream durable.StreamDescriptorRecord) bool {
	return validStreamIDMetadata(stream.ID) && validStrings(stream.Graph, stream.Path, stream.Attempt, stream.TypeID, stream.Version, stream.SourceID, stream.Terminal)
}

func validValueMetadata(value durable.ValueRecord) bool {
	return validStrings(value.Name, value.TypeID, value.Version, value.State, value.Digest) && (value.Stream == nil || validStreamMetadata(*value.Stream))
}

func validStrings(values ...string) bool {
	for _, value := range values {
		if !utf8.ValidString(value) {
			return false
		}
	}
	return true
}

// validJSONText 要求 JSON 语法有效，并拒绝有损 Unicode 修复。
func validJSONText(data []byte) bool {
	if !utf8.Valid(data) {
		return false
	}
	for i := 0; i < len(data); i++ {
		if data[i] != '"' {
			continue
		}
		for i++; data[i] != '"'; i++ {
			if data[i] != '\\' {
				continue
			}
			i++
			if data[i] != 'u' {
				continue
			}
			code, _ := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
			i += 4
			switch {
			case code >= 0xd800 && code <= 0xdbff:
				if i+6 >= len(data) || data[i+1] != '\\' || data[i+2] != 'u' {
					return false
				}
				low, err := strconv.ParseUint(string(data[i+3:i+7]), 16, 16)
				if err != nil || low < 0xdc00 || low > 0xdfff {
					return false
				}
				i += 6
			case code >= 0xdc00 && code <= 0xdfff:
				return false
			}
		}
	}
	return true
}
