package filestore

import (
	"strconv"
	"unicode/utf8"

	durable "github.com/Luo-root/pulse-durable"
)

// validMetadata 只检查持久元数据的字符串及 map 键；Data 是不透明的业务编码。
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
				if !validStrings(output.Name, output.TypeID, output.Version, output.State, output.Digest) {
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
	return true
}

func validStrings(values ...string) bool {
	for _, value := range values {
		if !utf8.ValidString(value) {
			return false
		}
	}
	return true
}

// validJSONText 拒绝 encoding/json 会静默替换的原始 UTF-8 和未配对代理项。
// 调用前须已通过 JSON 语法检查；合法 U+FFFD、代理项对及转义反斜杠均保留。
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
