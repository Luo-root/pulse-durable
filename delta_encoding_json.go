package durable

import (
	"bytes"
	"encoding/json"
	"strconv"
	"unicode/utf8"
)

// 本文件相当于 wire 解码器前的一层词法守卫：标准库负责完整 JSON 语法，
// 这里只补充重复键、原始 Unicode 和业务规定的深度上限。它不构造执行对象，
// 也不解释 operation；后面的类型化解码和事务检查仍决定一个批次能否执行。
//
// Data 是整数数组，容量上限附近会有数百万个数字。逐数字调用 Decoder.Token
// 会为每个字节分配接口、字符串和解码缓冲。改为在原缓冲区移动游标后，空间
// 只随对象键和嵌套深度增长，不随数组里的标量个数增长。
func rejectJSON(data []byte) error {
	if len(data) == 0 || len(data) > MaxChangeBatchBytes || !utf8.Valid(data) || !json.Valid(data) || !validWireUnicode(data) {
		return errInvalidChangeWire
	}
	s := wireJSONScanner{data: data}
	if err := s.value(0); err != nil {
		return err
	}
	s.space()
	if s.pos != len(data) {
		return errInvalidChangeWire
	}
	return nil
}

type wireJSONScanner struct {
	data []byte
	pos  int
}

func (s *wireJSONScanner) space() {
	for s.pos < len(s.data) {
		switch s.data[s.pos] {
		case ' ', '\t', '\r', '\n':
			s.pos++
		default:
			return
		}
	}
}

// stringEnd 仅在 json.Valid 已成功之后使用；跳过转义后的一个字符即可定位
// 结束引号，Unicode 转义和代理对的合法性由 validWireUnicode 单独检查。
func (s *wireJSONScanner) stringEnd() (start, end int, escaped bool) {
	start = s.pos
	s.pos++
	for s.data[s.pos] != '"' {
		if s.data[s.pos] == '\\' {
			escaped = true
			s.pos++
		}
		s.pos++
	}
	s.pos++
	return start, s.pos, escaped
}

func (s *wireJSONScanner) value(depth int) error {
	if depth > MaxChangeBatchDepth {
		return errInvalidChangeWire
	}
	s.space()
	switch s.data[s.pos] {
	case '{':
		s.pos++
		s.space()
		seen := make(map[string]struct{})
		for s.data[s.pos] != '}' {
			start, end, escaped := s.stringEnd()
			key := string(s.data[start+1 : end-1])
			if escaped && json.Unmarshal(s.data[start:end], &key) != nil {
				return errInvalidChangeWire
			}
			if _, exists := seen[key]; exists {
				return errInvalidChangeWire
			}
			seen[key] = struct{}{}
			s.space()
			s.pos++ // 已由标准 JSON 校验保证为冒号。
			if err := s.value(depth + 1); err != nil {
				return err
			}
			s.space()
			if s.data[s.pos] == ',' {
				s.pos++
				s.space()
			}
		}
		s.pos++
	case '[':
		s.pos++
		s.space()
		for s.data[s.pos] != ']' {
			if err := s.value(depth + 1); err != nil {
				return err
			}
			s.space()
			if s.data[s.pos] == ',' {
				s.pos++
				s.space()
			}
		}
		s.pos++
	case '"':
		s.stringEnd()
	default:
		start := s.pos
		exponent := false
		for s.pos < len(s.data) {
			c := s.data[s.pos]
			if c == ',' || c == '}' || c == ']' || c == ' ' || c == '\t' || c == '\r' || c == '\n' {
				break
			}
			exponent = exponent || c == 'e' || c == 'E'
			s.pos++
		}
		// 保留旧 Token 扫描器拒绝 float64 溢出的行为；常见字节整数无需转换。
		if c := s.data[start]; (c == '-' || c >= '0' && c <= '9') && (exponent || s.pos-start > 308) {
			if _, err := strconv.ParseFloat(string(s.data[start:s.pos]), 64); err != nil {
				return errInvalidChangeWire
			}
		}
	}
	return nil
}

// normalizeWireBytes 将 wire 的整数数组校验后转换成标准库 []byte 所需的
// Base64 JSON 字符串。转换只存在于解码器内部；持久 wire 仍是原整数数组。
// nil 与空数组分别产生 null 与空字符串，最终解码仍得到 nil 与非 nil 空切片。
func normalizeWireBytes(data []byte, depth int) ([]byte, error) {
	data = bytes.TrimSpace(data)
	if len(data) < 2 || data[0] != '[' || data[len(data)-1] != ']' {
		return nil, errInvalidChangeWire
	}
	var value []byte = make([]byte, 0, len(data)/2)
	s := wireJSONScanner{data: data, pos: 1}
	s.space()
	for s.pos < len(data)-1 {
		if depth+1 > MaxChangeBatchDepth || data[s.pos] < '0' || data[s.pos] > '9' {
			return nil, errInvalidChangeWire
		}
		start, n := s.pos, 0
		for s.pos < len(data)-1 && data[s.pos] >= '0' && data[s.pos] <= '9' {
			n = n*10 + int(data[s.pos]-'0')
			if n > 255 || s.pos-start >= 3 {
				return nil, errInvalidChangeWire
			}
			s.pos++
		}
		if s.pos-start > 1 && data[start] == '0' {
			return nil, errInvalidChangeWire
		}
		value = append(value, byte(n))
		s.space()
		if s.pos == len(data)-1 {
			break
		}
		if data[s.pos] != ',' {
			return nil, errInvalidChangeWire
		}
		s.pos++
		s.space()
		if s.pos == len(data)-1 {
			return nil, errInvalidChangeWire
		}
	}
	return json.Marshal(value)
}
