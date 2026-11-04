package recordio

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/internal/snapshotfmt"
)

const snapshotVersion = snapshotfmt.Version

type snapshotWire struct {
	Version int               `json:"version"`
	Record  durable.RunRecord `json:"record"`
}

// EncodeSnapshot 编码任意修订，不归一化 nil 容器
// 或空 Data。快照版本独立于 RunRecord.Format。身份、
// 模式及修订号验证仍由调用方负责。
func EncodeSnapshot(record durable.RunRecord) ([]byte, error) {
	if !validMetadata(record) {
		return nil, errors.New("recordio: snapshot metadata is not UTF-8")
	}
	data, err := snapshotfmt.Encode(record)
	if err != nil {
		return nil, errors.New("recordio: encode snapshot failed")
	}
	return data, nil
}

// DecodeSnapshot 仅接受完整、大小写敏感的带版本快照
// 模式。拒绝未知或重复字段（包括映射键）、有损 Unicode 及
// 尾随数据。错误绝不包含已存储元数据或 Data。
func DecodeSnapshot(data []byte) (durable.RunRecord, error) {
	invalid := errors.New("recordio: invalid snapshot")
	if !json.Valid(data) || !validJSONText(data) {
		return durable.RunRecord{}, invalid
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := snapshotCheck(decoder, reflect.TypeOf(snapshotWire{})); err != nil {
		return durable.RunRecord{}, invalid
	}
	if _, err := decoder.Token(); err != io.EOF {
		return durable.RunRecord{}, invalid
	}
	var wire snapshotWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return durable.RunRecord{}, invalid
	}
	if wire.Version != snapshotVersion {
		return durable.RunRecord{}, errors.New("recordio: unsupported snapshot version")
	}
	if !validMetadata(wire.Record) {
		return durable.RunRecord{}, invalid
	}
	return wire.Record, nil
}

// token 级验证先于 Unmarshal，因为 encoding/json 原本会
// 接受重复键及大小写不敏感的结构体字段匹配。
func snapshotCheck(decoder *json.Decoder, typ reflect.Type) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	for typ.Kind() == reflect.Pointer {
		if token == nil {
			return nil
		}
		typ = typ.Elem()
	}
	bad := errors.New("invalid snapshot field")
	switch typ.Kind() {
	case reflect.Struct, reflect.Map:
		if token == nil && typ.Kind() == reflect.Map {
			return nil
		}
		if token != json.Delim('{') {
			return bad
		}
		fields := make(map[string]reflect.Type)
		if typ.Kind() == reflect.Struct {
			for i := 0; i < typ.NumField(); i++ {
				name := snapshotfmt.FieldName(typ.Field(i))
				if name == "" || fields[name] != nil {
					return bad
				}
				fields[name] = typ.Field(i).Type
			}
		} else if typ.Key().Kind() != reflect.String {
			return bad
		}
		seen := make(map[string]bool)
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return bad
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return bad
			}
			seen[name] = true
			var child reflect.Type
			if typ.Kind() == reflect.Struct {
				child = fields[name]
			} else {
				child = typ.Elem()
			}
			if child == nil {
				return bad
			}
			if err := snapshotCheck(decoder, child); err != nil {
				return err
			}
		}
		if typ.Kind() == reflect.Struct && len(seen) != len(fields) {
			return bad
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return bad
		}
		return nil
	case reflect.Slice:
		if token == nil {
			return nil
		}
		if typ == reflect.TypeOf([]byte(nil)) {
			if _, ok := token.(string); ok {
				return nil // Unmarshal 验证 base64。
			}
			return bad
		}
		if token != json.Delim('[') {
			return bad
		}
		for decoder.More() {
			if err := snapshotCheck(decoder, typ.Elem()); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return bad
		}
		return nil
	case reflect.String:
		if _, ok := token.(string); ok {
			return nil
		}
	case reflect.Bool:
		if _, ok := token.(bool); ok {
			return nil
		}
	case reflect.Int, reflect.Uint64:
		if _, ok := token.(json.Number); ok {
			return nil // Unmarshal 验证整数语法及范围。
		}
	}
	return bad
}
