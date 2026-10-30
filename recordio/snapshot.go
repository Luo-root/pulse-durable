package recordio

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"

	durable "github.com/Luo-root/pulse-durable"
)

const snapshotVersion = 1

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
	value, err := snapshotValue(reflect.ValueOf(snapshotWire{snapshotVersion, record}))
	if err != nil {
		return nil, errors.New("recordio: unsupported snapshot field type")
	}
	data, err := json.Marshal(value)
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

// snapshotValue 遍历实际记录类型，而非维护第二份
// 字段副本。输出每个导出字段：JSON 的 null 与 {}
// 或 [] 区分映射或切片，null 与 "" 区分 []byte。指针
// 为 null 或完整值。标量保留其 Go JSON 编码。
// 不支持的种类会安全拒绝，而非悄然省略新记录字段。
func snapshotValue(v reflect.Value) (any, error) {
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return nil, nil
		}
		return snapshotValue(v.Elem())
	case reflect.Struct:
		out := make(map[string]any, v.NumField())
		for i := 0; i < v.NumField(); i++ {
			name := snapshotFieldName(v.Type().Field(i))
			if name == "" {
				return nil, errors.New("unsupported field")
			}
			if _, exists := out[name]; exists {
				return nil, errors.New("duplicate field")
			}
			value, err := snapshotValue(v.Field(i))
			if err != nil {
				return nil, err
			}
			out[name] = value
		}
		return out, nil
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return nil, errors.New("unsupported map key")
		}
		if v.IsNil() {
			return nil, nil
		}
		out := make(map[string]any, v.Len())
		iter := v.MapRange()
		for iter.Next() {
			value, err := snapshotValue(iter.Value())
			if err != nil {
				return nil, err
			}
			out[iter.Key().String()] = value
		}
		return out, nil
	case reflect.Slice:
		if v.IsNil() {
			return nil, nil
		}
		if v.Type() == reflect.TypeOf([]byte(nil)) {
			return v.Interface(), nil
		}
		out := make([]any, v.Len())
		for i := range out {
			value, err := snapshotValue(v.Index(i))
			if err != nil {
				return nil, err
			}
			out[i] = value
		}
		return out, nil
	case reflect.String, reflect.Bool, reflect.Int, reflect.Uint64:
		return v.Interface(), nil
	default:
		return nil, errors.New("unsupported kind")
	}
}

func snapshotFieldName(field reflect.StructField) string {
	if field.PkgPath != "" || field.Anonymous {
		return ""
	}
	name := strings.Split(field.Tag.Get("json"), ",")[0]
	if name == "-" {
		return ""
	}
	if name == "" {
		return field.Name
	}
	return name
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
				name := snapshotFieldName(typ.Field(i))
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
