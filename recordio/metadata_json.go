// 本文件为裸记录 JSON 提供有限深度、精确字段名和重复键检查。
// 它是编码边界的结构守卫，不解释领域语义，也不承担快照信封的降级兼容。
package recordio

import (
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
)

// checkBareJSON 在 encoding/json 的宽松大小写匹配之前，按当前裸记录
// 类型逐层核对真实字段名。裸编码允许 omitempty 字段缺席，因此这里只拒绝
// 未知、重复和别名字段，不复用快照的“全字段必填”规则。映射键仍是用户
// 持久身份，不能被当作结构字段做大小写归一化。
func checkBareJSON(decoder *json.Decoder, recordType reflect.Type) error {
	decoder.UseNumber()
	invalid := errors.New("recordio: invalid record JSON structure")
	var visit func(reflect.Type, int, bool) error
	visit = func(typ reflect.Type, depth int, root bool) error {
		if depth > 64 {
			return invalid
		}
		token, err := decoder.Token()
		if err != nil || root && token != json.Delim('{') {
			return invalid
		}
		for typ.Kind() == reflect.Pointer {
			if token == nil {
				return nil
			}
			typ = typ.Elem()
		}
		switch typ.Kind() {
		case reflect.Struct, reflect.Map:
			if token == nil && typ.Kind() == reflect.Map {
				return nil
			}
			if token != json.Delim('{') {
				return invalid
			}
			fields := make(map[string]reflect.Type)
			if typ.Kind() == reflect.Struct {
				for i := 0; i < typ.NumField(); i++ {
					field := typ.Field(i)
					if field.PkgPath != "" {
						continue
					}
					name := strings.Split(field.Tag.Get("json"), ",")[0]
					if name == "-" {
						continue
					}
					if name == "" {
						name = field.Name
					}
					if fields[name] != nil {
						return invalid
					}
					fields[name] = field.Type
				}
			} else if typ.Key().Kind() != reflect.String {
				return invalid
			}
			seen := make(map[string]bool)
			for decoder.More() {
				key, err := decoder.Token()
				name, ok := key.(string)
				if err != nil || !ok || seen[name] {
					return invalid
				}
				seen[name] = true
				var child reflect.Type
				if typ.Kind() == reflect.Struct {
					child = fields[name]
				} else {
					child = typ.Elem()
				}
				if child == nil {
					return invalid
				}
				if err := visit(child, depth+1, false); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return invalid
			}
		case reflect.Slice:
			if token == nil {
				return nil
			}
			if typ == reflect.TypeFor[[]byte]() {
				if _, ok := token.(string); ok {
					return nil // 后续 JSON 解码负责检查 base64。
				}
				return invalid
			}
			if token != json.Delim('[') {
				return invalid
			}
			for decoder.More() {
				if err := visit(typ.Elem(), depth+1, false); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return invalid
			}
		case reflect.String:
			if _, ok := token.(string); !ok {
				return invalid
			}
		case reflect.Bool:
			if _, ok := token.(bool); !ok {
				return invalid
			}
		case reflect.Int, reflect.Uint64:
			if _, ok := token.(json.Number); !ok {
				return invalid
			}
		default:
			return invalid
		}
		return nil
	}
	if err := visit(recordType, 0, true); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return invalid
	}
	return nil
}
