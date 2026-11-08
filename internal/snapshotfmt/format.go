// snapshotfmt 定义无损快照的字段、编码和精确字节计数规则。
// 它只依赖标准库，不认识业务记录或事务。
package snapshotfmt

import (
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"
)

// Version 仅标识编码封装，领域记录不携带对应版本字段。
// 版本 2 使用无领域格式号的固定 DTO；旧字节由边界明确拒绝，不原地迁移。
const Version = 2

var ErrInvalid = errors.New("snapshot: unsupported field, invalid metadata or size overflow")

// Encode 输出全部字段，保留 nil、空容器和不透明字节的区别。
func Encode(record any) ([]byte, error) {
	value, _, err := walk(reflect.ValueOf(record), true)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"version": Version, "record": value})
}

// RecordSize 包括版本信封，不分配完整 JSON 或 base64 数据。
func RecordSize(record any) (int64, error) {
	n, err := Size(record)
	if err != nil {
		return 0, err
	}
	return Add(n, int64(len(`{"record":,"version":}`)+len(strconv.Itoa(Version))))
}

// Size 使用与 Encode 相同的遍历规则计算单个快照值。
func Size(value any) (int64, error) {
	_, n, err := walk(reflect.ValueOf(value), false)
	return n, err
}

// QuotedSize 与 encoding/json 默认的 HTML 转义和 UTF-8 规则一致。
func QuotedSize(s string) (int64, error) {
	if !utf8.ValidString(s) {
		return 0, ErrInvalid
	}
	n := int64(2)
	for _, r := range s {
		var size int64
		switch r {
		case '"', '\\', '\b', '\f', '\n', '\r', '\t':
			size = 2
		case '<', '>', '&', '\u2028', '\u2029':
			size = 6
		default:
			if r < 0x20 {
				size = 6
			} else {
				size = int64(utf8.RuneLen(r))
			}
		}
		var err error
		n, err = Add(n, size)
		if err != nil {
			return 0, err
		}
	}
	return n, nil
}

// ContainerOverhead 只计算 null 或括号、逗号；成员本身另计。
func ContainerOverhead(isNil bool, count int) (int64, error) {
	if count < 0 || isNil && count != 0 {
		return 0, ErrInvalid
	}
	if isNil {
		return 4, nil
	}
	if count == 0 {
		return 2, nil
	}
	return Add(1, int64(count))
}

// Add 同时检查正向累加及有符号大小差额溢出。
func Add(a, b int64) (int64, error) {
	if b > 0 && a > math.MaxInt64-b || b < 0 && a < math.MinInt64-b {
		return 0, ErrInvalid
	}
	return a + b, nil
}

// FieldName 是编码、计数和严格解码共用的字段名规则。
func FieldName(field reflect.StructField) string {
	if field.PkgPath != "" || field.Anonymous {
		return ""
	}
	name := strings.Split(field.Tag.Get("json"), ",")[0]
	if name == "-" {
		return ""
	}
	if name == "" {
		name = field.Name
	}
	return name
}

type field struct {
	name     string
	size     int64
	optional bool // 仅为后来新增的可空字段省略 nil；非 nil 空容器仍必须编码。
}
type structPlan struct {
	fields []field
	err    error
}

var plans sync.Map

func fields(typ reflect.Type) structPlan {
	if cached, ok := plans.Load(typ); ok {
		return cached.(structPlan)
	}
	p := structPlan{fields: make([]field, typ.NumField())}
	seen := make(map[string]bool, typ.NumField())
	for i := range p.fields {
		name := FieldName(typ.Field(i))
		size, err := QuotedSize(name)
		if name == "" || seen[name] || err != nil {
			p.err = ErrInvalid
			break
		}
		seen[name] = true
		optional := typ.Field(i).Tag.Get("snapshot") == "optional"
		kind := typ.Field(i).Type.Kind()
		if optional && kind != reflect.Pointer && kind != reflect.Map && kind != reflect.Slice {
			p.err = ErrInvalid
			break
		}
		p.fields[i] = field{name: name, size: size + 1, optional: optional}
	}
	cached, _ := plans.LoadOrStore(typ, p)
	return cached.(structPlan)
}

// 一次遍历同时定义编码值和长度，计数模式不创建映射、切片或数据副本。
func walk(v reflect.Value, materialize bool) (any, int64, error) {
	if !v.IsValid() {
		return nil, 0, ErrInvalid
	}
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return nil, 4, nil
		}
		return walk(v.Elem(), materialize)
	case reflect.Struct:
		p := fields(v.Type())
		if p.err != nil {
			return nil, 0, p.err
		}
		count := len(p.fields)
		for i, f := range p.fields {
			if f.optional && v.Field(i).IsNil() {
				count--
			}
		}
		n, _ := ContainerOverhead(false, count)
		var out map[string]any
		if materialize {
			out = make(map[string]any, count)
		}
		for i, f := range p.fields {
			if f.optional && v.Field(i).IsNil() {
				continue
			}
			value, size, err := walk(v.Field(i), materialize)
			if err != nil {
				return nil, 0, err
			}
			n, err = Add(n, f.size)
			if err != nil {
				return nil, 0, err
			}
			n, err = Add(n, size)
			if err != nil {
				return nil, 0, err
			}
			if materialize {
				out[f.name] = value
			}
		}
		return out, n, nil
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return nil, 0, ErrInvalid
		}
		if v.IsNil() {
			return nil, 4, nil
		}
		n, _ := ContainerOverhead(false, v.Len())
		var out map[string]any
		if materialize {
			out = make(map[string]any, v.Len())
		}
		iter := v.MapRange()
		for iter.Next() {
			key := iter.Key().String()
			keySize, err := QuotedSize(key)
			if err != nil {
				return nil, 0, err
			}
			value, size, err := walk(iter.Value(), materialize)
			if err != nil {
				return nil, 0, err
			}
			n, err = Add(n, keySize+1)
			if err != nil {
				return nil, 0, err
			}
			n, err = Add(n, size)
			if err != nil {
				return nil, 0, err
			}
			if materialize {
				out[key] = value
			}
		}
		return out, n, nil
	case reflect.Slice:
		if v.IsNil() {
			return nil, 4, nil
		}
		if v.Type() == reflect.TypeFor[[]byte]() {
			length := int64(v.Len())
			if length > (math.MaxInt64-6)/4*3 {
				return nil, 0, ErrInvalid
			}
			n := int64(2) + 4*((length+2)/3)
			if materialize {
				return v.Interface(), n, nil
			}
			return nil, n, nil
		}
		n, _ := ContainerOverhead(false, v.Len())
		var out []any
		if materialize {
			out = make([]any, v.Len())
		}
		for i := 0; i < v.Len(); i++ {
			value, size, err := walk(v.Index(i), materialize)
			if err != nil {
				return nil, 0, err
			}
			n, err = Add(n, size)
			if err != nil {
				return nil, 0, err
			}
			if materialize {
				out[i] = value
			}
		}
		return out, n, nil
	case reflect.String:
		n, err := QuotedSize(v.String())
		if materialize && err == nil {
			return v.Interface(), n, nil
		}
		return nil, n, err
	case reflect.Bool:
		n := int64(5)
		if v.Bool() {
			n = 4
		}
		if materialize {
			return v.Interface(), n, nil
		}
		return nil, n, nil
	case reflect.Int:
		n := int64(len(strconv.FormatInt(v.Int(), 10)))
		if materialize {
			return v.Interface(), n, nil
		}
		return nil, n, nil
	case reflect.Uint64:
		n := int64(len(strconv.FormatUint(v.Uint(), 10)))
		if materialize {
			return v.Interface(), n, nil
		}
		return nil, n, nil
	default:
		return nil, 0, ErrInvalid
	}
}
