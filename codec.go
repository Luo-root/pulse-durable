package durable

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sync"
	"unicode/utf8"
)

// Codec 将一个真实 Go 类型与稳定的类型 ID、编码版本绑定。
// 实现须并发安全、同步完成、不修改业务值；相同值必须编码成相同字节，供输入指纹比较。
// 修改编码语义必须升级 Version；Decode 必须返回可赋给 Type 的值。
// typed nil 保留为 ready；声明为接口类型时 nil 接口也合法，缺失与 skip 由槽位状态区分。
// 编解码错误及 panic 会转换为不含业务值或编码字节的错误；返回错误的原因保留给 errors.Is/As。
type Codec interface {
	Type() reflect.Type
	ID() string
	Version() string
	Encode(any) ([]byte, error)
	Decode([]byte) (any, error)
}

// Codecs 按真实 Go 类型注册编码器；同一 ID 不能对应不同类型或编码版本。
// Runtime 在每次 Run/Resume 入口复制注册表，后续注册不改变该次执行的契约。
// 零值可注册自定义类型；NewCodecs 另提供标准标量和 []byte 的 JSON 编码器。
type Codecs struct {
	mu     sync.RWMutex
	byType map[reflect.Type]codecEntry
	byID   map[string]reflect.Type
}

type codecEntry struct {
	typ     reflect.Type
	id      string
	version string
	codec   Codec
}

// NewCodecs 创建带内置 bool、string、各整数/浮点类型和 []byte 的注册表。
// 内置 ID 为 go/<类型名>，版本为 1；命名业务类型仍须显式注册。
func NewCodecs() *Codecs {
	c := &Codecs{}
	_ = RegisterJSON[bool](c, "go/bool", "1")
	_ = RegisterJSON[string](c, "go/string", "1")
	_ = RegisterJSON[int](c, "go/int", "1")
	_ = RegisterJSON[int8](c, "go/int8", "1")
	_ = RegisterJSON[int16](c, "go/int16", "1")
	_ = RegisterJSON[int32](c, "go/int32", "1")
	_ = RegisterJSON[int64](c, "go/int64", "1")
	_ = RegisterJSON[uint](c, "go/uint", "1")
	_ = RegisterJSON[uint8](c, "go/uint8", "1")
	_ = RegisterJSON[uint16](c, "go/uint16", "1")
	_ = RegisterJSON[uint32](c, "go/uint32", "1")
	_ = RegisterJSON[uint64](c, "go/uint64", "1")
	_ = RegisterJSON[uintptr](c, "go/uintptr", "1")
	_ = RegisterJSON[float32](c, "go/float32", "1")
	_ = RegisterJSON[float64](c, "go/float64", "1")
	_ = RegisterJSON[[]byte](c, "go/bytes", "1")
	return c
}

// Register 原子注册一个编码器。重复类型或重复 ID 返回 ErrConflict，绝不替换旧编码器。
// channel、func、unsafe.Pointer（包括其嵌套声明）不属于首期值协议。
func (c *Codecs) Register(codec Codec) (err error) {
	if c == nil || nilValue(codec) {
		return fmt.Errorf("durable: codecs and codec are required")
	}
	// 按正常返回识别兼容模式的 panic(nil)；元数据注册 panic 沿原契约不保留原值。
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			err = fmt.Errorf("durable: codec registration panicked")
		}
	}()
	t, id, version := codec.Type(), codec.ID(), codec.Version()
	returned = true
	if t == nil || id == "" || version == "" {
		return fmt.Errorf("durable: codec type, ID and version are required")
	}
	if !supportedType(t, make(map[reflect.Type]bool)) {
		return fmt.Errorf("durable: codec %q has a non-persistable type", id)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.byType[t]; ok {
		return fmt.Errorf("%w: codec type already registered", ErrConflict)
	}
	if _, ok := c.byID[id]; ok {
		return fmt.Errorf("%w: codec ID already registered", ErrConflict)
	}
	if c.byType == nil {
		c.byType = make(map[reflect.Type]codecEntry)
		c.byID = make(map[string]reflect.Type)
	}
	c.byType[t] = codecEntry{typ: t, id: id, version: version, codec: codec}
	c.byID[id] = t
	return nil
}

// RegisterJSON 为 T 注册标准库 JSON 编解码器。结构体解码拒绝未知字段和尾随内容。
// interface（包括嵌套字段）须用自定义 Codec 恢复动态类型，不能使用此便利入口。
// 字符串（包括字段和 map 键）须为有效 UTF-8；任意字节使用 []byte 或自定义 Codec。
func RegisterJSON[T any](c *Codecs, id, version string) error {
	t := reflect.TypeFor[T]()
	if !concreteJSONType(t, make(map[reflect.Type]bool)) {
		return fmt.Errorf("durable: JSON codec requires recursively concrete types; use a custom Codec for interfaces")
	}
	return c.Register(jsonCodec[T]{id: id, version: version})
}

type jsonCodec[T any] struct{ id, version string }

func (c jsonCodec[T]) Type() reflect.Type { return reflect.TypeFor[T]() }
func (c jsonCodec[T]) ID() string         { return c.id }
func (c jsonCodec[T]) Version() string    { return c.version }
func (c jsonCodec[T]) Encode(v any) ([]byte, error) {
	if !validJSONStrings(reflect.ValueOf(v), make(map[valueVisit]bool)) {
		return nil, fmt.Errorf("invalid UTF-8 string")
	}
	data, err := json.Marshal(v)
	if err == nil && !utf8.Valid(data) {
		return nil, fmt.Errorf("invalid UTF-8 JSON")
	}
	return data, err
}
func (c jsonCodec[T]) Decode(data []byte) (any, error) {
	if !utf8.Valid(data) {
		return nil, fmt.Errorf("invalid UTF-8 JSON")
	}
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		switch reflect.TypeFor[T]().Kind() {
		case reflect.Pointer, reflect.Slice, reflect.Map:
			var zero T
			return zero, nil
		default:
			return nil, fmt.Errorf("null scalar")
		}
	}
	var v T
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&v); err != nil {
		return nil, err
	}
	var tail any
	if err := d.Decode(&tail); err != io.EOF {
		return nil, fmt.Errorf("trailing JSON data")
	}
	return v, nil
}

func (c *Codecs) snapshot() map[reflect.Type]codecEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make(map[reflect.Type]codecEntry, len(c.byType))
	for t, codec := range c.byType {
		out[t] = codec
	}
	return out
}

func (c codecEntry) encode(v any) (data []byte, err error) {
	if !validBusinessValue(c.typ, v) {
		return nil, fmt.Errorf("durable: codec %q received an invalid value type", c.id)
	}
	// 按正常返回识别兼容模式的 panic(nil)；operationPanic 保留 error 原因并脱敏正文。
	returned := false
	defer func() {
		if !returned {
			data, err = nil, operationPanic(fmt.Sprintf("codec %q encode", c.id), recover())
		}
	}()
	data, err = c.codec.Encode(v)
	returned = true
	if err != nil {
		return nil, &operationError{op: fmt.Sprintf("codec %q encode", c.id), cause: err}
	}
	return append([]byte(nil), data...), nil
}

func (c codecEntry) decode(data []byte) (v any, err error) {
	// 按正常返回识别兼容模式的 panic(nil)；operationPanic 保留 error 原因并脱敏正文。
	returned := false
	defer func() {
		if !returned {
			v, err = nil, operationPanic(fmt.Sprintf("codec %q decode", c.id), recover())
		}
	}()
	v, err = c.codec.Decode(append([]byte(nil), data...))
	returned = true
	if err != nil {
		return nil, &operationError{op: fmt.Sprintf("codec %q decode", c.id), cause: err}
	}
	if !validBusinessValue(c.typ, v) {
		return nil, fmt.Errorf("durable: codec %q decoded an invalid value type", c.id)
	}
	return v, nil
}

func validBusinessValue(t reflect.Type, v any) bool {
	if v == nil {
		return t.Kind() == reflect.Interface
	}
	return reflect.TypeOf(v).AssignableTo(t) && supportedValue(reflect.ValueOf(v), make(map[valueVisit]bool))
}

func nilValue(v any) bool {
	if v == nil {
		return true
	}
	r := reflect.ValueOf(v)
	switch r.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return r.IsNil()
	}
	return false
}

func supportedType(t reflect.Type, seen map[reflect.Type]bool) bool {
	if t == nil {
		return false
	}
	if seen[t] {
		return true
	}
	seen[t] = true
	switch t.Kind() {
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return false
	case reflect.Array, reflect.Slice, reflect.Pointer:
		return supportedType(t.Elem(), seen)
	case reflect.Map:
		return supportedType(t.Key(), seen) && supportedType(t.Elem(), seen)
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if !supportedType(t.Field(i).Type, seen) {
				return false
			}
		}
	}
	return true
}
