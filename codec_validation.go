package durable

import (
	"reflect"
	"unicode/utf8"
)

// 接口字段的动态值也属于值契约。只用反射结构，不调用业务方法或取未导出字段的 Interface。
// 指针/map/slice 身份防循环；同一底层 slice 的不同长度不能相互遮蔽后半段。
type valueVisit struct {
	typ reflect.Type
	ptr uintptr
	len int
}

func supportedValue(v reflect.Value, seen map[valueVisit]bool) bool {
	if !v.IsValid() {
		return true
	}
	if !supportedType(v.Type(), make(map[reflect.Type]bool)) {
		return false
	}
	switch v.Kind() {
	case reflect.Interface:
		return v.IsNil() || supportedValue(v.Elem(), seen)
	case reflect.Pointer, reflect.Map, reflect.Slice:
		if v.IsNil() {
			return true
		}
		visit := valueVisit{typ: v.Type(), ptr: uintptr(v.UnsafePointer())}
		if v.Kind() == reflect.Slice {
			visit.len = v.Len()
		}
		if seen[visit] {
			return true
		}
		seen[visit] = true
	}
	switch v.Kind() {
	case reflect.Pointer:
		return supportedValue(v.Elem(), seen)
	case reflect.Map:
		iterator := v.MapRange()
		for iterator.Next() {
			if !supportedValue(iterator.Key(), seen) || !supportedValue(iterator.Value(), seen) {
				return false
			}
		}
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			if !supportedValue(v.Index(i), seen) {
				return false
			}
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if !supportedValue(v.Field(i), seen) {
				return false
			}
		}
	}
	return true
}

// JSON 不保存 interface 的动态 Go 类型。递归拒绝这种声明，让调用方用自定义 Codec 明确恢复。
func concreteJSONType(t reflect.Type, seen map[reflect.Type]bool) bool {
	if seen[t] {
		return true
	}
	seen[t] = true
	switch t.Kind() {
	case reflect.Interface:
		return false
	case reflect.Pointer, reflect.Array, reflect.Slice:
		return concreteJSONType(t.Elem(), seen)
	case reflect.Map:
		return concreteJSONType(t.Key(), seen) && concreteJSONType(t.Elem(), seen)
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if !concreteJSONType(t.Field(i).Type, seen) {
				return false
			}
		}
	}
	return true
}

// json.Marshal 会把非法 UTF-8 改为替换字符；编码前拒绝，避免值漂移和不同输入的指纹碰撞。
// 不调用用户方法、不取未导出字段的 Interface；循环仅防遍历挂死，JSON 自己仍会拒绝对象环。
func validJSONStrings(v reflect.Value, seen map[valueVisit]bool) bool {
	if !v.IsValid() {
		return true
	}
	switch v.Kind() {
	case reflect.String:
		return utf8.ValidString(v.String())
	case reflect.Interface:
		return v.IsNil() || validJSONStrings(v.Elem(), seen)
	case reflect.Pointer, reflect.Map, reflect.Slice:
		if v.IsNil() {
			return true
		}
		visit := valueVisit{typ: v.Type(), ptr: uintptr(v.UnsafePointer())}
		if v.Kind() == reflect.Slice {
			visit.len = v.Len()
		}
		if seen[visit] {
			return true
		}
		seen[visit] = true
	}
	switch v.Kind() {
	case reflect.Pointer:
		return validJSONStrings(v.Elem(), seen)
	case reflect.Map:
		iter := v.MapRange()
		for iter.Next() {
			if !validJSONStrings(iter.Key(), seen) || !validJSONStrings(iter.Value(), seen) {
				return false
			}
		}
	case reflect.Array, reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			if !validJSONStrings(v.Index(i), seen) {
				return false
			}
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if !validJSONStrings(v.Field(i), seen) {
				return false
			}
		}
	}
	return true
}
