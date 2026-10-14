package durable

import "reflect"

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
