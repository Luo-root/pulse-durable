package filestore

import (
	"reflect"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
)

func incrementalMetadataRecord() durable.RunRecord {
	r := testRecord("run")
	descriptor := durable.StreamDescriptorRecord{ID: durable.StreamIDRecord{Scope: "scope", NodeID: "source", Key: "stream", Group: "default", Generation: 1}, Graph: "graph", Path: "mount", Attempt: "attempt", TypeID: "go/int", Version: "1", SourceID: "source", Capacity: 3, Terminal: "completed"}
	value := durable.ValueRecord{Name: "element", TypeID: "go/int", Version: "1", State: "ready", Digest: "digest", Data: []byte{0xff, 0xfe}}
	r.Streams = map[string]durable.StreamRecord{"stream": {Stream: descriptor, LastSequence: 1, Pending: []durable.StreamMessageRecord{{Sequence: 1, Value: value}}, Cursor: &value}}
	r.Messages = map[string]durable.MessageAttemptRecord{"message": {Scope: "scope", Parent: "attempt", NodeID: "processor", Input: descriptor.ID, Sequence: 1, Index: 1, Limit: 2, State: durable.AttemptActive}}
	r.Managed = map[string]durable.ManagedRecord{"managed": {Scope: "scope", Parent: "attempt", NodeID: "processor", State: []durable.ValueRecord{value}, Values: []durable.ValueRecord{value}}}
	scope := r.Scopes["scope"]
	node := scope.Nodes["node"]
	node.Outputs[0].Stream = &descriptor
	scope.Nodes["node"] = node
	r.Scopes["scope"] = scope
	return r
}

// 对新增纯数据结构逐字段核验，Data 是不透明字节，字符串及每层 map 键全部属于元数据。
func TestIncrementalMetadataRejectsEveryStringAndMapKey(t *testing.T) {
	base := incrementalMetadataRecord()
	mutations := metadataMutations(reflect.ValueOf(base), "")
	bad := string([]byte{0xff})
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			r := mutation.change(reflect.ValueOf(incrementalMetadataRecord()), bad).Interface().(durable.RunRecord)
			if validMetadata(r) {
				t.Fatal("incremental metadata accepted invalid UTF-8")
			}
		})
	}
	// Runtime 后端发布由 TestRuntimeRejectsInvalidFileMetadata 覆盖；
	// 已完成记录不能通过 Initialize 导入。
}

type metadataMutation struct {
	name   string
	change func(reflect.Value, string) reflect.Value
}

func metadataMutations(value reflect.Value, path string) []metadataMutation {
	var out []metadataMutation
	switch value.Kind() {
	case reflect.String:
		return []metadataMutation{{path, func(v reflect.Value, text string) reflect.Value { return reflect.ValueOf(text).Convert(v.Type()) }}}
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			field := value.Type().Field(i)
			childPath := field.Name
			if path != "" {
				childPath = path + "/" + field.Name
			}
			for _, child := range metadataMutations(value.Field(i), childPath) {
				index, change := i, child.change
				out = append(out, metadataMutation{child.name, func(v reflect.Value, text string) reflect.Value {
					copy := reflect.New(v.Type()).Elem()
					copy.Set(v)
					copy.Field(index).Set(change(v.Field(index), text))
					return copy
				}})
			}
		}
	case reflect.Pointer:
		if value.IsNil() {
			break
		}
		for _, child := range metadataMutations(value.Elem(), path) {
			change := child.change
			out = append(out, metadataMutation{child.name, func(v reflect.Value, text string) reflect.Value {
				copy := reflect.New(v.Type().Elem())
				copy.Elem().Set(change(v.Elem(), text))
				return copy
			}})
		}
	case reflect.Slice:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			break
		}
		for i := 0; i < value.Len(); i++ {
			for _, child := range metadataMutations(value.Index(i), path) {
				index, change := i, child.change
				out = append(out, metadataMutation{child.name, func(v reflect.Value, text string) reflect.Value {
					copy := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
					reflect.Copy(copy, v)
					copy.Index(index).Set(change(v.Index(index), text))
					return copy
				}})
			}
		}
	case reflect.Map:
		for _, key := range value.MapKeys() {
			key := key
			clone := func(v reflect.Value) reflect.Value {
				copy := reflect.MakeMapWithSize(v.Type(), v.Len())
				for _, key := range v.MapKeys() {
					copy.SetMapIndex(key, v.MapIndex(key))
				}
				return copy
			}
			out = append(out, metadataMutation{path + "/key", func(v reflect.Value, text string) reflect.Value {
				copy := clone(v)
				copy.SetMapIndex(key, reflect.Value{})
				copy.SetMapIndex(reflect.ValueOf(text), v.MapIndex(key))
				return copy
			}})
			for _, child := range metadataMutations(value.MapIndex(key), path) {
				change := child.change
				out = append(out, metadataMutation{child.name, func(v reflect.Value, text string) reflect.Value {
					copy := clone(v)
					copy.SetMapIndex(key, change(v.MapIndex(key), text))
					return copy
				}})
			}
		}
	}
	return out
}
