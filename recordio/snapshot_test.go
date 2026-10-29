package recordio_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
)

func snapshotRoundTrip(t *testing.T, want durable.RunRecord) []byte {
	t.Helper()
	data, err := recordio.EncodeSnapshot(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := recordio.DecodeSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot changed record shape: got %#v, want %#v", got, want)
	}
	again, err := recordio.EncodeSnapshot(got)
	if err != nil || !bytes.Equal(data, again) {
		t.Fatalf("unstable snapshot encoding: %v", err)
	}
	return data
}

func TestSnapshotNilEmptyCombinations(t *testing.T) {
	snapshotRoundTrip(t, durable.RunRecord{})
	// Exercise all top-level maps independently, not just all-nil/all-empty.
	for mask := 0; mask < 32; mask++ {
		r := durable.RunRecord{Revision: 987}
		if mask&1 != 0 {
			r.Scopes = map[string]durable.ScopeRecord{}
		}
		if mask&2 != 0 {
			r.Attempts = map[string]durable.AttemptRecord{}
		}
		if mask&4 != 0 {
			r.Streams = map[string]durable.StreamRecord{}
		}
		if mask&8 != 0 {
			r.Messages = map[string]durable.MessageAttemptRecord{}
		}
		if mask&16 != 0 {
			r.Managed = map[string]durable.ManagedRecord{}
		}
		snapshotRoundTrip(t, r)
	}
	for mask := 0; mask < 256; mask++ {
		t.Run(fmt.Sprint(mask), func(t *testing.T) {
			value := durable.ValueRecord{}
			if mask&1 != 0 {
				value.Data = []byte{}
			}
			if mask&2 != 0 {
				value.Stream = &durable.StreamDescriptorRecord{}
			}
			scope := durable.ScopeRecord{}
			if mask&4 != 0 {
				scope.Nodes = map[string]durable.NodeRecord{}
			}
			var outputs []durable.ValueRecord
			if mask&8 != 0 {
				outputs = []durable.ValueRecord{}
			}
			stream := durable.StreamRecord{}
			if mask&16 != 0 {
				stream.Pending = []durable.StreamMessageRecord{}
			}
			if mask&32 != 0 {
				stream.Cursor = &value
			}
			managed := durable.ManagedRecord{}
			if mask&64 != 0 {
				managed.State = []durable.ValueRecord{}
			}
			if mask&128 != 0 {
				managed.Values = []durable.ValueRecord{}
			}
			r := durable.RunRecord{
				Scopes: map[string]durable.ScopeRecord{
					"scope": scope,
					"nodes": {Nodes: map[string]durable.NodeRecord{
						"shape": {Outputs: outputs}, "value": {Outputs: []durable.ValueRecord{value}},
					}},
				},
				Streams: map[string]durable.StreamRecord{
					"shape": stream, "value": {Pending: []durable.StreamMessageRecord{{Value: value}}},
				},
				Managed: map[string]durable.ManagedRecord{
					"shape": managed, "value": {State: []durable.ValueRecord{value}, Values: []durable.ValueRecord{value}},
				},
			}
			snapshotRoundTrip(t, r)
		})
	}
}

// Populate every current field, including scalars and nested stream descriptors.
// New unsupported field kinds fail this test rather than going untested.
func populateSnapshotValue(t *testing.T, v reflect.Value) {
	t.Helper()
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			populateSnapshotValue(t, v.Field(i))
		}
	case reflect.Map:
		v.Set(reflect.MakeMap(v.Type()))
		value := reflect.New(v.Type().Elem()).Elem()
		populateSnapshotValue(t, value)
		v.SetMapIndex(reflect.ValueOf("键/🚀/\ufffd"), value)
	case reflect.Slice:
		if v.Type() == reflect.TypeOf([]byte(nil)) {
			v.SetBytes([]byte{0xff, 0xfe, 0, 0x80, '"', '\\'})
		} else {
			v.Set(reflect.MakeSlice(v.Type(), 1, 1))
			populateSnapshotValue(t, v.Index(0))
		}
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		populateSnapshotValue(t, v.Elem())
	case reflect.String:
		v.SetString("元数据/🚀/\ufffd/\\uD800")
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int:
		v.SetInt(-123)
	case reflect.Uint64:
		v.SetUint(^uint64(0))
	default:
		t.Fatalf("uncovered field type %v", v.Type())
	}
}

func TestSnapshotAllFields(t *testing.T) {
	var record durable.RunRecord
	populateSnapshotValue(t, reflect.ValueOf(&record).Elem())
	data := snapshotRoundTrip(t, record)
	got, err := recordio.DecodeSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	// Decoding allocates independent Data and descriptor pointers.
	for _, scope := range got.Scopes {
		for _, node := range scope.Nodes {
			node.Outputs[0].Data[0] = 0
			node.Outputs[0].Stream.Graph = "changed"
		}
	}
	if reflect.DeepEqual(got, record) {
		t.Fatal("mutation did not affect decoded record")
	}
	snapshotRoundTrip(t, record)
}

func TestSnapshotRejectsCorruption(t *testing.T) {
	base := string(snapshotRoundTrip(t, durable.RunRecord{RunID: "PRIVATE_MARKER"}))
	cases := []string{
		"", "null", "[]", "{}", base + "{}", base + " PRIVATE_MARKER",
		strings.Replace(base, `"version":1`, `"version":2`, 1),
		strings.Replace(base, `"version":1`, `"version":null`, 1),
		strings.Replace(base, `"version":1`, `"Version":1`, 1),
		strings.Replace(base, `"version":1`, `"version":1,"version":1`, 1),
		strings.Replace(base, `"version":1`, `"version":1,"vers\u0069on":1`, 1),
		strings.Replace(base, `"version":1`, `"version":1,"PRIVATE_MARKER":0`, 1),
		strings.Replace(base, `"record":{`, `"record":{"PRIVATE_MARKER":0,`, 1),
		strings.Replace(base, `"run_id":"PRIVATE_MARKER"`, `"run_id":"a","run_id":"b"`, 1),
		strings.Replace(base, `"run_id":"PRIVATE_MARKER"`, `"Run_ID":"PRIVATE_MARKER"`, 1),
		strings.Replace(base, `"run_id":"PRIVATE_MARKER"`, `"run_id":null`, 1),
		strings.Replace(base, `"run_id":"PRIVATE_MARKER"`, `"run_id":"\ud800"`, 1),
		strings.Replace(base, `"run_id":"PRIVATE_MARKER"`, `"run_id":"\udfff"`, 1),
		strings.Replace(base, `"run_id":"PRIVATE_MARKER"`, `"run_id":"\ud800\u0041"`, 1),
		strings.Replace(base, "PRIVATE_MARKER", string([]byte{0xff}), 1),
		strings.Replace(base, `"revision":0`, `"revision":18446744073709551616`, 1),
		strings.Replace(base, `"revision":0`, `"revision":-1`, 1),
		strings.Replace(base, `"revision":0`, `"revision":0.5`, 1),
		strings.Replace(base, `"revision":0`, `"revision":0,"REVISION":0`, 1),
		strings.Replace(base, `"scopes":null`, `"scopes":{"x":null}`, 1),
		strings.Replace(base, `"scopes":null`, `"scopes":{"x":{},"\u0078":{}}`, 1),
		strings.Replace(base, `"revision":0,`, "", 1),
	}
	// Inject attacks at every nested struct/map/slice/pointer level.
	var record durable.RunRecord
	populateSnapshotValue(t, reflect.ValueOf(&record).Elem())
	full := string(snapshotRoundTrip(t, record))
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal([]byte(full), &envelope); err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(envelope["record"], &fields); err != nil {
		t.Fatal(err)
	}
	var scopes map[string]json.RawMessage
	if err := json.Unmarshal(fields["scopes"], &scopes); err != nil {
		t.Fatal(err)
	}
	for _, scope := range scopes {
		duplicate := `{"x":` + string(scope) + `,"\u0078":` + string(scope) + `}`
		cases = append(cases, strings.Replace(full, string(fields["scopes"]), duplicate, 1))
	}
	for _, field := range []string{"outputs", "nodes", "pending", "cursor", "state", "values", "data", "stream", "id", "generation", "bindings"} {
		needle := `"` + field + `":`
		cases = append(cases, strings.Replace(full, needle, needle+`null,"`+field+`":`, 1))
	}
	cases = append(cases,
		strings.Replace(full, `"data":"//4AgCJc"`, `"data":"PRIVATE_MARKER!"`, 1),
		strings.Replace(full, `"data":"//4AgCJc"`, `"data":[]`, 1),
	)
	for i, data := range cases {
		got, err := recordio.DecodeSnapshot([]byte(data))
		if err == nil || !reflect.DeepEqual(got, durable.RunRecord{}) || strings.Contains(err.Error(), "PRIVATE_MARKER") {
			t.Fatalf("case %d accepted corrupt snapshot or leaked data: %v", i, err)
		}
	}
	// Every proper truncation of a valid snapshot must fail without panicking.
	for i := 0; i < len(base); i++ {
		if _, err := recordio.DecodeSnapshot([]byte(base[:i])); err == nil {
			t.Fatalf("accepted truncation %d", i)
		}
	}
}

func TestSnapshotMetadataValidation(t *testing.T) {
	bad := "PRIVATE_MARKER" + string([]byte{0xff})
	for _, record := range []durable.RunRecord{
		{RunID: bad},
		{Scopes: map[string]durable.ScopeRecord{bad: {}}},
		{Managed: map[string]durable.ManagedRecord{"m": {Values: []durable.ValueRecord{{Stream: &durable.StreamDescriptorRecord{SourceID: bad}}}}}},
		{Streams: map[string]durable.StreamRecord{"s": {Cursor: &durable.ValueRecord{Name: bad}}}},
	} {
		data, err := recordio.EncodeSnapshot(record)
		if err == nil || data != nil || strings.Contains(err.Error(), "PRIVATE_MARKER") {
			t.Fatalf("accepted bad metadata or leaked data: %v", err)
		}
	}
	// Existing Encode/Decode retain their legacy wire representation.
	record := durable.RunRecord{Streams: map[string]durable.StreamRecord{}}
	legacy, err := recordio.Encode(record)
	want, marshalErr := json.Marshal(record)
	if err != nil || marshalErr != nil || !bytes.Equal(legacy, want) {
		t.Fatal("legacy codec changed")
	}
}

func FuzzDecodeSnapshot(f *testing.F) {
	data, err := recordio.EncodeSnapshot(durable.RunRecord{})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(data)
	f.Add([]byte(`{"version":1,"record":null}`))
	f.Add([]byte(`{"version":1,"version":1}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		record, err := recordio.DecodeSnapshot(data)
		if err == nil {
			snapshotRoundTrip(t, record)
		} else if !reflect.DeepEqual(record, durable.RunRecord{}) {
			t.Fatal("error returned a partial record")
		}
	})
}
