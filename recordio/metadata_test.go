package recordio_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
)

func TestCompatibleRoundTrip(t *testing.T) {
	record := durable.RunRecord{RunID: "运行/🚀/\ufffd/\\uD800", Revision: 1,
		Scopes: map[string]durable.ScopeRecord{"scope": {Nodes: map[string]durable.NodeRecord{"node": {Outputs: []durable.ValueRecord{{Data: []byte{0xff, 0xfe, 0, 0x80}}}}}}}}
	data, err := recordio.Encode(record)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, legacy) {
		t.Fatal("changed JSON format")
	}
	got, err := recordio.Decode(data)
	if err != nil || !reflect.DeepEqual(got, record) {
		t.Fatalf("round trip: %v", err)
	}
}

func TestRejectsInvalidMetadata(t *testing.T) {
	bad := "PRIVATE_MARKER" + string([]byte{0xff})
	for _, record := range []durable.RunRecord{
		{RunID: bad},
		{Scopes: map[string]durable.ScopeRecord{bad: {}}},
		{Managed: map[string]durable.ManagedRecord{"m": {Values: []durable.ValueRecord{{TypeID: bad}}}}},
	} {
		data, err := recordio.Encode(record)
		if err == nil || data != nil || strings.Contains(err.Error(), "PRIVATE_MARKER") {
			t.Fatalf("invalid metadata: %v", err)
		}
	}
}

func TestDecodeCorruptIsNotMissingAndRedacted(t *testing.T) {
	for _, data := range [][]byte{
		nil, []byte(`{"RunID":`), []byte(`{"RunID":"PRIVATE_MARKER","Revision":"PRIVATE_MARKER"}`),
		[]byte(`{"RunID":"\ud800"}`), []byte(`{"RunID":"\udfff"}`),
		[]byte(`{"RunID":"\ud800\u0041"}`), []byte{'{', '"', 'R', 'u', 'n', 'I', 'D', '"', ':', '"', 0xff, '"', '}'},
	} {
		got, err := recordio.Decode(data)
		if err == nil || errors.Is(err, durable.ErrNotFound) || got.RunID != "" || strings.Contains(err.Error(), "PRIVATE_MARKER") {
			t.Fatalf("corrupt decode: %v", err)
		}
	}
	for _, data := range []string{`{"RunID":"\ud83d\ude80"}`, `{"RunID":"\\uD800"}`, `{"RunID":"\ufffd"}`} {
		if _, err := recordio.Decode([]byte(data)); err != nil {
			t.Fatal(err)
		}
	}
}
