package postgres

import (
	"encoding/json"
	"errors"
	"strconv"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
)

func snapshotLedgerFixture(t *testing.T, encoded []byte) json.RawMessage {
	t.Helper()
	ledger, err := snapshotLedgerFromSnapshot(encoded)
	if err != nil {
		t.Fatal(err)
	}
	objects := make([]any, 0, len(ledger.objects))
	containers := make([]any, 0, len(ledger.containers))
	for key, size := range ledger.objects {
		objects = append(objects, []any{key.kind, key.key1, key.key2, key.key3, strconv.FormatInt(size, 10)})
	}
	for key, value := range ledger.containers {
		containers = append(containers, []any{key.kind, key.key1, value.nilValue, strconv.FormatInt(value.count, 10), strconv.FormatInt(value.sum, 10)})
	}
	raw, err := json.Marshal(map[string]any{"fixed_bytes": strconv.FormatInt(ledger.fixed, 10), "objects": objects, "containers": containers})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestSnapshotLedgerRejectsMalformedRows(t *testing.T) {
	r := record(taskFixture(t, "tenant", "ledger-rows"), 9)
	scope := r.Scopes[r.RootScope]
	scope.Nodes["a"] = snapshotDataNode("a", 1)
	scope.Nodes["b"] = snapshotDataNode("b", 2)
	r.Scopes[r.RootScope] = scope
	r.Managed = map[string]durable.ManagedRecord{"m": {State: []durable.ValueRecord{{Name: "value"}}, Values: []durable.ValueRecord{}}}
	encoded, err := recordio.EncodeSnapshot(r)
	if err != nil {
		t.Fatal(err)
	}
	valid := snapshotLedgerFixture(t, encoded)
	if err = validateSnapshotLedger(valid, encoded); err != nil {
		t.Fatal("valid complete ledger", err)
	}
	for _, kind := range []string{"missing", "missing_fixed", "null_fixed", "wrong_fixed", "extra_field", "missing_objects", "null_objects", "missing_containers", "null_containers", "missing_object", "duplicate_object", "wrong_kind", "wrong_key", "null_key", "wrong_size", "null_size", "noncanonical_size", "equal_sum", "missing_container", "duplicate_container", "wrong_parent", "wrong_count", "wrong_sum", "nil_empty_swap", "null_nil", "extra_column", "missing_column"} {
		t.Run(kind, func(t *testing.T) {
			var image map[string]any
			if err := json.Unmarshal(valid, &image); err != nil {
				t.Fatal(err)
			}
			objects := image["objects"].([]any)
			containers := image["containers"].([]any)
			row := objects[0].([]any)
			container := containers[0].([]any)
			switch kind {
			case "missing":
				image = nil
			case "missing_fixed":
				delete(image, "fixed_bytes")
			case "null_fixed":
				image["fixed_bytes"] = nil
			case "wrong_fixed":
				image["fixed_bytes"] = "0"
			case "extra_field":
				image["future"] = true
			case "missing_objects":
				delete(image, "objects")
			case "null_objects":
				image["objects"] = nil
			case "missing_containers":
				delete(image, "containers")
			case "null_containers":
				image["containers"] = nil
			case "missing_object":
				image["objects"] = objects[1:]
			case "duplicate_object":
				objects[1] = objects[0]
			case "wrong_kind":
				row[0] = "future"
			case "wrong_key":
				row[3] = "unexpected-suffix"
			case "null_key":
				row[3] = nil
			case "wrong_size":
				row[4] = "0"
			case "null_size":
				row[4] = nil
			case "noncanonical_size":
				row[4] = "0" + row[4].(string)
			case "equal_sum":
				a := objects[0].([]any)
				b := objects[1].([]any)
				n, _ := strconv.ParseInt(a[4].(string), 10, 64)
				m, _ := strconv.ParseInt(b[4].(string), 10, 64)
				a[4] = strconv.FormatInt(n+1, 10)
				b[4] = strconv.FormatInt(m-1, 10)
			case "missing_container":
				image["containers"] = containers[1:]
			case "duplicate_container":
				containers[1] = containers[0]
			case "wrong_parent":
				container[1] = "wrong-parent"
			case "wrong_count":
				container[3] = "999"
			case "wrong_sum":
				container[4] = "999"
			case "nil_empty_swap":
				container[2] = !container[2].(bool)
			case "null_nil":
				container[2] = nil
			case "extra_column":
				objects[0] = append(row, "extra")
			case "missing_column":
				objects[0] = row[:4]
			}
			raw, err := json.Marshal(image)
			if err != nil {
				t.Fatal(err)
			}
			if err = validateSnapshotLedger(raw, encoded); !errors.Is(err, ErrInvalidRecordBudget) {
				t.Fatalf("malformed ledger accepted: %s: %v", kind, err)
			}
		})
	}
}
