package postgres

import (
	"bytes"
	"encoding/json"
	"strconv"
)

type snapshotObjectKey struct {
	kind, key1, key2, key3 string
}

type snapshotContainerKey struct {
	kind, key1 string
}

type snapshotContainerSize struct {
	nilValue   bool
	count, sum int64
}

type snapshotLedger struct {
	fixed      int64
	objects    map[snapshotObjectKey]int64
	containers map[snapshotContainerKey]snapshotContainerSize
}

// 只拆分已验证记录的 EncodeSnapshot 产物。叶与头的贡献直接取原始 JSON 长度，
// 不重复编码 Data、推导字段 schema 或执行业务规则；此全量工作只发生在 Load 边界。
func snapshotLedgerFromSnapshot(encoded []byte) (snapshotLedger, error) {
	out := snapshotLedger{fixed: int64(len(encoded)), objects: make(map[snapshotObjectKey]int64), containers: make(map[snapshotContainerKey]snapshotContainerSize)}
	var image struct {
		Record map[string]json.RawMessage `json:"record"`
	}
	if json.Unmarshal(encoded, &image) != nil || image.Record == nil {
		return snapshotLedger{}, invalidRecord(ErrInvalidRecordBudget)
	}
	addObject := func(key snapshotObjectKey, size int64) bool {
		if _, exists := out.objects[key]; exists || size < 0 {
			return false
		}
		out.objects[key] = size
		return true
	}
	addContainer := func(kind, key string, raw json.RawMessage, array bool) error {
		value := snapshotContainerSize{nilValue: bytes.Equal(raw, []byte("null"))}
		if !value.nilValue {
			if array {
				var entries []json.RawMessage
				if json.Unmarshal(raw, &entries) != nil || entries == nil {
					return invalidRecord(ErrInvalidRecordBudget)
				}
				value.count = int64(len(entries))
				for _, entry := range entries {
					value.sum += int64(len(entry))
				}
			} else {
				var entries map[string]json.RawMessage
				if json.Unmarshal(raw, &entries) != nil || entries == nil {
					return invalidRecord(ErrInvalidRecordBudget)
				}
				value.count = int64(len(entries))
				for key, entry := range entries {
					quoted, err := json.Marshal(key)
					if err != nil {
						return invalidRecord(ErrInvalidRecordBudget)
					}
					value.sum += int64(len(quoted) + 1 + len(entry))
				}
			}
		}
		out.containers[snapshotContainerKey{kind, key}] = value
		return nil
	}
	for _, kind := range []string{"scopes", "attempts", "streams", "messages", "managed"} {
		raw, exists := image.Record[kind]
		if !exists {
			return snapshotLedger{}, invalidRecord(ErrInvalidRecordBudget)
		}
		out.fixed -= int64(len(raw))
		if err := addContainer(kind, "", raw, false); err != nil {
			return snapshotLedger{}, err
		}
		if bytes.Equal(raw, []byte("null")) {
			continue
		}
		var entries map[string]json.RawMessage
		if json.Unmarshal(raw, &entries) != nil {
			return snapshotLedger{}, invalidRecord(ErrInvalidRecordBudget)
		}
		for key, entry := range entries {
			size := int64(len(entry))
			objectKind := ""
			switch kind {
			case "attempts":
				objectKind = "attempt"
			case "messages":
				objectKind = "message"
			case "scopes":
				objectKind = "scope"
				var scope struct {
					Nodes json.RawMessage `json:"nodes"`
				}
				if json.Unmarshal(entry, &scope) != nil {
					return snapshotLedger{}, invalidRecord(ErrInvalidRecordBudget)
				}
				size -= int64(len(scope.Nodes))
				if err := addContainer("nodes", key, scope.Nodes, false); err != nil {
					return snapshotLedger{}, err
				}
				var nodes map[string]json.RawMessage
				if json.Unmarshal(scope.Nodes, &nodes) != nil {
					return snapshotLedger{}, invalidRecord(ErrInvalidRecordBudget)
				}
				for node, value := range nodes {
					if !addObject(snapshotObjectKey{"node", key, node, ""}, int64(len(value))) {
						return snapshotLedger{}, invalidRecord(ErrInvalidRecordBudget)
					}
				}
			case "streams":
				objectKind = "stream"
				var stream struct {
					Pending json.RawMessage `json:"pending"`
				}
				if json.Unmarshal(entry, &stream) != nil {
					return snapshotLedger{}, invalidRecord(ErrInvalidRecordBudget)
				}
				size -= int64(len(stream.Pending))
				if err := addContainer("pending", key, stream.Pending, true); err != nil {
					return snapshotLedger{}, err
				}
				var pending []json.RawMessage
				if json.Unmarshal(stream.Pending, &pending) != nil {
					return snapshotLedger{}, invalidRecord(ErrInvalidRecordBudget)
				}
				for _, value := range pending {
					var message struct {
						Sequence uint64 `json:"sequence"`
					}
					if json.Unmarshal(value, &message) != nil || !addObject(snapshotObjectKey{"pending", key, strconv.FormatUint(message.Sequence, 10), ""}, int64(len(value))) {
						return snapshotLedger{}, invalidRecord(ErrInvalidRecordBudget)
					}
				}
			case "managed":
				objectKind = "managed"
				var managed struct {
					State  json.RawMessage `json:"state"`
					Values json.RawMessage `json:"values"`
				}
				if json.Unmarshal(entry, &managed) != nil {
					return snapshotLedger{}, invalidRecord(ErrInvalidRecordBudget)
				}
				size -= int64(len(managed.State) + len(managed.Values))
				for _, list := range []struct {
					kind, key string
					raw       json.RawMessage
				}{{"managed_state", "state", managed.State}, {"managed_values", "values", managed.Values}} {
					if err := addContainer(list.kind, key, list.raw, true); err != nil {
						return snapshotLedger{}, err
					}
					var values []json.RawMessage
					if json.Unmarshal(list.raw, &values) != nil {
						return snapshotLedger{}, invalidRecord(ErrInvalidRecordBudget)
					}
					for _, value := range values {
						var item struct {
							Name string `json:"name"`
						}
						if json.Unmarshal(value, &item) != nil || !addObject(snapshotObjectKey{"managed_entry", key, item.Name, list.key}, int64(len(value))) {
							return snapshotLedger{}, invalidRecord(ErrInvalidRecordBudget)
						}
					}
				}
			}
			if !addObject(snapshotObjectKey{objectKind, key, "", ""}, size) {
				return snapshotLedger{}, invalidRecord(ErrInvalidRecordBudget)
			}
		}
	}
	if out.fixed < 0 {
		return snapshotLedger{}, invalidRecord(ErrInvalidRecordBudget)
	}
	return out, nil
}

// SQL 返回完整复合键行；比较与行顺序无关，并拒绝重复行及任何缺失、额外或 null 字段。
func validateSnapshotLedger(raw, encoded []byte) error {
	var image struct {
		Fixed      json.RawMessage     `json:"fixed_bytes"`
		Objects    [][]json.RawMessage `json:"objects"`
		Containers [][]json.RawMessage `json:"containers"`
	}
	bad := invalidRecord(ErrInvalidRecordBudget)
	if !receiptUnicode(raw) || strictMaintenanceJSON(raw, &image) != nil || image.Objects == nil || image.Containers == nil {
		return bad
	}
	expected, err := snapshotLedgerFromSnapshot(encoded)
	if err != nil {
		return err
	}
	fixed, ok := snapshotLedgerInt(image.Fixed)
	if !ok || fixed != expected.fixed || len(image.Objects) != len(expected.objects) || len(image.Containers) != len(expected.containers) {
		return bad
	}
	for _, row := range image.Objects {
		if len(row) != 5 {
			return bad
		}
		var keys [4]string
		for i := range keys {
			var valid bool
			keys[i], valid = snapshotLedgerText(row[i])
			if !valid {
				return bad
			}
		}
		key := snapshotObjectKey{keys[0], keys[1], keys[2], keys[3]}
		size, ok := snapshotLedgerInt(row[4])
		want, exists := expected.objects[key]
		if !ok || !exists || size != want {
			return bad
		}
		delete(expected.objects, key)
	}
	for _, row := range image.Containers {
		if len(row) != 5 {
			return bad
		}
		kind, ok := snapshotLedgerText(row[0])
		if !ok {
			return bad
		}
		parent, ok := snapshotLedgerText(row[1])
		if !ok {
			return bad
		}
		key := snapshotContainerKey{kind, parent}
		want, exists := expected.containers[key]
		var nilValue bool
		if !exists || (string(row[2]) != "true" && string(row[2]) != "false") || json.Unmarshal(row[2], &nilValue) != nil || nilValue != want.nilValue {
			return bad
		}
		count, ok := snapshotLedgerInt(row[3])
		if !ok || count != want.count {
			return bad
		}
		sum, ok := snapshotLedgerInt(row[4])
		if !ok || sum != want.sum {
			return bad
		}
		delete(expected.containers, key)
	}
	if len(expected.objects) != 0 || len(expected.containers) != 0 {
		return bad
	}
	return nil
}

func snapshotLedgerText(raw []byte) (string, bool) {
	raw = bytes.TrimSpace(raw)
	var out string
	if len(raw) == 0 || raw[0] != '"' || json.Unmarshal(raw, &out) != nil {
		return "", false
	}
	return out, true
}
func snapshotLedgerInt(raw []byte) (int64, bool) {
	text, ok := snapshotLedgerText(raw)
	if !ok {
		return 0, false
	}
	value, err := strconv.ParseInt(text, 10, 64)
	return value, err == nil && value >= 0 && strconv.FormatInt(value, 10) == text
}
