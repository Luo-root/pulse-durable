package recordio

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"

	durable "github.com/Luo-root/pulse-durable"
)

const snapshotVersion = 1

type snapshotWire struct {
	Version int               `json:"version"`
	Record  durable.RunRecord `json:"record"`
}

// EncodeSnapshot encodes an arbitrary revision without normalizing nil containers
// or empty Data. Snapshot versioning is independent of RunRecord.Format. Identity,
// schema and revision validation remain the caller's responsibility.
func EncodeSnapshot(record durable.RunRecord) ([]byte, error) {
	if !validMetadata(record) {
		return nil, errors.New("recordio: snapshot metadata is not UTF-8")
	}
	value, err := snapshotValue(reflect.ValueOf(snapshotWire{snapshotVersion, record}))
	if err != nil {
		return nil, errors.New("recordio: unsupported snapshot field type")
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, errors.New("recordio: encode snapshot failed")
	}
	return data, nil
}

// DecodeSnapshot accepts only the complete, case-sensitive versioned snapshot
// schema. Unknown or duplicate fields (including map keys), lossy Unicode and
// trailing data are rejected. Errors never include stored metadata or Data.
func DecodeSnapshot(data []byte) (durable.RunRecord, error) {
	invalid := errors.New("recordio: invalid snapshot")
	if !json.Valid(data) || !validJSONText(data) {
		return durable.RunRecord{}, invalid
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := snapshotCheck(decoder, reflect.TypeOf(snapshotWire{})); err != nil {
		return durable.RunRecord{}, invalid
	}
	if _, err := decoder.Token(); err != io.EOF {
		return durable.RunRecord{}, invalid
	}
	var wire snapshotWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return durable.RunRecord{}, invalid
	}
	if wire.Version != snapshotVersion {
		return durable.RunRecord{}, errors.New("recordio: unsupported snapshot version")
	}
	if !validMetadata(wire.Record) {
		return durable.RunRecord{}, invalid
	}
	return wire.Record, nil
}

// snapshotValue walks the actual record types rather than maintaining a second
// copy of their fields. Every exported field is emitted: JSON's null versus {}
// or [] preserves maps/slices, and null versus "" preserves []byte. Pointers are
// either null or their complete value. Scalars retain their Go JSON encoding.
// Unsupported kinds fail closed instead of silently omitting new record fields.
func snapshotValue(v reflect.Value) (any, error) {
	switch v.Kind() {
	case reflect.Pointer:
		if v.IsNil() {
			return nil, nil
		}
		return snapshotValue(v.Elem())
	case reflect.Struct:
		out := make(map[string]any, v.NumField())
		for i := 0; i < v.NumField(); i++ {
			name := snapshotFieldName(v.Type().Field(i))
			if name == "" {
				return nil, errors.New("unsupported field")
			}
			if _, exists := out[name]; exists {
				return nil, errors.New("duplicate field")
			}
			value, err := snapshotValue(v.Field(i))
			if err != nil {
				return nil, err
			}
			out[name] = value
		}
		return out, nil
	case reflect.Map:
		if v.Type().Key().Kind() != reflect.String {
			return nil, errors.New("unsupported map key")
		}
		if v.IsNil() {
			return nil, nil
		}
		out := make(map[string]any, v.Len())
		iter := v.MapRange()
		for iter.Next() {
			value, err := snapshotValue(iter.Value())
			if err != nil {
				return nil, err
			}
			out[iter.Key().String()] = value
		}
		return out, nil
	case reflect.Slice:
		if v.IsNil() {
			return nil, nil
		}
		if v.Type() == reflect.TypeOf([]byte(nil)) {
			return v.Interface(), nil
		}
		out := make([]any, v.Len())
		for i := range out {
			value, err := snapshotValue(v.Index(i))
			if err != nil {
				return nil, err
			}
			out[i] = value
		}
		return out, nil
	case reflect.String, reflect.Bool, reflect.Int, reflect.Uint64:
		return v.Interface(), nil
	default:
		return nil, errors.New("unsupported kind")
	}
}

func snapshotFieldName(field reflect.StructField) string {
	if field.PkgPath != "" || field.Anonymous {
		return ""
	}
	name := strings.Split(field.Tag.Get("json"), ",")[0]
	if name == "-" {
		return ""
	}
	if name == "" {
		return field.Name
	}
	return name
}

// Token-level validation precedes Unmarshal because encoding/json otherwise
// accepts duplicate keys and case-insensitive struct field matches.
func snapshotCheck(decoder *json.Decoder, typ reflect.Type) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	for typ.Kind() == reflect.Pointer {
		if token == nil {
			return nil
		}
		typ = typ.Elem()
	}
	bad := errors.New("invalid snapshot field")
	switch typ.Kind() {
	case reflect.Struct, reflect.Map:
		if token == nil && typ.Kind() == reflect.Map {
			return nil
		}
		if token != json.Delim('{') {
			return bad
		}
		fields := make(map[string]reflect.Type)
		if typ.Kind() == reflect.Struct {
			for i := 0; i < typ.NumField(); i++ {
				name := snapshotFieldName(typ.Field(i))
				if name == "" || fields[name] != nil {
					return bad
				}
				fields[name] = typ.Field(i).Type
			}
		} else if typ.Key().Kind() != reflect.String {
			return bad
		}
		seen := make(map[string]bool)
		for decoder.More() {
			key, err := decoder.Token()
			if err != nil {
				return bad
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return bad
			}
			seen[name] = true
			var child reflect.Type
			if typ.Kind() == reflect.Struct {
				child = fields[name]
			} else {
				child = typ.Elem()
			}
			if child == nil {
				return bad
			}
			if err := snapshotCheck(decoder, child); err != nil {
				return err
			}
		}
		if typ.Kind() == reflect.Struct && len(seen) != len(fields) {
			return bad
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return bad
		}
		return nil
	case reflect.Slice:
		if token == nil {
			return nil
		}
		if typ == reflect.TypeOf([]byte(nil)) {
			if _, ok := token.(string); ok {
				return nil // Unmarshal validates base64.
			}
			return bad
		}
		if token != json.Delim('[') {
			return bad
		}
		for decoder.More() {
			if err := snapshotCheck(decoder, typ.Elem()); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return bad
		}
		return nil
	case reflect.String:
		if _, ok := token.(string); ok {
			return nil
		}
	case reflect.Bool:
		if _, ok := token.(bool); ok {
			return nil
		}
	case reflect.Int, reflect.Uint64:
		if _, ok := token.(json.Number); ok {
			return nil // Unmarshal validates integer syntax and range.
		}
	}
	return bad
}
