package durable

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
)

// normalizeWire checks exact case-sensitive fields and presence before the Go
// decoder can apply its permissive case aliases or null-to-zero rules.
func normalizeWire(data []byte, typ reflect.Type, payload bool, depth int) ([]byte, error) {
	if depth > MaxChangeBatchDepth {
		return nil, errInvalidChangeWire
	}
	if typ == reflect.TypeFor[json.RawMessage]() {
		return data, nil
	}
	null := bytes.Equal(bytes.TrimSpace(data), []byte("null"))
	if typ.Kind() == reflect.Pointer {
		if null {
			return []byte("null"), nil
		}
		return normalizeWire(data, typ.Elem(), payload, depth+1)
	}
	switch typ.Kind() {
	case reflect.Struct:
		if null {
			return nil, errInvalidChangeWire
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(data, &fields) != nil || fields == nil {
			return nil, errInvalidChangeWire
		}
		out := make(map[string]json.RawMessage)
		allowed := make(map[string]bool)
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			if f.PkgPath != "" {
				continue
			}
			parts := strings.Split(f.Tag.Get("json"), ",")
			name := parts[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = f.Name
			}
			allowed[name] = true
			v, ok := fields[name]
			optional := !payload && len(parts) > 1 && parts[1] == "omitempty"
			if !ok {
				if optional {
					continue
				}
				return nil, errInvalidChangeWire
			}
			b, e := normalizeWire(v, f.Type, payload, depth+1)
			if e != nil {
				return nil, e
			}
			out[name] = b
		}
		for k := range fields {
			if !allowed[k] {
				return nil, errInvalidChangeWire
			}
		}
		return json.Marshal(out)
	case reflect.Uint64:
		var s string
		if null || json.Unmarshal(data, &s) != nil {
			return nil, errInvalidChangeWire
		}
		v, ok := parseU64(s)
		if !ok {
			return nil, errInvalidChangeWire
		}
		return []byte(u64(v)), nil
	case reflect.Slice:
		if null {
			return []byte("null"), nil
		}
		var a []json.RawMessage
		if json.Unmarshal(data, &a) != nil {
			return nil, errInvalidChangeWire
		}
		for i := range a {
			b, e := normalizeWire(a[i], typ.Elem(), payload, depth+1)
			if e != nil {
				return nil, e
			}
			a[i] = b
		}
		return json.Marshal(a)
	case reflect.Map:
		if null {
			return []byte("null"), nil
		}
		var m map[string]json.RawMessage
		if json.Unmarshal(data, &m) != nil {
			return nil, errInvalidChangeWire
		}
		for k, v := range m {
			b, e := normalizeWire(v, typ.Elem(), payload, depth+1)
			if e != nil {
				return nil, e
			}
			m[k] = b
		}
		return json.Marshal(m)
	default:
		if null {
			return nil, errInvalidChangeWire
		}
		v := reflect.New(typ)
		if json.Unmarshal(data, v.Interface()) != nil {
			return nil, errInvalidChangeWire
		}
		return data, nil
	}
}

type wireTuple struct {
	kind, key, scope, name, sequence string
	state                            bool
}

func (w wireOp) tuple() wireTuple {
	return wireTuple{w.Kind, w.Key, w.Scope, w.Name, w.Sequence, w.State}
}
func tupleLess(a, b wireTuple) bool {
	av := [5]string{a.kind, a.key, a.scope, a.name, a.sequence}
	bv := [5]string{b.kind, b.key, b.scope, b.name, b.sequence}
	for i := range av {
		if av[i] != bv[i] {
			return av[i] < bv[i]
		}
	}
	return !a.state && b.state
}
