package durable

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

// ChangeBatchWireVersion 标识规范的 ChangeBatch JSON 封装。
const ChangeBatchWireVersion = 1

const (
	MaxChangeBatchBytes = 4 << 20
	MaxChangeBatchOps   = 4096
	MaxChangeBatchDepth = 64
)

var errInvalidChangeWire = errors.New("durable: invalid change batch wire")

type wireHeader struct {
	BatchFormat      string `json:"batch_format"`
	LogicalFormat    string `json:"logical_format"`
	RunID            string `json:"run_id"`
	BusinessVersion  string `json:"business_version"`
	DefinitionDigest string `json:"definition_digest"`
	InputsDigest     string `json:"inputs_digest"`
	RootScope        string `json:"root_scope"`
	ExpectedRevision string `json:"expected_revision"`
	Revision         string `json:"revision"`
}
type wireOp struct {
	Kind                              string          `json:"kind"`
	Key                               string          `json:"key"`
	Scope                             string          `json:"scope,omitempty"`
	Name                              string          `json:"name,omitempty"`
	Sequence                          string          `json:"sequence,omitempty"`
	State                             bool            `json:"state,omitempty"`
	Expected                          json.RawMessage `json:"expected"`
	Value                             json.RawMessage `json:"value"`
	sortScope, sortName, sortSequence string
	sortState                         bool
}
type wireBatch struct {
	Version int        `json:"version"`
	Header  wireHeader `json:"header"`
	Ops     []wireOp   `json:"ops"`
}

func u64(v uint64) string { return strconv.FormatUint(v, 10) }
func parseU64(s string) (uint64, bool) {
	if s == "" || s[0] < '0' || s[0] > '9' || (len(s) > 1 && s[0] == '0') {
		return 0, false
	}
	v, e := strconv.ParseUint(s, 10, 64)
	return v, e == nil
}

var wireUintFields = map[string]bool{"revision": true, "generation": true, "sequence": true, "last_sequence": true, "acked_through": true}

func transformWire(v any, encode bool) any {
	switch x := v.(type) {
	case map[string]any:
		for k, val := range x {
			if wireUintFields[k] {
				if encode {
					if n, ok := val.(json.Number); ok {
						x[k] = n.String()
					}
				} else if s, ok := val.(string); ok {
					x[k] = json.Number(s)
				}
			} else {
				x[k] = transformWire(val, encode)
			}
		}
		return x
	case []any:
		for i, val := range x {
			x[i] = transformWire(val, encode)
		}
		return x
	default:
		return v
	}
}
func raw(v any) json.RawMessage {
	rv := reflect.ValueOf(v)
	if rv.IsValid() && rv.Kind() == reflect.Map {
		b, _ := json.Marshal(v)
		var x any
		d := json.NewDecoder(bytes.NewReader(b))
		d.UseNumber()
		if d.Decode(&x) == nil {
			out, _ := json.Marshal(transformWire(x, true))
			return out
		}
	}
	out, ok := encodeWireReflect(rv, true, 0)
	if !ok {
		return nil
	}
	return out
}
func encodeWireReflect(v reflect.Value, encode bool, depth int) ([]byte, bool) {
	if depth > MaxChangeBatchDepth {
		return nil, false
	}
	if !v.IsValid() {
		return []byte("null"), true
	}
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return []byte("null"), true
		}
		return encodeWireReflect(v.Elem(), encode, depth+1)
	}
	if v.Kind() == reflect.Interface {
		if v.IsNil() {
			return []byte("null"), true
		}
		return encodeWireReflect(v.Elem(), encode, depth+1)
	}
	if v.Kind() == reflect.Struct {
		m := make(map[string]json.RawMessage)
		t := v.Type()
		for i := 0; i < v.NumField(); i++ {
			f := t.Field(i)
			if f.PkgPath != "" {
				continue
			}
			tag := strings.Split(f.Tag.Get("json"), ",")[0]
			if tag == "-" {
				continue
			}
			if tag == "" {
				tag = f.Name
			}
			b, ok := encodeWireReflect(v.Field(i), encode, depth+1)
			if !ok {
				return nil, false
			}
			m[tag] = b
		}
		return marshalWireObject(m)
	}
	if v.Kind() == reflect.Map {
		if v.IsNil() {
			return []byte("null"), true
		}
		m := make(map[string]json.RawMessage)
		for _, k := range v.MapKeys() {
			b, ok := encodeWireReflect(v.MapIndex(k), encode, depth+1)
			if !ok {
				return nil, false
			}
			m[k.String()] = b
		}
		return marshalWireObject(m)
	}
	if v.Kind() == reflect.Slice {
		if v.IsNil() {
			return []byte("null"), true
		}
		a := make([]json.RawMessage, v.Len())
		for i := range a {
			b, ok := encodeWireReflect(v.Index(i), encode, depth+1)
			if !ok {
				return nil, false
			}
			a[i] = b
		}
		return marshalWireArray(a)
	}
	if v.Kind() == reflect.Uint64 {
		if encode {
			return marshalWireScalar(strconv.FormatUint(v.Uint(), 10))
		}
		return marshalWireScalar(v.Uint())
	}
	if v.Kind() == reflect.String && !utf8.ValidString(v.String()) {
		return nil, false
	}
	return marshalWireScalar(v.Interface())
}
func marshalWireObject(v map[string]json.RawMessage) ([]byte, bool) {
	b, e := json.Marshal(v)
	return b, e == nil
}
func marshalWireArray(v []json.RawMessage) ([]byte, bool) {
	b, e := json.Marshal(v)
	return b, e == nil
}
func marshalWireScalar(v any) ([]byte, bool) { b, e := json.Marshal(v); return b, e == nil }
func rawNull() json.RawMessage               { return json.RawMessage("null") }

func headerWire(h ChangeHeader) wireHeader {
	return wireHeader{u64(uint64(h.BatchFormat)), u64(uint64(h.LogicalFormat)), h.RunID, h.BusinessVersion, h.DefinitionDigest, h.InputsDigest, h.RootScope, u64(h.ExpectedRevision), u64(h.Revision)}
}
func headerUnwire(h wireHeader) (ChangeHeader, error) {
	a, ok := parseU64(h.BatchFormat)
	if !ok || a > uint64(^uint(0)>>1) {
		return ChangeHeader{}, errInvalidChangeWire
	}
	l, ok := parseU64(h.LogicalFormat)
	if !ok || l > uint64(^uint(0)>>1) {
		return ChangeHeader{}, errInvalidChangeWire
	}
	e, ok := parseU64(h.ExpectedRevision)
	if !ok {
		return ChangeHeader{}, errInvalidChangeWire
	}
	r, ok := parseU64(h.Revision)
	if !ok {
		return ChangeHeader{}, errInvalidChangeWire
	}
	if a != 1 || l != RecordFormat || e == ^uint64(0) || r != e+1 || h.RunID == "" || h.RootScope == "" {
		return ChangeHeader{}, errInvalidChangeWire
	}
	return ChangeHeader{int(a), int(l), h.RunID, h.BusinessVersion, h.DefinitionDigest, h.InputsDigest, h.RootScope, e, r}, nil
}

func addWireOp(out *[]wireOp, kind, key, scope, name, seq string, state bool, expected, value any, absent, remove bool) {
	e := raw(expected)
	if absent {
		e = rawNull()
	}
	v := raw(value)
	if remove {
		v = rawNull()
	}
	*out = append(*out, wireOp{Kind: kind, Key: key, Scope: scope, Name: name, Sequence: seq, State: state, Expected: e, Value: v, sortScope: scope, sortName: name, sortSequence: seq, sortState: state})
}
func (b ChangeBatch) wireOps() ([]wireOp, error) {
	if b.tx == nil || b.tx.err != nil {
		return nil, errInvalidChangeWire
	}
	var o []wireOp
	for k, v := range b.tx.attempts {
		addWireOp(&o, "attempt", k, "", "", "", false, v.expected, v.value, v.absent, false)
	}
	for k, v := range b.tx.scopes {
		addWireOp(&o, "scope", k, "", "", "", false, nil, v, false, false)
	}
	for k, v := range b.tx.nodes {
		addWireOp(&o, "node", "", k.scope, k.node, "", false, v.expected, v.value, v.absent, false)
	}
	for k, v := range b.tx.streams {
		addWireOp(&o, "stream", k, "", "", "", false, v.expected, v.value, v.absent, false)
	}
	for k, v := range b.tx.pending {
		addWireOp(&o, "pending", "", k.stream, "", u64(k.sequence), false, v.expected, v.value, v.absent, v.remove)
	}
	for k, v := range b.tx.messages {
		addWireOp(&o, "message", k, "", "", "", false, v.expected, v.value, v.absent, false)
	}
	for k, v := range b.tx.managed {
		addWireOp(&o, "managed", k, "", "", "", false, v.expected, v.value, v.absent, false)
	}
	for k, v := range b.tx.managedEntries {
		addWireOp(&o, "managed_entry", "", k.managed, k.name, "", k.state, v.expected, v.value, v.absent, v.remove)
	}
	sort.Slice(o, func(i, j int) bool {
		return tupleLess(o[i].tuple(), o[j].tuple())
	})
	return o, nil
}

// MarshalChangeBatch 返回规范且确定性的字节。返回的字节不会与批次共享别名。
func MarshalChangeBatch(b ChangeBatch) ([]byte, error) {
	if b.tx == nil {
		return nil, errInvalidChangeWire
	}
	o, e := b.wireOps()
	if e != nil {
		return nil, e
	}
	if len(o) > MaxChangeBatchOps {
		return nil, errInvalidChangeWire
	}
	for _, op := range o {
		for _, s := range []string{op.Kind, op.Key, op.Scope, op.Name, op.Sequence} {
			if !utf8.ValidString(s) {
				return nil, errInvalidChangeWire
			}
		}
	}
	w := wireBatch{ChangeBatchWireVersion, headerWire(b.header), o}
	out, e := json.Marshal(w)
	if e != nil || len(out) > MaxChangeBatchBytes {
		return nil, errInvalidChangeWire
	}
	if !validStringFields(w.Header) {
		return nil, errInvalidChangeWire
	}
	if _, err := UnmarshalChangeBatch(out); err != nil {
		return nil, err
	}
	return out, e
}

// ChangeBatchDigest 是规范 MarshalChangeBatch 字节的 SHA-256。
func ChangeBatchDigest(b ChangeBatch) ([32]byte, error) {
	var z [32]byte
	p, e := MarshalChangeBatch(b)
	if e != nil {
		return z, e
	}
	return sha256.Sum256(p), nil
}

func rejectJSON(data []byte) error {
	if len(data) == 0 || len(data) > MaxChangeBatchBytes || !utf8.Valid(data) || !validWireUnicode(data) {
		return errInvalidChangeWire
	}
	d := json.NewDecoder(bytes.NewReader(data))
	if err := scanJSON(d, 0); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return errInvalidChangeWire
	}
	return nil
}

func validWireUnicode(data []byte) bool {
	for i := 0; i < len(data); i++ {
		if data[i] != '"' {
			continue
		}
		for i++; i < len(data) && data[i] != '"'; i++ {
			if data[i] != '\\' {
				continue
			}
			i++
			if i >= len(data) {
				return false
			}
			if data[i] != 'u' || i+4 >= len(data) {
				continue
			}
			v, e := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
			if e != nil {
				return false
			}
			i += 4
			if v >= 0xd800 && v <= 0xdbff {
				if i+6 >= len(data) || data[i+1] != '\\' || data[i+2] != 'u' {
					return false
				}
				low, e := strconv.ParseUint(string(data[i+3:i+7]), 16, 16)
				if e != nil || low < 0xdc00 || low > 0xdfff {
					return false
				}
				i += 6
			} else if v >= 0xdc00 && v <= 0xdfff {
				return false
			}
		}
	}
	return true
}

// scanJSON 在任何类型化反序列化修复或丢弃数据前，消费恰好一个 JSON 值，
// 并检查重复对象键、Unicode 字符串和嵌套深度。
func scanJSON(d *json.Decoder, depth int) error {
	if depth > MaxChangeBatchDepth {
		return errInvalidChangeWire
	}
	t, err := d.Token()
	if err != nil {
		return errInvalidChangeWire
	}
	switch x := t.(type) {
	case json.Delim:
		switch x {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				kt, e := d.Token()
				if e != nil {
					return errInvalidChangeWire
				}
				key, ok := kt.(string)
				if !ok || !utf8.ValidString(key) || seen[key] {
					return errInvalidChangeWire
				}
				seen[key] = true
				if e := scanJSON(d, depth+1); e != nil {
					return e
				}
			}
			end, e := d.Token()
			if e != nil || end != json.Delim('}') {
				return errInvalidChangeWire
			}
		case '[':
			for d.More() {
				if e := scanJSON(d, depth+1); e != nil {
					return e
				}
			}
			end, e := d.Token()
			if e != nil || end != json.Delim(']') {
				return errInvalidChangeWire
			}
		default:
			return errInvalidChangeWire
		}
	case string:
		if !utf8.ValidString(x) {
			return errInvalidChangeWire
		}
	case float64, bool, nil:
	default:
		return errInvalidChangeWire
	}
	return nil
}
func decodeStrict(data []byte, dst any) error {
	if e := rejectJSON(data); e != nil {
		return e
	}
	normalized, err := normalizeWire(data, reflect.TypeOf(dst).Elem(), false, 0)
	if err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(normalized))
	d.DisallowUnknownFields()
	if e := d.Decode(dst); e != nil {
		return errInvalidChangeWire
	}
	var extra any
	if d.Decode(&extra) != nil {
		return nil
	}
	return errInvalidChangeWire
}
func validStringFields(h wireHeader) bool {
	return utf8.ValidString(h.RunID) && utf8.ValidString(h.BusinessVersion) && utf8.ValidString(h.DefinitionDigest) && utf8.ValidString(h.InputsDigest) && utf8.ValidString(h.RootScope)
}

func unmarshalRecord(data json.RawMessage, dst any) error {
	if bytes.Equal(data, []byte("null")) {
		return errInvalidChangeWire
	}
	if e := rejectJSON(data); e != nil {
		return e
	}
	converted, err := normalizeWire(data, reflect.TypeOf(dst).Elem(), true, 0)
	if err != nil {
		return err
	}
	if json.Unmarshal(converted, dst) != nil {
		return errInvalidChangeWire
	}
	return nil
}
func (w wireOp) validate() error {
	if w.Kind == "" || !utf8.ValidString(w.Kind) || !utf8.ValidString(w.Key) || len(w.Expected) == 0 || len(w.Value) == 0 {
		return errInvalidChangeWire
	}
	switch w.Kind {
	case "attempt", "scope", "node", "stream", "pending", "message", "managed", "managed_entry":
	default:
		return errInvalidChangeWire
	}
	compound := w.Kind == "node" || w.Kind == "pending" || w.Kind == "managed_entry"
	if compound {
		if w.Key != "" || w.Scope == "" {
			return errInvalidChangeWire
		}
	} else if w.Key == "" || w.Scope != "" || w.Name != "" || w.Sequence != "" || w.State {
		return errInvalidChangeWire
	}
	if w.Kind == "node" && (w.Name == "" || w.Sequence != "" || w.State) {
		return errInvalidChangeWire
	}
	if w.Kind == "managed_entry" && w.Sequence != "" {
		return errInvalidChangeWire
	}
	if w.Kind == "scope" && !bytes.Equal(w.Expected, rawNull()) {
		return errInvalidChangeWire
	}
	if w.Kind != "pending" && w.Kind != "managed_entry" && bytes.Equal(w.Value, rawNull()) {
		return errInvalidChangeWire
	}
	if w.Kind == "pending" {
		if w.Name != "" || w.State {
			return errInvalidChangeWire
		}
		if w.Scope == "" {
			return errInvalidChangeWire
		}
		if _, ok := parseU64(w.Sequence); !ok {
			return errInvalidChangeWire
		}
	}
	if w.Kind == "managed_entry" && w.Name == "" {
		return errInvalidChangeWire
	}
	return nil
}

// UnmarshalChangeBatch 在构造任何事务状态前校验完整封装。
func UnmarshalChangeBatch(data []byte) (ChangeBatch, error) {
	var z ChangeBatch
	if e := rejectJSON(data); e != nil {
		return z, e
	}
	var w wireBatch
	if e := decodeStrict(data, &w); e != nil {
		return z, e
	}
	if w.Version != ChangeBatchWireVersion || len(w.Ops) > MaxChangeBatchOps || !validStringFields(w.Header) {
		return z, errInvalidChangeWire
	}
	h, e := headerUnwire(w.Header)
	if e != nil {
		return z, e
	}
	tx := newRecordTransaction(h.ExpectedRevision)
	managedHeaders := make(map[string]bool)
	managedEntries := make(map[string]bool)
	for _, op := range w.Ops {
		if op.Kind == "managed" {
			managedHeaders[op.Key] = true
		}
		if op.Kind == "managed_entry" {
			managedEntries[op.Scope] = true
		}
	}
	for key := range managedEntries {
		if !managedHeaders[key] {
			return z, errInvalidChangeWire
		}
	}
	seen := make(map[wireTuple]bool, len(w.Ops))
	var prev wireTuple
	first := true
	for _, op := range w.Ops {
		if e := op.validate(); e != nil {
			return z, e
		}
		order := op.tuple()
		if seen[order] || (!first && tupleLess(order, prev)) {
			return z, errInvalidChangeWire
		}
		seen[order] = true
		prev, first = order, false
		var e error
		switch op.Kind {
		case "attempt":
			var a, v AttemptRecord
			if !bytes.Equal(op.Expected, []byte("null")) {
				e = unmarshalRecord(op.Expected, &a)
			}
			if e == nil && !bytes.Equal(op.Value, []byte("null")) {
				e = unmarshalRecord(op.Value, &v)
			}
			if e == nil {
				var ep *AttemptRecord
				if !bytes.Equal(op.Expected, []byte("null")) {
					ep = &a
				}
				e = tx.putAttempt(op.Key, ep, v)
			}
		case "scope":
			var v ScopeRecord
			e = unmarshalRecord(op.Value, &v)
			if e == nil {
				e = tx.createScope(op.Key, v)
			}
		case "node":
			var a, v NodeRecord
			if !bytes.Equal(op.Expected, []byte("null")) {
				e = unmarshalRecord(op.Expected, &a)
			}
			if e == nil {
				e = unmarshalRecord(op.Value, &v)
			}
			if e == nil {
				var ep *NodeRecord
				if !bytes.Equal(op.Expected, []byte("null")) {
					ep = &a
				}
				e = tx.putNode(op.Scope, op.Name, ep, v)
			}
		case "stream":
			var a, v streamHeader
			if !bytes.Equal(op.Expected, []byte("null")) {
				e = unmarshalRecord(op.Expected, &a)
			}
			if e == nil {
				e = unmarshalRecord(op.Value, &v)
			}
			if e == nil {
				var ep *streamHeader
				if !bytes.Equal(op.Expected, []byte("null")) {
					ep = &a
				}
				e = tx.putStream(op.Key, ep, v)
			}
		case "pending":
			n, _ := parseU64(op.Sequence)
			var a, v StreamMessageRecord
			if !bytes.Equal(op.Expected, []byte("null")) {
				e = unmarshalRecord(op.Expected, &a)
			}
			if e == nil && !bytes.Equal(op.Value, []byte("null")) {
				e = unmarshalRecord(op.Value, &v)
			}
			if e == nil {
				var ep, vp *StreamMessageRecord
				if !bytes.Equal(op.Expected, []byte("null")) {
					ep = &a
				}
				if !bytes.Equal(op.Value, []byte("null")) {
					vp = &v
				}
				e = tx.putPending(op.Scope, n, ep, vp)
			}
		case "message":
			var a, v MessageAttemptRecord
			if !bytes.Equal(op.Expected, []byte("null")) {
				e = unmarshalRecord(op.Expected, &a)
			}
			if e == nil {
				e = unmarshalRecord(op.Value, &v)
			}
			if e == nil {
				var ep *MessageAttemptRecord
				if !bytes.Equal(op.Expected, []byte("null")) {
					ep = &a
				}
				e = tx.putMessage(op.Key, ep, v)
			}
		case "managed":
			var a, v managedHeader
			if !bytes.Equal(op.Expected, []byte("null")) {
				e = unmarshalRecord(op.Expected, &a)
			}
			if e == nil {
				e = unmarshalRecord(op.Value, &v)
			}
			if e == nil {
				var ep *managedHeader
				if !bytes.Equal(op.Expected, []byte("null")) {
					ep = &a
				}
				e = tx.putManaged(op.Key, ep, v)
			}
		case "managed_entry":
			n := managedEntryKey{op.Scope, op.Name, op.State}
			var a, v ValueRecord
			if !bytes.Equal(op.Expected, []byte("null")) {
				e = unmarshalRecord(op.Expected, &a)
			}
			if e == nil && !bytes.Equal(op.Value, []byte("null")) {
				e = unmarshalRecord(op.Value, &v)
			}
			if e == nil {
				var ep, vp *ValueRecord
				if !bytes.Equal(op.Expected, []byte("null")) {
					ep = &a
				}
				if !bytes.Equal(op.Value, []byte("null")) {
					vp = &v
				}
				e = tx.putManagedEntry(n, ep, vp)
			}
		}
		if e != nil {
			return z, errInvalidChangeWire
		}
	}
	if tx.err != nil {
		return z, errInvalidChangeWire
	}
	return ChangeBatch{header: h, tx: tx}, nil
}

func (b ChangeBatch) StringDigest() string {
	d, e := ChangeBatchDigest(b)
	if e != nil {
		return ""
	}
	return fmt.Sprintf("%x", d)
}
