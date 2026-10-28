package sqlitestore

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
)

func cloneWire(t *testing.T, b durable.ChangeBatch) atomicWire {
	t.Helper()
	wire, e := durable.MarshalChangeBatch(b)
	if e != nil {
		t.Fatal(e)
	}
	var envelope atomicWire
	if e = json.Unmarshal(wire, &envelope); e != nil {
		t.Fatal(e)
	}
	return envelope
}
func cloneDecodeWire(t *testing.T, w atomicWire) durable.ChangeBatch {
	t.Helper()
	data, e := json.Marshal(w)
	if e != nil {
		t.Fatal(e)
	}
	b, e := durable.UnmarshalChangeBatch(data)
	if e != nil {
		t.Fatal(e)
	}
	canonical, e := durable.MarshalChangeBatch(b)
	if e != nil {
		t.Fatal(e)
	}
	b, e = durable.UnmarshalChangeBatch(canonical)
	if e != nil {
		t.Fatal(e)
	}
	again, e := durable.MarshalChangeBatch(b)
	if e != nil || !bytes.Equal(canonical, again) {
		t.Fatal("noncanonical batch", e)
	}
	return b
}

// The trigger is valid public typed wire, not a claim about ordinary Codec
// output. Empty Data must survive Load because it participates in exact expected.
func TestSQLiteLoadPreservesTypedWireNilEmptyData(t *testing.T) {
	initial, batches, second := atomicMessageFixture(t)
	first := -1
	for i, b := range batches[:second] {
		w := cloneWire(t, b)
		for _, op := range w.Ops {
			if op.Kind == "managed_entry" && op.State && op.Name == "remove" && !bytes.Equal(op.Value, []byte("null")) {
				first = i
			}
		}
	}
	if first < 0 {
		t.Fatal("no first State commit")
	}
	for _, empty := range []bool{false, true} {
		for _, reopen := range []bool{false, true} {
			for _, remove := range []bool{false, true} {
				t.Run(fmt.Sprintf("empty=%t/reopen=%t/remove=%t", empty, reopen, remove), func(t *testing.T) {
					path := filepath.Join(t.TempDir(), "typed.sqlite")
					s := openStore(t, path)
					ss := acquire(t, s, initial.RunID)
					if e := ss.Initialize(ctx, initial); e != nil {
						t.Fatal(e)
					}
					authoritative := durable.CloneRecord(*ss.(*session).cache)
					managedID := ""
					for i, b := range batches[:second] {
						if i == first {
							w := cloneWire(t, b)
							for j := range w.Ops {
								op := &w.Ops[j]
								if op.Kind == "managed_entry" && op.State && op.Name == "remove" {
									var value map[string]json.RawMessage
									if e := json.Unmarshal(op.Value, &value); e != nil {
										t.Fatal(e)
									}
									if empty {
										value["data"] = json.RawMessage(`[]`)
									} else {
										value["data"] = json.RawMessage(`null`)
									}
									value["digest"] = json.RawMessage(fmt.Sprintf("%q", fmt.Sprintf("%x", sha256.Sum256(nil))))
									var e error
									op.Value, e = json.Marshal(value)
									if e != nil {
										t.Fatal(e)
									}
									managedID = op.Scope
								}
							}
							b = cloneDecodeWire(t, w)
						}
						if _, e := ss.Apply(ctx, b); e != nil {
							t.Fatal(e)
						}
						if _, e := b.ApplyTo(&authoritative); e != nil {
							t.Fatal(e)
						}
					}
					if managedID == "" {
						t.Fatal("missing managed identity")
					}
					if reopen {
						ss.Close()
						s.Close()
						s = openStore(t, path)
						ss = acquire(t, s, initial.RunID)
					}
					loaded, e := ss.Load(ctx)
					if e != nil {
						t.Fatal(e)
					}
					if !reflect.DeepEqual(loaded, authoritative) {
						t.Fatal("Load changed authoritative typed nil/empty representation")
					}
					var expected durable.ValueRecord
					found := false
					for _, v := range loaded.Managed[managedID].State {
						if v.Name == "remove" {
							expected = v
							found = true
						}
					}
					if !found || (expected.Data != nil) != empty {
						t.Fatalf("Data nil=%t want empty=%t", expected.Data == nil, empty)
					}
					// Keep every explicit payload field from a real canonical operation; replace
					// its exact expected using the value just returned by Load (not private cache).
					next := cloneWire(t, batches[second])
					for j := range next.Ops {
						op := &next.Ops[j]
						if op.Kind == "managed_entry" && op.State && op.Name == "remove" {
							var old map[string]json.RawMessage
							if e = json.Unmarshal(op.Expected, &old); e != nil {
								t.Fatal(e)
							}
							data, e := json.Marshal(expected.Data)
							if e != nil {
								t.Fatal(e)
							}
							if expected.Data != nil && len(expected.Data) == 0 {
								data = []byte("[]")
							}
							old["data"] = data
							digest, e := json.Marshal(expected.Digest)
							if e != nil {
								t.Fatal(e)
							}
							old["digest"] = digest
							op.Expected, e = json.Marshal(old)
							if e != nil {
								t.Fatal(e)
							}
							if remove {
								op.Value = json.RawMessage("null")
							}
						}
					}
					b := cloneDecodeWire(t, next)
					if _, e = ss.Apply(ctx, b); e != nil {
						t.Fatal("Load-derived expected failed", e)
					}
					if _, e = b.ApplyTo(&authoritative); e != nil {
						t.Fatal(e)
					}
					loaded, e = ss.Load(ctx)
					if e != nil || !reflect.DeepEqual(loaded, authoritative) {
						t.Fatal("postmutation Load", e)
					}
					ss.Close()
					s.Close()
					s = openStore(t, path)
					loaded, e = acquire(t, s, initial.RunID).Load(ctx)
					if e != nil || !reflect.DeepEqual(loaded, authoritative) {
						t.Fatal("reopened mutation", e)
					}
					remains := false
					for _, v := range loaded.Managed[managedID].State {
						if v.Name == "remove" {
							remains = true
							if !remove && string(v.Data) != "2" {
								t.Fatal("update did not persist")
							}
						}
					}
					if remains == remove {
						t.Fatal("unexpected deleted State presence")
					}
				})
			}
		}
	}
}

func TestCloneProjectionExactAndIsolated(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprint(empty), func(t *testing.T) {
			var data []byte
			if empty {
				data = []byte{}
			}
			value := durable.ValueRecord{Data: data, Stream: &durable.StreamDescriptorRecord{Graph: "original"}}
			r := durable.RunRecord{
				Scopes:   map[string]durable.ScopeRecord{"scope": {Nodes: map[string]durable.NodeRecord{"node": {Outputs: []durable.ValueRecord{value}}}}, "empty": {Nodes: map[string]durable.NodeRecord{}}},
				Attempts: map[string]durable.AttemptRecord{"attempt": {NodeID: "original"}},
				Streams:  map[string]durable.StreamRecord{"stream": {Pending: []durable.StreamMessageRecord{{Value: value}}, Cursor: &value}, "empty": {Pending: []durable.StreamMessageRecord{}}},
				Messages: map[string]durable.MessageAttemptRecord{"message": {NodeID: "original"}},
				Managed:  map[string]durable.ManagedRecord{"managed": {State: []durable.ValueRecord{value}, Values: []durable.ValueRecord{value}}, "empty": {State: []durable.ValueRecord{}, Values: []durable.ValueRecord{}}},
			}
			copy := durable.CloneRecord(r)
			if !reflect.DeepEqual(r, copy) {
				t.Fatal("copy normalized nil/empty")
			}
			copy.Scopes["scope"].Nodes["node"].Outputs[0].Stream.Graph = "changed"
			copy.Streams["stream"].Pending[0].Value.Stream.Graph = "changed"
			copy.Streams["stream"].Cursor.Stream.Graph = "changed"
			copy.Managed["managed"].State[0].Stream.Graph = "changed"
			copy.Managed["managed"].Values[0].Stream.Graph = "changed"
			delete(copy.Scopes, "empty")
			delete(copy.Attempts, "attempt")
			delete(copy.Streams, "empty")
			delete(copy.Messages, "message")
			delete(copy.Managed, "empty")
			if r.Scopes["scope"].Nodes["node"].Outputs[0].Stream.Graph != "original" || r.Streams["stream"].Pending[0].Value.Stream.Graph != "original" || r.Streams["stream"].Cursor.Stream.Graph != "original" || r.Managed["managed"].State[0].Stream.Graph != "original" || r.Managed["managed"].Values[0].Stream.Graph != "original" || len(r.Scopes) != 2 || len(r.Attempts) != 1 || len(r.Streams) != 2 || len(r.Messages) != 1 || len(r.Managed) != 2 {
				t.Fatal("copy aliases source")
			}
		})
	}
	// Nonempty bytes need independent backing arrays too, at every value site.
	v := durable.ValueRecord{Data: []byte{7}}
	r := durable.RunRecord{Scopes: map[string]durable.ScopeRecord{"s": {Nodes: map[string]durable.NodeRecord{"n": {Outputs: []durable.ValueRecord{v}}}}}, Streams: map[string]durable.StreamRecord{"s": {Pending: []durable.StreamMessageRecord{{Value: v}}, Cursor: &v}}, Managed: map[string]durable.ManagedRecord{"m": {State: []durable.ValueRecord{v}, Values: []durable.ValueRecord{v}}}}
	c := durable.CloneRecord(r)
	c.Scopes["s"].Nodes["n"].Outputs[0].Data[0] = 9
	c.Streams["s"].Pending[0].Value.Data[0] = 9
	c.Streams["s"].Cursor.Data[0] = 9
	c.Managed["m"].State[0].Data[0] = 9
	c.Managed["m"].Values[0].Data[0] = 9
	if v.Data[0] != 7 {
		t.Fatal("Data backing array aliases")
	}
	if !reflect.DeepEqual(durable.CloneRecord(durable.RunRecord{}), durable.RunRecord{}) {
		t.Fatal("nil maps normalized")
	}
}
