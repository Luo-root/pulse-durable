package durable

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"github.com/Luo-root/pulse"
)

func streamID(id pulse.CheckpointStreamID) StreamIDRecord {
	return StreamIDRecord{Scope: id.Scope, NodeID: id.NodeID, Key: id.Key, Group: id.Group, Generation: id.Generation}
}

func (id StreamIDRecord) checkpoint() pulse.CheckpointStreamID {
	return pulse.CheckpointStreamID{Scope: id.Scope, NodeID: id.NodeID, Key: id.Key, Group: id.Group, Generation: id.Generation}
}

// 长度无歧义的稳定身份；Generation 单独核验，不因恢复更换消息预算的归属。
func streamKey(id StreamIDRecord) string {
	data, _ := json.Marshal([4]string{id.Scope, id.NodeID, id.Key, id.Group})
	return digest(data)
}

func managedKey(scope, parent, node string) string {
	data, _ := json.Marshal([3]string{scope, parent, node})
	return digest(data)
}

func (d *definition) encodeDescriptor(key pulse.CheckpointKey, value pulse.CheckpointStreamDescriptor) (StreamDescriptorRecord, error) {
	c, err := d.codec(key)
	if err != nil {
		return StreamDescriptorRecord{}, err
	}
	id := value.ID
	if key.Element == nil || key.Element != value.Element || !validIdentity(id.Scope) || id.NodeID == "" || id.Key == "" || id.Group == "" ||
		id.Generation == 0 || !validIdentity(value.Attempt) || value.Graph == "" || value.Source.ID == "" || value.Capacity < 1 || !streamTerminal(string(value.Terminal), value.Stopped) {
		return StreamDescriptorRecord{}, fmt.Errorf("durable: invalid stream descriptor")
	}
	return StreamDescriptorRecord{ID: streamID(id), Graph: value.Graph, Path: value.Path, Attempt: value.Attempt,
		TypeID: c.id, Version: c.version, SourceID: value.Source.ID, Resumable: value.Source.Resumable, Capacity: value.Capacity,
		Terminal: string(value.Terminal), Stopped: value.Stopped}, nil
}

func (d *definition) decodeDescriptor(key pulse.CheckpointKey, record StreamDescriptorRecord) (pulse.CheckpointStreamDescriptor, error) {
	value := pulse.CheckpointStreamDescriptor{ID: record.ID.checkpoint(), Graph: record.Graph, Path: record.Path, Attempt: record.Attempt,
		Element: key.Element, Source: pulse.CheckpointStreamSource{ID: record.SourceID, Resumable: record.Resumable}, Capacity: record.Capacity,
		Terminal: pulse.StreamTerminal(record.Terminal), Stopped: record.Stopped}
	encoded, err := d.encodeDescriptor(key, value)
	if err != nil {
		return value, err
	}
	if encoded != record {
		return value, fmt.Errorf("durable: stream element codec mismatch")
	}
	return value, nil
}

func streamTerminal(terminal string, stopped bool) bool {
	// stopped 是写端在读取组已停止后得出的终态，读取组 Stop 不能倒退。
	if terminal == string(pulse.StreamStopped) && !stopped {
		return false
	}
	return terminal == "" || terminal == string(pulse.StreamCompleted) || terminal == string(pulse.StreamStopped) ||
		terminal == string(pulse.StreamFailed) || terminal == string(pulse.StreamCanceled)
}

func (d *definition) decodeValue(key pulse.CheckpointKey, v ValueRecord) (pulse.CheckpointOutput, error) {
	o := pulse.CheckpointOutput{Key: key, State: pulse.SlotState(v.State)}
	c, err := d.codec(key)
	if err != nil {
		return o, err
	}
	if v.Name != key.Name || v.TypeID != c.id || v.Version != c.version {
		return o, fmt.Errorf("durable: value codec or name mismatch")
	}
	if o.State == pulse.SlotStateSkipped {
		if v.Data != nil || v.Digest != "" || v.Stream != nil {
			return o, fmt.Errorf("durable: skipped value contains data")
		}
		return o, nil
	}
	if o.State != pulse.SlotStateReady {
		return o, fmt.Errorf("durable: invalid value state")
	}
	if key.Element != nil {
		if v.Stream == nil || len(v.Data) != 0 || v.Digest != "" {
			return o, fmt.Errorf("durable: stream value must contain only a descriptor")
		}
		value, err := d.decodeDescriptor(key, *v.Stream)
		o.Stream = &value
		return o, err
	}
	if v.Stream != nil || !validDigest(v.Digest) || digest(v.Data) != v.Digest {
		return o, fmt.Errorf("durable: encoded value checksum mismatch")
	}
	o.Value, err = c.decode(v.Data)
	return o, err
}

func (d *definition) encodePayload(name string, typ reflect.Type, value any) (ValueRecord, error) {
	values, err := d.encodeOutputs([]pulse.CheckpointOutput{{Key: pulse.CheckpointKey{Name: name, Type: typ}, State: pulse.SlotStateReady, Value: value}})
	if err != nil {
		return ValueRecord{}, err
	}
	return values[0], nil
}

func (d *definition) payloadType(value ValueRecord) (reflect.Type, error) {
	for typ, codec := range d.codecs {
		if codec.id == value.TypeID && codec.version == value.Version {
			return typ, nil
		}
	}
	return nil, fmt.Errorf("durable: payload has no matching registered codec")
}

func (d *definition) decodePayload(value ValueRecord) (pulse.CheckpointState, error) {
	typ, err := d.payloadType(value)
	if err != nil {
		return pulse.CheckpointState{}, err
	}
	if value.State != string(pulse.SlotStateReady) || value.Stream != nil {
		return pulse.CheckpointState{}, fmt.Errorf("durable: invalid payload state")
	}
	output, err := d.decodeValue(pulse.CheckpointKey{Name: value.Name, Type: typ}, value)
	return pulse.CheckpointState{Name: value.Name, Type: typ, Value: output.Value}, err
}

// Bindings 包含尚未到达的 In，故只对固定输入编码；绑定身份自身参与指纹。
// 无绑定时保持格式 1 的原指纹，固定值变化仍会在 OpenChild 处拒绝。
func (s *runState) childFingerprint(loc location, req pulse.CheckpointChild, parentScope string) (string, error) {
	if len(req.Bindings) == 0 {
		return s.def.inputsFingerprint(loc, req.Inputs)
	}
	type binding struct {
		Scope, Attempt, Graph, Path string
		Parent, Child               keyFingerprint
	}
	bindings := make([]binding, 0, len(req.Bindings))
	bound := make(map[string]bool)
	parent := s.record.Scopes[parentScope]
	parentSchema := s.def.graphs[location{parent.Graph, parent.Path}]
	for _, b := range req.Bindings {
		childKey, ok := s.def.graphs[loc].inputs[b.Child.Name]
		if !ok || childKey != b.Child || bound[b.Child.Name] || b.Scope != parentScope || b.Attempt != req.Attempt ||
			(location{b.Source.Graph, b.Source.Path}) != (location{parent.Graph, parent.Path}) {
			return "", fmt.Errorf("durable: invalid child binding identity")
		}
		var parentKey pulse.CheckpointKey
		if producer, ok := parentSchema.producers[b.Parent.Name]; ok {
			for _, key := range parentSchema.nodes[producer].Provides {
				if key.Name == b.Parent.Name {
					parentKey = key
				}
			}
		} else {
			parentKey = parentSchema.inputs[b.Parent.Name]
		}
		if parentKey != b.Parent || parentKey.Type != childKey.Type {
			return "", fmt.Errorf("durable: child binding declaration mismatch")
		}
		pk, err := s.def.keyFingerprints([]pulse.CheckpointKey{b.Parent, b.Child})
		if err != nil {
			return "", err
		}
		bound[b.Child.Name] = true
		bindings = append(bindings, binding{b.Scope, b.Attempt, b.Source.Graph, b.Source.Path, pk[0], pk[1]})
	}
	fixed := make([]pulse.CheckpointOutput, 0, len(req.Inputs))
	for _, input := range req.Inputs {
		if !bound[input.Key.Name] {
			fixed = append(fixed, input)
		}
	}
	fixedHash, err := s.def.inputsFingerprint(loc, fixed)
	if err != nil {
		return "", err
	}
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].Child.Name < bindings[j].Child.Name })
	data, err := json.Marshal(struct {
		Fixed    string
		Bindings []binding
	}{fixedHash, bindings})
	if err != nil {
		return "", err
	}
	return digest(data), nil
}
