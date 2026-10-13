package durable

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"github.com/Luo-root/pulse"
)

type location struct{ graph, path string }

type graphSchema struct {
	nodes     map[string]pulse.CheckpointNode
	order     []string
	inputs    map[string]pulse.CheckpointKey
	outputs   map[string]pulse.CheckpointKey
	producers map[string]string
	children  map[string]location
}

type definition struct {
	fingerprint string
	root        location
	graphs      map[location]*graphSchema
	codecs      map[reflect.Type]codecEntry
}

type keyFingerprint struct {
	Name, TypeID, Version string
}

type nodeFingerprint struct {
	Graph, Path, ID, Stream string
	Requires, Provides      []keyFingerprint
}

type portFingerprint struct {
	Graph, Path   string
	Key           keyFingerprint
	Input, Output bool
}

func inspectDefinition(g *pulse.Graph, codecs map[reflect.Type]codecEntry) (*definition, error) {
	if g == nil {
		return nil, fmt.Errorf("durable: graph is required")
	}
	if err := g.Validate(); err != nil {
		return nil, err
	}
	top := g.Topology()
	d := &definition{root: location{top.Graph.ID, top.Graph.Path}, graphs: make(map[location]*graphSchema), codecs: codecs}
	var walk func(*pulse.Topology)
	walk = func(t *pulse.Topology) {
		s := &graphSchema{nodes: make(map[string]pulse.CheckpointNode), inputs: make(map[string]pulse.CheckpointKey),
			outputs: make(map[string]pulse.CheckpointKey), producers: make(map[string]string), children: make(map[string]location)}
		for _, n := range t.Nodes {
			s.order = append(s.order, n.ID)
			for _, k := range n.Provides {
				s.producers[k.Name] = n.ID
			}
			if n.Sub != nil {
				s.children[n.ID] = location{n.Sub.Graph.ID, n.Sub.Graph.Path}
				walk(n.Sub)
			}
		}
		d.graphs[location{t.Graph.ID, t.Graph.Path}] = s
	}
	walk(top)
	var nodes []nodeFingerprint
	for _, n := range g.CheckpointNodes() {
		s, ok := d.graphs[location{n.Graph, n.Path}]
		if !ok || n.Stream != "" {
			return nil, fmt.Errorf("durable: stream or unknown checkpoint declaration at %q", n.Path)
		}
		if _, exists := s.nodes[n.ID]; exists {
			return nil, fmt.Errorf("durable: duplicate checkpoint node declaration")
		}
		nf := nodeFingerprint{Graph: n.Graph, Path: n.Path, ID: n.ID, Stream: n.Stream}
		var err error
		if nf.Requires, err = d.keyFingerprints(n.Requires); err != nil {
			return nil, err
		}
		if nf.Provides, err = d.keyFingerprints(n.Provides); err != nil {
			return nil, err
		}
		s.nodes[n.ID] = n
		nodes = append(nodes, nf)
	}
	var ports []portFingerprint
	for _, p := range g.CheckpointPorts() {
		s, ok := d.graphs[location{p.Graph, p.Path}]
		if !ok || (!p.Input && !p.Output) {
			return nil, fmt.Errorf("durable: invalid checkpoint port declaration")
		}
		keys, err := d.keyFingerprints([]pulse.CheckpointKey{p.Key})
		if err != nil {
			return nil, err
		}
		if p.Input {
			s.inputs[p.Key.Name] = p.Key
		}
		if p.Output {
			s.outputs[p.Key.Name] = p.Key
		}
		ports = append(ports, portFingerprint{Graph: p.Graph, Path: p.Path, Key: keys[0], Input: p.Input, Output: p.Output})
	}
	for _, s := range d.graphs {
		if len(s.order) != len(s.nodes) {
			return nil, fmt.Errorf("durable: incomplete checkpoint declarations")
		}
	}
	data, err := json.Marshal(struct {
		Topology *pulse.Topology
		Nodes    []nodeFingerprint
		Ports    []portFingerprint
	}{top, nodes, ports})
	if err != nil {
		return nil, fmt.Errorf("durable: definition fingerprint failed")
	}
	d.fingerprint = digest(data)
	return d, nil
}

func (d *definition) keyFingerprints(keys []pulse.CheckpointKey) ([]keyFingerprint, error) {
	out := make([]keyFingerprint, 0, len(keys))
	for _, k := range keys {
		c, err := d.codec(k)
		if err != nil {
			return nil, err
		}
		out = append(out, keyFingerprint{Name: k.Name, TypeID: c.id, Version: c.version})
	}
	return out, nil
}

func (d *definition) codec(k pulse.CheckpointKey) (codecEntry, error) {
	if k.Name == "" || !supportedType(k.Type, make(map[reflect.Type]bool)) {
		return codecEntry{}, fmt.Errorf("durable: key %q has a non-persistable type", k.Name)
	}
	c, ok := d.codecs[k.Type]
	if !ok {
		return codecEntry{}, fmt.Errorf("durable: key %q has no registered codec", k.Name)
	}
	return c, nil
}

func (d *definition) encodeOutputs(outputs []pulse.CheckpointOutput) ([]ValueRecord, error) {
	out := make([]ValueRecord, 0, len(outputs))
	seen := make(map[string]bool, len(outputs))
	for _, o := range outputs {
		if seen[o.Key.Name] {
			return nil, fmt.Errorf("durable: duplicate checkpoint output")
		}
		seen[o.Key.Name] = true
		c, err := d.codec(o.Key)
		if err != nil {
			return nil, err
		}
		v := ValueRecord{Name: o.Key.Name, TypeID: c.id, Version: c.version, State: string(o.State)}
		switch o.State {
		case pulse.SlotStateReady:
			v.Data, err = c.encode(o.Value)
			if err != nil {
				return nil, err
			}
			v.Digest = digest(v.Data)
		case pulse.SlotStateSkipped:
			if o.Value != nil {
				return nil, fmt.Errorf("durable: skipped output contains a value")
			}
		default:
			return nil, fmt.Errorf("durable: checkpoint output has invalid state")
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func (d *definition) inputsFingerprint(loc location, inputs []pulse.CheckpointOutput) (string, error) {
	s, ok := d.graphs[loc]
	if !ok {
		return "", fmt.Errorf("durable: unknown input scope")
	}
	for _, in := range inputs {
		k, ok := s.inputs[in.Key.Name]
		if !ok || k.Type != in.Key.Type {
			return "", fmt.Errorf("durable: invalid input declaration")
		}
	}
	values, err := d.encodeOutputs(inputs)
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(values)
	if err != nil {
		return "", fmt.Errorf("durable: input fingerprint failed")
	}
	return digest(data), nil
}

func (d *definition) decodeNode(loc location, node NodeRecord) (pulse.NodeCheckpoint, error) {
	s, ok := d.graphs[loc]
	if !ok {
		return pulse.NodeCheckpoint{}, fmt.Errorf("durable: unknown record scope")
	}
	n, ok := s.nodes[node.NodeID]
	if !ok || (node.Reason != string(pulse.NodeCompleted) && node.Reason != string(pulse.NodeSkipped)) || len(node.Outputs) != len(n.Provides) {
		return pulse.NodeCheckpoint{}, fmt.Errorf("durable: invalid complete node record")
	}
	keys := make(map[string]pulse.CheckpointKey, len(n.Provides))
	for _, k := range n.Provides {
		keys[k.Name] = k
	}
	out := pulse.NodeCheckpoint{NodeID: node.NodeID, Reason: pulse.NodeFinishReason(node.Reason)}
	for _, v := range node.Outputs {
		k, ok := keys[v.Name]
		if !ok {
			return pulse.NodeCheckpoint{}, fmt.Errorf("durable: unknown or duplicate output record")
		}
		delete(keys, v.Name)
		c, err := d.codec(k)
		if err != nil {
			return pulse.NodeCheckpoint{}, err
		}
		if v.TypeID != c.id || v.Version != c.version {
			return pulse.NodeCheckpoint{}, fmt.Errorf("durable: output codec mismatch")
		}
		o := pulse.CheckpointOutput{Key: k, State: pulse.SlotState(v.State)}
		switch o.State {
		case pulse.SlotStateReady:
			if !validDigest(v.Digest) || digest(v.Data) != v.Digest {
				return pulse.NodeCheckpoint{}, fmt.Errorf("durable: encoded output checksum mismatch")
			}
			o.Value, err = c.decode(v.Data)
			if err != nil {
				return pulse.NodeCheckpoint{}, err
			}
		case pulse.SlotStateSkipped:
			if len(v.Data) != 0 || v.Digest != "" {
				return pulse.NodeCheckpoint{}, fmt.Errorf("durable: skipped record contains encoded data")
			}
		default:
			return pulse.NodeCheckpoint{}, fmt.Errorf("durable: invalid output record state")
		}
		out.Outputs = append(out.Outputs, o)
	}
	return out, nil
}

func digest(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func validDigest(s string) bool {
	decoded, err := hex.DecodeString(s)
	return err == nil && len(decoded) == sha256.Size
}
