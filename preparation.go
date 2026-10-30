package durable

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/Luo-root/pulse"
)

// CodecBinding 标识每个图键，包括托管流元素编解码器。
// Graph/Path 区分不同子图中的同名键。
type CodecBinding struct {
	Graph   string `json:"graph"`
	Path    string `json:"path"`
	Name    string `json:"name"`
	TypeID  string `json:"type_id"`
	Version string `json:"version"`
	Stream  bool   `json:"stream"`
}

// Inspection 与 Run/Resume 使用相同的定义指纹。它不标识
// 业务函数体；调用方仍须为业务代码管理版本。
type Inspection struct {
	Definition    string
	CodecManifest []CodecBinding
}

// Preparation 包含经 Run/Resume 编解码器编码的有限实际输入。
// Payload 是排序后的 ValueRecords 的规范 JSON（包括显式跳过项）。
// 不启动执行，也不调用存储或 BeforeStart 回调。
type Preparation struct {
	Inspection
	Inputs  string
	Payload []byte
}

// Inspect 对编解码器取快照并验证图声明，不执行节点。
func Inspect(g *pulse.Graph, codecs *Codecs) (out Inspection, err error) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			out = Inspection{}
			err = fmt.Errorf("durable: inspection panicked")
		}
	}()
	out, err = inspectForPreparation(g, codecs)
	returned = true
	return
}

func inspectForPreparation(g *pulse.Graph, codecs *Codecs) (Inspection, error) {
	if codecs == nil {
		return Inspection{}, fmt.Errorf("durable: codecs are required")
	}
	d, err := inspectDefinition(g, codecs.snapshot())
	if err != nil {
		return Inspection{}, err
	}
	bindings := make(map[CodecBinding]bool)
	for loc, s := range d.graphs {
		add := func(k pulse.CheckpointKey) error {
			c, err := d.codec(k)
			if err != nil {
				return err
			}
			bindings[CodecBinding{loc.graph, loc.path, k.Name, c.id, c.version, k.Element != nil}] = true
			return nil
		}
		for _, n := range s.nodes {
			for _, keys := range [][]pulse.CheckpointKey{n.Requires, n.Provides} {
				for _, k := range keys {
					if err := add(k); err != nil {
						return Inspection{}, err
					}
				}
			}
		}
		for _, keys := range []map[string]pulse.CheckpointKey{s.inputs, s.outputs} {
			for _, k := range keys {
				if err := add(k); err != nil {
					return Inspection{}, err
				}
			}
		}
	}
	out := Inspection{Definition: d.fingerprint, CodecManifest: make([]CodecBinding, 0, len(bindings))}
	for b := range bindings {
		out.CodecManifest = append(out.CodecManifest, b)
	}
	sort.Slice(out.CodecManifest, func(i, j int) bool {
		a, _ := json.Marshal(out.CodecManifest[i])
		b, _ := json.Marshal(out.CodecManifest[j])
		return bytes.Compare(a, b) < 0
	})
	return out, nil
}

// Prepare 使用待启动的 Pulse Execution 验证 Inputs，随后取消它，
// 不调用 Start。编解码器实现必须确定且无业务副作用。
// 拒绝直接外部流输入；图内托管流仍是定义的一部分，
// 并使用现有 Runtime 流恢复契约。
func Prepare(ctx context.Context, g *pulse.Graph, in pulse.Inputs, codecs *Codecs) (out Preparation, err error) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			out = Preparation{}
			err = fmt.Errorf("durable: preparation panicked")
		}
	}()
	out, err = prepare(ctx, g, in, codecs)
	returned = true
	return
}

func prepare(ctx context.Context, g *pulse.Graph, in pulse.Inputs, codecs *Codecs) (Preparation, error) {
	if err := checkContext(ctx); err != nil {
		return Preparation{}, err
	}
	if codecs == nil {
		return Preparation{}, fmt.Errorf("durable: codecs are required")
	}
	// 检查、编码及指纹计算使用同一不可变编解码器快照。
	snapshot := &Codecs{byType: codecs.snapshot()}
	inspection, err := inspectForPreparation(g, snapshot)
	if err != nil {
		return Preparation{}, err
	}
	d, err := inspectDefinition(g, snapshot.snapshot())
	if err != nil {
		return Preparation{}, err
	}
	e, err := g.NewExecution(ctx, in)
	if err != nil {
		return Preparation{}, err
	}
	defer e.Cancel()
	values := e.CheckpointInputs()
	for _, v := range values {
		if v.Key.Element != nil || v.Stream != nil {
			return Preparation{}, fmt.Errorf("durable: external stream input is not portable")
		}
	}
	fingerprint, err := d.inputsFingerprint(d.root, values)
	if err != nil {
		return Preparation{}, err
	}
	encoded, err := d.encodeOutputs(values)
	if err != nil {
		return Preparation{}, err
	}
	payload, err := json.Marshal(encoded)
	if err != nil {
		return Preparation{}, fmt.Errorf("durable: input envelope encoding failed")
	}
	// 拒绝非确定性编解码器，而非附加不匹配的指纹。
	if digest(payload) != fingerprint {
		return Preparation{}, fmt.Errorf("durable: non-deterministic input encoding")
	}
	return Preparation{Inspection: inspection, Inputs: fingerprint, Payload: payload}, nil
}

// DecodeInputs 在新进程中使用已注册类型化键重建 Inputs。
// keys 必须包含所有根输入键（pulse.RegisterKey）。Payload 必须是
// Prepare 生成的精确规范字节。调用方须在调用前限制长度；
// distributed 线格式固定此上限。不启动任何业务代码。
func DecodeInputs(ctx context.Context, g *pulse.Graph, codecs *Codecs, keys *pulse.Registry, payload []byte) (in pulse.Inputs, err error) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			in = pulse.Inputs{}
			err = fmt.Errorf("durable: input decoding panicked")
		}
	}()
	in, err = decodeInputs(ctx, g, codecs, keys, payload)
	returned = true
	return
}

func decodeInputs(ctx context.Context, g *pulse.Graph, codecs *Codecs, keys *pulse.Registry, payload []byte) (pulse.Inputs, error) {
	var in pulse.Inputs
	if err := checkContext(ctx); err != nil {
		return in, err
	}
	if codecs == nil || keys == nil {
		return in, fmt.Errorf("durable: codecs and input key registry are required")
	}
	snapshot := &Codecs{byType: codecs.snapshot()}
	d, err := inspectDefinition(g, snapshot.snapshot())
	if err != nil {
		return in, err
	}
	var values []ValueRecord
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&values); err != nil {
		return in, fmt.Errorf("durable: invalid input envelope")
	}
	canonical, err := json.Marshal(values)
	if err != nil || !bytes.Equal(canonical, payload) {
		return in, fmt.Errorf("durable: non-canonical input envelope")
	}
	previous := ""
	for i, v := range values {
		if (i > 0 && v.Name <= previous) || v.Stream != nil {
			return pulse.Inputs{}, fmt.Errorf("durable: invalid input ordering or stream")
		}
		previous = v.Name
		k, ok := d.graphs[d.root].inputs[v.Name]
		if !ok || k.Element != nil {
			return pulse.Inputs{}, fmt.Errorf("durable: unknown or stream input")
		}
		o, err := d.decodeValue(k, v)
		if err != nil {
			return pulse.Inputs{}, err
		}
		if o.State == pulse.SlotStateSkipped {
			err = pulse.SkipSeedByName(&in, keys, k.Name, k.Type.String())
		} else {
			err = pulse.SeedByName(&in, keys, k.Name, k.Type.String(), o.Value)
		}
		if err != nil {
			return pulse.Inputs{}, fmt.Errorf("durable: input key reconstruction failed")
		}
	}
	prepared, err := prepare(ctx, g, in, snapshot)
	if err != nil {
		return pulse.Inputs{}, err
	}
	if !bytes.Equal(prepared.Payload, payload) {
		return pulse.Inputs{}, fmt.Errorf("durable: input round-trip mismatch")
	}
	return in, nil
}
