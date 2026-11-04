package distributed

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)

// Assembly 是本地代码，绝不序列化。Keys 注册带类型的根输入键。
// Factory 必须只组装新的图、编解码器和键，不得执行 I/O 或产生业务副作用。
// Factory 和编解码器可能在验证期间运行，但此处不会调用任何节点，
// 也不会调用宿主的 BeforeStart 回调。函数和切面由宿主管理版本。
type Assembly struct {
	Graph  *pulse.Graph
	Codecs *durable.Codecs
	Keys   *pulse.Registry
}
type Factory func() (Assembly, error)

// Registry 支持并发安全的注册和查找。条目必须精确匹配；
// 不会回退到最新版本，也不会自动加载代码。
type Registry struct {
	mu      sync.RWMutex
	entries map[Route]Factory
}

func assemble(f Factory) (a Assembly, err error) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			a = Assembly{}
			err = ErrInvalid
		}
	}()
	if f == nil {
		return Assembly{}, ErrInvalid
	}
	a, err = f()
	returned = true
	if err != nil || a.Graph == nil || a.Codecs == nil || a.Keys == nil {
		return Assembly{}, ErrInvalid
	}
	return a, nil
}
func routeFor(taskType, version string, a Assembly) (Route, error) {
	if !text(taskType) || !text(version) {
		return Route{}, ErrInvalid
	}
	i, err := durable.Inspect(a.Graph, a.Codecs)
	if err != nil {
		return Route{}, ErrInvalid
	}
	m, _ := json.Marshal(i.CodecManifest)
	return Route{taskType, version, i.Definition, hash(m), Protocol, durable.RecordFormat, PayloadFormat}, nil
}

// Register 调用一次纯组装过程以推导其能力。完全相同的路由重复注册会报错
// （即使使用同一个 Factory 也是如此）。不同的显式版本可以共存。
func (r *Registry) Register(taskType, version string, f Factory) (Route, error) {
	if r == nil {
		return Route{}, ErrInvalid
	}
	a, err := assemble(f)
	if err != nil {
		return Route{}, err
	}
	route, err := routeFor(taskType, version, a)
	if err != nil {
		return Route{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.entries[route]; ok {
		return Route{}, ErrDuplicate
	}
	if r.entries == nil {
		r.entries = make(map[Route]Factory)
	}
	r.entries[route] = f
	return route, nil
}
func (r *Registry) Capabilities() []Route {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Route, 0, len(r.entries))
	for route := range r.entries {
		out = append(out, route)
	}
	return out
}

// Resolve 先验证规范字节，再验证精确路由以及新组装的定义和清单，
// 然后解码并重新计算输入指纹。它只返回通过验证的图和输入。
// 后续 Runtime 的已认领入口仍必须验证所有已存储的 RunRecord 引用及格式，
// 并且必须在执行前重新确认所有权。
// Resolve 既不确认租约有效，也不启动执行。
func (r *Registry) Resolve(ctx context.Context, t Task) (Assembly, pulse.Inputs, error) {
	var in pulse.Inputs
	validated, err := ParseTask(t.Bytes())
	if err != nil {
		return Assembly{}, in, err
	}
	if r == nil {
		return Assembly{}, in, ErrMismatch
	}
	route := validated.Route()
	r.mu.RLock()
	f, ok := r.entries[route]
	r.mu.RUnlock()
	if !ok {
		return Assembly{}, in, ErrMismatch
	}
	a, err := assemble(f)
	if err != nil {
		return Assembly{}, in, err
	}
	local, err := routeFor(route.TaskType, route.BusinessVersion, a)
	if err != nil || local != route {
		return Assembly{}, in, ErrMismatch
	}
	in, err = durable.DecodeInputs(ctx, a.Graph, a.Codecs, a.Keys, validated.Payload())
	if err != nil {
		return Assembly{}, pulse.Inputs{}, ErrInvalid
	}
	p, err := durable.Prepare(ctx, a.Graph, in, a.Codecs)
	if err != nil || p.Inputs != validated.InputsFingerprint() {
		return Assembly{}, pulse.Inputs{}, ErrMismatch
	}
	return a, in, nil
}
