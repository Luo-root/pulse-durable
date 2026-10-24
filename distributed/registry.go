package distributed

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)

// Assembly is local code, never serialized. Keys registers typed root input keys.
// Factory must only assemble fresh graphs, codecs and keys, without I/O or business
// side effects. It and codecs may run during validation, but no node or host
// BeforeStart callback is invoked here. Functions/aspects are versioned by host.
type Assembly struct {
	Graph  *pulse.Graph
	Codecs *durable.Codecs
	Keys   *pulse.Registry
}
type Factory func() (Assembly, error)

// Registry is safe for concurrent registration and lookup. Entries are exact;
// no latest-version fallback or automatic code loading is performed.
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

// Register invokes a pure assembly once to derive its capability. Repeated exact
// routes are errors (even the same Factory). Different explicit versions coexist.
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

// Resolve validates canonical bytes first, then exact route and freshly assembled
// definition/manifest, then decodes and recomputes input fingerprints. It returns
// only validated graph/inputs. The later Runtime claimed entry MUST still validate
// all stored RunRecord references and formats and ownership must be re-confirmed
// before execution. Resolve neither asserts a lease nor starts an execution.
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
