package durable

import (
	"sync/atomic"
	"testing"

	"github.com/Luo-root/pulse"
)

func childChain(t *testing.T, calls *[3]atomic.Int32) (*pulse.Graph, pulse.Key[int]) {
	t.Helper()
	child := testGraph(t, "child", pulse.WithoutAspects())
	one, two, three := pulse.NewKey[int]("one"), pulse.NewKey[int]("two"), pulse.NewKey[int]("three")
	testAdd(t, child, pulse.NewNode("C1", "", nil, pulse.Provides(one), func(rc *pulse.RunCtx) error { calls[0].Add(1); return pulse.Set(rc, one, 1) }))
	testAdd(t, child, pulse.NewNode("C2", "", pulse.Requires(one), pulse.Provides(two), func(rc *pulse.RunCtx) error { calls[1].Add(1); return pulse.Set(rc, two, 2) }))
	testAdd(t, child, pulse.NewNode("C3", "", pulse.Requires(two), pulse.Provides(three), func(rc *pulse.RunCtx) error { calls[2].Add(1); return pulse.Set(rc, three, 3) }))
	if err := child.DeclareOutput(pulse.NewPort(three)); err != nil {
		t.Fatal(err)
	}
	return child, three
}
