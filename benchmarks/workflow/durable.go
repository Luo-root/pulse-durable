package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/filestore"
)

const runID = "scale"

type durableRunner struct {
	store       durable.Store
	runtime     *durable.Runtime
	graph       *pulse.Graph
	output      pulse.Key[uint64]
	parallelism int
	meter       *meterStore
}

func prepareDurable(ctx context.Context, c config, dir string, a *audit, crash int) (runner, error) {
	var store durable.Store = durable.NewMemoryStore()
	if c.Backend == "file" {
		var err error
		store, err = filestore.New(dir)
		if err != nil {
			return nil, err
		}
	}
	s := &durableRunner{store: store, parallelism: c.Parallelism}
	if c.Mode == "growth" || crash > 0 {
		s.meter = &meterStore{Store: store, crash: crash, marker: filepath.Join(dir, "confirmed.json")}
		if c.Mode == "growth" {
			s.meter.budget = c.MaxWriteBytes
		}
		store = s.meter
	}
	var err error
	s.runtime, err = durable.New(store, durable.NewCodecs())
	if err != nil {
		return nil, err
	}
	s.graph, err = pulse.New("chain", "one node per business step")
	if err != nil {
		return nil, err
	}
	keys := make([]pulse.Key[uint64], c.Nodes)
	for i := range keys {
		keys[i] = pulse.NewKey[uint64](fmt.Sprintf("v%d", i))
	}
	for i := range keys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		requires := pulse.Requires[uint64]()
		if i > 0 {
			requires = pulse.Requires(keys[i-1])
		}
		n := pulse.NewNode(fmt.Sprintf("n%d", i), "compute", requires, pulse.Provides(keys[i]), func(rc *pulse.RunCtx) error {
			leave, err := a.enter(i)
			if err != nil {
				return err
			}
			defer leave()
			input := uint64(1)
			if i > 0 {
				v, err := pulse.Get(rc, keys[i-1])
				if err != nil {
					return err
				}
				input += v
			}
			return pulse.Set(rc, keys[i], compute(input, i, c.Rounds))
		})
		if err := s.graph.Add(n); err != nil {
			return nil, err
		}
	}
	s.output = keys[len(keys)-1]
	if err := s.graph.DeclareOutput(pulse.NewPort(s.output)); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *durableRunner) run(ctx context.Context, resume bool) (uint64, error) {
	opts := durable.RunOptions{RunID: runID, Version: "compute/1", ExecOptions: []pulse.ExecOption{pulse.WithExecMaxRunning(s.parallelism)}}
	var e *pulse.Execution
	var err error
	if resume {
		e, err = s.runtime.Resume(ctx, s.graph, pulse.Inputs{}, opts)
	} else {
		e, err = s.runtime.Run(ctx, s.graph, pulse.Inputs{}, opts)
	}
	if err != nil {
		return 0, err
	}
	v, state, err := pulse.ReadOutput(e, s.output)
	if err == nil && state != pulse.SlotStateReady {
		err = fmt.Errorf("terminal output state %s", state)
	}
	return v, err
}
func (s *durableRunner) close() error { return nil }
func (s *durableRunner) inspect(ctx context.Context) (int, map[string]any) {
	m := map[string]any{"persistence": "MemoryStore isolated Go records", "node_records": 0}
	if s.meter != nil {
		m["save_calls"], m["successful_saves"], m["logical_json_bytes"] = s.meter.calls, s.meter.successes, s.meter.bytes
	}
	if _, ok := s.store.(*filestore.Store); ok {
		m["persistence"] = "filestore full JSON snapshots; file Sync + atomic replacement; no promised power-loss durability"
	}
	session, err := s.store.Acquire(ctx, runID)
	if err != nil {
		m["inspection_error"] = err.Error()
		return 0, m
	}
	defer session.Close()
	r, err := session.Load(ctx)
	if err != nil {
		m["inspection_error"] = err.Error()
		return 0, m
	}
	nodes := recordNodes(r)
	m["node_records"] = nodes
	if s.meter != nil {
		data, err := json.Marshal(r)
		if err != nil {
			m["inspection_error"] = err.Error()
		} else {
			m["final_record_json_bytes"] = len(data)
		}
		m["metric_scope"] = "sum of JSON encodings passed to successful Save calls; excludes filesystem rereads, temp/rename and physical I/O"
	}
	return nodes, m
}
func recordNodes(r durable.RunRecord) int {
	n := 0
	for _, scope := range r.Scopes {
		n += len(scope.Nodes)
	}
	return n
}

type meterStore struct {
	durable.Store
	mu                              sync.Mutex
	calls, successes, bytes, budget uint64
	crash                           int
	marker                          string
}

func (m *meterStore) Acquire(ctx context.Context, id string) (durable.Session, error) {
	s, err := m.Store.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	return &meterSession{Session: s, m: m}, nil
}

type meterSession struct {
	durable.Session
	m *meterStore
}

func (s *meterSession) Save(ctx context.Context, r durable.RunRecord, revision uint64) error {
	m := s.m
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if m.budget > 0 && uint64(len(data)) > m.budget-m.bytes {
		return errBudget
	}
	if err = s.Session.Save(ctx, r, revision); err != nil {
		return err
	}
	m.successes++
	m.bytes += uint64(len(data))
	if m.crash > 0 && recordNodes(r) == m.crash {
		// Save has returned successfully; publish to downstream has not happened yet.
		if err := writeJSON(m.marker, map[string]any{"confirmed_steps": m.crash, "boundary": "successful Save before downstream publication"}); err != nil {
			return err
		}
		os.Exit(crashExit)
	}
	return nil
}

func writeJSON(path string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	return errors.Join(err, f.Close())
}
