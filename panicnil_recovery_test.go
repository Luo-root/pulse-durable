package durable_test

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/filestore"
)

// TestPanicNilModes 在两个隔离子进程中均选中这些测试。
func requirePanicNilRecoveryChild(t *testing.T) {
	t.Helper()
	if os.Getenv("PULSE_DURABLE_PANICNIL_MODE") == "" {
		t.Skip("covered in isolated default and legacy mode subprocesses")
	}
}

type panicNilFileStore struct {
	durable.Store
	after         bool
	fired         atomic.Bool
	saves         atomic.Int32
	panicSave     atomic.Int32
	panicExpected atomic.Uint64
}

func (s *panicNilFileStore) Acquire(ctx context.Context, id string) (durable.Session, error) {
	session, err := s.Store.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	return &panicNilFileSession{Session: session, owner: s}, nil
}

type panicNilFileSession struct {
	durable.Session
	owner *panicNilFileStore
}

func (s *panicNilFileSession) Apply(ctx context.Context, batch durable.ChangeBatch) (durable.CommitAck, error) {
	record, err := s.Session.Load(ctx)
	if err != nil {
		return durable.CommitAck{}, err
	}
	if _, err := batch.ApplyTo(&record); err != nil {
		return durable.CommitAck{}, err
	}
	expected := batch.Header().ExpectedRevision
	call := s.owner.saves.Add(1)
	if recoveryHasNode(record, "panicnil-file", "work") && s.owner.fired.CompareAndSwap(false, true) {
		s.owner.panicSave.Store(call)
		s.owner.panicExpected.Store(expected)
		if s.owner.after {
			if _, err := s.Session.Apply(ctx, batch); err != nil {
				return durable.CommitAck{}, err
			}
		}
		panic(nil)
	}
	return s.Session.Apply(ctx, batch)
}

func panicNilValueGraph(t *testing.T, body *atomic.Int32) (*pulse.Graph, pulse.Key[int]) {
	t.Helper()
	g := recoveryGraph(t, "panicnil-file")
	key := pulse.NewKey[int]("output")
	recoveryAdd(t, g, pulse.NewNode("work", "", nil, pulse.Provides(key), func(rc *pulse.RunCtx) error {
		body.Add(1)
		return pulse.Set(rc, key, 7)
	}))
	if err := g.DeclareOutput(pulse.NewPort(key)); err != nil {
		t.Fatal(err)
	}
	return g, key
}

func panicNilAssertOutput(t *testing.T, execution *pulse.Execution, key pulse.Key[int], wantReady bool) {
	t.Helper()
	value, state, err := pulse.ReadOutput(execution, key)
	if (wantReady && (err != nil || state != pulse.SlotStateReady || value != 7)) || (!wantReady && (state == pulse.SlotStateReady || value != 0)) {
		t.Fatalf("output=%d/%s err=%v, want ready=%v with value 7", value, state, err, wantReady)
	}
}

func TestPanicNilFileSaveAuthority(t *testing.T) {
	requirePanicNilRecoveryChild(t)
	for _, after := range []bool{false, true} {
		name := "before-save"
		if after {
			name = "after-save"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			base, err := filestore.New(dir)
			if err != nil {
				t.Fatal(err)
			}
			store := &panicNilFileStore{Store: base, after: after}
			var body, downstream atomic.Int32
			g, key := panicNilValueGraph(t, &body)
			recoveryAdd(t, g, pulse.NewNode("downstream", "", pulse.Requires(key), nil, func(*pulse.RunCtx) error {
				downstream.Add(1)
				return nil
			}))
			execution, err := recoveryRuntime(t, store, durable.NewCodecs()).Run(context.Background(), g, pulse.Inputs{}, recoveryOptions("run"))
			checkRecoveryFailure(t, err, nil)
			if execution == nil || execution.Snapshot().Graph.Status != pulse.GraphStatusFailed || !store.fired.Load() || body.Load() != 1 || downstream.Load() != 0 || store.saves.Load() != store.panicSave.Load() {
				t.Fatalf("Save panic published or continued: body=%d downstream=%d saves=%d err=%v", body.Load(), downstream.Load(), store.saves.Load(), err)
			}
			panicNilAssertOutput(t, execution, key, false)
			fresh, err := filestore.New(dir)
			if err != nil {
				t.Fatal(err)
			}
			before := recoveryRecord(t, fresh, "run")
			wantRevision := store.panicExpected.Load()
			if after {
				wantRevision++
			}
			if before.Revision != wantRevision || recoveryHasNode(before, "panicnil-file", "work") != after {
				t.Fatalf("file authority disagrees with injected boundary: revision=%d confirmed=%v", before.Revision, recoveryHasNode(before, "panicnil-file", "work"))
			}
			rt := recoveryRuntime(t, fresh, durable.NewCodecs())
			execution, err = rt.Resume(context.Background(), g, pulse.Inputs{}, recoveryOptions("run"))
			if err != nil {
				t.Fatal(err)
			}
			panicNilAssertOutput(t, execution, key, true)
			wantBody := int32(2)
			if after {
				wantBody = 1
			}
			if body.Load() != wantBody || downstream.Load() != 1 {
				t.Fatalf("Resume ignored file authority: body=%d downstream=%d", body.Load(), downstream.Load())
			}
			confirmed := recoveryRecord(t, fresh, "run")
			if !recoveryHasNode(confirmed, "panicnil-file", "work") || !recoveryHasNode(confirmed, "panicnil-file", "downstream") {
				t.Fatal("Resume output lacks confirmed file records")
			}
			if _, err := rt.Resume(context.Background(), g, pulse.Inputs{}, recoveryOptions("run")); err != nil || body.Load() != wantBody || downstream.Load() != 1 {
				t.Fatalf("completed file record replayed business: %v", err)
			}
		})
	}
}

type panicNilValueCodec struct {
	encodePanic atomic.Bool
	decodePanic atomic.Bool
}

func (*panicNilValueCodec) Type() reflect.Type { return reflect.TypeFor[int]() }
func (*panicNilValueCodec) ID() string         { return "panicnil/int" }
func (*panicNilValueCodec) Version() string    { return "1" }
func (c *panicNilValueCodec) Encode(v any) ([]byte, error) {
	if c.encodePanic.Load() {
		panic(nil)
	}
	return json.Marshal(v)
}
func (c *panicNilValueCodec) Decode(data []byte) (any, error) {
	if c.decodePanic.Load() {
		panic(nil)
	}
	// 空值是零的合法编码。Encode panic 绝不能凭空生成它。
	var value int
	if len(data) == 0 {
		return value, nil
	}
	err := json.Unmarshal(data, &value)
	return value, err
}

func panicNilCodecs(t *testing.T, codec *panicNilValueCodec) *durable.Codecs {
	t.Helper()
	registry := &durable.Codecs{}
	if err := registry.Register(codec); err != nil {
		t.Fatal(err)
	}
	return registry
}

func TestPanicNilFileCodecAuthority(t *testing.T) {
	requirePanicNilRecoveryChild(t)
	for _, failure := range []string{"encode-commit", "decode-commit", "decode-resume"} {
		t.Run(failure, func(t *testing.T) {
			dir := t.TempDir()
			store, err := filestore.New(dir)
			if err != nil {
				t.Fatal(err)
			}
			codec := &panicNilValueCodec{}
			rt := recoveryRuntime(t, store, panicNilCodecs(t, codec))
			var body atomic.Int32
			g, key := panicNilValueGraph(t, &body)
			if failure == "decode-resume" {
				execution, err := rt.Run(context.Background(), g, pulse.Inputs{}, recoveryOptions("run"))
				if err != nil {
					t.Fatal(err)
				}
				panicNilAssertOutput(t, execution, key, true)
			}
			codec.encodePanic.Store(strings.HasPrefix(failure, "encode-"))
			codec.decodePanic.Store(strings.HasPrefix(failure, "decode-"))
			var execution *pulse.Execution
			if failure == "decode-resume" {
				execution, err = rt.Resume(context.Background(), g, pulse.Inputs{}, recoveryOptions("run"))
			} else {
				execution, err = rt.Run(context.Background(), g, pulse.Inputs{}, recoveryOptions("run"))
			}
			checkRecoveryFailure(t, err, nil)
			if execution == nil || body.Load() != 1 || !strings.Contains(err.Error(), "panicked") {
				t.Fatalf("Codec panic was accepted: body=%d err=%v", body.Load(), err)
			}
			panicNilAssertOutput(t, execution, key, false)
			fresh, err := filestore.New(dir)
			if err != nil {
				t.Fatal(err)
			}
			before := recoveryRecord(t, fresh, "run")
			confirmed := failure == "decode-resume"
			wantRevision := uint64(2)
			if confirmed {
				wantRevision = 3
			}
			if before.Revision != wantRevision || recoveryHasNode(before, "panicnil-file", "work") != confirmed {
				t.Fatalf("Codec panic changed file authority: revision=%d confirmed=%v", before.Revision, recoveryHasNode(before, "panicnil-file", "work"))
			}
			rt = recoveryRuntime(t, fresh, panicNilCodecs(t, &panicNilValueCodec{}))
			for range 2 {
				execution, err = rt.Resume(context.Background(), g, pulse.Inputs{}, recoveryOptions("run"))
				if err != nil {
					t.Fatal(err)
				}
				panicNilAssertOutput(t, execution, key, true)
			}
			wantBody := int32(2)
			if confirmed {
				wantBody = 1
			}
			if body.Load() != wantBody {
				t.Fatalf("Codec recovery ignored confirmed work: body=%d want=%d", body.Load(), wantBody)
			}
			after := recoveryRecord(t, fresh, "run")
			outputs := after.Scopes[after.RootScope].Nodes["work"].Outputs
			if after.Revision != 3 || len(outputs) != 1 || string(outputs[0].Data) != "7" {
				t.Fatalf("file value differs from resumed output: revision=%d outputs=%+v", after.Revision, outputs)
			}
		})
	}
}
