package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
)

type deltaCoordinator struct {
	*fake
	mu                sync.Mutex
	batches           []durable.ChangeBatch
	mutations         []d.Mutation
	seed              *durable.RunRecord
	applyCalls        atomic.Int32
	applied           chan struct{}
	injectedErr       error
	panicApply        bool
	wrongAck          bool
	commitThenErr     error
	commitThenPanic   bool
	blockApply        chan struct{}
	enteredApply      chan struct{}
	ignoreApplyCancel bool
	staleRejects      atomic.Int32
}

func (f *deltaCoordinator) SaveOwned(ctx context.Context, req d.SaveRequest) error {
	err := f.fake.SaveOwned(ctx, req)
	if err == nil && req.ExpectedRevision == 0 {
		f.mu.Lock()
		if f.seed == nil {
			r := copyRecord(req.Record)
			f.seed = &r
		}
		f.mu.Unlock()
	}
	return err
}

func (f *deltaCoordinator) ApplyOwned(ctx context.Context, token d.Token, m d.Mutation, batch durable.ChangeBatch) (durable.CommitAck, error) {
	f.applyCalls.Add(1)
	if f.blockApply != nil {
		select {
		case f.enteredApply <- struct{}{}:
		default:
		}
		if f.ignoreApplyCancel {
			<-f.blockApply
		} else {
			select {
			case <-f.blockApply:
			case <-ctx.Done():
				return durable.CommitAck{}, ctx.Err()
			}
		}
	}
	wire, err := durable.MarshalChangeBatch(batch)
	if err != nil {
		return durable.CommitAck{}, err
	}
	decoded, err := durable.UnmarshalChangeBatch(wire)
	if err != nil {
		return durable.CommitAck{}, err
	}
	if decoded.Header() != batch.Header() {
		return durable.CommitAck{}, errors.New("wire header changed")
	}
	f.mu.Lock()
	fault := f.injectedErr
	panicApply := f.panicApply
	f.mu.Unlock()
	if panicApply {
		panic("delta backend panic")
	}
	if fault != nil {
		return durable.CommitAck{}, fault
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if token != f.token {
		f.staleRejects.Add(1)
		return durable.CommitAck{}, d.ErrLeaseLost
	}
	if f.record == nil || batch.Header().RunID != f.record.RunID || batch.Header().BusinessVersion != f.record.Version || batch.Header().DefinitionDigest != f.record.Definition || batch.Header().InputsDigest != f.record.Inputs || batch.Header().ExpectedRevision != f.record.Revision || batch.Header().Revision != f.record.Revision+1 {
		return durable.CommitAck{}, d.ErrRevisionConflict
	}
	semantics, _ := json.Marshal(struct {
		Operation string
		Token     d.Token
		Batch     []byte
	}{"apply_owned", token, wire})
	h := sha256.Sum256(semantics)
	if m.Digest != hex.EncodeToString(h[:]) || m.ID == "" {
		return durable.CommitAck{}, errors.New("bad mutation digest")
	}
	ack, err := decoded.ApplyTo(f.record)
	if err != nil {
		return durable.CommitAck{}, err
	}
	f.batches = append(f.batches, decoded)
	f.mutations = append(f.mutations, m)
	if f.applied != nil {
		select {
		case f.applied <- struct{}{}:
		default:
		}
	}
	if f.commitThenPanic {
		panic("delta commit then panic")
	}
	if f.commitThenErr != nil {
		return durable.CommitAck{}, f.commitThenErr
	}
	if f.wrongAck {
		ack.Header.Revision++
	}
	return ack, nil
}

func (f *deltaCoordinator) saved() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.batches) }

func deltaConfig(c d.Coordinator, r *d.Registry, require bool) Config {
	return Config{Coordinator: c, Renewer: c, Registry: r, Namespace: "ns", WorkerID: "one", Capacity: 1, BatchMax: 1, TTL: time.Second, RenewInterval: 20 * time.Millisecond, RenewTimeout: 50 * time.Millisecond, OperationTimeout: 300 * time.Millisecond, PollMin: time.Millisecond, PollMax: 2 * time.Millisecond, RequireDelta: require}
}

func TestRequireDeltaRejectsUnsupportedAndTypedNilBeforeClaim(t *testing.T) {
	w, f := fixture(t, func(*pulse.RunCtx) error { return nil })
	if _, err := New(deltaConfig(f, w.cfg.Registry, true)); !errors.Is(err, durable.ErrUnsupportedCapability) {
		t.Fatalf("unsupported=%v", err)
	}
	var typed *deltaCoordinator
	if _, err := New(deltaConfig(typed, w.cfg.Registry, true)); !errors.Is(err, durable.ErrUnsupportedCapability) {
		t.Fatalf("typed nil=%v", err)
	}
	var nilMap nilDeltaMap
	if _, err := New(deltaConfig(nilMap, w.cfg.Registry, true)); !errors.Is(err, durable.ErrUnsupportedCapability) {
		t.Fatalf("map nil=%v", err)
	}
	var nilFunc nilDeltaFunc
	if _, err := New(deltaConfig(nilFunc, w.cfg.Registry, true)); !errors.Is(err, durable.ErrUnsupportedCapability) {
		t.Fatalf("func nil=%v", err)
	}
	f.mu.Lock()
	claims := f.claims
	f.mu.Unlock()
	if claims != 0 {
		t.Fatal("capability rejection claimed work")
	}
}

func deltaWorkflow(t *testing.T, fn func(*pulse.RunCtx) error) (*Worker, *deltaCoordinator) {
	t.Helper()
	factory := func() (d.Assembly, error) {
		g, err := pulse.New("delta-worker", "test")
		if err != nil {
			return d.Assembly{}, err
		}
		key := pulse.NewKey[pulse.Stream[int]]("numbers")
		if err = pulse.ProduceStream(g, "source", "", key, func(_ *pulse.RunCtx, w *pulse.StreamWriter[int]) error {
			if w.Position().Sequence == 0 {
				if err := w.SendAt(1, 1); err != nil {
					return err
				}
			}
			return nil
		}, pulse.WithBuffer(1), pulse.WithStreamSource("delta/source", true)); err != nil {
			return d.Assembly{}, err
		}
		if err = pulse.ConsumeStream(g, "sink", "", key, func(rc *pulse.RunCtx, _ int) error { return fn(rc) }, pulse.WithMessageHandling(pulse.WithMessageRetry(1, 0))); err != nil {
			return d.Assembly{}, err
		}
		return d.Assembly{Graph: g, Codecs: durable.NewCodecs(), Keys: pulse.NewRegistry()}, nil
	}
	reg := &d.Registry{}
	route, err := reg.Register("task", "v1", factory)
	if err != nil {
		t.Fatal(err)
	}
	a, err := factory()
	if err != nil {
		t.Fatal(err)
	}
	task, err := d.NewTask(context.Background(), d.Identity{Namespace: "ns", RunID: "delta-run"}, "task", "v1", a.Graph, pulse.Inputs{}, a.Codecs)
	if err != nil {
		t.Fatal(err)
	}
	f := &fake{task: task, finish: make(chan d.FinishRequest, 2), block: make(chan string, 2)}
	_ = route
	dc := &deltaCoordinator{fake: f}
	w, err := New(deltaConfig(dc, reg, true))
	if err != nil {
		t.Fatal(err)
	}
	return w, dc
}

func TestDeltaRuntimeUsesWireAuthoritySeedAndFinish(t *testing.T) {
	var calls atomic.Int32
	w, dc := deltaWorkflow(t, func(*pulse.RunCtx) error { calls.Add(1); return nil })
	cancel, _ := runWorker(t, w)
	defer cancel()
	select {
	case <-dc.finish:
	case <-time.After(3 * time.Second):
		dc.mu.Lock()
		t.Fatalf("finish timeout calls=%d claims=%d record=%+v batches=%d mutations=%d", calls.Load(), dc.claims, dc.record, len(dc.batches), len(dc.mutations))
		dc.mu.Unlock()
	}
	if calls.Load() == 0 || dc.saved() == 0 {
		t.Fatalf("calls=%d delta=%d", calls.Load(), dc.saved())
	}
	dc.mu.Lock()
	batches := append([]durable.ChangeBatch(nil), dc.batches...)
	muts := append([]d.Mutation(nil), dc.mutations...)
	dc.mu.Unlock()
	if len(batches) == 0 || len(muts) != len(batches) {
		t.Fatal("missing delta records")
	}
	cancel()
	if dc.releases != 0 {
		t.Fatal("successful delta run released")
	}
}

func TestLegacyDefaultStillFullSave(t *testing.T) {
	w, f := fixture(t, func(*pulse.RunCtx) error { return nil })
	if _, err := New(deltaConfig(f, w.cfg.Registry, false)); err != nil {
		t.Fatal(err)
	}
	runWorker(t, w)
	await(t, f.finish)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.record == nil {
		t.Fatal("legacy path did not seed with Save")
	}
}

func TestDeltaSessionDigestFenceAndFailures(t *testing.T) {
	w, dc := deltaWorkflow(t, func(*pulse.RunCtx) error { return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &session{c: dc, token: d.Token{Identity: dc.task.Identity(), Worker: d.Worker{ID: "one", BootID: "boot"}, LeaseID: "lease", Epoch: 1}, timeout: time.Second, ctx: ctx, cancel: cancel}
	// Obtain a real batch and authoritative seed through the Runtime path.
	cancelRun, _ := runWorker(t, w)
	await(t, dc.finish)
	cancelRun()
	dc.mu.Lock()
	b := dc.batches[0]
	dc.mu.Unlock()
	ds := &deltaSession{session: s, coordinator: dc}
	if _, err := ds.Apply(context.Background(), b); !errors.Is(err, d.ErrLeaseLost) {
		t.Fatalf("stale owner=%v", err)
	}
	if dc.staleRejects.Load() == 0 {
		t.Fatal("stale owner not checked")
	}
}

func TestDeltaRuntimeFaultsCommitThenSeal(t *testing.T) {
	for _, tc := range []struct {
		name        string
		wrongAck    bool
		injectedErr error
		commitPanic bool
	}{
		{name: "wrong_ack", wrongAck: true},
		{name: "unknown", injectedErr: d.ErrUnknown},
		{name: "panic", commitPanic: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, dc := deltaWorkflow(t, func(*pulse.RunCtx) error { return nil })
			dc.mu.Lock()
			dc.wrongAck, dc.commitThenErr, dc.commitThenPanic = tc.wrongAck, tc.injectedErr, tc.commitPanic
			dc.applied = make(chan struct{}, 4)
			dc.mu.Unlock()
			cancel, _ := runWorker(t, w)
			await(t, dc.applied)
			until(t, func() bool { return w.Active() == 0 })
			drainCtx, drainCancel := context.WithTimeout(context.Background(), time.Second)
			if err := w.Drain(drainCtx); err != nil {
				t.Fatal(err)
			}
			drainCancel()
			cancel()
			dc.mu.Lock()
			revision := dc.record.Revision
			batches := len(dc.batches)
			releases := dc.releases
			dc.mu.Unlock()
			if revision < 2 || batches == 0 {
				t.Fatalf("authority did not commit prefix revision=%d batches=%d", revision, batches)
			}
			if got := dc.applyCalls.Load(); got != 1 {
				t.Fatalf("fault path replayed Apply calls=%d", got)
			}
			if releases != 0 {
				t.Fatalf("Release after %s", tc.name)
			}
			select {
			case <-dc.finish:
				t.Fatalf("Finish after %s", tc.name)
			default:
			}

		})
	}
}

func TestDeltaDirectFaultSentinelCommitsThenSeals(t *testing.T) {
	for _, tc := range []struct {
		name        string
		wrongAck    bool
		commitErr   error
		commitPanic bool
		expected    error
	}{
		{name: "wrong_ack", wrongAck: true, expected: durable.ErrConflict},
		{name: "commit_unknown", commitErr: d.ErrUnknown, expected: d.ErrUnknown},
		{name: "commit_panic", commitPanic: true, expected: d.ErrUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, source := deltaWorkflow(t, func(*pulse.RunCtx) error { return nil })
			cancel, _ := runWorker(t, w)
			await(t, source.finish)
			until(t, func() bool { return w.Active() == 0 })
			cancel()
			source.mu.Lock()
			seed := copyRecord(*source.seed)
			batch := source.batches[0]
			token := source.token
			task := source.task
			source.mu.Unlock()
			authority := &fake{task: task, token: token, record: &seed, finish: make(chan d.FinishRequest, 1), block: make(chan string, 1)}
			dc := &deltaCoordinator{fake: authority, wrongAck: tc.wrongAck, commitThenErr: tc.commitErr, commitThenPanic: tc.commitPanic}
			ctx, cancelSession := context.WithCancel(context.Background())
			s := &session{c: dc, token: token, timeout: time.Second, ctx: ctx, cancel: cancelSession}
			ds := &deltaSession{session: s, coordinator: dc}
			_, err := ds.Apply(context.Background(), batch)
			if !errors.Is(err, tc.expected) {
				t.Fatalf("Apply err=%v want=%v", err, tc.expected)
			}
			if authority.record.Revision != batch.Header().Revision {
				t.Fatalf("authority revision=%d want=%d", authority.record.Revision, batch.Header().Revision)
			}
			if !s.uncertain() {
				t.Fatal("fault did not seal session")
			}
			calls := dc.applyCalls.Load()
			if _, err = ds.Apply(context.Background(), batch); !errors.Is(err, d.ErrLeaseLost) {
				t.Fatalf("sealed Apply err=%v", err)
			}
			if dc.applyCalls.Load() != calls {
				t.Fatal("sealed session called backend again")
			}
			if authority.record.Revision != batch.Header().Revision {
				t.Fatal("authority revision regressed")
			}
			cancelSession()
		})
	}
}

func TestDeltaCloseWaitsForBlockedApply(t *testing.T) {
	w, dc := deltaWorkflow(t, func(*pulse.RunCtx) error { return nil })
	cancelRun, _ := runWorker(t, w)
	await(t, dc.finish)
	cancelRun()
	dc.mu.Lock()
	b := dc.batches[0]
	token := dc.token
	dc.mu.Unlock()
	dc.blockApply, dc.enteredApply, dc.ignoreApplyCancel = make(chan struct{}), make(chan struct{}, 1), true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &session{c: dc, token: token, timeout: time.Second, ctx: ctx, cancel: cancel}
	ds := &deltaSession{session: s, coordinator: dc}
	applyDone := make(chan struct{})
	go func() { _, _ = ds.Apply(context.Background(), b); close(applyDone) }()
	await(t, dc.enteredApply)
	closeDone := make(chan struct{})
	go func() { _ = ds.Close(); close(closeDone) }()
	select {
	case <-closeDone:
		t.Fatal("Close did not wait for Apply")
	case <-time.After(20 * time.Millisecond):
	}
	close(dc.blockApply)
	await(t, applyDone)
	await(t, closeDone)
}

type nilDeltaMap map[string]any
type nilDeltaFunc func()

func nilSubmit(context.Context, d.Task) (d.SubmitResult, error) {
	return d.SubmitResult{}, d.ErrInvalid
}
func nilGet(context.Context, d.Identity) (d.Snapshot, error)               { return d.Snapshot{}, d.ErrInvalid }
func nilList(context.Context, d.ListRequest) ([]d.Snapshot, error)         { return nil, d.ErrInvalid }
func nilClaim(context.Context, d.ClaimRequest) ([]d.Claim, error)          { return nil, d.ErrInvalid }
func nilFind(context.Context, string, d.Worker, string) ([]d.Claim, error) { return nil, d.ErrInvalid }
func nilResolve(context.Context, string, d.Worker, string) (d.ClaimResolution, error) {
	return d.ClaimResolution{}, d.ErrInvalid
}
func nilRenew(context.Context, d.Token, time.Duration) (time.Time, error) {
	return time.Time{}, d.ErrInvalid
}
func nilLoad(context.Context, d.Token) (durable.RunRecord, error) {
	return durable.RunRecord{}, d.ErrInvalid
}
func nilSave(context.Context, d.SaveRequest) error { return d.ErrInvalid }
func nilFinish(context.Context, d.FinishRequest) (d.Snapshot, error) {
	return d.Snapshot{}, d.ErrInvalid
}
func nilRelease(context.Context, d.Token, d.Mutation) error       { return d.ErrInvalid }
func nilBlock(context.Context, d.Token, string, d.Mutation) error { return d.ErrInvalid }
func nilCancel(context.Context, d.Identity, d.Mutation) (d.Snapshot, error) {
	return d.Snapshot{}, d.ErrInvalid
}

func (nilDeltaMap) Submit(c context.Context, t d.Task) (d.SubmitResult, error) {
	return nilSubmit(c, t)
}
func (nilDeltaMap) Get(c context.Context, i d.Identity) (d.Snapshot, error) { return nilGet(c, i) }
func (nilDeltaMap) List(c context.Context, r d.ListRequest) ([]d.Snapshot, error) {
	return nilList(c, r)
}
func (nilDeltaMap) Claim(c context.Context, r d.ClaimRequest) ([]d.Claim, error) {
	return nilClaim(c, r)
}
func (nilDeltaMap) FindClaim(c context.Context, n string, w d.Worker, i string) ([]d.Claim, error) {
	return nilFind(c, n, w, i)
}
func (nilDeltaMap) ResolveClaim(c context.Context, n string, w d.Worker, i string) (d.ClaimResolution, error) {
	return nilResolve(c, n, w, i)
}
func (nilDeltaMap) Renew(c context.Context, t d.Token, ttl time.Duration) (time.Time, error) {
	return nilRenew(c, t, ttl)
}
func (nilDeltaMap) LoadOwned(c context.Context, t d.Token) (durable.RunRecord, error) {
	return nilLoad(c, t)
}
func (nilDeltaMap) SaveOwned(c context.Context, r d.SaveRequest) error { return nilSave(c, r) }
func (nilDeltaMap) Finish(c context.Context, r d.FinishRequest) (d.Snapshot, error) {
	return nilFinish(c, r)
}
func (nilDeltaMap) Release(c context.Context, t d.Token, m d.Mutation) error {
	return nilRelease(c, t, m)
}
func (nilDeltaMap) Block(c context.Context, t d.Token, s string, m d.Mutation) error {
	return nilBlock(c, t, s, m)
}
func (nilDeltaMap) Cancel(c context.Context, i d.Identity, m d.Mutation) (d.Snapshot, error) {
	return nilCancel(c, i, m)
}
func (nilDeltaMap) ApplyOwned(context.Context, d.Token, d.Mutation, durable.ChangeBatch) (durable.CommitAck, error) {
	return durable.CommitAck{}, d.ErrInvalid
}

func (nilDeltaFunc) Submit(c context.Context, t d.Task) (d.SubmitResult, error) {
	return nilSubmit(c, t)
}
func (nilDeltaFunc) Get(c context.Context, i d.Identity) (d.Snapshot, error) { return nilGet(c, i) }
func (nilDeltaFunc) List(c context.Context, r d.ListRequest) ([]d.Snapshot, error) {
	return nilList(c, r)
}
func (nilDeltaFunc) Claim(c context.Context, r d.ClaimRequest) ([]d.Claim, error) {
	return nilClaim(c, r)
}
func (nilDeltaFunc) FindClaim(c context.Context, n string, w d.Worker, i string) ([]d.Claim, error) {
	return nilFind(c, n, w, i)
}
func (nilDeltaFunc) ResolveClaim(c context.Context, n string, w d.Worker, i string) (d.ClaimResolution, error) {
	return nilResolve(c, n, w, i)
}
func (nilDeltaFunc) Renew(c context.Context, t d.Token, ttl time.Duration) (time.Time, error) {
	return nilRenew(c, t, ttl)
}
func (nilDeltaFunc) LoadOwned(c context.Context, t d.Token) (durable.RunRecord, error) {
	return nilLoad(c, t)
}
func (nilDeltaFunc) SaveOwned(c context.Context, r d.SaveRequest) error { return nilSave(c, r) }
func (nilDeltaFunc) Finish(c context.Context, r d.FinishRequest) (d.Snapshot, error) {
	return nilFinish(c, r)
}
func (nilDeltaFunc) Release(c context.Context, t d.Token, m d.Mutation) error {
	return nilRelease(c, t, m)
}
func (nilDeltaFunc) Block(c context.Context, t d.Token, s string, m d.Mutation) error {
	return nilBlock(c, t, s, m)
}
func (nilDeltaFunc) Cancel(c context.Context, i d.Identity, m d.Mutation) (d.Snapshot, error) {
	return nilCancel(c, i, m)
}
func (nilDeltaFunc) ApplyOwned(context.Context, d.Token, d.Mutation, durable.ChangeBatch) (durable.CommitAck, error) {
	return durable.CommitAck{}, d.ErrInvalid
}
