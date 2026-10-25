package worker

import (
	"context"
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

// fake exercises local loop interleavings only, not database fencing/atomicity.
type fake struct {
	d.Coordinator
	mu                                      sync.Mutex
	task                                    d.Task
	token                                   d.Token
	record                                  *durable.RunRecord
	claims                                  int
	claimUnknown, saveUnknown, renewUnknown bool
	resolveUnknown, resolveClosed           bool
	finish                                  chan d.FinishRequest
	block                                   chan string
	renewCalled                             chan struct{}
	saveEntered, saveExit                   chan struct{}
	finds                                   []string
	releases                                int
}

func (f *fake) Claim(ctx context.Context, r d.ClaimRequest) ([]d.Claim, error) {
	if _, ok := ctx.Deadline(); !ok {
		panic("unbounded Claim")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims++
	if f.claims > 1 {
		return nil, nil
	}
	if len(r.LeaseIDs) != r.Limit || r.Limit != 1 || len(r.Capabilities) != 1 || r.Capabilities[0] != f.task.Route() {
		panic("bad reservation/route")
	}
	f.token = d.Token{Identity: f.task.Identity(), Worker: r.Worker, LeaseID: r.LeaseIDs[0], Epoch: 1}
	if f.claimUnknown {
		return nil, d.ErrUnknown
	}
	return []d.Claim{{Token: f.token, Snapshot: d.Snapshot{Task: f.task, Status: d.Running}}}, nil
}
func (f *fake) ResolveClaim(ctx context.Context, ns string, w d.Worker, id string) (d.ClaimResolution, error) {
	if _, ok := ctx.Deadline(); !ok {
		panic("unbounded ResolveClaim")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.finds = append(f.finds, id)
	if f.resolveUnknown {
		return d.ClaimResolution{}, d.ErrUnknown
	}
	if f.resolveClosed {
		return d.ClaimResolution{Closed: true}, nil
	}
	return d.ClaimResolution{Claim: &d.Claim{Token: f.token, Snapshot: d.Snapshot{Task: f.task, Status: d.Running}}}, nil
}
func (f *fake) Renew(ctx context.Context, t d.Token, ttl time.Duration) (time.Time, error) {
	if _, ok := ctx.Deadline(); !ok {
		panic("unbounded Renew")
	}
	f.mu.Lock()
	unknown := f.renewUnknown
	f.mu.Unlock()
	if f.renewCalled != nil {
		select {
		case f.renewCalled <- struct{}{}:
		default:
		}
	}
	if unknown {
		return time.Time{}, d.ErrUnknown
	}
	return time.Now().Add(ttl), nil
}
func copyRecord(r durable.RunRecord) durable.RunRecord {
	b, _ := json.Marshal(r)
	var out durable.RunRecord
	_ = json.Unmarshal(b, &out)
	return out
}
func (f *fake) LoadOwned(ctx context.Context, t d.Token) (durable.RunRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.record == nil {
		return durable.RunRecord{}, d.ErrNotFound
	}
	return copyRecord(*f.record), nil
}
func (f *fake) SaveOwned(ctx context.Context, r d.SaveRequest) error {
	if _, ok := ctx.Deadline(); !ok {
		panic("unbounded Save")
	}
	if r.Mutation.ID == "" || r.Mutation.Digest == "" {
		panic("missing mutation")
	}
	if f.saveEntered != nil {
		select {
		case f.saveEntered <- struct{}{}:
		default:
		}
		<-f.saveExit
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	var rev uint64
	if f.record != nil {
		rev = f.record.Revision
	}
	if r.ExpectedRevision != rev {
		return d.ErrRevisionConflict
	}
	rr := copyRecord(r.Record)
	f.record = &rr
	if f.saveUnknown {
		return d.ErrUnknown
	}
	return nil
}
func (f *fake) Finish(ctx context.Context, r d.FinishRequest) (d.Snapshot, error) {
	f.finish <- r
	return d.Snapshot{}, nil
}
func (f *fake) Release(ctx context.Context, t d.Token, m d.Mutation) error {
	f.mu.Lock()
	f.releases++
	f.mu.Unlock()
	return nil
}
func (f *fake) Block(ctx context.Context, t d.Token, s string, m d.Mutation) error {
	switch s {
	case "incompatible", "invalid_payload", "invalid_record", "invalid_task", "unsupported_format":
	default:
		return d.ErrInvalid
	}
	f.block <- s
	return nil
}
func fixture(t *testing.T, fn func(*pulse.RunCtx) error) (*Worker, *fake) {
	t.Helper()
	factory := func() (d.Assembly, error) {
		g, err := pulse.New("work", "test")
		if err != nil {
			return d.Assembly{}, err
		}
		if err = g.Add(pulse.NewNode("node", "", nil, nil, fn)); err != nil {
			return d.Assembly{}, err
		}
		return d.Assembly{Graph: g, Codecs: durable.NewCodecs(), Keys: pulse.NewRegistry()}, nil
	}
	reg := &d.Registry{}
	_, err := reg.Register("task", "v1", factory)
	if err != nil {
		t.Fatal(err)
	}
	a, _ := factory()
	task, err := d.NewTask(context.Background(), d.Identity{Namespace: "ns", RunID: "run"}, "task", "v1", a.Graph, pulse.Inputs{}, a.Codecs)
	if err != nil {
		t.Fatal(err)
	}
	f := &fake{task: task, finish: make(chan d.FinishRequest, 10), block: make(chan string, 10)}
	w, err := New(Config{Coordinator: f, Renewer: f, Registry: reg, Namespace: "ns", WorkerID: "one", Capacity: 1, BatchMax: 1, TTL: time.Second, RenewInterval: 20 * time.Millisecond, RenewTimeout: 50 * time.Millisecond, OperationTimeout: 100 * time.Millisecond, PollMin: time.Millisecond, PollMax: 2 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return w, f
}
func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatal("timed out")
		var z T
		return z
	}
}
func runWorker(t *testing.T, w *Worker) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() { cancel(); await(t, done) })
	return cancel, done
}
func until(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !fn() {
		if time.Now().After(deadline) {
			t.Fatal("condition timed out")
		}
		time.Sleep(time.Millisecond)
	}
}
func TestUnknownClaimOriginalIdentityAndRuntimeFinish(t *testing.T) {
	var calls atomic.Int32
	w, f := fixture(t, func(*pulse.RunCtx) error { calls.Add(1); return nil })
	f.claimUnknown = true
	runWorker(t, w)
	req := await(t, f.finish)
	if req.Outcome != durable.OutcomeCompleted || req.ExpectedRevision == 0 || calls.Load() != 1 {
		t.Fatalf("invalid finish: %+v calls=%d", req, calls.Load())
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.finds) != 1 || f.finds[0] != f.token.LeaseID {
		t.Fatal("claim identity replayed")
	}
}
func TestSaveUnknownNeverFinishesOrReleases(t *testing.T) {
	w, f := fixture(t, func(*pulse.RunCtx) error { return nil })
	f.saveUnknown = true
	runWorker(t, w)
	until(t, func() bool { f.mu.Lock(); defer f.mu.Unlock(); return f.record != nil })
	until(t, func() bool { return w.Active() == 0 })
	select {
	case <-f.finish:
		t.Fatal("fabricated terminal")
	default:
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.releases != 0 {
		t.Fatal("released uncertain save")
	}
}
func TestRenewUnknownRetainsExecutionAndCapacityUntilQuiescent(t *testing.T) {
	entered := make(chan struct{})
	exit := make(chan struct{})
	var calls atomic.Int32
	w, f := fixture(t, func(*pulse.RunCtx) error { calls.Add(1); close(entered); <-exit; return nil })
	f.renewCalled = make(chan struct{}, 20)
	runWorker(t, w)
	await(t, entered)
	f.mu.Lock()
	f.renewUnknown = true
	f.mu.Unlock()
	until(t, func() bool { // wait until a failed renewal event seals the local session
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.claims == 1
	})
	// Observe cancellation through the explicit renewal event, not a sleep.
	until(t, func() bool { return len(f.renewCalled) >= 2 })
	drainCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if !errors.Is(w.Drain(drainCtx), context.DeadlineExceeded) {
		t.Fatal("drain reused non-quiescent slot")
	}
	if w.Active() != 1 || calls.Load() != 1 {
		t.Fatal("execution overlap/capacity loss")
	}
	close(exit)
	until(t, func() bool { return w.Active() == 0 })
	select {
	case <-f.finish:
		t.Fatal("finished after uncertain renewal")
	default:
	}
}
func TestPauseResumeDrainAndBoot(t *testing.T) {
	w, f := fixture(t, func(*pulse.RunCtx) error { return nil })
	other, _ := fixture(t, func(*pulse.RunCtx) error { return nil })
	if w.Identity().BootID == other.Identity().BootID || len(w.Identity().BootID) != 32 {
		t.Fatal("boot identity")
	}
	w.Pause()
	runWorker(t, w)
	if w.Active() != 0 {
		t.Fatal("paused admission")
	}
	w.Resume()
	await(t, f.finish)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	w.Resume()
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.draining {
		t.Fatal("resume undid drain")
	}
}
func TestSessionSealWaitsForInflightSave(t *testing.T) {
	w, f := fixture(t, func(*pulse.RunCtx) error { return nil })
	f.saveEntered = make(chan struct{}, 1)
	f.saveExit = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &session{c: f, token: d.Token{}, timeout: time.Second, ctx: ctx, cancel: cancel}
	saved := make(chan error, 1)
	go func() { saved <- s.Save(ctx, durable.RunRecord{Revision: 1}, 0) }()
	await(t, f.saveEntered)
	s.seal(true)
	closed := make(chan struct{})
	go func() { _ = s.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("close failed to join Save")
	default:
	}
	newSave := make(chan error, 1)
	go func() { newSave <- s.Save(context.Background(), durable.RunRecord{}, 0) }()
	close(f.saveExit)
	if err := await(t, newSave); !errors.Is(err, d.ErrLeaseLost) {
		t.Fatal(err)
	}
	await(t, saved)
	await(t, closed)
	if !s.uncertain() {
		t.Fatal("lost uncertainty")
	}
	_ = w
}
func TestRuntimeRejectedRecordBlocksBeforeBusiness(t *testing.T) {
	var calls atomic.Int32
	w, f := fixture(t, func(*pulse.RunCtx) error { calls.Add(1); return nil })
	f.record = &durable.RunRecord{RunID: "wrong", Format: durable.RecordFormat}
	runWorker(t, w)
	if got := await(t, f.block); got != "invalid_record" {
		t.Fatalf("block reason=%s", got)
	}
	if calls.Load() != 0 {
		t.Fatal("bypassed record validation")
	}
	select {
	case <-f.finish:
		t.Fatal("rejected record finished")
	default:
	}
}
func TestAcquireUnsupported(t *testing.T) {
	if _, err := (claimedOnlyStore{}).Acquire(context.Background(), "run"); !errors.Is(err, ErrAcquireUnsupported) {
		t.Fatal(err)
	}
}
