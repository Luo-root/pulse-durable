// Package worker runs experimental capacity-bounded distributed Durable workers.
// The coordinator must enforce fencing in every owned operation. Unit tests of
// this package are not evidence of database atomicity or cross-process safety.
package worker

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"
	"unicode/utf8"

	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
)

// Renewer should use a dedicated backend pool/connection budget, independent of
// Claim and checkpoint traffic. A goroutine alone cannot reserve database capacity.
type Renewer interface {
	Renew(context.Context, d.Token, time.Duration) (time.Time, error)
}

// Event contains identities and fixed reason codes, never payloads or raw errors.
// Observe is synchronous, must be fast and concurrency-safe, and must not call
// Drain synchronously. No metrics or high-cardinality metric labels are installed.
type Event struct {
	Kind, Reason string
	Worker       d.Worker
	Token        d.Token
	Revision     uint64
	Mutation     d.Mutation
	Duration     time.Duration
}
type Config struct {
	Coordinator                                        d.Coordinator
	Renewer                                            Renewer
	Registry                                           *d.Registry
	Namespace, WorkerID                                string
	Capacity, BatchMax                                 int
	TTL, RenewInterval, OperationTimeout, RenewTimeout time.Duration
	PollMin, PollMax                                   time.Duration
	Observe                                            func(Event)
	// RequireDelta explicitly opts this worker into typed delta checkpoints.
	// New rejects a coordinator without DeltaCoordinator before any claim or
	// business execution. The zero value preserves legacy full-save behavior.
	RequireDelta bool
}

// Worker is single-use. New creates an independent random 128-bit boot identity.
// Pause/Resume affect admission only. Drain never issues business Cancel.
type Worker struct {
	cfg                       Config
	identity                  d.Worker
	mu                        sync.Mutex
	started, paused, draining bool
	active                    int
	changed                   chan struct{}
	// pollTimer is an internal deterministic test seam; nil uses a real timer.
	pollTimer func(time.Duration) (<-chan time.Time, func())
}

func validText(s string) bool { return len(s) > 0 && len(s) <= d.MaxTextBytes && utf8.ValidString(s) }

func deltaCapability(c d.Coordinator) (d.DeltaCoordinator, bool) {
	capability, ok := c.(d.DeltaCoordinator)
	if !ok || capability == nil {
		return nil, false
	}
	v := reflect.ValueOf(capability)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if v.IsNil() {
			return nil, false
		}
	}
	return capability, true
}

func New(c Config) (*Worker, error) {
	if c.Coordinator == nil || c.Renewer == nil || c.Registry == nil || !validText(c.Namespace) || !validText(c.WorkerID) {
		return nil, d.ErrInvalid
	}
	if c.RequireDelta {
		if _, ok := deltaCapability(c.Coordinator); !ok {
			return nil, durable.ErrUnsupportedCapability
		}
	}
	if _, err := (d.PullPolicy{Capacity: c.Capacity, BatchMax: c.BatchMax}).Limit(0); err != nil {
		return nil, err
	}
	if c.TTL <= 0 || c.RenewInterval <= 0 || c.OperationTimeout <= 0 || c.RenewTimeout <= 0 || c.RenewInterval >= c.TTL || c.RenewTimeout >= c.TTL-c.RenewInterval || c.PollMin <= 0 || c.PollMax < c.PollMin || c.PollMax > time.Hour {
		return nil, d.ErrInvalid
	}
	boot, err := randomID()
	if err != nil {
		return nil, err
	}
	return &Worker{cfg: c, identity: d.Worker{ID: c.WorkerID, BootID: boot}, changed: make(chan struct{}, 1)}, nil
}
func (w *Worker) Identity() d.Worker { return w.identity }
func (w *Worker) signal() {
	select {
	case w.changed <- struct{}{}:
	default:
	}
}
func (w *Worker) Pause() { w.mu.Lock(); w.paused = true; w.mu.Unlock(); w.signal() }

// Resume cannot undo Drain or make a Worker reusable after Run returns.
func (w *Worker) Resume() {
	w.mu.Lock()
	if !w.draining {
		w.paused = false
	}
	w.mu.Unlock()
	w.signal()
}
func (w *Worker) Active() int { w.mu.Lock(); defer w.mu.Unlock(); return w.active }

// Drain stops admission, retains renewal, and waits for every local reservation.
// A deadline returns its error without canceling work. Unresolved claims and
// non-cooperative code can keep reservations indefinitely; they are never reused.
func (w *Worker) Drain(ctx context.Context) error {
	w.mu.Lock()
	w.draining = true
	w.mu.Unlock()
	w.signal()
	// Do not consume Run's notification: separate bounded checks avoid lost wakeups.
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if w.Active() == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
func (w *Worker) emit(e Event) {
	if w.cfg.Observe != nil {
		e.Worker = w.identity
		func() { defer func() { _ = recover() }(); w.cfg.Observe(e) }()
	}
}
func reason(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, d.ErrUnknown):
		return "unknown"
	case errors.Is(err, d.ErrLeaseLost):
		return "lease_lost"
	case errors.Is(err, d.ErrTerminal):
		return "terminal"
	case errors.Is(err, d.ErrClockRegression):
		return "clock_regression"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "interrupted"
	default:
		return "infrastructure"
	}
}

// adjustSlots is synchronous admission bookkeeping, not a wakeup event.
func (w *Worker) adjustSlots(n int) { w.mu.Lock(); w.active -= n; w.mu.Unlock() }

// Asynchronous completions wake the poller only when capacity actually changes.
func (w *Worker) releaseSlots(n int) {
	if n == 0 {
		return
	}
	w.adjustSlots(n)
	w.signal()
}
func (w *Worker) reserve() (int, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, _ := (d.PullPolicy{Capacity: w.cfg.Capacity, BatchMax: w.cfg.BatchMax, Paused: w.paused, Draining: w.draining}).Limit(w.active)
	w.active += n
	return n, w.draining && w.active == 0
}
func (w *Worker) delay() time.Duration {
	// Cryptographic bytes avoid shared PRNG state and correlated process polling.
	id, err := randomID()
	if err != nil {
		return w.cfg.PollMax
	}
	var n uint64
	for i := 0; i < 12; i++ {
		n = n*33 + uint64(id[i])
	}
	return w.cfg.PollMin + time.Duration(n%uint64(w.cfg.PollMax-w.cfg.PollMin+1))
}
func (w *Worker) wait(ctx context.Context) bool {
	var tick <-chan time.Time
	var stop func()
	if w.pollTimer != nil {
		tick, stop = w.pollTimer(w.delay())
	} else {
		t := time.NewTimer(w.delay())
		tick = t.C
		stop = func() { t.Stop() }
	}
	defer stop()
	select {
	case <-ctx.Done():
		return false
	case <-tick:
		return true
	case <-w.changed:
		return true
	}
}

// Run blocks until drained or canceled, and may be called only once. Cancellation
// seals sessions through local contexts, not Coordinator.Cancel. Run can return
// before non-cooperative business code exits; Active retains those reservations
// and cleanup releases only after execution and all saves have actually returned.
func (w *Worker) Run(ctx context.Context) error {
	if ctx == nil {
		return d.ErrInvalid
	}
	w.mu.Lock()
	if w.started {
		w.mu.Unlock()
		return d.ErrInvalid
	}
	w.started = true
	w.mu.Unlock()
	defer func() { w.mu.Lock(); w.draining = true; w.mu.Unlock(); w.signal() }()
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		n, drained := w.reserve()
		if drained {
			return nil
		}
		if n > 0 {
			w.claim(ctx, n)
		}
		if !w.wait(ctx) {
			return ctx.Err()
		}
	}
}
func (w *Worker) claim(ctx context.Context, n int) {
	ids := make([]string, n)
	for i := range ids {
		id, err := randomID()
		if err != nil {
			w.adjustSlots(n)
			w.emit(Event{Kind: "claim", Reason: "random_failed"})
			return
		}
		ids[i] = id
	}
	req := d.ClaimRequest{Namespace: w.cfg.Namespace, Worker: w.identity, Capabilities: w.cfg.Registry.Capabilities(), Limit: n, TTL: w.cfg.TTL, LeaseIDs: ids}
	op, cancel := context.WithTimeout(ctx, w.cfg.OperationTimeout)
	start := time.Now()
	claims, err := w.cfg.Coordinator.Claim(op, req)
	cancel()
	w.emit(Event{Kind: "claim", Reason: reason(err), Duration: time.Since(start)})
	if err != nil {
		// Any transport failure may hide a commit, not only an explicit ErrUnknown.
		for _, id := range ids {
			go w.resolve(ctx, id)
		}
		return
	}
	byID := make(map[string]d.Claim, len(claims))
	valid := len(claims) <= n
	allowed := make(map[string]bool, n)
	for _, id := range ids {
		allowed[id] = true
	}
	for _, c := range claims {
		if !allowed[c.Token.LeaseID] || byID[c.Token.LeaseID].Token.LeaseID != "" || !w.validClaim(c) {
			valid = false
		}
		byID[c.Token.LeaseID] = c
	}
	if !valid {
		w.emit(Event{Kind: "claim", Reason: "invalid_reply"})
		for _, id := range ids {
			go w.resolve(ctx, id)
		}
		return
	}
	w.adjustSlots(n - len(claims))
	for _, c := range claims {
		go w.execute(ctx, c)
	}
}
func (w *Worker) validClaim(c d.Claim) bool {
	return c.Token.Worker == w.identity && c.Token.Identity.Namespace == w.cfg.Namespace && c.Token.Identity == c.Snapshot.Task.Identity() && c.Token.Epoch > 0 && c.Token.LeaseID != "" && c.Snapshot.Status == d.Running
}
func (w *Worker) resolve(ctx context.Context, id string) {
	for ctx.Err() == nil {
		op, cancel := context.WithTimeout(ctx, w.cfg.OperationTimeout)
		resolution, err := w.cfg.Coordinator.ResolveClaim(op, w.cfg.Namespace, w.identity, id)
		cancel()
		if err == nil {
			if resolution.Closed && resolution.Claim == nil {
				w.emit(Event{Kind: "resolve_claim", Reason: "closed"})
				w.releaseSlots(1)
				return
			}
			if !resolution.Closed && resolution.Claim != nil && resolution.Claim.Token.LeaseID == id && w.validClaim(*resolution.Claim) {
				w.emit(Event{Kind: "resolve_claim", Token: resolution.Claim.Token, Reason: "claimed"})
				w.execute(ctx, *resolution.Claim)
				return
			}
		}
		w.emit(Event{Kind: "resolve_claim", Reason: "unresolved"})
		// Unknown or malformed responses do not prove durable closure. Keep the
		// reservation and resolve only this original ID; never replay Claim.
		t := time.NewTimer(w.delay())
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
	// No token known: neither Release nor freeing this reservation is safe.
}
func (w *Worker) renew(ctx context.Context, token d.Token) error {
	op, cancel := context.WithTimeout(ctx, w.cfg.RenewTimeout)
	defer cancel()
	start := time.Now()
	_, err := w.cfg.Renewer.Renew(op, token, w.cfg.TTL)
	w.emit(Event{Kind: "renew", Token: token, Reason: reason(err), Duration: time.Since(start)})
	return err
}
func (w *Worker) execute(parent context.Context, c d.Claim) {
	defer w.releaseSlots(1)
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	s := &session{c: w.cfg.Coordinator, token: c.Token, timeout: w.cfg.OperationTimeout, ctx: ctx, cancel: cancel, observe: w.emit}
	var claimed durable.ClaimedSession = s
	if w.cfg.RequireDelta {
		capability, ok := deltaCapability(w.cfg.Coordinator)
		if !ok {
			s.seal(true)
			return
		}
		claimed = &deltaSession{session: s, coordinator: capability}
	}
	// Authoritative renewal is required even after a successful Claim/ResolveClaim.
	renewSession := func() bool {
		start := time.Now()
		if w.renew(ctx, c.Token) != nil || !s.confirmed(start, w.cfg.TTL, w.cfg.RenewTimeout) {
			s.seal(true)
			return false
		}
		return true
	}
	if !renewSession() {
		return
	}
	stop := make(chan struct{})
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		ticker := time.NewTicker(w.cfg.RenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				s.seal(true)
				return
			case <-ticker.C:
				if !renewSession() {
					return
				}
			}
		}
	}()
	var stopOnce sync.Once
	stopRenew := func() { stopOnce.Do(func() { close(stop) }); <-joined }
	defer func() { stopRenew(); _ = s.Close() }()
	a, in, err := w.cfg.Registry.Resolve(ctx, c.Snapshot.Task)
	outcome := durable.OutcomeRejected
	// Registry exposes ErrInvalid for payload decoding and assembly validation,
	// and ErrMismatch for exact route/definition/input incompatibility.
	blockReason := "incompatible"
	if errors.Is(err, d.ErrInvalid) {
		blockReason = "invalid_payload"
	}
	if _, parseErr := d.ParseTask(c.Snapshot.Task.Bytes()); parseErr != nil {
		blockReason = "invalid_task"
	}
	if err == nil {
		blockReason = "invalid_record"
		rt, newErr := durable.New(claimedOnlyStore{}, a.Codecs)
		if newErr != nil {
			err = newErr
		} else {
			_, loadErr := s.Load(ctx)
			opts := durable.RunOptions{RunID: c.Token.Identity.RunID, Version: c.Snapshot.Task.Route().BusinessVersion}
			switch {
			case errors.Is(loadErr, durable.ErrNotFound):
				opts.RequireDelta = w.cfg.RequireDelta
				_, outcome, err = rt.RunClaimed(ctx, claimed, a.Graph, in, opts)
			case loadErr == nil:
				opts.RequireDelta = w.cfg.RequireDelta
				_, outcome, err = rt.ResumeClaimed(ctx, claimed, a.Graph, in, opts)
			default:
				outcome = durable.OutcomeInfrastructure
				err = loadErr
			}
		}
	}
	// Join any renewal already in flight before deciding a terminal verdict. A
	// late unknown renewal must not race the local uncertainty check below.
	stopRenew()
	// Runtime Wait has joined business/checkpoint work. Seal new local operations
	// and explicitly join in-flight calls before any coordinated terminal/release.
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.inflight.Wait()
	w.emit(Event{Kind: "execution", Token: c.Token, Reason: string(outcome)})
	if s.uncertain() || ctx.Err() != nil {
		// Shutdown interruption can Release once quiescent, but uncertain ownership
		// or any failed Save must expire instead. Never manufacture terminal status.
		if !s.uncertain() {
			w.release(c.Token)
		}
		return
	}
	switch outcome {
	case durable.OutcomeCompleted, durable.OutcomeFailed:
		op, done := context.WithTimeout(ctx, w.cfg.OperationTimeout)
		record, loadErr := w.cfg.Coordinator.LoadOwned(op, c.Token)
		done()
		if loadErr != nil {
			w.emit(Event{Kind: "finish", Token: c.Token, Reason: reason(loadErr)})
			return
		}
		req := d.FinishRequest{Token: c.Token, ExpectedRevision: record.Revision, Outcome: outcome}
		req.Mutation, err = mutation(req)
		if err != nil {
			return
		}
		op, done = context.WithTimeout(ctx, w.cfg.OperationTimeout)
		_, err = w.cfg.Coordinator.Finish(op, req)
		done()
		w.emit(Event{Kind: "finish", Token: c.Token, Revision: record.Revision, Mutation: req.Mutation, Reason: reason(err)})
	case durable.OutcomeRejected:
		m, merr := mutation(struct {
			Token  d.Token
			Reason string
		}{c.Token, blockReason})
		if merr != nil {
			return
		}
		op, done := context.WithTimeout(ctx, w.cfg.OperationTimeout)
		err = w.cfg.Coordinator.Block(op, c.Token, blockReason, m)
		done()
		w.emit(Event{Kind: "block", Token: c.Token, Mutation: m, Reason: reason(err)})
	default:
		w.release(c.Token)
	}
}
func (w *Worker) release(token d.Token) {
	m, err := mutation(token)
	if err != nil {
		return
	}
	op, cancel := context.WithTimeout(context.Background(), w.cfg.OperationTimeout)
	defer cancel()
	err = w.cfg.Coordinator.Release(op, token, m)
	w.emit(Event{Kind: "release", Token: token, Mutation: m, Reason: reason(err)})
}
