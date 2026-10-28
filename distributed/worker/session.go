package worker

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"

	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
)

// ErrAcquireUnsupported guards against accidentally using the legacy execution path.
var ErrAcquireUnsupported = errors.New("worker: Acquire unsupported; use claimed execution")

type claimedOnlyStore struct{}

func (claimedOnlyStore) Acquire(context.Context, string) (durable.Session, error) {
	return nil, ErrAcquireUnsupported
}

func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
func mutation(v any) (d.Mutation, error) {
	id, err := randomID()
	if err != nil {
		return d.Mutation{}, err
	}
	b, err := json.Marshal(v)
	if err != nil {
		return d.Mutation{}, err
	}
	h := sha256.Sum256(b)
	return d.Mutation{ID: id, Digest: hex.EncodeToString(h[:])}, nil
}

// session gates admission before waiting for in-flight operations. Closing is local
// only: it never releases a lease, especially one a new epoch may already own.
type session struct {
	c              d.Coordinator
	token          d.Token
	timeout        time.Duration
	ctx            context.Context
	cancel         context.CancelFunc
	observe        func(Event)
	mu             sync.Mutex
	saveMu         sync.Mutex
	leaseDeadline  time.Time
	leaseTimer     *time.Timer
	closed, unsafe bool
	inflight       sync.WaitGroup
}

var _ durable.ClaimedSession = (*session)(nil)

func (s *session) RunID() string { return s.token.Identity.RunID }
func (s *session) begin(ctx context.Context) (context.Context, func(), error) {
	s.mu.Lock()
	if s.closed || (!s.leaseDeadline.IsZero() && !time.Now().Before(s.leaseDeadline)) {
		s.mu.Unlock()
		return nil, nil, d.ErrLeaseLost
	}
	s.inflight.Add(1)
	s.mu.Unlock()
	op, cancel := context.WithTimeout(ctx, s.timeout)
	stop := context.AfterFunc(s.ctx, cancel)
	return op, func() { stop(); cancel(); s.inflight.Done() }, nil
}
func (s *session) seal(unsafe bool) {
	s.mu.Lock()
	s.closed = true
	if s.leaseTimer != nil {
		s.leaseTimer.Stop()
	}
	s.unsafe = s.unsafe || unsafe
	s.mu.Unlock()
	s.cancel()
}
func (s *session) Close() error {
	s.seal(false)
	s.inflight.Wait()
	s.c.DropOwnedProjection(s.token)
	return nil
}
func (s *session) uncertain() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.unsafe }
func (s *session) Load(ctx context.Context) (durable.RunRecord, error) {
	op, done, err := s.begin(ctx)
	if err != nil {
		return durable.RunRecord{}, err
	}
	defer done()
	r, err := s.c.LoadOwned(op, s.token)
	if errors.Is(err, d.ErrNotFound) || errors.Is(err, durable.ErrNotFound) {
		return r, durable.ErrNotFound
	}
	if err != nil {
		s.seal(true)
	}
	return r, err
}

// confirmed uses the request start, not response arrival, as the conservative
// local lease budget. The database remains the only ownership authority.
func (s *session) confirmed(start time.Time, ttl, margin time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.leaseDeadline = start.Add(ttl - margin)
	if s.leaseTimer != nil {
		s.leaseTimer.Stop()
	}
	if !time.Now().Before(s.leaseDeadline) {
		return false
	}
	s.leaseTimer = time.AfterFunc(time.Until(s.leaseDeadline), func() {
		s.mu.Lock()
		expired := !time.Now().Before(s.leaseDeadline)
		if expired {
			s.closed = true
			s.unsafe = true
		}
		s.mu.Unlock()
		if expired {
			s.cancel()
		}
	})
	return true
}

// Contain backend panics at the uncertainty boundary, before runtime recovery
// could classify infrastructure failure and attempt Release. Track return rather
// than recover's value so legacy panic(nil) also seals and cancels the session.
func (s *session) initializeOwned(ctx context.Context, req d.InitializeRequest) (err error) {
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			s.seal(true)
			err = d.ErrUnknown
		}
	}()
	err = s.c.InitializeOwned(ctx, req)
	returned = true
	return err
}

func (s *session) Apply(ctx context.Context, batch durable.ChangeBatch) (ack durable.CommitAck, err error) {
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	op, done, err := s.begin(ctx)
	if err != nil {
		return durable.CommitAck{}, err
	}
	defer done()
	id, err := randomID()
	if err != nil {
		s.seal(true)
		return durable.CommitAck{}, err
	}
	// Digest binds the canonical batch wire to the operation and immutable
	// ownership token. The random ID is only an idempotency identity.
	digest, err := d.DeltaMutationDigest(s.token, batch)
	if err != nil {
		s.seal(true)
		return durable.CommitAck{}, err
	}
	m := d.Mutation{ID: id, Digest: digest}
	start := time.Now()
	returned := false
	defer func() {
		if !returned {
			_ = recover()
			s.seal(true)
			ack, err = durable.CommitAck{}, d.ErrUnknown
			return
		}
		if err != nil {
			s.seal(true)
		}
		if s.observe != nil {
			s.observe(Event{Kind: "apply", Token: s.token, Revision: batch.Header().Revision, Mutation: m, Reason: reason(err), Duration: time.Since(start)})
		}
	}()
	ack, err = s.c.ApplyOwned(op, s.token, m, batch)
	returned = true
	if err == nil && ack.Header != batch.Header() {
		err = durable.ErrConflict
	}
	return ack, err
}

func (s *session) Initialize(ctx context.Context, r durable.RunRecord) error {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return d.ErrLeaseLost
	}
	s.saveMu.Lock()
	defer s.saveMu.Unlock()
	op, done, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	req := d.InitializeRequest{Token: s.token, Record: r}
	req.Mutation, err = mutation(req)
	start := time.Now()
	if err == nil {
		err = s.initializeOwned(op, req)
	}
	if err != nil {
		s.seal(true)
	}
	if s.observe != nil {
		s.observe(Event{Kind: "save", Token: s.token, Revision: r.Revision, Mutation: req.Mutation, Reason: reason(err), Duration: time.Since(start)})
	}
	return err
}
