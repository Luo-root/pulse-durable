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

// ErrAcquireUnsupported 防止意外使用旧执行路径。
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

// session 在等待进行中的操作前封闭准入。关闭仅作用于本地：
// 绝不释放租约，尤其是新纪元可能已拥有的租约。
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
	// 确定性损坏出现在成功的权威加载之后。保持
	// 续租以便 execute 调用 Block；其他失败仍是不安全的。
	if err != nil && !errors.Is(err, d.ErrInvalidRecord) {
		s.seal(true)
	}
	return r, err
}

// confirmed 使用请求开始时间而非响应到达时间，作为保守的
// 本地租约预算。数据库仍是唯一的所有权权威。
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

// 在不确定性边界隔离后端 panic，避免运行时恢复逻辑
// 将其归类为基础设施失败并尝试 Release。跟踪是否正常返回，
// 而非 recover 的值，以确保旧式 panic(nil) 也封闭并取消会话。
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
	// Digest 将规范批次线格式绑定到操作及不可变的
	// 所有权令牌。随机 ID 仅是幂等身份。
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
