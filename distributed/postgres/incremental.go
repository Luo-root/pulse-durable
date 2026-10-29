package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strconv"
	"sync/atomic"

	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/Luo-root/pulse-durable/recordio"
)

func (c *Coordinator) DropOwnedProjection(t d.Token) { c.discardProjection(t) }

func (c *Coordinator) discardProjection(t d.Token) {
	c.projectionMu.Lock()
	defer c.projectionMu.Unlock()
	if p := c.projections[t]; p != nil {
		p.revoked.Store(true)
		delete(c.projections, t)
	}
}
func (c *Coordinator) discardIdentity(i d.Identity) {
	c.projectionMu.Lock()
	defer c.projectionMu.Unlock()
	for t, p := range c.projections {
		if t.Identity == i {
			p.revoked.Store(true)
			delete(c.projections, t)
		}
	}
}

// 全局锁只保护映射成员关系。令牌关卡支持上下文；
// 某个所有者的慢网络或重放绝不会串行阻塞无关运行。
func (c *Coordinator) projectionGate(ctx context.Context, t d.Token) (*projection, func(), error) {
	if ctx == nil {
		return nil, nil, d.ErrInvalid
	}
	if e := ctx.Err(); e != nil {
		return nil, nil, e
	}
	c.projectionMu.Lock()
	if c.closed {
		c.projectionMu.Unlock()
		return nil, nil, d.ErrLeaseLost
	}
	if c.projections == nil {
		c.projections = make(map[d.Token]*projection)
	}
	p := c.projections[t]
	if p == nil {
		p = &projection{gate: make(chan struct{}, 1)}
		c.projections[t] = p
	}
	c.projectionMu.Unlock()
	select {
	case p.gate <- struct{}{}:
		if p.revoked.Load() {
			<-p.gate
			return nil, nil, d.ErrLeaseLost
		}
		return p, func() { <-p.gate }, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

type projection struct {
	gate    chan struct{}
	revoked atomic.Bool
	loaded  bool
	record  durable.RunRecord
	chain   [32]byte
}
type storedBatch struct {
	Revision string `json:"revision"`
	Wire     []byte `json:"wire"`
	Digest   string `json:"digest"`
	Parent   string `json:"parent_chain"`
	Next     string `json:"next_chain"`
}
type storedLog struct {
	BaseRevision string        `json:"base_revision"`
	BaseChain    string        `json:"base_chain"`
	Digest       string        `json:"snapshot_digest"`
	Chain        string        `json:"head_chain"`
	Batches      []storedBatch `json:"batches"`
}

func chainText(h [32]byte) string { return hex.EncodeToString(h[:]) }

// LoadOwned 读取一次加锁的数据库观测，验证不可变链及
// 类型化前置条件，再安装所有者专属的私有投影。
func (c *Coordinator) LoadOwned(ctx context.Context, t d.Token) (durable.RunRecord, error) {
	p, done, e := c.projectionGate(ctx, t)
	if e != nil {
		return durable.RunRecord{}, e
	}
	defer done()
	r, err := c.loadProjection(ctx, t, p)
	if err != nil {
		c.discardProjection(t)
	}
	return r, err
}
func (c *Coordinator) loadProjection(ctx context.Context, t d.Token, p *projection) (durable.RunRecord, error) {
	p.loaded = false
	if !token(t) {
		return durable.RunRecord{}, d.ErrInvalid
	}
	b, e := c.call(ctx, "load", owned(t), false)
	if e != nil {
		return durable.RunRecord{}, e
	}
	return c.decodeProjection(t, p, b)
}

// 只有成功且受栅栏保护的加载响应才能判定记录损坏。上方 SQL、
// 传输、时钟和令牌错误保留其保守语义。
func (c *Coordinator) decodeProjection(t d.Token, p *projection, b []byte) (r durable.RunRecord, err error) {
	s, owner, snapshot, e := decodeSnapshot(b)
	if e != nil {
		return durable.RunRecord{}, invalidRecord(ErrInvalidRecordSnapshot)
	}
	if owner != t {
		return durable.RunRecord{}, invalidRecord(ErrInvalidRecordIdentity)
	}
	if s.Status != d.Running {
		return durable.RunRecord{}, invalidRecord(ErrInvalidRecordStatus)
	}
	defer func() {
		if errors.Is(err, d.ErrInvalid) {
			err = errors.Join(d.ErrInvalidRecord, err)
		}
	}()
	if s.Revision == 0 {
		return durable.RunRecord{}, d.ErrNotFound
	}
	var log storedLog
	if json.Unmarshal(b, &log) != nil {
		return durable.RunRecord{}, invalidRecord(ErrInvalidRecordLog)
	}
	digest := sha256.Sum256(snapshot)
	if chainText(digest) != log.Digest {
		return durable.RunRecord{}, invalidRecord(ErrInvalidRecordDigest)
	}
	base := uint64(1)
	if log.BaseRevision != "" {
		base, e = strconv.ParseUint(log.BaseRevision, 10, 64)
		if e != nil || base == 0 || strconv.FormatUint(base, 10) != log.BaseRevision {
			return durable.RunRecord{}, invalidRecord(ErrInvalidRecordLog)
		}
	}
	r, e = decodeRecord(s.Task, base, snapshot)
	if e != nil {
		return durable.RunRecord{}, e
	}
	chain := durable.SeedChain(digest)
	if log.BaseChain != "" {
		if !fingerprint(log.BaseChain) {
			return durable.RunRecord{}, invalidRecord(ErrInvalidRecordChain)
		}
		b, _ := hex.DecodeString(log.BaseChain)
		copy(chain[:], b)
	} else if base != 1 {
		return durable.RunRecord{}, invalidRecord(ErrInvalidRecordChain)
	}
	for _, entry := range log.Batches {
		if entry.Revision != strconv.FormatUint(r.Revision+1, 10) || entry.Parent != chainText(chain) {
			return durable.RunRecord{}, invalidRecord(ErrInvalidRecordBatch)
		}
		digest = sha256.Sum256(entry.Wire)
		if chainText(digest) != entry.Digest {
			return durable.RunRecord{}, invalidRecord(ErrInvalidRecordDigest)
		}
		batch, err := durable.UnmarshalChangeBatch(entry.Wire)
		if err != nil {
			return durable.RunRecord{}, invalidRecord(ErrInvalidRecordBatch)
		}
		if _, err = batch.ApplyTo(&r); err != nil {
			return durable.RunRecord{}, invalidRecord(ErrInvalidRecordBatch)
		}
		chain, err = durable.NextChain(chain, digest, r.Revision)
		if err != nil || entry.Next != chainText(chain) {
			return durable.RunRecord{}, invalidRecord(ErrInvalidRecordChain)
		}
	}
	if r.Revision != s.Revision {
		return durable.RunRecord{}, invalidRecord(ErrInvalidRecordRevision)
	}
	if log.Chain != chainText(chain) {
		return durable.RunRecord{}, invalidRecord(ErrInvalidRecordChain)
	}
	if p.revoked.Load() {
		return durable.RunRecord{}, d.ErrLeaseLost
	}
	p.record = r
	p.chain = chain
	p.loaded = true
	return durable.CloneRecord(r), nil
}
func (c *Coordinator) InitializeOwned(ctx context.Context, q d.InitializeRequest) error {
	if !token(q.Token) || !mutation(q.Mutation) || durable.ValidateInitialRecord(q.Record, q.Token.Identity.RunID) != nil {
		return d.ErrInvalid
	}
	b, e := recordio.Encode(q.Record)
	if e != nil || len(b) > MaxRecordBytes {
		return d.ErrInvalid
	}
	p, done, e := c.projectionGate(ctx, q.Token)
	if e != nil {
		return e
	}
	defer done()
	p.loaded = false
	a := owned(q.Token)
	mutate(a, q.Mutation)
	a["expected"] = "0"
	a["record"] = b
	_, _, e = c.ownerCall(ctx, "initialize", q.Token, a, true)
	if e != nil {
		c.discardProjection(q.Token)
		return e
	}
	if p.revoked.Load() {
		return d.ErrUnknown
	}
	p.record = durable.CloneRecord(q.Record)
	p.chain = durable.SeedChain(sha256.Sum256(b))
	p.loaded = true
	return nil
}
func (c *Coordinator) ApplyOwned(ctx context.Context, t d.Token, m d.Mutation, batch durable.ChangeBatch) (durable.CommitAck, error) {
	if !token(t) || !mutation(m) {
		return durable.CommitAck{}, d.ErrInvalid
	}
	digest, e := d.DeltaMutationDigest(t, batch)
	if e != nil || digest != m.Digest {
		return durable.CommitAck{}, d.ErrInvalid
	}
	wire, e := durable.MarshalChangeBatch(batch)
	if e != nil || len(wire) > MaxRecordBytes {
		return durable.CommitAck{}, d.ErrInvalid
	}
	p, done, e := c.projectionGate(ctx, t)
	if e != nil {
		return durable.CommitAck{}, e
	}
	defer done()
	if !p.loaded {
		if _, e = c.loadProjection(ctx, t, p); e != nil {
			c.discardProjection(t)
			return durable.CommitAck{}, e
		}
	}
	if p.revoked.Load() {
		return durable.CommitAck{}, d.ErrLeaseLost
	}
	// ApplyTo 在修改缓存前验证全部前置条件。此后任何失败
	// 均丢弃缓存；只有数据库头的 CAS 能发布提案。
	parent := p.chain
	ack, e := batch.ApplyTo(&p.record)
	if e != nil {
		return durable.CommitAck{}, e
	}
	next, e := durable.NextChain(parent, sha256.Sum256(wire), batch.Header().Revision)
	if e != nil {
		c.discardProjection(t)
		return durable.CommitAck{}, e
	}
	a := owned(t)
	mutate(a, m)
	a["expected"] = strconv.FormatUint(batch.Header().ExpectedRevision, 10)
	a["wire"] = wire
	a["batch_digest"] = chainText(sha256.Sum256(wire))
	a["parent_chain"] = chainText(parent)
	a["next_chain"] = chainText(next)
	// 使用小响应，避免每个检查点都重传快照。
	_, e = c.call(ctx, "apply", a, true)
	if e != nil {
		c.discardProjection(t)
		c.observeReject(t, e)
		return durable.CommitAck{}, e
	}
	if p.revoked.Load() {
		return durable.CommitAck{}, d.ErrUnknown
	}
	p.chain = next
	return ack, nil
}
