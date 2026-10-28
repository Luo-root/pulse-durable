// Package postgres implements the experimental PostgreSQL 16+ coordinator.
// Migrate is an explicit administrator operation; New never runs DDL. Worker
// credentials must not own the schema, inherit its owner, or have table DML.
package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/Luo-root/pulse-durable/recordio"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const MaxRecordBytes = 16 << 20
const OperationTimeout = 5 * time.Second

var (
	ErrInvalidRecordSnapshot   = errors.New("postgres: invalid record snapshot")
	ErrInvalidRecordIdentity   = errors.New("postgres: invalid record identity")
	ErrInvalidRecordRevision   = errors.New("postgres: invalid record revision")
	ErrInvalidRecordFormat     = errors.New("postgres: invalid record format")
	ErrInvalidRecordVersion    = errors.New("postgres: invalid record version")
	ErrInvalidRecordDefinition = errors.New("postgres: invalid record definition")
	ErrInvalidRecordInputs     = errors.New("postgres: invalid record inputs")
	ErrInvalidRecordRoot       = errors.New("postgres: invalid record root")
	ErrInvalidRecordShapes     = errors.New("postgres: invalid record shapes")
)

func invalidRecord(kind error) error { return errors.Join(kind, d.ErrInvalid) }

type Coordinator struct {
	pool         *pgxpool.Pool
	projectionMu sync.Mutex
	closed       bool
	projections  map[d.Token]*projection
	// Test-only observer, installed before use; receives no raw database errors.
	clockDiagnostic func(clockDiagnostic)
}

var _ d.Coordinator = (*Coordinator)(nil)

func bounded(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		return nil, nil, d.ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	c, cancel := context.WithTimeout(ctx, OperationTimeout)
	return c, cancel, nil
}
func connect(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	if dsn == "" {
		return nil, d.ErrInvalid
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("distributed postgres: invalid DSN")
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = "pulse-durable-distributed"
	cfg.ConnConfig.RuntimeParams["search_path"] = "pg_catalog,pulse_durable_distributed,pg_temp"
	cfg.ConnConfig.RuntimeParams["default_transaction_read_only"] = "off"
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "5000"
	cfg.ConnConfig.RuntimeParams["lock_timeout"] = "4000"
	cfg.ConnConfig.RuntimeParams["idle_in_transaction_session_timeout"] = "5000"
	cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = "read committed"
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, scrub(err, false)
	}
	if err = p.Ping(ctx); err != nil {
		p.Close()
		return nil, scrub(err, false)
	}
	return p, nil
}
func New(ctx context.Context, dsn string) (*Coordinator, error) {
	ctx, cancel, err := bounded(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	p, err := connect(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err = validate(ctx, p, true); err != nil {
		p.Close()
		return nil, err
	}
	return &Coordinator{pool: p, projections: make(map[d.Token]*projection)}, nil
}

// Close only closes local connections; it never releases a lease.
func (c *Coordinator) Close() {
	if c != nil && c.pool != nil {
		c.projectionMu.Lock()
		c.closed = true
		for _, p := range c.projections {
			p.revoked.Store(true)
		}
		clear(c.projections)
		c.projectionMu.Unlock()
		c.pool.Close()
	}
}
func scrub(err error, write bool) error {
	if err == nil {
		return nil
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		switch pe.Code {
		case "PD001", "23505", "22P02", "22021", "22003", "22023", "22032", "22P05", "2200D", "23514", "23502":
			return fmt.Errorf("distributed postgres: SQLSTATE %s: %w", pe.Code, d.ErrInvalid)
		case "PD002":
			return d.ErrSubmitConflict
		case "PD003":
			return d.ErrNotFound
		case "PD004":
			return d.ErrLeaseLost
		case "PD005":
			return d.ErrRevisionConflict
		case "PD006":
			return d.ErrTerminal
		case "PD007":
			return d.ErrClockRegression
		}
		return fmt.Errorf("distributed postgres: SQLSTATE %s", pe.Code)
	}
	if write {
		return d.ErrUnknown
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	return errors.New("distributed postgres: database unavailable")
}
func validText(s string) bool {
	return len(s) > 0 && len(s) <= d.MaxTextBytes && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}
func identity(i d.Identity) bool { return validText(i.Namespace) && validText(i.RunID) }
func fingerprint(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) == 32 && hex.EncodeToString(b) == s
}
func lease(s string) bool {
	b, e := hex.DecodeString(s)
	return e == nil && len(b) >= 16 && len(b) <= 64 && hex.EncodeToString(b) == s
}
func validWorker(w d.Worker) bool { return validText(w.ID) && validText(w.BootID) }
func ttl(t time.Duration) bool    { return t >= time.Millisecond && t <= 24*time.Hour }
func token(t d.Token) bool {
	return identity(t.Identity) && validWorker(t.Worker) && lease(t.LeaseID) && t.Epoch > 0
}
func mutation(m d.Mutation) bool { return validText(m.ID) && fingerprint(m.Digest) }
func args(i d.Identity) map[string]any {
	return map[string]any{"namespace": i.Namespace, "run_id": i.RunID}
}
func owned(t d.Token) map[string]any {
	a := args(t.Identity)
	a["worker_id"] = t.Worker.ID
	a["boot_id"] = t.Worker.BootID
	a["lease_id"] = t.LeaseID
	a["epoch"] = t.Epoch
	return a
}
func mutate(a map[string]any, m d.Mutation) { a["mutation_id"] = m.ID; a["mutation_digest"] = m.Digest }
func (c *Coordinator) call(ctx context.Context, op string, a map[string]any, write bool) (json.RawMessage, error) {
	ctx, cancel, err := bounded(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	b, err := json.Marshal(a)
	if err != nil {
		return nil, d.ErrInvalid
	}
	var result []byte
	if c == nil || c.pool == nil {
		return nil, errors.New("distributed postgres: closed coordinator")
	}
	// A single function invocation is one implicit transaction, including its events.
	err = c.pool.QueryRow(ctx, "SELECT pulse_durable_distributed.api($1,$2::jsonb)", op, b).Scan(&result)
	if err != nil {
		c.observeClockDiagnostic(err)
		return nil, scrub(err, write)
	}
	return result, nil
}

type wireSnapshot struct {
	Task       []byte     `json:"task"`
	Digest     string     `json:"digest"`
	Namespace  string     `json:"namespace"`
	RunID      string     `json:"run_id"`
	Route      d.Route    `json:"route"`
	Status     d.Status   `json:"status"`
	Epoch      int64      `json:"epoch"`
	Revision   string     `json:"revision"`
	LeaseUntil *time.Time `json:"lease_until"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
	Reason     string     `json:"reason"`
	LeaseID    *string    `json:"lease_id"`
	WorkerID   *string    `json:"worker_id"`
	BootID     *string    `json:"boot_id"`
	Record     []byte     `json:"record"`
}

func decodeSnapshot(b []byte) (d.Snapshot, d.Token, []byte, error) {
	var w wireSnapshot
	if json.Unmarshal(b, &w) != nil {
		return d.Snapshot{}, d.Token{}, nil, d.ErrInvalid
	}
	t, err := d.ParseTask(w.Task)
	if err != nil {
		return d.Snapshot{}, d.Token{}, nil, d.ErrInvalid
	}
	n, err := strconv.ParseUint(w.Revision, 10, 64)
	if err != nil || strconv.FormatUint(n, 10) != w.Revision || t.SubmissionDigest() != w.Digest || t.Identity() != (d.Identity{Namespace: w.Namespace, RunID: w.RunID}) || t.Route() != w.Route || w.Epoch < 0 || w.CreatedAt.IsZero() || w.UpdatedAt.Before(w.CreatedAt) {
		return d.Snapshot{}, d.Token{}, nil, d.ErrInvalid
	}
	switch w.Status {
	case d.Queued, d.Running, d.Blocked, d.Succeeded, d.Failed, d.Canceled:
	default:
		return d.Snapshot{}, d.Token{}, nil, d.ErrInvalid
	}
	tok := d.Token{Identity: t.Identity(), Epoch: w.Epoch}
	s := d.Snapshot{Task: t, Status: w.Status, Epoch: w.Epoch, Revision: n, CreatedAt: w.CreatedAt, UpdatedAt: w.UpdatedAt, Reason: w.Reason}
	if w.Status == d.Running {
		if w.LeaseUntil == nil || w.LeaseID == nil || w.WorkerID == nil || w.BootID == nil {
			return d.Snapshot{}, d.Token{}, nil, d.ErrInvalid
		}
		tok.LeaseID = *w.LeaseID
		tok.Worker = d.Worker{ID: *w.WorkerID, BootID: *w.BootID}
		if !token(tok) {
			return d.Snapshot{}, d.Token{}, nil, d.ErrInvalid
		}
		s.LeaseUntil = *w.LeaseUntil
	} else if w.LeaseUntil != nil || w.LeaseID != nil || w.WorkerID != nil || w.BootID != nil {
		return d.Snapshot{}, d.Token{}, nil, d.ErrInvalid
	}
	if (w.Status == d.Succeeded || w.Status == d.Failed) && n == 0 {
		return d.Snapshot{}, d.Token{}, nil, d.ErrInvalid
	}
	if w.Status == d.Blocked {
		switch w.Reason {
		case "incompatible", "invalid_payload", "invalid_record", "invalid_task", "unsupported_format":
		default:
			return d.Snapshot{}, d.Token{}, nil, d.ErrInvalid
		}
	} else if w.Reason != "" {
		return d.Snapshot{}, d.Token{}, nil, d.ErrInvalid
	}
	if n == 0 {
		if w.Record != nil {
			return d.Snapshot{}, d.Token{}, nil, d.ErrInvalid
		}
	}
	// Scheduling observations (including Cancel/Block) do not validate checkpoint
	// payloads: a corrupt checkpoint must not prevent an operator from stopping
	// a run. Only LoadOwned grants a verified projection to the runtime.
	return s, tok, w.Record, nil
}
func decodeRecord(t d.Task, n uint64, b []byte) (durable.RunRecord, error) {
	if len(b) == 0 || len(b) > MaxRecordBytes {
		return durable.RunRecord{}, invalidRecord(ErrInvalidRecordSnapshot)
	}
	// Initial/legacy worker checkpoints are bare RunRecord JSON. A published
	// maintenance representation is a lossless versioned snapshot envelope.
	// Accept both at this boundary, but keep one identity/revision validation
	// path below so compaction cannot weaken ordinary checkpoint checks.
	r, e := recordio.DecodeSnapshot(b)
	if e != nil {
		r, e = recordio.Decode(b)
	}
	if e != nil {
		return durable.RunRecord{}, invalidRecord(ErrInvalidRecordSnapshot)
	}
	if r.RunID != t.Identity().RunID {
		return durable.RunRecord{}, invalidRecord(ErrInvalidRecordIdentity)
	}
	if r.Revision != n {
		return durable.RunRecord{}, invalidRecord(ErrInvalidRecordRevision)
	}
	if r.Format != t.Route().RecordFormat {
		return durable.RunRecord{}, invalidRecord(ErrInvalidRecordFormat)
	}
	if r.Version != t.Route().BusinessVersion {
		return durable.RunRecord{}, invalidRecord(ErrInvalidRecordVersion)
	}
	if r.Definition != t.Route().Definition {
		return durable.RunRecord{}, invalidRecord(ErrInvalidRecordDefinition)
	}
	if r.Inputs != t.InputsFingerprint() {
		return durable.RunRecord{}, invalidRecord(ErrInvalidRecordInputs)
	}
	if r.RootScope == "" {
		return durable.RunRecord{}, invalidRecord(ErrInvalidRecordRoot)
	}
	if r.Scopes == nil || r.Attempts == nil {
		return durable.RunRecord{}, invalidRecord(ErrInvalidRecordShapes)
	}
	return r, nil
}
func (c *Coordinator) Submit(ctx context.Context, t d.Task) (d.SubmitResult, error) {
	t, e := d.ParseTask(t.Bytes())
	if e != nil || !identity(t.Identity()) {
		return d.SubmitResult{}, d.ErrInvalid
	}
	a := args(t.Identity())
	a["task"] = t.Bytes()
	a["digest"] = t.SubmissionDigest()
	b, e := c.call(ctx, "submit", a, true)
	if e != nil {
		return d.SubmitResult{}, e
	}
	var w struct {
		Snapshot     json.RawMessage `json:"snapshot"`
		Deduplicated bool            `json:"deduplicated"`
	}
	if json.Unmarshal(b, &w) != nil {
		return d.SubmitResult{}, d.ErrInvalid
	}
	s, _, _, e := decodeSnapshot(w.Snapshot)
	if e != nil {
		return d.SubmitResult{}, e
	}
	if _, e = d.CompareSubmission(s.Task, t); e != nil {
		return d.SubmitResult{}, e
	}
	return d.SubmitResult{Snapshot: s, Deduplicated: w.Deduplicated}, nil
}
func (c *Coordinator) Get(ctx context.Context, i d.Identity) (d.Snapshot, error) {
	if !identity(i) {
		return d.Snapshot{}, d.ErrInvalid
	}
	b, e := c.call(ctx, "get", args(i), false)
	if e != nil {
		return d.Snapshot{}, e
	}
	s, _, _, e := decodeSnapshot(b)
	if e == nil && s.Task.Identity() != i {
		e = d.ErrInvalid
	}
	return s, e
}
func (c *Coordinator) List(ctx context.Context, q d.ListRequest) ([]d.Snapshot, error) {
	if !validText(q.Namespace) || q.Limit < 1 || q.Limit > 1024 || len(q.AfterRunID) > 256 || !utf8.ValidString(q.AfterRunID) {
		return nil, d.ErrInvalid
	}
	a := args(d.Identity{Namespace: q.Namespace})
	a["limit"] = q.Limit
	a["status"] = q.Status
	a["after"] = q.AfterRunID
	b, e := c.call(ctx, "list", a, false)
	if e != nil {
		return nil, e
	}
	var rows []json.RawMessage
	if json.Unmarshal(b, &rows) != nil {
		return nil, d.ErrInvalid
	}
	out := make([]d.Snapshot, 0, len(rows))
	for _, r := range rows {
		s, _, _, e := decodeSnapshot(r)
		if e != nil {
			return nil, e
		}
		if s.Task.Identity().Namespace != q.Namespace {
			return nil, d.ErrInvalid
		}
		out = append(out, s)
	}
	return out, nil
}
func claims(b []byte, ns string, w d.Worker) ([]d.Claim, error) {
	var rows []json.RawMessage
	if json.Unmarshal(b, &rows) != nil {
		return nil, d.ErrInvalid
	}
	out := make([]d.Claim, 0, len(rows))
	for _, r := range rows {
		s, t, _, e := decodeSnapshot(r)
		if e != nil {
			return nil, e
		}
		if s.Status != d.Running || t.Worker != w || t.Identity.Namespace != ns {
			return nil, d.ErrInvalid
		}
		out = append(out, d.Claim{Snapshot: s, Token: t})
	}
	return out, nil
}
func (c *Coordinator) Claim(ctx context.Context, q d.ClaimRequest) ([]d.Claim, error) {
	if !validText(q.Namespace) || !validWorker(q.Worker) || !ttl(q.TTL) || q.Limit < 1 || q.Limit > 1024 || len(q.LeaseIDs) != q.Limit || len(q.Capabilities) == 0 || len(q.Capabilities) > 4096 {
		return nil, d.ErrInvalid
	}
	seen := map[string]bool{}
	for _, id := range q.LeaseIDs {
		if !lease(id) || seen[id] {
			return nil, d.ErrInvalid
		}
		seen[id] = true
	}
	a := args(d.Identity{Namespace: q.Namespace})
	a["worker_id"] = q.Worker.ID
	a["boot_id"] = q.Worker.BootID
	a["ttl_us"] = q.TTL.Microseconds()
	a["limit"] = q.Limit
	a["lease_ids"] = q.LeaseIDs
	a["capabilities"] = q.Capabilities
	b, e := c.call(ctx, "claim", a, true)
	if e != nil {
		return nil, e
	}
	out, e := claims(b, q.Namespace, q.Worker)
	if e != nil {
		return nil, e
	}
	for _, r := range out {
		if !seen[r.Token.LeaseID] {
			return nil, d.ErrInvalid
		}
		match := false
		for _, route := range q.Capabilities {
			if r.Snapshot.Task.Route() == route {
				match = true
			}
		}
		if !match {
			return nil, d.ErrInvalid
		}
	}
	return out, nil
}
func (c *Coordinator) FindClaim(ctx context.Context, ns string, w d.Worker, id string) ([]d.Claim, error) {
	if !validText(ns) || !validWorker(w) || !lease(id) {
		return nil, d.ErrInvalid
	}
	a := args(d.Identity{Namespace: ns})
	a["worker_id"] = w.ID
	a["boot_id"] = w.BootID
	a["lease_id"] = id
	b, e := c.call(ctx, "find", a, false)
	if e != nil {
		return nil, e
	}
	out, e := claims(b, ns, w)
	for _, r := range out {
		if r.Token.LeaseID != id {
			return nil, d.ErrInvalid
		}
	}
	return out, e
}
func (c *Coordinator) ownerCall(ctx context.Context, op string, t d.Token, a map[string]any, write bool) (d.Snapshot, []byte, error) {
	if !token(t) {
		return d.Snapshot{}, nil, d.ErrInvalid
	}
	b, e := c.call(ctx, op, a, write)
	if e != nil {
		if errors.Is(e, d.ErrNotFound) {
			e = d.ErrLeaseLost
		}
		c.observeReject(t, e)
		return d.Snapshot{}, nil, e
	}
	s, _, record, e := decodeSnapshot(b)
	if e == nil && s.Task.Identity() != t.Identity {
		e = d.ErrInvalid
	}
	return s, record, e
}
func (c *Coordinator) Renew(ctx context.Context, t d.Token, extension time.Duration) (time.Time, error) {
	if !ttl(extension) {
		return time.Time{}, d.ErrInvalid
	}
	a := owned(t)
	a["ttl_us"] = extension.Microseconds()
	s, _, e := c.ownerCall(ctx, "renew", t, a, true)
	if e != nil {
		c.discardProjection(t)
	}
	return s.LeaseUntil, e
}

// Finish trusts the runtime's Outcome witness. This enum is not an unforgeable
// capability: only the trusted Runtime may request a business terminal status.
func (c *Coordinator) Finish(ctx context.Context, q d.FinishRequest) (d.Snapshot, error) {
	defer c.discardProjection(q.Token)
	if !token(q.Token) || !mutation(q.Mutation) || (q.Outcome != durable.OutcomeCompleted && q.Outcome != durable.OutcomeFailed) {
		return d.Snapshot{}, d.ErrInvalid
	}
	a := owned(q.Token)
	mutate(a, q.Mutation)
	a["expected"] = strconv.FormatUint(q.ExpectedRevision, 10)
	a["outcome"] = q.Outcome
	s, _, e := c.ownerCall(ctx, "finish", q.Token, a, true)
	return s, e
}
func (c *Coordinator) Release(ctx context.Context, t d.Token, m d.Mutation) error {
	defer c.discardProjection(t)
	if !mutation(m) {
		return d.ErrInvalid
	}
	a := owned(t)
	mutate(a, m)
	_, _, e := c.ownerCall(ctx, "release", t, a, true)
	return e
}
func (c *Coordinator) Block(ctx context.Context, t d.Token, reason string, m d.Mutation) error {
	defer c.discardProjection(t)
	if !mutation(m) {
		return d.ErrInvalid
	}
	a := owned(t)
	mutate(a, m)
	a["reason"] = reason
	_, _, e := c.ownerCall(ctx, "block", t, a, true)
	return e
}
func (c *Coordinator) Cancel(ctx context.Context, i d.Identity, m d.Mutation) (d.Snapshot, error) {
	defer c.discardIdentity(i)
	if !identity(i) || !mutation(m) {
		return d.Snapshot{}, d.ErrInvalid
	}
	a := args(i)
	mutate(a, m)
	b, e := c.call(ctx, "cancel", a, true)
	if e != nil {
		return d.Snapshot{}, e
	}
	s, _, _, e := decodeSnapshot(b)
	if e == nil && s.Task.Identity() != i {
		e = d.ErrInvalid
	}
	return s, e
}
func (c *Coordinator) observeReject(t d.Token, err error) {
	reason := ""
	switch {
	case errors.Is(err, d.ErrLeaseLost):
		reason = "lease_lost"
	case errors.Is(err, d.ErrRevisionConflict):
		reason = "revision_conflict"
	case errors.Is(err, d.ErrTerminal):
		reason = "terminal"
	case errors.Is(err, d.ErrInvalid):
		reason = "invalid"
	}
	if reason == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	a := owned(t)
	a["reason"] = reason
	_, _ = c.call(ctx, "reject", a, true)
}
func digest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
