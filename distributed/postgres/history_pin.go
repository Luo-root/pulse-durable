package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"time"

	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/jackc/pgx/v5/pgconn"
)

var _ d.HistoryPinner = (*Coordinator)(nil)
var _ d.HistoryPinMutationReader = (*Coordinator)(nil)

// 整数在传输层使用十进制字符串，摘要仍遵循契约的 JSON 整数编码。
func pinArgs(i d.Identity, id string, m d.Mutation, holder string, rev, version uint64, chain string, ttl *time.Duration) map[string]any {
	a := args(i)
	a["pin_id"] = id
	mutate(a, m)
	a["holder_digest"] = holder
	a["revision"] = strconv.FormatUint(rev, 10)
	a["expected_version"] = strconv.FormatUint(version, 10)
	a["chain"] = chain
	a["ttl_ns"] = nil
	if ttl != nil {
		a["ttl_ns"] = strconv.FormatInt(int64(*ttl), 10)
	}
	return a
}
func pinError(err error, write bool) error {
	if err == nil {
		return nil
	}
	var pe *pgconn.PgError
	if errors.As(err, &pe) {
		switch pe.Code {
		case "PH001", "22008", "22015":
			return d.ErrPinInvalid
		case "PH002":
			return d.ErrPinConflict
		case "PH003":
			return d.ErrPinNotFound
		case "PH004":
			return d.ErrPinExpired
		case "PH005":
			return d.ErrPinUnknown
		}
	}
	e := scrub(err, write)
	if errors.Is(e, d.ErrUnknown) {
		return d.ErrPinUnknown
	}
	if errors.Is(e, d.ErrInvalid) {
		return d.ErrPinInvalid
	}
	return e
}

// 固定白名单，不拼接调用方传入的 SQL 标识符。
func (c *Coordinator) pinCall(ctx context.Context, op string, a map[string]any, write bool) ([]byte, error) {
	var query string
	switch op {
	case "create":
		query = "SELECT pulse_durable_distributed.pin_create($1::jsonb)"
	case "renew":
		query = "SELECT pulse_durable_distributed.pin_renew($1::jsonb)"
	case "release":
		query = "SELECT pulse_durable_distributed.pin_release($1::jsonb)"
	case "find":
		query = "SELECT pulse_durable_distributed.pin_find($1::jsonb)"
	case "find_mutation":
		query = "SELECT pulse_durable_distributed.pin_find_mutation($1::jsonb)"
	default:
		return nil, d.ErrPinInvalid
	}
	ctx, cancel, err := bounded(ctx)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if c == nil || c.pool == nil {
		return nil, d.ErrPinInvalid
	}
	b, err := json.Marshal(a)
	if err != nil {
		return nil, d.ErrPinInvalid
	}
	var out []byte
	err = c.pool.QueryRow(ctx, query, b).Scan(&out)
	return out, pinError(err, write)
}

type pinWireMutation struct {
	ID     string `json:"id"`
	Digest string `json:"digest"`
}

func (m pinWireMutation) value() d.Mutation { return d.Mutation{ID: m.ID, Digest: m.Digest} }

type pinWireState struct {
	PinID        string          `json:"pin_id"`
	Identity     d.Identity      `json:"identity"`
	Revision     string          `json:"revision"`
	Chain        string          `json:"chain"`
	Version      string          `json:"version"`
	LongLived    bool            `json:"long_lived"`
	ExpiresAt    *time.Time      `json:"expires_at"`
	HolderDigest string          `json:"holder_digest"`
	Mutation     pinWireMutation `json:"mutation"`
}
type pinWireResult struct {
	Status d.PinStatus   `json:"status"`
	State  *pinWireState `json:"state"`
}

func (w pinWireResult) decode(i d.Identity, id string) (d.PinResult, error) {
	out := d.PinResult{Status: w.Status}
	if w.State != nil {
		s := w.State
		rev, ok := positiveDecimal(s.Revision)
		if !ok {
			return d.PinResult{}, d.ErrPinUnknown
		}
		ver, ok := positiveDecimal(s.Version)
		if !ok {
			return d.PinResult{}, d.ErrPinUnknown
		}
		if s.Identity != i || s.PinID != id || s.LongLived != (s.ExpiresAt == nil) {
			return d.PinResult{}, d.ErrPinUnknown
		}
		state := d.PinState{PinID: s.PinID, Identity: s.Identity, Revision: rev, Chain: s.Chain, Version: ver, LongLived: s.LongLived, HolderDigest: s.HolderDigest, Mutation: s.Mutation.value()}
		if s.ExpiresAt != nil {
			state.ExpiresAt = *s.ExpiresAt
		}
		out.State = &state
	}
	if out.ValidateFindOutcome(nil) != nil {
		return d.PinResult{}, d.ErrPinUnknown
	}
	return out, nil
}

// 每一层必须包含全部协议字段；null 只能用于可空指针。
func strictPinJSON(b []byte, out any) error {
	if strictMaintenanceJSON(b, out) != nil {
		return d.ErrPinUnknown
	}
	var v any
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if dec.Decode(&v) != nil {
		return d.ErrPinUnknown
	}
	var shape func(any, reflect.Type) bool
	shape = func(v any, t reflect.Type) bool {
		if t.Kind() == reflect.Pointer {
			return v == nil || shape(v, t.Elem())
		}
		if v == nil {
			return false
		}
		if t == reflect.TypeOf(time.Time{}) {
			_, ok := v.(string)
			return ok
		}
		if t.Kind() != reflect.Struct {
			return true
		}
		obj, ok := v.(map[string]any)
		if !ok || len(obj) != t.NumField() {
			return false
		}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			name := f.Tag.Get("json")
			if name == "" {
				name = f.Name
			}
			value, ok := obj[name]
			if !ok || !shape(value, f.Type) {
				return false
			}
		}
		return true
	}
	if !shape(v, reflect.TypeOf(out).Elem()) {
		return d.ErrPinUnknown
	}
	return nil
}
func decodePinResult(b []byte, i d.Identity, id string) (d.PinResult, error) {
	var w pinWireResult
	if strictPinJSON(b, &w) != nil {
		return d.PinResult{}, d.ErrPinUnknown
	}
	return w.decode(i, id)
}
func (c *Coordinator) PinHistory(ctx context.Context, q d.PinRequest) (d.PinResult, error) {
	q, e := q.Validate()
	if e != nil {
		return d.PinResult{}, e
	}
	b, e := c.pinCall(ctx, "create", pinArgs(q.Identity, q.PinID, q.Mutation, q.HolderDigest, q.Revision, 0, q.Chain, q.TTL), true)
	if e != nil {
		return d.PinResult{}, e
	}
	out, e := decodePinResult(b, q.Identity, q.PinID)
	if e != nil || out.Status != d.PinActive || out.State == nil || out.State.Revision != q.Revision || out.State.Chain != q.Chain || out.State.Mutation != q.Mutation || out.State.Version != 1 || out.State.HolderDigest != q.HolderDigest || out.State.LongLived != (q.TTL == nil) {
		return d.PinResult{}, d.ErrPinUnknown
	}
	return out, nil
}
func (c *Coordinator) RenewPin(ctx context.Context, q d.RenewPinRequest) (d.PinResult, error) {
	q, e := q.Validate()
	if e != nil {
		return d.PinResult{}, e
	}
	b, e := c.pinCall(ctx, "renew", pinArgs(q.Identity, q.PinID, q.Mutation, q.HolderDigest, 0, q.ExpectedVersion, "", q.TTL), true)
	if e != nil {
		return d.PinResult{}, e
	}
	out, e := decodePinResult(b, q.Identity, q.PinID)
	next, ve := d.NextPinVersion(q.ExpectedVersion)
	if e != nil || ve != nil || out.Status != d.PinActive || out.State == nil || out.State.Version != next || out.State.HolderDigest != q.HolderDigest || q.TTL != nil && out.State.LongLived {
		return d.PinResult{}, d.ErrPinUnknown
	}
	return out, nil
}
func (c *Coordinator) ReleasePin(ctx context.Context, q d.ReleasePinRequest) error {
	if e := q.Validate(); e != nil {
		return e
	}
	b, e := c.pinCall(ctx, "release", pinArgs(q.Identity, q.PinID, q.Mutation, q.HolderDigest, 0, q.ExpectedVersion, "", nil), true)
	if e != nil {
		return e
	}
	out, e := decodePinResult(b, q.Identity, q.PinID)
	next, ve := d.NextPinVersion(q.ExpectedVersion)
	if e != nil || ve != nil || out.Status != d.PinReleased || out.State == nil || out.State.Version != next {
		return d.ErrPinUnknown
	}
	return nil
}
func (c *Coordinator) FindPin(ctx context.Context, q d.PinQuery) (d.PinResult, error) {
	if e := q.Validate(); e != nil {
		return d.PinResult{}, e
	}
	a := args(q.Identity)
	a["pin_id"] = q.PinID
	b, e := c.pinCall(ctx, "find", a, false)
	if e != nil {
		return d.PinResult{}, e
	}
	return decodePinResult(b, q.Identity, q.PinID)
}

type pinWireReceipt struct {
	Identity      d.Identity      `json:"identity"`
	PinID         string          `json:"pin_id"`
	Mutation      pinWireMutation `json:"mutation"`
	Operation     d.PinOperation  `json:"operation"`
	Version       string          `json:"version"`
	OccurredAt    time.Time       `json:"occurred_at"`
	RequestDigest string          `json:"request_digest"`
	Result        pinWireResult   `json:"result"`
}

func decodePinMutation(b []byte, q d.PinMutationQuery) (d.PinMutationResult, error) {
	var w struct {
		Status  d.PinMutationStatus `json:"status"`
		Receipt *pinWireReceipt     `json:"receipt"`
	}
	if strictPinJSON(b, &w) != nil {
		return d.PinMutationResult{}, d.ErrPinUnknown
	}
	out := d.PinMutationResult{Status: w.Status}
	if w.Receipt != nil {
		v := w.Receipt
		version, ok := positiveDecimal(v.Version)
		if !ok {
			return d.PinMutationResult{}, d.ErrPinUnknown
		}
		result, e := v.Result.decode(q.Identity, q.PinID)
		if e != nil {
			return d.PinMutationResult{}, e
		}
		out.Receipt = &d.PinMutationReceipt{Identity: v.Identity, PinID: v.PinID, Mutation: v.Mutation.value(), Operation: v.Operation, Version: version, OccurredAt: v.OccurredAt, RequestDigest: v.RequestDigest, Result: result}
	}
	if out.ValidateOutcome(q, nil) != nil {
		return d.PinMutationResult{}, d.ErrPinUnknown
	}
	return out, nil
}
func (c *Coordinator) FindPinMutation(ctx context.Context, q d.PinMutationQuery) (d.PinMutationResult, error) {
	if e := q.Validate(); e != nil {
		return d.PinMutationResult{}, e
	}
	a := args(q.Identity)
	a["pin_id"] = q.PinID
	mutate(a, q.Mutation)
	a["operation"] = q.Operation
	b, e := c.pinCall(ctx, "find_mutation", a, false)
	if e != nil {
		return d.PinMutationResult{}, e
	}
	return decodePinMutation(b, q)
}
