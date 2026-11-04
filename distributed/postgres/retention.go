package postgres

import (
	"context"
	"encoding/json"
	"errors"
	d "github.com/Luo-root/pulse-durable/distributed"
	"strconv"
	"time"
)

type retentionPolicyWire struct {
	Enabled            bool
	TerminalPayloadTTL *int64
	ReplayHistoryTTL   *int64
	EventTTL           *int64
	ReceiptTTL         *int64
}

func retentionPolicyJSON(p d.RetentionPolicy) ([]byte, error) {
	q, e := p.Validate()
	if e != nil {
		return nil, e
	}
	w := map[string]any{"Enabled": q.Enabled, "EventTTL": nil, "ReceiptTTL": nil, "ReplayHistoryTTL": nil, "TerminalPayloadTTL": nil}
	for _, x := range []struct {
		key string
		v   *time.Duration
	}{{"TerminalPayloadTTL", q.TerminalPayloadTTL}, {"ReplayHistoryTTL", q.ReplayHistoryTTL}, {"EventTTL", q.EventTTL}, {"ReceiptTTL", q.ReceiptTTL}} {
		if x.v != nil {
			w[x.key] = int64(*x.v)
		}
	}
	return json.Marshal(w)
}
func retentionPolicyArgs(p d.RetentionPolicy) (map[string]any, string, error) {
	b, e := retentionPolicyJSON(p)
	if e != nil {
		return nil, "", e
	}
	dg := digest(b)
	var w retentionPolicyWire
	_ = json.Unmarshal(b, &w)
	return map[string]any{"Enabled": w.Enabled, "TerminalPayloadTTL": w.TerminalPayloadTTL, "ReplayHistoryTTL": w.ReplayHistoryTTL, "EventTTL": w.EventTTL, "ReceiptTTL": w.ReceiptTTL}, dg, nil
}
func decodeRetentionPolicy(v json.RawMessage) (d.RetentionPolicy, error) {
	var w retentionPolicyWire
	if json.Unmarshal(v, &w) != nil {
		return d.RetentionPolicy{}, d.ErrInvalid
	}
	p := d.RetentionPolicy{Enabled: w.Enabled}
	for _, x := range []struct {
		v *int64
		o **time.Duration
	}{{w.TerminalPayloadTTL, &p.TerminalPayloadTTL}, {w.ReplayHistoryTTL, &p.ReplayHistoryTTL}, {w.EventTTL, &p.EventTTL}, {w.ReceiptTTL, &p.ReceiptTTL}} {
		if x.v != nil {
			z := time.Duration(*x.v)
			*x.o = &z
		}
	}
	_, e := p.Validate()
	return p, e
}

func cursorValid(c d.RetentionCursor) bool {
	return (!c.Set && c.Value == "") || (c.Set && validText(c.Value))
}

type RetentionPolicyResult struct {
	Namespace     string            `json:"namespace"`
	PolicyVersion string            `json:"policy_version"`
	Policy        d.RetentionPolicy `json:"policy"`
	PolicyDigest  string            `json:"policy_digest"`
	UpdatedAt     time.Time         `json:"updated_at"`
}

func decodeRetentionPolicyResult(b []byte, expectedNamespace string) (RetentionPolicyResult, error) {
	var raw struct {
		Namespace     string          `json:"namespace"`
		PolicyVersion string          `json:"policy_version"`
		Policy        json.RawMessage `json:"policy"`
		PolicyDigest  string          `json:"policy_digest"`
		UpdatedAt     time.Time       `json:"updated_at"`
	}
	if json.Unmarshal(b, &raw) != nil || raw.Namespace != expectedNamespace || raw.UpdatedAt.IsZero() || !fingerprint(raw.PolicyDigest) || raw.PolicyVersion == "" || (raw.PolicyVersion[0] == '0') {
		return RetentionPolicyResult{}, d.ErrInvalid
	}
	p, e := decodeRetentionPolicy(raw.Policy)
	if e != nil {
		return RetentionPolicyResult{}, e
	}
	n, e := strconv.ParseUint(raw.PolicyVersion, 10, 64)
	if e != nil || n == 0 {
		return RetentionPolicyResult{}, d.ErrInvalid
	}
	a, dg, e := retentionPolicyArgs(p)
	_ = a
	if e != nil || dg != raw.PolicyDigest {
		return RetentionPolicyResult{}, d.ErrInvalid
	}
	return RetentionPolicyResult{raw.Namespace, raw.PolicyVersion, p, raw.PolicyDigest, raw.UpdatedAt}, nil
}
func (c *Coordinator) retentionCall(ctx context.Context, fn string, a map[string]any, write bool) ([]byte, error) {
	var q string
	switch fn {
	case "retention_policy_get":
		q = "SELECT pulse_durable_distributed.retention_policy_get($1::jsonb)"
	case "retention_policy_preview":
		q = "SELECT pulse_durable_distributed.retention_policy_preview($1::jsonb)"
	case "retention_policy_update":
		q = "SELECT pulse_durable_distributed.retention_policy_update($1::jsonb)"
	default:
		return nil, d.ErrInvalid
	}
	ctx, cancel, e := bounded(ctx)
	if e != nil {
		return nil, e
	}
	defer cancel()
	b, e := json.Marshal(a)
	if e != nil {
		return nil, d.ErrInvalid
	}
	var out []byte
	if c == nil || c.pool == nil {
		return nil, d.ErrInvalid
	}
	e = c.pool.QueryRow(ctx, q, b).Scan(&out)
	return out, scrub(e, write)
}
func (c *Coordinator) GetRetentionPolicy(ctx context.Context, ns string) (RetentionPolicyResult, error) {
	if !validText(ns) {
		return RetentionPolicyResult{}, d.ErrInvalid
	}
	b, e := c.retentionCall(ctx, "retention_policy_get", map[string]any{"namespace": ns}, false)
	if e != nil {
		return RetentionPolicyResult{}, e
	}
	return decodeRetentionPolicyResult(b, ns)
}
func (c *Coordinator) UpdateRetentionPolicy(ctx context.Context, q d.RetentionPolicyUpdate) (RetentionPolicyResult, error) {
	if !validText(q.Namespace) || q.ExpectedVersion == 0 {
		return RetentionPolicyResult{}, d.ErrInvalid
	}
	p, dg, e := retentionPolicyArgs(q.Policy)
	if e != nil {
		return RetentionPolicyResult{}, e
	}
	a := map[string]any{"namespace": q.Namespace, "expected_version": strconv.FormatUint(q.ExpectedVersion, 10), "policy": p, "policy_digest": dg}
	if q.DryRunID != "" {
		a["dry_run_id"] = q.DryRunID
	}
	b, e := c.retentionCall(ctx, "retention_policy_update", a, true)
	if e != nil {
		return RetentionPolicyResult{}, e
	}
	return decodeRetentionPolicyResult(b, q.Namespace)
}

type RetentionPolicyPreview struct {
	DryRunID        string
	Namespace       string
	PolicyVersion   uint64
	Policy          d.RetentionPolicy
	PolicyDigest    string
	ObservedFrom    time.Time
	ObservedThrough time.Time
	Summary         d.RetentionCandidateSummary
	Cursor          d.RetentionCursor
	NextCursor      d.RetentionCursor
	ReceiptDigest   string
}

func (c *Coordinator) PreviewRetentionPolicy(ctx context.Context, id, ns string, ver uint64, p d.RetentionPolicy, cursor d.RetentionCursor, limit uint64, from, through time.Time) (RetentionPolicyPreview, error) {
	if !validText(id) || !validText(ns) || ver == 0 || !cursorValid(cursor) || limit == 0 || limit > 1000000 || from.IsZero() || through.Before(from) {
		return RetentionPolicyPreview{}, d.ErrInvalid
	}
	w, dg, e := retentionPolicyArgs(p)
	if e != nil {
		return RetentionPolicyPreview{}, e
	}
	a := map[string]any{"dry_run_id": id, "namespace": ns, "policy_version": strconv.FormatUint(ver, 10), "policy": w, "policy_digest": dg, "observed_from": from, "observed_through": through, "cursor": cursor, "limit": limit}
	b, e := c.retentionCall(ctx, "retention_policy_preview", a, false)
	if e != nil {
		return RetentionPolicyPreview{}, e
	}
	var r struct {
		DryRunID, Namespace, PolicyVersion, PolicyDigest, ReceiptDigest string
		Policy                                                          json.RawMessage
		ObservedFrom, ObservedThrough                                   time.Time
		Summary                                                         d.RetentionCandidateSummary
		Cursor, NextCursor                                              d.RetentionCursor
	}
	if json.Unmarshal(b, &r) != nil {
		return RetentionPolicyPreview{}, d.ErrInvalid
	}
	n, ee := strconv.ParseUint(r.PolicyVersion, 10, 64)
	if ee != nil {
		return RetentionPolicyPreview{}, d.ErrInvalid
	}
	pp, ee := decodeRetentionPolicy(r.Policy)
	q := d.RetentionDryRun{Namespace: ns, PolicyVersion: ver, Policy: p, ObservedFrom: from, ObservedThrough: through, Cursor: cursor, Limit: limit}
	full := d.RetentionDryRunResult{DryRunID: r.DryRunID, Namespace: r.Namespace, PolicyVersion: n, Policy: pp, PolicyDigest: r.PolicyDigest, ObservedFrom: r.ObservedFrom, ObservedThrough: r.ObservedThrough, Summary: r.Summary, Cursor: r.Cursor, NextCursor: r.NextCursor, ReceiptDigest: r.ReceiptDigest}
	if ee != nil || full.Validate(q) != nil {
		return RetentionPolicyPreview{}, errors.Join(d.ErrInvalid, ee)
	}
	return RetentionPolicyPreview{r.DryRunID, r.Namespace, n, pp, r.PolicyDigest, r.ObservedFrom, r.ObservedThrough, r.Summary, r.Cursor, r.NextCursor, r.ReceiptDigest}, nil
}
