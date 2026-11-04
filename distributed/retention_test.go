package distributed

import (
	"errors"
	"math"
	"strings"
	"testing"
	"time"
)

func retentionTTL(v time.Duration) *time.Duration { return &v }

func TestRetentionPolicyValidation(t *testing.T) {
	zero, err := (RetentionPolicy{}).Validate()
	if err != nil || zero.Enabled || zero.TerminalPayloadTTL != nil || zero.ReplayHistoryTTL != nil || zero.EventTTL != nil || zero.ReceiptTTL != nil {
		t.Fatalf("unsafe default: %+v, %v", zero, err)
	}
	for _, value := range []time.Duration{0, -time.Hour, time.Hour - 1, time.Hour + 1, time.Duration(math.MinInt64), time.Duration(math.MaxInt64)} {
		for field := 0; field < 4; field++ {
			p := RetentionPolicy{}
			fields := []**time.Duration{&p.TerminalPayloadTTL, &p.ReplayHistoryTTL, &p.EventTTL, &p.ReceiptTTL}
			*fields[field] = retentionTTL(value)
			if _, err := p.Validate(); !errors.Is(err, ErrInvalid) {
				t.Errorf("field %d accepted invalid TTL %v: %v", field, value, err)
			}
		}
	}
	// 最大的整秒 duration 在验证期间不得溢出。
	largest := time.Duration(math.MaxInt64) / time.Second * time.Second
	if _, err := (RetentionPolicy{ReceiptTTL: &largest}).Validate(); err != nil {
		t.Fatal(err)
	}
	// Runtime duration 算术溢出为负值时必须拒绝。
	overflow := time.Duration(math.MaxInt64)
	overflow += time.Second
	if _, err := (RetentionPolicy{EventTTL: &overflow}).Validate(); err == nil {
		t.Fatal("accepted wrapped negative duration")
	}
}

func TestRetentionPolicyIsolation(t *testing.T) {
	shared := time.Hour
	p := RetentionPolicy{true, &shared, &shared, &shared, &shared}
	out, err := p.Validate()
	if err != nil {
		t.Fatal(err)
	}
	shared = 2 * time.Hour
	fields := []*time.Duration{out.TerminalPayloadTTL, out.ReplayHistoryTTL, out.EventTTL, out.ReceiptTTL}
	for i, field := range fields {
		if *field != time.Hour || field == &shared {
			t.Fatalf("field %d aliases input", i)
		}
		*field = time.Duration(i+3) * time.Hour
	}
	if *out.TerminalPayloadTTL != 3*time.Hour || *out.ReplayHistoryTTL != 4*time.Hour || *out.EventTTL != 5*time.Hour || *out.ReceiptTTL != 6*time.Hour || shared != 2*time.Hour {
		t.Fatal("output fields alias each other or input")
	}
}

func TestRetentionContractions(t *testing.T) {
	for _, tc := range []struct {
		name      string
		old, next RetentionPolicy
		want      bool
	}{
		{"unchanged", RetentionPolicy{}, RetentionPolicy{}, false},
		{"enable", RetentionPolicy{}, RetentionPolicy{Enabled: true}, true},
		{"disable", RetentionPolicy{Enabled: true}, RetentionPolicy{}, false},
		{"infinite to finite", RetentionPolicy{}, RetentionPolicy{ReceiptTTL: retentionTTL(time.Hour)}, true},
		{"finite to infinite", RetentionPolicy{ReceiptTTL: retentionTTL(time.Hour)}, RetentionPolicy{}, false},
		{"shorter", RetentionPolicy{EventTTL: retentionTTL(2 * time.Hour)}, RetentionPolicy{EventTTL: retentionTTL(time.Hour)}, true},
		{"longer", RetentionPolicy{ReplayHistoryTTL: retentionTTL(time.Hour)}, RetentionPolicy{ReplayHistoryTTL: retentionTTL(2 * time.Hour)}, false},
		{"equal", RetentionPolicy{TerminalPayloadTTL: retentionTTL(time.Hour)}, RetentionPolicy{TerminalPayloadTTL: retentionTTL(time.Hour)}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.old.RequiresDryRun(tc.next)
			if err != nil || got != tc.want {
				t.Fatalf("got %v, %v; want %v", got, err, tc.want)
			}
		})
	}
	bad := RetentionPolicy{ReceiptTTL: retentionTTL(0)}
	if _, err := bad.RequiresDryRun(RetentionPolicy{}); err == nil {
		t.Fatal("invalid current policy")
	}
	if _, err := (RetentionPolicy{}).RequiresDryRun(bad); err == nil {
		t.Fatal("invalid next policy")
	}
	// 每个 TTL 都独立参与收缩检测。
	for field := 0; field < 4; field++ {
		p := RetentionPolicy{}
		fields := []**time.Duration{&p.TerminalPayloadTTL, &p.ReplayHistoryTTL, &p.EventTTL, &p.ReceiptTTL}
		*fields[field] = retentionTTL(time.Hour)
		if got, err := (RetentionPolicy{}).RequiresDryRun(p); err != nil || !got {
			t.Fatalf("field %d: %v, %v", field, got, err)
		}
	}
}

func TestRetentionPolicyCAS(t *testing.T) {
	q := RetentionPolicyUpdate{Namespace: "ns", ExpectedVersion: 7, Policy: RetentionPolicy{Enabled: true}}
	if _, err := q.Validate(7, RetentionPolicy{}); !errors.Is(err, ErrInvalid) {
		t.Fatal("contraction without preview", err)
	}
	q.DryRunID = "separate-preview-reference"
	if _, err := q.Validate(7, RetentionPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Validate(8, RetentionPolicy{}); !errors.Is(err, ErrRevisionConflict) {
		t.Fatal(err)
	}
	if _, err := q.Validate(0, RetentionPolicy{}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	for _, change := range []func(*RetentionPolicyUpdate){
		func(q *RetentionPolicyUpdate) { q.ExpectedVersion = 0 },
		func(q *RetentionPolicyUpdate) { q.Namespace = "" },
		func(q *RetentionPolicyUpdate) { q.DryRunID = strings.Repeat("x", MaxTextBytes+1) },
		func(q *RetentionPolicyUpdate) { q.Policy.EventTTL = retentionTTL(0) },
	} {
		bad := q
		change(&bad)
		if _, err := bad.Validate(7, RetentionPolicy{}); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	q.Policy.ReceiptTTL = retentionTTL(time.Hour)
	out, err := q.Validate(7, RetentionPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	*out.ReceiptTTL = 2 * time.Hour
	if *q.Policy.ReceiptTTL != time.Hour {
		t.Fatal("CAS output aliases request")
	}
}

func retentionQuery() MutationQuery {
	start := time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC)
	epoch, revision := int64(3), uint64(4)
	return MutationQuery{Identity: Identity{"ns", "run"}, Mutation: Mutation{"mutation", strings.Repeat("a", 64)}, Operation: "apply", ExpectedEpoch: &epoch, ExpectedRevision: &revision, RequestedWindow: MutationWindow{start, start.Add(time.Hour)}}
}

func TestMutationQueryValidation(t *testing.T) {
	q := retentionQuery()
	for _, op := range []string{"initialize", "apply", "finish", "release", "block", "cancel"} {
		q.Operation = op
		if err := q.Validate(); err != nil {
			t.Fatal(op, err)
		}
	}
	for _, change := range []func(*MutationQuery){
		func(q *MutationQuery) { q.Identity.Namespace = "" },
		func(q *MutationQuery) { q.Identity.RunID = "" },
		func(q *MutationQuery) { q.Mutation.ID = "" },
		func(q *MutationQuery) { q.Mutation.Digest = "bad" },
		func(q *MutationQuery) { q.Operation = "unknown" },
		func(q *MutationQuery) { negative := int64(-1); q.ExpectedEpoch = &negative },
		func(q *MutationQuery) { q.RequestedWindow.From = time.Time{} },
		func(q *MutationQuery) { q.RequestedWindow.Through = time.Time{} },
		func(q *MutationQuery) { q.RequestedWindow.Through = q.RequestedWindow.From.Add(-time.Nanosecond) },
	} {
		bad := q
		change(&bad)
		if !errors.Is(bad.Validate(), ErrInvalid) {
			t.Fatalf("accepted %+v", bad)
		}
	}
	epoch, revision := int64(0), uint64(0)
	q.ExpectedEpoch, q.ExpectedRevision = &epoch, &revision
	q.RequestedWindow.Through = q.RequestedWindow.From.In(time.FixedZone("UTC+8", 8*3600))
	if err := q.Validate(); err != nil {
		t.Fatal("equal fixed-zone instant", err)
	}
}

func TestMutationQueryOptionalMatches(t *testing.T) {
	q := retentionQuery()
	q.Operation = "cancel"
	q.ExpectedEpoch, q.ExpectedRevision = nil, nil
	receipt := MutationReceipt{q.Identity, q.Mutation, q.Operation, 7, 12, q.RequestedWindow.From}
	result := MutationQueryResult{Found, q.RequestedWindow, &receipt}
	if err := result.Validate(q); err != nil {
		t.Fatal("Cancel with unknown committed epoch/revision", err)
	}
	receipt.Epoch = -1
	if err := result.Validate(q); !errors.Is(err, ErrInvalid) {
		t.Fatal("negative receipt epoch with unknown expectations", err)
	}
	receipt.Epoch, receipt.Revision = 0, 0
	epoch, revision := int64(0), uint64(0)
	q.ExpectedEpoch, q.ExpectedRevision = &epoch, &revision
	if err := result.Validate(q); err != nil {
		t.Fatal("explicit exact zero", err)
	}
	receipt.Epoch = 1
	if err := result.Validate(q); !errors.Is(err, ErrInvalid) {
		t.Fatal("zero epoch treated as wildcard", err)
	}
	q.ExpectedEpoch = nil
	if err := result.Validate(q); err != nil {
		t.Fatal("unknown epoch with exact zero revision", err)
	}
	receipt.Revision = 1
	if err := result.Validate(q); !errors.Is(err, ErrInvalid) {
		t.Fatal("zero revision treated as wildcard", err)
	}
	q.ExpectedEpoch, q.ExpectedRevision = &epoch, nil
	if err := result.Validate(q); !errors.Is(err, ErrInvalid) {
		t.Fatal("known epoch mismatch with unknown revision", err)
	}
	receipt.Epoch = 0
	if err := result.Validate(q); err != nil {
		t.Fatal("exact zero epoch with unknown revision", err)
	}
}

func TestMutationQueryResults(t *testing.T) {
	q := retentionQuery()
	base := MutationReceipt{q.Identity, q.Mutation, q.Operation, *q.ExpectedEpoch, *q.ExpectedRevision, q.RequestedWindow.From}
	found := MutationQueryResult{Found, q.RequestedWindow, &base}
	if err := found.Validate(q); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*MutationReceipt){
		func(r *MutationReceipt) { r.Identity.Namespace = "other" },
		func(r *MutationReceipt) { r.Identity.RunID = "other" },
		func(r *MutationReceipt) { r.Mutation.ID = "other" },
		func(r *MutationReceipt) { r.Mutation.Digest = strings.Repeat("b", 64) },
		func(r *MutationReceipt) { r.Operation = "finish" },
		func(r *MutationReceipt) { r.Epoch++ },
		func(r *MutationReceipt) { r.Revision++ },
		func(r *MutationReceipt) { r.OccurredAt = time.Time{} },
		func(r *MutationReceipt) { r.OccurredAt = q.RequestedWindow.From.Add(-time.Nanosecond) },
		func(r *MutationReceipt) { r.OccurredAt = q.RequestedWindow.Through.Add(time.Nanosecond) },
	} {
		r := base
		change(&r)
		bad := found
		bad.Receipt = &r
		if !errors.Is(bad.Validate(q), ErrInvalid) {
			t.Fatalf("accepted mismatched receipt %+v", r)
		}
	}
	short := MutationWindow{q.RequestedWindow.From.Add(time.Minute), q.RequestedWindow.Through}
	future := MutationWindow{q.RequestedWindow.From, q.RequestedWindow.Through.Add(-time.Minute)}
	for _, tc := range []struct {
		result MutationQueryResult
		valid  bool
	}{
		{MutationQueryResult{AbsentWithinWindow, q.RequestedWindow, nil}, true},
		{MutationQueryResult{AbsentWithinWindow, short, nil}, false},
		{MutationQueryResult{AbsentWithinWindow, future, nil}, false},
		{MutationQueryResult{AbsentWithinWindow, q.RequestedWindow, &base}, false},
		{MutationQueryResult{Expired, short, nil}, true},
		{MutationQueryResult{Expired, q.RequestedWindow, nil}, false},
		{MutationQueryResult{Expired, future, nil}, false},
		{MutationQueryResult{Expired, short, &base}, false},
		{MutationQueryResult{Found, q.RequestedWindow, nil}, false},
		{MutationQueryResult{Found, short, &base}, true}, // 保留回执早于连续覆盖范围。
		{MutationQueryResult{"unknown", q.RequestedWindow, nil}, false},
		{MutationQueryResult{AbsentWithinWindow, MutationWindow{}, nil}, false},
		{MutationQueryResult{Expired, MutationWindow{q.RequestedWindow.Through, q.RequestedWindow.From}, nil}, false},
	} {
		if err := tc.result.Validate(q); (err == nil) != tc.valid {
			t.Errorf("%+v: %v, want valid=%v", tc.result, err, tc.valid)
		}
	}
	base.OccurredAt = q.RequestedWindow.Through
	futureReceipt := found
	futureReceipt.Coverage = future
	if err := futureReceipt.Validate(q); !errors.Is(err, ErrInvalid) {
		t.Fatal("accepted receipt after authoritative observation upper bound", err)
	}
	base.OccurredAt = q.RequestedWindow.Through.In(time.FixedZone("UTC+8", 8*3600))
	if err := found.Validate(q); err != nil {
		t.Fatal("inclusive fixed-zone endpoint", err)
	}
	found.Coverage.From = found.Coverage.From.Add(-time.Hour)
	found.Coverage.Through = found.Coverage.Through.Add(time.Hour)
	if err := found.Validate(q); err != nil {
		t.Fatal("wider coverage", err)
	}
	bad := q
	bad.Mutation.ID = ""
	if err := found.Validate(bad); err == nil {
		t.Fatal("invalid query accepted")
	}
}
