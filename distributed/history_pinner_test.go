package distributed

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

const pinDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func validPinRequest() PinRequest {
	ttl := 10 * time.Second
	r := PinRequest{Identity: Identity{"ns", "run"}, PinID: "pin", Mutation: Mutation{ID: "create"}, Revision: 3, Chain: pinDigest, TTL: &ttl, HolderDigest: pinDigest}
	r.Mutation.Digest = r.Digest()
	return r
}
func renewRequest() RenewPinRequest {
	c := validPinRequest()
	r := RenewPinRequest{Identity: c.Identity, PinID: c.PinID, Mutation: Mutation{ID: "renew"}, ExpectedVersion: 1, HolderDigest: c.HolderDigest}
	r.Mutation.Digest = r.Digest()
	return r
}
func releaseRequest() ReleasePinRequest {
	c := validPinRequest()
	r := ReleasePinRequest{Identity: c.Identity, PinID: c.PinID, Mutation: Mutation{ID: "release"}, ExpectedVersion: 2, HolderDigest: c.HolderDigest}
	r.Mutation.Digest = r.Digest()
	return r
}
func pinState() PinState {
	c := validPinRequest()
	return PinState{Identity: c.Identity, PinID: c.PinID, Revision: c.Revision, Chain: c.Chain, Version: 1, ExpiresAt: time.Unix(10, 0), HolderDigest: c.HolderDigest, Mutation: c.Mutation}
}

func TestPinNumericBoundaries(t *testing.T) {
	maxDuration := time.Duration(1<<63 - 1)
	if PinTTLMax != 9223372036*time.Second || maxDuration-PinTTLMax != 854775807*time.Nanosecond {
		t.Fatal("unexpected duration boundary")
	}
	for _, tt := range []struct {
		name  string
		ttl   time.Duration
		valid bool
	}{
		{"maximum whole seconds", PinTTLMax, true},
		{"MaxInt64 is fractional seconds", maxDuration, false},
		{"one nanosecond above maximum", PinTTLMax + time.Nanosecond, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := validPinRequest()
			c.TTL = durationPin(tt.ttl)
			c.Mutation.Digest = c.Digest()
			if _, err := c.Validate(); (err == nil) != tt.valid {
				t.Fatal(err)
			}
			r := renewRequest()
			r.TTL = durationPin(tt.ttl)
			r.Mutation.Digest = r.Digest()
			if _, err := r.Validate(); (err == nil) != tt.valid {
				t.Fatal(err)
			}
		})
	}
	maxVersion := ^uint64(0)
	for _, tt := range []struct {
		current, next uint64
		err           error
	}{
		{0, 0, ErrPinInvalid}, {1, 2, nil}, {maxVersion - 1, maxVersion, nil}, {maxVersion, 0, ErrPinConflict},
	} {
		got, err := NextPinVersion(tt.current)
		if got != tt.next || !errors.Is(err, tt.err) {
			t.Fatalf("NextPinVersion(%d) = %d, %v", tt.current, got, err)
		}
	}
	// 请求形状允许 max，以免在后端授权/精确回执解析之前阻断幂等重放。
	r := renewRequest()
	r.ExpectedVersion = maxVersion
	r.Mutation.Digest = r.Digest()
	if _, err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	release := releaseRequest()
	release.ExpectedVersion = maxVersion
	release.Mutation.Digest = release.Digest()
	if err := release.Validate(); err != nil {
		t.Fatal(err)
	}
	// pin 不递增业务 revision；最大历史 revision 和最大状态版本均合法。
	c := validPinRequest()
	c.Revision = maxVersion
	c.Mutation.Digest = c.Digest()
	if _, err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	s := pinState()
	s.Version, s.Revision = maxVersion, maxVersion
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	// 最后一次成功续期（max-1 -> max）的旧 receipt 仍可独立核对。
	r.ExpectedVersion = maxVersion - 1
	r.Mutation.Digest = r.Digest()
	q := PinMutationQuery{s.Identity, s.PinID, r.Mutation, PinRenewOperation}
	receipt := PinMutationReceipt{s.Identity, s.PinID, r.Mutation, PinRenewOperation, maxVersion, time.Unix(1, 0), r.Mutation.Digest, PinResult{PinActive, &s}}
	if err := (PinMutationResult{PinMutationFound, &receipt}).Validate(q); err != nil {
		t.Fatal(err)
	}
}

func TestPinDigestCanonicalEncoding(t *testing.T) {
	r := validPinRequest()
	wire := `{"Format":"history-pin-v1","Operation":"create","Identity":{"namespace":"ns","run_id":"run"},"PinID":"pin","MutationID":"create","Revision":3,"Chain":"` + pinDigest + `","HolderDigest":"` + pinDigest + `","ExpectedVersion":0,"TTL":10000000000}`
	if r.Digest() != hash([]byte(wire)) {
		t.Fatal("canonical v1 encoding changed")
	}
	got, err := r.Validate()
	if err != nil || got.Digest() != r.Digest() {
		t.Fatal("normalization changed digest", err)
	}
}

func TestPinCreateDigestBindings(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*PinRequest)
	}{
		{"namespace", func(r *PinRequest) { r.Identity.Namespace = "other" }},
		{"run", func(r *PinRequest) { r.Identity.RunID = "other" }},
		{"pin", func(r *PinRequest) { r.PinID = "other" }},
		{"mutation id", func(r *PinRequest) { r.Mutation.ID = "other" }},
		{"revision", func(r *PinRequest) { r.Revision++ }},
		{"chain", func(r *PinRequest) { r.Chain = hash([]byte("other")) }},
		{"holder", func(r *PinRequest) { r.HolderDigest = hash([]byte("other")) }},
		{"nil ttl", func(r *PinRequest) { r.TTL = nil }},
		{"finite ttl", func(r *PinRequest) { v := time.Second; r.TTL = &v }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := validPinRequest()
			original := r.Digest()
			tt.change(&r)
			if r.Digest() == original {
				t.Fatal("binding not hashed")
			}
			if _, err := r.Validate(); !errors.Is(err, ErrPinInvalid) {
				t.Fatal("stale digest accepted", err)
			}
			r.Mutation.Digest = r.Digest()
			if _, err := r.Validate(); err != nil {
				t.Fatal(err)
			}
		})
	}
	r := validPinRequest()
	d := r.Digest()
	r.Mutation.Digest = "anything"
	if r.Digest() != d {
		t.Fatal("self digest included")
	}
}
func TestPinRenewReleaseDigestBindings(t *testing.T) {
	for _, field := range []string{"namespace", "run", "pin", "mutation", "holder", "version", "ttl"} {
		t.Run(field, func(t *testing.T) {
			r := renewRequest()
			s := releaseRequest()
			rd, sd := r.Digest(), s.Digest()
			switch field {
			case "namespace":
				r.Identity.Namespace = "other"
				s.Identity.Namespace = "other"
			case "run":
				r.Identity.RunID = "other"
				s.Identity.RunID = "other"
			case "pin":
				r.PinID = "other"
				s.PinID = "other"
			case "mutation":
				r.Mutation.ID = "other"
				s.Mutation.ID = "other"
			case "holder":
				r.HolderDigest = hash([]byte("other"))
				s.HolderDigest = r.HolderDigest
			case "version":
				r.ExpectedVersion++
				s.ExpectedVersion++
			case "ttl":
				v := time.Second
				r.TTL = &v
			}
			if rd == r.Digest() {
				t.Fatal("renew binding not hashed")
			}
			if _, err := r.Validate(); !errors.Is(err, ErrPinInvalid) {
				t.Fatal(err)
			}
			r.Mutation.Digest = r.Digest()
			if _, err := r.Validate(); err != nil {
				t.Fatal(err)
			}
			if field != "ttl" {
				if sd == s.Digest() {
					t.Fatal("release binding not hashed")
				}
				if !errors.Is(s.Validate(), ErrPinInvalid) {
					t.Fatal("stale release digest accepted")
				}
				s.Mutation.Digest = s.Digest()
				if err := s.Validate(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
	// 同 Identity+Mutation.ID 跨操作使用不同摘要；冲突的原子裁决只由后端负责。
	c := validPinRequest()
	r := renewRequest()
	s := releaseRequest()
	r.Mutation.ID = c.Mutation.ID
	s.Mutation.ID = c.Mutation.ID
	r.ExpectedVersion = s.ExpectedVersion
	if c.Digest() == r.Digest() || r.Digest() == s.Digest() || c.Digest() == s.Digest() {
		t.Fatal("operation not bound")
	}
}
func TestPinTTLAndShape(t *testing.T) {
	for _, tt := range []struct {
		name string
		ttl  *time.Duration
		ok   bool
	}{
		{"nil", nil, true}, {"zero", durationPin(0), false}, {"negative", durationPin(-time.Second), false}, {"below minimum", durationPin(time.Millisecond), false}, {"precision", durationPin(1500 * time.Millisecond), false}, {"minimum", durationPin(time.Second), true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := validPinRequest()
			c.TTL = tt.ttl
			c.Mutation.Digest = c.Digest()
			got, e := c.Validate()
			if (e == nil) != tt.ok {
				t.Fatal(e)
			}
			if e == nil && c.TTL != nil && got.TTL == c.TTL {
				t.Fatal("aliased TTL")
			}
			r := renewRequest()
			r.TTL = tt.ttl
			r.Mutation.Digest = r.Digest()
			out, e := r.Validate()
			if (e == nil) != tt.ok {
				t.Fatal(e)
			}
			if e == nil && r.TTL == nil && out.TTL != nil {
				t.Fatal("nil must preserve policy")
			}
			if e == nil && r.TTL != nil && out.TTL == r.TTL {
				t.Fatal("aliased TTL")
			}
		})
	}
	for _, change := range []func(*PinRequest){func(r *PinRequest) { r.Revision = 0 }, func(r *PinRequest) { r.Chain = "bad" }, func(r *PinRequest) { r.HolderDigest = "token" }, func(r *PinRequest) { r.Identity.Namespace = "" }, func(r *PinRequest) { r.Identity.RunID = "" }, func(r *PinRequest) { r.PinID = "" }, func(r *PinRequest) { r.Mutation.ID = "" }} {
		c := validPinRequest()
		change(&c)
		c.Mutation.Digest = c.Digest()
		if _, e := c.Validate(); !errors.Is(e, ErrPinInvalid) {
			t.Fatal(e)
		}
	}
	r := renewRequest()
	r.ExpectedVersion = 0
	r.Mutation.Digest = r.Digest()
	if _, e := r.Validate(); !errors.Is(e, ErrPinInvalid) {
		t.Fatal(e)
	}
	s := releaseRequest()
	s.ExpectedVersion = 0
	s.Mutation.Digest = s.Digest()
	if !errors.Is(s.Validate(), ErrPinInvalid) {
		t.Fatal("zero release version")
	}
	for _, typ := range []reflect.Type{reflect.TypeOf(PinRequest{}), reflect.TypeOf(RenewPinRequest{}), reflect.TypeOf(ReleasePinRequest{})} {
		for _, name := range []string{"Token", "Worker", "Epoch", "LeaseID", "Owner"} {
			if _, ok := typ.FieldByName(name); ok {
				t.Fatal("authorization field", name)
			}
		}
	}
	if !errors.Is(ErrPinInvalid, ErrInvalid) || !errors.Is(ErrPinUnknown, ErrUnknown) || !errors.Is(ErrPinNotFound, ErrNotFound) {
		t.Fatal("error categories")
	}
}
func durationPin(v time.Duration) *time.Duration { return &v }
func TestPinStateResults(t *testing.T) {
	for _, status := range []PinStatus{PinActive, PinExpired, PinReleased} {
		for _, long := range []bool{false, true} {
			for _, zero := range []bool{false, true} {
				s := pinState()
				s.LongLived = long
				if zero {
					s.ExpiresAt = time.Time{}
				}
				want := long == zero
				if e := s.Validate(); (e == nil) != want {
					t.Fatal("state shape", e)
				}
				if e := (PinResult{status, &s}).Validate(); (e == nil) != (want && !(long && status == PinExpired)) {
					t.Fatal(status, long, zero, e)
				}
			}
		}
	}
	for _, tt := range []struct {
		r           PinResult
		e           error
		write, find bool
	}{
		{PinResult{Status: PinAbsent}, nil, false, true},
		{PinResult{Status: PinUnknown}, ErrPinUnknown, true, false},
		{PinResult{Status: PinConflict}, ErrPinConflict, true, false},
		{PinResult{Status: PinUnknown}, nil, false, false},
		{PinResult{Status: PinConflict}, nil, false, false},
		{PinResult{}, ErrPinExpired, true, true},
		{PinResult{Status: PinUnknown, State: &PinState{}}, ErrPinUnknown, false, false},
		{PinResult{Status: PinConflict}, ErrPinUnknown, false, false},
	} {
		if e := tt.r.ValidateOutcome(tt.e); (e == nil) != tt.write {
			t.Fatal(e)
		}
		if e := tt.r.ValidateFindOutcome(tt.e); (e == nil) != tt.find {
			t.Fatal(e)
		}
	}
}
func TestPinMutationReceiptResolution(t *testing.T) {
	for _, op := range []PinOperation{PinCreateOperation, PinRenewOperation, PinReleaseOperation} {
		t.Run(string(op), func(t *testing.T) {
			s := pinState()
			m := s.Mutation
			if op == PinRenewOperation {
				r := renewRequest()
				m = r.Mutation
				s.Version = 2
			}
			status := PinActive
			if op == PinReleaseOperation {
				r := releaseRequest()
				m = r.Mutation
				s.Version = 3
				status = PinReleased
			}
			q := PinMutationQuery{s.Identity, s.PinID, m, op}
			receipt := PinMutationReceipt{s.Identity, s.PinID, m, op, s.Version, time.Unix(1, 0), m.Digest, PinResult{status, &s}}
			result := PinMutationResult{PinMutationFound, &receipt}
			if e := result.ValidateOutcome(q, nil); e != nil {
				t.Fatal(e)
			}
			// B 的当前状态无关 A 的不可变回执；这里不实现任何后端或假原子 CAS。
			current := s
			current.Version++
			if e := result.Validate(q); e != nil {
				t.Fatal(e)
			}
			for _, change := range []func(*PinMutationReceipt){
				func(v *PinMutationReceipt) { v.PinID = "other" }, func(v *PinMutationReceipt) { v.Identity.RunID = "other" }, func(v *PinMutationReceipt) { v.Operation = "invalid" }, func(v *PinMutationReceipt) { v.Mutation.ID = "other" }, func(v *PinMutationReceipt) { v.RequestDigest = pinDigest }, func(v *PinMutationReceipt) { v.Version++ }, func(v *PinMutationReceipt) { v.OccurredAt = time.Time{} }, func(v *PinMutationReceipt) { v.Result = PinResult{Status: PinAbsent} },
			} {
				v := receipt
				change(&v)
				if e := (PinMutationResult{PinMutationFound, &v}).Validate(q); !errors.Is(e, ErrPinInvalid) {
					t.Fatal("mismatched receipt accepted", e)
				}
			}
			for _, tt := range []struct {
				r  PinMutationResult
				e  error
				ok bool
			}{
				{PinMutationResult{Status: PinMutationAbsent}, nil, true},
				{PinMutationResult{Status: PinMutationUnknown}, ErrPinUnknown, true},
				{PinMutationResult{Status: PinMutationUnknown}, nil, false},
				{PinMutationResult{Status: PinMutationAbsent, Receipt: &receipt}, nil, false},
				{PinMutationResult{Status: PinMutationUnknown, Receipt: &receipt}, ErrPinUnknown, false},
				{PinMutationResult{}, ErrPinUnknown, true},
			} {
				if e := tt.r.ValidateOutcome(q, tt.e); (e == nil) != tt.ok {
					t.Fatal(e)
				}
			}
			q.Operation = "apply"
			if !errors.Is(q.Validate(), ErrPinInvalid) {
				t.Fatal("invalid operation")
			}
		})
	}
}
