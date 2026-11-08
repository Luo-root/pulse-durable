package distributed

import (
	"strings"
	"testing"
	"time"
)

func TestSemanticCompressionRequestAndReceiptContract(t *testing.T) {
	q := SemanticCompressionRequest{Identity: Identity{Namespace: "ns", RunID: "run"}, ExpectedRevision: 4, ExpectedHeadChain: strings.Repeat("a", 64), Mutation: Mutation{ID: "compress-1", Digest: strings.Repeat("b", 64)}, MaxObjects: 100, MaxBytes: 1 << 20}
	if err := q.Validate(); err != nil {
		t.Fatal(err)
	}
	r := SemanticCompressionReceipt{Identity: q.Identity, Mutation: q.Mutation, ExpectedRevision: 4, AppliedRevision: 5, ExpectedHeadChain: q.ExpectedHeadChain, AppliedHeadChain: strings.Repeat("c", 64), DeletedObjects: 2, SourceBytes: 99, OccurredAt: time.Date(2026, 11, 10, 12, 0, 0, 123456000, time.UTC)}
	r.ReceiptDigest = r.Digest()
	if err := r.Validate(q); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*SemanticCompressionRequest){
		"revision": func(x *SemanticCompressionRequest) { x.ExpectedRevision++ },
		"chain":    func(x *SemanticCompressionRequest) { x.ExpectedHeadChain = strings.Repeat("d", 64) },
		"mutation": func(x *SemanticCompressionRequest) { x.Mutation.ID = "other" },
		"budget":   func(x *SemanticCompressionRequest) { x.MaxObjects = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			changed := q
			mutate(&changed)
			if changed.Digest() == q.Digest() || name == "budget" && changed.Validate() == nil {
				t.Fatal("semantic compression request binding or invalid budget was not detected")
			}
		})
	}
	for name, mutate := range map[string]func(*SemanticCompressionReceipt){
		"identity":         func(x *SemanticCompressionReceipt) { x.Identity.RunID = "other" },
		"applied revision": func(x *SemanticCompressionReceipt) { x.AppliedRevision++ },
		"applied chain":    func(x *SemanticCompressionReceipt) { x.AppliedHeadChain = strings.Repeat("d", 64) },
		"time":             func(x *SemanticCompressionReceipt) { x.OccurredAt = x.OccurredAt.Add(time.Nanosecond) },
		"digest":           func(x *SemanticCompressionReceipt) { x.ReceiptDigest = strings.Repeat("d", 64) },
	} {
		t.Run("receipt/"+name, func(t *testing.T) {
			changed := r
			mutate(&changed)
			if changed.Validate(q) == nil {
				t.Fatal("changed semantic compression receipt accepted")
			}
		})
	}
	if q.Validate() != nil {
		t.Fatal("valid request unexpectedly invalid")
	}
}
