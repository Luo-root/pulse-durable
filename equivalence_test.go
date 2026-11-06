package durable

import (
	"errors"
	"testing"
)

func TestCompressionEquivalenceAllowsOnlyPlannedAttemptRemoval(t *testing.T) {
	record := compressionPlanRecord(t)
	request := HistoryCompressionRequest{RunID: record.RunID, ThroughRevision: record.Revision, MaxObjects: 100, MaxBytes: 1 << 20}
	plan, err := PlanHistoryCompression(record, request)
	if err != nil {
		t.Fatal(err)
	}
	after := CloneRecord(record)
	for _, object := range plan.DeleteObjects {
		delete(after.Attempts, object.ID)
	}
	if err := ValidateHistoryCompressionEquivalence(record, after, plan); err != nil {
		t.Fatal(err)
	}
}

func TestCompressionEquivalenceRejectsUnknownOrChangedProjection(t *testing.T) {
	record := compressionPlanRecord(t)
	request := HistoryCompressionRequest{RunID: record.RunID, ThroughRevision: record.Revision, MaxObjects: 100, MaxBytes: 1 << 20}
	plan, err := PlanHistoryCompression(record, request)
	if err != nil {
		t.Fatal(err)
	}
	unknown := plan
	unknown.DeleteObjects = append(append([]CompressionObjectRef(nil), plan.DeleteObjects...), CompressionObjectRef{Kind: "attempt", ID: "missing"})
	unknown.Rows++
	unknown.PlanDigest = unknown.Digest()
	if err := ValidateHistoryCompressionEquivalence(record, record, unknown); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unknown deletion accepted: %v", err)
	}
	for _, mode := range []string{"subset", "empty-protections", "replacement"} {
		t.Run(mode, func(t *testing.T) {
			forged := plan
			after := CloneRecord(record)
			switch mode {
			case "subset":
				forged.DeleteObjects = append([]CompressionObjectRef(nil), plan.DeleteObjects[:1]...)
				forged.Rows = 1
				delete(after.Attempts, forged.DeleteObjects[0].ID)
			case "empty-protections":
				forged.ProtectedObjects = nil
				for _, object := range plan.DeleteObjects {
					delete(after.Attempts, object.ID)
				}
			case "replacement":
				forged.DeleteObjects = []CompressionObjectRef{{Kind: "attempt", ID: "a2"}}
				forged.Rows = 1
				delete(after.Attempts, "a2")
			}
			forged.PlanDigest = forged.Digest()
			if err := ValidateHistoryCompressionEquivalence(record, after, forged); !errors.Is(err, ErrInvalid) {
				t.Fatalf("caller re-signed plan accepted: %v", err)
			}
		})
	}
	changed := CloneRecord(record)
	changed.RootScope = "changed"
	if err := ValidateHistoryCompressionEquivalence(record, changed, plan); !errors.Is(err, ErrInvalidRecord) {
		t.Fatalf("changed retained projection accepted: %v", err)
	}
}
