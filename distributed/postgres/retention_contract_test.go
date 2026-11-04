package postgres

import (
	"errors"
	d "github.com/Luo-root/pulse-durable/distributed"
	"strings"
	"testing"
)

func TestRetentionPolicySQLContract(t *testing.T) {
	for _, fragment := range []string{
		"retention_policy_validate(p jsonb)",
		"jsonb_object_keys(p)",
		"NOT retention_policy_validate(proposed)",
		"jsonb_object_keys(p) AS keys(key)",
		"cursor jsonb NOT NULL",
		"limit integer NOT NULL CHECK(limit BETWEEN 1 AND 1000000)",
		"cur::text||':'||lim::text",
		"ON CONFLICT(dry_run_id) DO NOTHING",
		"retention preview binding conflict",
		"coalesce(octet_length(cur->>'value'),0) NOT BETWEEN 1 AND 256",
	} {
		if !strings.Contains(retentionPolicySQL, fragment) {
			t.Fatalf("missing SQL contract %q", fragment)
		}
	}
	if ErrUnsupportedCapability == nil {
		t.Fatal("unsupported capability sentinel unavailable")
	}
}

func TestRetentionPolicyDecoderRequiresNamespaceAndTimestamp(t *testing.T) {
	if _, err := decodeRetentionPolicyResult([]byte(`{"namespace":"other","policy_version":"1","policy":{"Enabled":false,"EventTTL":null,"ReceiptTTL":null,"ReplayHistoryTTL":null,"TerminalPayloadTTL":null},"policy_digest":"0000000000000000000000000000000000000000000000000000000000000000"}`), "ns"); !errors.Is(err, d.ErrInvalid) {
		t.Fatalf("foreign or incomplete result accepted: %v", err)
	}
}
