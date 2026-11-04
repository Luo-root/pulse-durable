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
