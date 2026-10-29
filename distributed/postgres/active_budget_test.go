package postgres

import (
	"strings"
	"testing"
)

func TestActiveSuffixBudgetSQLContract(t *testing.T) {
	for _, want := range []string{
		"active_log_bytes bigint NOT NULL DEFAULT 0 CHECK(active_log_bytes BETWEEN 0 AND 268435456)",
		"active_log_count bigint NOT NULL DEFAULT 0 CHECK(active_log_count BETWEEN 0 AND 100000)",
		"r.active_log_bytes+octet_length(b)>268435456 OR r.active_log_count>=100000",
		"active_log_bytes=active_log_bytes+octet_length(b),active_log_count=active_log_count+1",
	} {
		if !strings.Contains(schemaSQL, want) {
			t.Fatal("missing active suffix contract", want)
		}
	}
	apply := strings.Split(strings.Split(schemaSQL, " IF op='apply' THEN")[1], " ELSIF op='initialize'")[0]
	for _, forbidden := range []string{"rev>=100000", "sum(", "count(", "FROM batches", "FROM pulse_durable_distributed.batches"} {
		if strings.Contains(apply, forbidden) {
			t.Fatal("Apply must not budget retained history or scan batches", forbidden)
		}
	}
	lock := strings.Index(maintenanceSQL, "FOR UPDATE;")
	recount := strings.Index(maintenanceSQL, "SELECT coalesce(sum(octet_length(wire)),0),count(*) INTO suffix_bytes,suffix_count FROM pulse_durable_distributed.batches WHERE namespace=ns AND run_id=rid AND revision>p.through_revision AND revision<=r.revision;")
	publish := strings.Index(maintenanceSQL, "active_log_bytes=suffix_bytes,active_log_count=suffix_count")
	if lock < 0 || recount <= lock || publish <= recount {
		t.Fatal("publish must recount the current suffix under the run lock before switching budget")
	}
	for _, forbidden := range []string{"DELETE FROM", "SET revision=", "SET head_chain=", "SET lease_until=", "INSERT INTO pulse_durable_distributed.events"} {
		if strings.Contains(maintenanceSQL, forbidden) {
			t.Fatal("synthesis changed retained/business state", forbidden)
		}
	}
}
