package soaktest

import "testing"

func TestLocalEndpoints(t *testing.T) {
	t.Setenv("PULSE_DISTRIBUTED_TEST_ALLOW", "yes")
	t.Setenv("PGHOST", "127.0.0.1")
	c := Default()
	c.Namespace = "regression"
	cases := []struct {
		name, admin, worker string
		valid               bool
	}{
		{"local", "host=127.0.0.1 port=55432 dbname=soak user=admin sslmode=disable", "host=127.0.0.1 port=55432 dbname=soak user=worker sslmode=disable", true},
		{"remote fallback", "host=127.0.0.1,remote.invalid port=55432 dbname=soak user=admin sslmode=disable", "host=127.0.0.1 port=55432 dbname=soak user=worker sslmode=disable", false},
		{"different port", "host=127.0.0.1 port=55432 dbname=soak user=admin sslmode=disable", "host=127.0.0.1 port=55433 dbname=soak user=worker sslmode=disable", false},
		{"local other endpoint fallback", "host=127.0.0.1,localhost port=55432 dbname=soak user=admin sslmode=disable", "host=127.0.0.1 port=55432 dbname=soak user=worker sslmode=disable", false},
		{"TLS fallback", "host=localhost port=55432 dbname=soak user=admin sslmode=prefer", "host=localhost port=55432 dbname=soak user=worker sslmode=prefer", true},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(c.AdminEnv, tt.admin)
			t.Setenv(c.WorkerEnv, tt.worker)
			if got := c.Validate() == nil; got != tt.valid {
				t.Fatalf("accepted=%v want %v", got, tt.valid)
			}
		})
	}
}
func TestFailuresNotGloballySuppressed(t *testing.T) {
	for _, e := range []Event{{Kind: "claim", Reason: "unknown"}, {Kind: "claim", Reason: "interrupted"}, {Kind: "renew", Reason: "lease_lost"}} {
		if !badEvent(e) {
			t.Fatalf("fault suppressed: %+v", e)
		}
	}
}
