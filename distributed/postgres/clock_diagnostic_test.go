package postgres

import (
	"errors"
	"strings"
	"testing"

	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestClockDiagnosticStrictGrammar(t *testing.T) {
	for _, tc := range []struct {
		wire string
		want clockDiagnostic
	}{
		{"phase=owned_first;delta_us=-1", clockDiagnostic{Phase: clockOwnedFirst, DeltaUS: -1}},
		{"phase=owned_first;delta_us=0", clockDiagnostic{Phase: clockOwnedFirst}},
		{"phase=owned_first;delta_us=42", clockDiagnostic{Phase: clockOwnedFirst, DeltaUS: 42}},
		{"phase=owned_first;delta_us=9223372036854775807", clockDiagnostic{Phase: clockOwnedFirst, DeltaUS: 9223372036854775807}},
		{"phase=owned_final;delta_us=-230;step_us=-100", clockDiagnostic{Phase: clockOwnedFinal, DeltaUS: -230, StepUS: -100}},
		{"phase=owned_final;delta_us=0;step_us=0", clockDiagnostic{Phase: clockOwnedFinal}},
		{"phase=owned_final;delta_us=42;step_us=3", clockDiagnostic{Phase: clockOwnedFinal, DeltaUS: 42, StepUS: 3}},
		{"phase=owned_final;delta_us=-9223372036854775808;step_us=9223372036854775807", clockDiagnostic{Phase: clockOwnedFinal, DeltaUS: -9223372036854775808, StepUS: 9223372036854775807}},
	} {
		t.Run(tc.wire, func(t *testing.T) {
			got, ok := parseClockDiagnostic(tc.wire)
			if !ok || got != tc.want {
				t.Fatalf("fixed timing grammar rejected: %#v %v", got, ok)
			}
		})
	}
	for _, wire := range []string{
		"", "phase=other;delta_us=-1", "phase=owned_first;delta_us=-1;unknown=2",
		"phase=owned_first;delta_us=-1;step_us=1", "phase=owned_final;delta_us=-1",
		"phase=owned_final;delta_us=-1;extra=2", "phase=owned_first;delta_us=-1 extra",
		"phase=owned_first;delta_us=-9223372036854775809", "phase=owned_first;delta_us=9223372036854775808",
		"phase=owned_final;delta_us=-1;step_us=9223372036854775808",
		"phase=owned_first;delta_us=-1\r\n", "phase=owned_first;delta_us=0\r", "phase=owned_final;delta_us=42;step_us=3\n",
		"phase=owned_first;delta_us= -1", "phase=owned_first;delta_us=-01", "phase=owned_first;delta_us=01",
		"phase=owned_first;delta_us=-0", "phase=owned_first;delta_us=+1", "phase=owned_final;delta_us=42;step_us=+3",
		"phase=owned_final;delta_us=0;step_us=0;unknown=2",
	} {
		if _, ok := parseClockDiagnostic(wire); ok {
			t.Fatal("untrusted diagnostic accepted")
		}
	}
}
func TestClockDiagnosticScrubAndPanicIsolation(t *testing.T) {
	for _, tc := range []struct {
		wire string
		want clockDiagnostic
	}{
		{"phase=owned_final;delta_us=-42;step_us=-3", clockDiagnostic{Phase: clockOwnedFinal, DeltaUS: -42, StepUS: -3}},
		{"phase=owned_first;delta_us=0", clockDiagnostic{Phase: clockOwnedFirst}},
		{"phase=owned_first;delta_us=42", clockDiagnostic{Phase: clockOwnedFirst, DeltaUS: 42}},
		{"phase=owned_final;delta_us=0;step_us=0", clockDiagnostic{Phase: clockOwnedFinal}},
		{"phase=owned_final;delta_us=42;step_us=3", clockDiagnostic{Phase: clockOwnedFinal, DeltaUS: 42, StepUS: 3}},
	} {
		t.Run(tc.wire, func(t *testing.T) {
			pe := &pgconn.PgError{Code: "PD007", Message: "clock regression with private details", Detail: tc.wire}
			for _, write := range []bool{false, true} {
				err := scrub(pe, write)
				if !errors.Is(err, d.ErrClockRegression) || strings.Contains(err.Error(), "phase=") || strings.Contains(err.Error(), "42") || strings.Contains(err.Error(), "private") {
					t.Fatal("clock detail leaked through public error")
				}
			}
			calls := 0
			c := &Coordinator{clockDiagnostic: func(value clockDiagnostic) {
				calls++
				if value != tc.want {
					t.Errorf("wrong typed timing: got %+v, want %+v", value, tc.want)
				}
				panic("diagnostic callback")
			}}
			c.observeClockDiagnostic(pe)
			if calls != 1 {
				t.Fatal("valid diagnostic not observed")
			}
			c.observeClockDiagnostic(&pgconn.PgError{Code: "PD001", Detail: pe.Detail})
			c.observeClockDiagnostic(errors.New(pe.Detail))
			c.observeClockDiagnostic(&pgconn.PgError{Code: "PD007", Detail: pe.Detail + ";payload=secret"})
			if calls != 1 {
				t.Fatal("raw/unknown diagnostic reached observer")
			}
			if !errors.Is(scrub(pe, true), d.ErrClockRegression) {
				t.Fatal("diagnostic panic changed rejection semantics")
			}
		})
	}
}
