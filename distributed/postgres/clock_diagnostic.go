package postgres

import (
	"errors"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

type clockPhase uint8

const (
	clockOwnedFirst clockPhase = iota + 1
	clockOwnedFinal
)

func (p clockPhase) String() string {
	switch p {
	case clockOwnedFirst:
		return "owned_first"
	case clockOwnedFinal:
		return "owned_final"
	default:
		return "unknown"
	}
}

type clockDiagnostic struct {
	Phase   clockPhase
	DeltaUS int64
	StepUS  int64 // 只有 owned_final 含第二次采样。
}

// 仅接受模式生成的固定语法，不接受任意错误文本。
// 规范整数拒绝空白、加号、前导零和溢出。
func parseClockDiagnostic(detail string) (clockDiagnostic, bool) {
	var result clockDiagnostic
	if len(detail) > 160 || strings.ContainsAny(detail, "\r\n") {
		return result, false
	}
	parts := strings.Split(detail, ";")
	if len(parts) < 2 {
		return result, false
	}
	switch parts[0] {
	case "phase=owned_first":
		if len(parts) != 2 {
			return result, false
		}
		result.Phase = clockOwnedFirst
	case "phase=owned_final":
		if len(parts) != 3 {
			return result, false
		}
		result.Phase = clockOwnedFinal
	default:
		return result, false
	}
	parse := func(field, prefix string) (int64, bool) {
		if !strings.HasPrefix(field, prefix) {
			return 0, false
		}
		raw := strings.TrimPrefix(field, prefix)
		n, e := strconv.ParseInt(raw, 10, 64)
		return n, e == nil && strconv.FormatInt(n, 10) == raw
	}
	var ok bool
	result.DeltaUS, ok = parse(parts[1], "delta_us=")
	if !ok || result.DeltaUS >= 0 {
		return clockDiagnostic{}, false
	}
	if result.Phase == clockOwnedFinal {
		result.StepUS, ok = parse(parts[2], "step_us=")
		if !ok {
			return clockDiagnostic{}, false
		}
	}
	return result, true
}

func (c *Coordinator) observeClockDiagnostic(err error) {
	if c.clockDiagnostic == nil {
		return
	}
	var pe *pgconn.PgError
	if !errors.As(err, &pe) || pe.Code != "PD007" {
		return
	}
	value, ok := parseClockDiagnostic(pe.Detail)
	if !ok {
		return
	}
	// 诊断不能把明确拒绝转成 panic 或提交结果未知。
	defer func() { _ = recover() }()
	c.clockDiagnostic(value)
}
