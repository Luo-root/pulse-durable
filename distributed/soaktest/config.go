package soaktest

import (
	"errors"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Config has no credentials. Separate explicit DSN environment variable names are mandatory.
type Config struct {
	Namespace      string        `json:"namespace"`
	Workers        int           `json:"workers"`
	Capacity       int           `json:"capacity"`
	Warm           time.Duration `json:"warm_ns"`
	Steady         time.Duration `json:"steady_ns"`
	Repeat         int           `json:"repeat"`
	MaxOutstanding int           `json:"max_outstanding"`
	CPURounds      int           `json:"cpu_rounds"`
	Wait           time.Duration `json:"wait_ns"`
	AuditTimeout   time.Duration `json:"audit_timeout_ns"`
	AdminEnv       string        `json:"admin_env"`
	WorkerEnv      string        `json:"worker_env"`
}

const defaultAuditTimeout = 10 * time.Minute

// Zero preserves compatibility with callers constructing Config directly.
func (c Config) auditTimeout() time.Duration {
	if c.AuditTimeout == 0 {
		return defaultAuditTimeout
	}
	return c.AuditTimeout
}

func Default() Config {
	return Config{Workers: 1, Capacity: 1, Warm: 3 * time.Second, Steady: 5 * time.Second, Repeat: 1, MaxOutstanding: 16, CPURounds: 10000, Wait: time.Millisecond, AuditTimeout: defaultAuditTimeout, AdminEnv: "PULSE_SOAK_ADMIN_DSN", WorkerEnv: "PULSE_SOAK_WORKER_DSN"}
}
func (c Config) Validate() error {
	if c.Namespace == "" || len(c.Namespace) > 180 || c.Workers < 1 || c.Workers > 64 || c.Capacity < 1 || c.Capacity > 128 || c.Repeat < 1 || c.Repeat > 100 || c.Warm < 0 || c.Steady <= 0 || c.MaxOutstanding < c.Workers*c.Capacity || c.MaxOutstanding > 100000 || c.CPURounds < 1 || c.CPURounds > 10000000 || c.Wait < 0 || c.Wait > time.Second || c.AuditTimeout < 0 || c.AdminEnv == "" || c.WorkerEnv == "" || c.AdminEnv == c.WorkerEnv {
		return errors.New("invalid soak configuration")
	}
	for _, key := range []string{c.AdminEnv, c.WorkerEnv} {
		if strings.ContainsAny(key, "=\x00") {
			return errors.New("invalid env name")
		}
	}
	if os.Getenv("PULSE_DISTRIBUTED_TEST_ALLOW") != "yes" || os.Getenv(c.AdminEnv) == "" || os.Getenv(c.WorkerEnv) == "" || os.Getenv(c.AdminEnv) == os.Getenv(c.WorkerEnv) {
		return errors.New("explicit local database opt-in and distinct admin/worker DSNs required")
	}
	a, ea := pgx.ParseConfig(os.Getenv(c.AdminEnv))
	w, ew := pgx.ParseConfig(os.Getenv(c.WorkerEnv))
	if ea != nil || ew != nil || a.User == w.User || a.Host != w.Host || a.Port != w.Port || a.Database != w.Database || a.Host == "" || a.Database == "" {
		return errors.New("admin/worker must be distinct roles on the same explicit local database")
	}
	for _, cfg := range []*pgx.ConnConfig{a, w} {
		h := cfg.Host
		if h != "localhost" && h != "127.0.0.1" && h != "::1" && h != "/var/run/postgresql" {
			return errors.New("soak refuses non-local database host")
		}
		// pgx expands multi-host and TLS fallback combinations. Every candidate
		// must target the exact same endpoint, not merely another local server.
		if cfg.TLSConfig != nil && cfg.TLSConfig.ServerName != "" && cfg.TLSConfig.ServerName != h {
			return errors.New("TLS server name does not match local endpoint")
		}
		for _, fallback := range cfg.Fallbacks {
			if fallback == nil || fallback.Host != h || fallback.Port != cfg.Port {
				return errors.New("soak refuses different fallback endpoints")
			}
			if fallback.TLSConfig != nil && fallback.TLSConfig.ServerName != "" && fallback.TLSConfig.ServerName != h {
				return errors.New("fallback TLS server name does not match local endpoint")
			}
		}
	}
	return nil
}
