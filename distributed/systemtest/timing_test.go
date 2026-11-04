package systemtest

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/Luo-root/pulse-durable/distributed/worker"
)

// 嵌入接口仅为 New 验证提供非 nil 依赖；
// 这些测试绝不 Run 工作进程或调用后端。
type timingBackend struct{ d.Coordinator }

func timingConfig(c Config) worker.Config {
	b := &timingBackend{}
	return c.workerConfig(b, b, &d.Registry{}, nil)
}
func TestTimingDefaults(t *testing.T) {
	c := Config{Namespace: "timing", ID: "worker", Capacity: 1}
	cfg := timingConfig(c)
	if cfg.TTL != 2*time.Second || cfg.RenewInterval != 300*time.Millisecond || cfg.RenewTimeout != 400*time.Millisecond || cfg.OperationTimeout != time.Second || cfg.PollMin != 20*time.Millisecond || cfg.PollMax != 50*time.Millisecond {
		t.Fatal("local timing defaults changed")
	}
	if _, err := worker.New(cfg); err != nil {
		t.Fatal(err)
	}
}
func TestTimingCrossMachineJSON(t *testing.T) {
	original := Config{Namespace: "timing", ID: "remote", Capacity: 2, TTL: 15 * time.Second, RenewInterval: 3 * time.Second, RenewTimeout: 2 * time.Second, OperationTimeout: 5 * time.Second, PollMin: 100 * time.Millisecond, PollMax: 300 * time.Millisecond}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var c Config
	if err = json.Unmarshal(data, &c); err != nil {
		t.Fatal(err)
	}
	if c != original {
		t.Fatal("JSON timing configuration changed")
	}
	cfg := timingConfig(c)
	if cfg.TTL != original.TTL || cfg.RenewInterval != original.RenewInterval || cfg.RenewTimeout != original.RenewTimeout || cfg.OperationTimeout != original.OperationTimeout || cfg.PollMin != original.PollMin || cfg.PollMax != original.PollMax {
		t.Fatal("explicit timing overridden")
	}
	if _, err = worker.New(cfg); err != nil {
		t.Fatal(err)
	}
}
func TestTimingInvalid(t *testing.T) {
	cases := map[string]func(*Config){
		"negative TTL":               func(c *Config) { c.TTL = -time.Second },
		"negative renew interval":    func(c *Config) { c.RenewInterval = -time.Second },
		"negative renew timeout":     func(c *Config) { c.RenewTimeout = -time.Second },
		"negative operation timeout": func(c *Config) { c.OperationTimeout = -time.Second },
		"negative poll min":          func(c *Config) { c.PollMin = -time.Second },
		"negative poll max":          func(c *Config) { c.PollMax = -time.Second },
		"renew at TTL":               func(c *Config) { c.RenewInterval = 2 * time.Second },
		"renew timeout budget":       func(c *Config) { c.RenewTimeout = 1700 * time.Millisecond },
		"reversed poll":              func(c *Config) { c.PollMin = 100 * time.Millisecond },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			c := Config{Namespace: "timing", ID: "worker", Capacity: 1}
			change(&c)
			if _, err := worker.New(timingConfig(c)); !errors.Is(err, d.ErrInvalid) {
				t.Fatalf("invalid timing accepted: %v", err)
			}
		})
	}
}
