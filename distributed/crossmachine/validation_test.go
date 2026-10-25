package crossmachine

import (
	"context"
	"errors"
	"testing"
)

func TestRejectAdminForwarding(t *testing.T) {
	c := testConfig(1)
	c.AdminDSNEnv = "CM_ADMIN_DSN"
	c.Hosts[0].Command.Env = map[string]string{"PULSE_DISTRIBUTED_TEST_DSN": "cm_admin_dsn"}
	if c.Validate() == nil {
		t.Fatal("admin credential forwarding accepted")
	}
}
func TestEarlyExitFails(t *testing.T) {
	db := newFake()
	ctl := fakeController(db)
	base := ctl.Launch
	ctl.Launch = func(ctx context.Context, c Command, args, env []string) (Process, error) {
		p, e := base(ctx, c, args, env)
		fp := p.(*fakeProcess)
		fp.events = nil
		fp.exited = true
		return p, e
	}
	r := ctl.Run(context.Background(), testConfig(1), db)
	if r.Result != "FAIL" || r.Reason != "ready" {
		t.Fatalf("%+v", r)
	}
}
func TestLaunchErrorSanitized(t *testing.T) {
	db := newFake()
	ctl := Controller{Launch: func(context.Context, Command, []string, []string) (Process, error) {
		return nil, errors.New("postgres://private:password@host")
	}}
	r := ctl.Run(context.Background(), testConfig(1), db)
	if r.Result != "FAIL" || r.Reason != "launch" {
		t.Fatalf("%+v", r)
	}
}
func TestKillFailureStopsRelease(t *testing.T) {
	db := newFake()
	ctl := fakeController(db)
	ctl.Kill = func(context.Context, WorkerEvidence) error { return errors.New("unconfirmed") }
	c := testConfig(1)
	c.Scenario = "checkpoint-kill"
	c.KillHost = "vps"
	r := ctl.Run(context.Background(), c, db)
	if r.Result != "FAIL" || r.Reason != "kill_confirmation" {
		t.Fatalf("%+v", r)
	}
	for _, p := range db.ps {
		if p.released {
			t.Fatal("released before confirmed kill")
		}
	}
}
