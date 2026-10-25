package main

import (
	"strings"
	"testing"

	cm "github.com/Luo-root/pulse-durable/distributed/crossmachine"
)

func exampleRemote() remoteConfig {
	return remoteConfig{SSHExecutable: "C:/fake/ssh.exe", IdentityFile: "C:/fake keys/test key.pem", Target: "tester@example.invalid", Container: "test-runner", ExpectedSHA: strings.Repeat("a", 64), HostLabel: "vps"}
}

func TestKillCommandRejectsUnsafeTarget(t *testing.T) {
	for _, tc := range []struct {
		pid      int
		host, ns string
	}{
		{0, "vps", "cm-test"}, {1, "vps", "cm-test"}, {-2, "vps", "cm-test"},
		{7, "windows", "cm-test"}, {7, "vps", ""}, {7, "vps", "cm; kill -9 1"}, {7, "vps", "cm'quoted"}, {7, "vps", "cm\nother"}, {7, "vps", "-option"},
	} {
		if _, err := killCommand(exampleRemote(), cm.WorkerEvidence{PID: tc.pid, Host: tc.host}, tc.ns); err == nil {
			t.Errorf("accepted %+v", tc)
		}
	}
}
func TestFixedKillCommand(t *testing.T) {
	c, err := killCommand(exampleRemote(), cm.WorkerEvidence{PID: 7, Host: "vps", Boot: "not-a-shell-input"}, "cm-20261025-kill-vps-retry1")
	if err != nil {
		t.Fatal(err)
	}
	if c.Path != "C:/fake/ssh.exe" {
		t.Fatal(c.Path)
	}
	wantPrefix := []string{"-n", "-T", "-i", "C:/fake keys/test key.pem", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=10", "tester@example.invalid", "sudo", "-n", "docker", "exec", "test-runner", "sh", "-c"}
	if len(c.Args) != len(wantPrefix)+1 {
		t.Fatal("unexpected argument count")
	}
	for i, want := range wantPrefix {
		if c.Args[i] != want {
			t.Fatalf("arg %d: %q", i, c.Args[i])
		}
	}
	// Undo the POSIX single-quote envelope to inspect the exact script text.
	quoted := c.Args[len(c.Args)-1]
	if !strings.HasPrefix(quoted, "'") || !strings.HasSuffix(quoted, "'") {
		t.Fatal("missing shell quote envelope")
	}
	script := strings.ReplaceAll(quoted[1:len(quoted)-1], "'\"'\"'", "'")
	for _, s := range []string{"pid=7\n", "ns=cm-20261025-kill-vps-retry1\n", exampleRemote().ExpectedSHA, "sha256sum \"/proc/$pid/exe\"", "before=$(starttime)", "after=$(starttime)", "kill -KILL \"$pid\"", "[ \"$i\" -lt 50 ]", "$0==ns", "same=true"} {
		if !strings.Contains(script, s) {
			t.Errorf("missing %q", s)
		}
	}
	if strings.Index(script, "after=$(starttime)") > strings.Index(script, "kill -KILL") {
		t.Fatal("starttime check occurs after kill")
	}
	if strings.Contains(script, "__PID__") || strings.Contains(script, "__NS__") || strings.Contains(script, "__SHA__") {
		t.Fatal("unexpanded marker")
	}
	for _, s := range []string{"DSN", "PASSWORD", "not-a-shell-input"} {
		if strings.Contains(script, s) {
			t.Fatalf("unexpected remote input %s", s)
		}
	}
}
func TestOutputBound(t *testing.T) {
	var out boundedOutput
	if n, err := out.Write(make([]byte, 4096)); n != 4096 || err != nil {
		t.Fatal("limit write")
	}
	if _, err := out.Write([]byte("x")); err == nil {
		t.Fatal("unbounded output")
	}
}
