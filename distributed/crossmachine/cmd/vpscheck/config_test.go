package main

import (
	cm "github.com/Luo-root/pulse-durable/distributed/crossmachine"
	"strings"
	"testing"
)

func TestRemoteConfigRequired(t *testing.T) {
	if (remoteConfig{}).validate() == nil {
		t.Fatal("accepted missing configuration")
	}
	for _, field := range []string{"ssh", "identity", "target", "container", "sha", "host"} {
		t.Run(field, func(t *testing.T) {
			c := exampleRemote()
			switch field {
			case "ssh":
				c.SSHExecutable = ""
			case "identity":
				c.IdentityFile = ""
			case "target":
				c.Target = ""
			case "container":
				c.Container = ""
			case "sha":
				c.ExpectedSHA = ""
			case "host":
				c.HostLabel = ""
			}
			if c.validate() == nil {
				t.Fatal("accepted missing field")
			}
		})
	}
}
func TestRemoteConfigInjectionRejected(t *testing.T) {
	for _, s := range []string{"-option", "user@-option", "user@example.invalid;id", "user@example.invalid\nother", "host/path", "host:22", "a@b@c", "$(id)", "host name"} {
		c := exampleRemote()
		c.Target = s
		if c.validate() == nil {
			t.Errorf("target accepted: %q", s)
		}
	}
	for _, s := range []string{"-option", "runner;id", "runner'quoted", "runner\nother", "runner name", "runner/path"} {
		c := exampleRemote()
		c.Container = s
		if c.validate() == nil {
			t.Errorf("container accepted: %q", s)
		}
	}
	for _, s := range []string{"abcd", strings.Repeat("g", 64), strings.Repeat("A", 64), strings.Repeat("a", 63) + "'"} {
		c := exampleRemote()
		c.ExpectedSHA = s
		if c.validate() == nil {
			t.Errorf("hash accepted: %q", s)
		}
	}
	for _, s := range []string{"bad\x00path", "bad\npath", "bad\rpath"} {
		c := exampleRemote()
		c.IdentityFile = s
		if c.validate() == nil {
			t.Error("control character path accepted")
		}
	}
}
func TestConfigNeverEntersRemoteScript(t *testing.T) {
	cfg := exampleRemote()
	cfg.IdentityFile = "C:/fake key's/$(not-shell).pem"
	cfg.HostLabel = "remote-test"
	command, err := killCommand(cfg, cm.WorkerEvidence{Host: "remote-test", PID: 22}, "unit-test")
	if err != nil {
		t.Fatal(err)
	}
	if command.Args[3] != cfg.IdentityFile {
		t.Fatal("identity path was not preserved as exec argv")
	}
	script := command.Args[len(command.Args)-1]
	for _, value := range []string{cfg.IdentityFile, cfg.Target, cfg.Container, cfg.SSHExecutable, cfg.HostLabel} {
		if strings.Contains(script, value) {
			t.Fatalf("configuration value entered shell script: %q", value)
		}
	}
	if _, err := killCommand(cfg, cm.WorkerEvidence{Host: "vps", PID: 22}, "unit-test"); err == nil {
		t.Fatal("host label mismatch accepted")
	}
}
