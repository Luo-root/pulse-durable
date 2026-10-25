// vpscheck is restricted to the parent's explicitly authorized test runner.
// It never accepts arbitrary remote shell commands or credentials on argv.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"

	cm "github.com/Luo-root/pulse-durable/distributed/crossmachine"
)

type remoteConfig struct {
	SSHExecutable string
	IdentityFile  string
	Target        string
	Container     string
	ExpectedSHA   string
	HostLabel     string
}

var safeNamespace = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
var safeTarget = regexp.MustCompile(`^([A-Za-z0-9_][A-Za-z0-9_.-]*@)?[A-Za-z0-9][A-Za-z0-9.-]{0,252}$`)
var safeSHA = regexp.MustCompile(`^[0-9a-f]{64}$`)

func (c remoteConfig) validate() error {
	if c.SSHExecutable == "" || c.IdentityFile == "" || strings.ContainsAny(c.SSHExecutable+c.IdentityFile, "\x00\r\n") ||
		!safeTarget.MatchString(c.Target) || !safeNamespace.MatchString(c.Container) || !safeSHA.MatchString(c.ExpectedSHA) || !safeNamespace.MatchString(c.HostLabel) {
		return errors.New("explicit valid SSH executable, identity file, target, container, SHA and host label required")
	}
	return nil
}

// No environment, cmdline text, executable paths or raw SSH output are reported.
type killEvidence struct {
	Kind           string `json:"kind"`
	ProcExists     bool   `json:"proc_exists"`
	SHAMatch       bool   `json:"sha_match"`
	NamespaceMatch bool   `json:"namespace_match"`
	StartMatch     bool   `json:"start_match"`
	KillSent       bool   `json:"kill_sent"`
	Gone           bool   `json:"gone"`
}

// Values substituted below are validated numeric/safe-identifier tokens only.
// stat's comm may contain spaces or ')'; strip through its last ') ' first,
// then starttime (original field 22) is field 20 of the remaining text.
const remoteScript = `set -eu
pid=__PID__
ns=__NS__
proc=false; sha=false; namespace=false; same=false; sent=false; gone=false
emit() { printf '{"kind":"kill_identity","proc_exists":%s,"sha_match":%s,"namespace_match":%s,"start_match":%s,"kill_sent":%s,"gone":%s}\n' "$proc" "$sha" "$namespace" "$same" "$sent" "$gone"; }
starttime() { raw=$(cat "/proc/$pid/stat" 2>/dev/null) || return 1; rest=${raw##*) }; set -- $rest; [ "$#" -ge 20 ] || return 1; shift 19; printf '%s' "$1"; }
if [ ! -d "/proc/$pid" ]; then emit; exit 10; fi
proc=true
before=$(starttime) || { emit; exit 11; }
actual=$(sha256sum "/proc/$pid/exe" 2>/dev/null) || { emit; exit 12; }
actual=${actual%% *}
[ "$actual" != '__SHA__' ] || sha=true
# Match an exact argv pair, not a namespace substring. Accept Go flag's = form too.
if tr '\000' '\n' < "/proc/$pid/cmdline" | awk -v ns="$ns" 'prev=="-namespace" && $0==ns {ok=1} $0=="-namespace="ns {ok=1} {prev=$0} END {exit !ok}'; then namespace=true; fi
after=$(starttime) || { emit; exit 13; }
[ "$before" != "$after" ] || same=true
if [ "$sha" != true ] || [ "$namespace" != true ] || [ "$same" != true ]; then emit; exit 14; fi
kill -KILL "$pid" 2>/dev/null || { emit; exit 15; }
sent=true
i=0
while [ "$i" -lt 50 ]; do
 if [ ! -d "/proc/$pid" ]; then gone=true; emit; exit 0; fi
 # A reused PID is not the killed test process; do not signal it again.
 now=$(starttime) || now=''
 if [ -n "$now" ] && [ "$now" != "$before" ]; then emit; exit 16; fi
 sleep 0.1
 i=$((i+1))
done
emit
exit 17
`

func killCommand(config remoteConfig, w cm.WorkerEvidence, namespace string) (cm.Command, error) {
	if config.validate() != nil || w.Host != config.HostLabel || w.PID < 2 || !safeNamespace.MatchString(namespace) {
		return cm.Command{}, errors.New("invalid test kill target")
	}
	script := strings.NewReplacer("__PID__", strconv.Itoa(w.PID), "__NS__", namespace, "__SHA__", config.ExpectedSHA).Replace(remoteScript)
	// OpenSSH joins remote args through a shell. Quote the entire fixed script;
	// none of the validated substitutions can contain a single quote.
	quoted := "'" + strings.ReplaceAll(script, "'", "'\"'\"'") + "'"
	return cm.Command{Path: config.SSHExecutable, Args: []string{
		"-n", "-T", "-i", config.IdentityFile, "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=10",
		config.Target, "sudo", "-n", "docker", "exec", config.Container, "sh", "-c", quoted,
	}}, nil
}

// boundedOutput prevents unexpected remote output from exhausting local memory.
type boundedOutput struct{ bytes.Buffer }

func (b *boundedOutput) Write(p []byte) (int, error) {
	if b.Len()+len(p) > 4096 {
		return 0, errors.New("remote evidence too large")
	}
	return b.Buffer.Write(p)
}

func killRemote(ctx context.Context, config remoteConfig, w cm.WorkerEvidence, namespace string) error {
	c, err := killCommand(config, w, namespace)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, c.Path, c.Args...)
	cmd.WaitDelay = time.Second
	var stdout boundedOutput
	cmd.Stdout = &stdout
	// Never expose SSH/backend diagnostics, which may contain sensitive data.
	cmd.Stderr = nil
	err = cmd.Run()
	var e killEvidence
	dec := json.NewDecoder(bytes.NewReader(stdout.Bytes()))
	dec.DisallowUnknownFields()
	if dec.Decode(&e) != nil || e.Kind != "kill_identity" {
		return errors.New("remote kill evidence unavailable")
	}
	_ = json.NewEncoder(os.Stdout).Encode(e)
	if err != nil || !e.ProcExists || !e.SHAMatch || !e.NamespaceMatch || !e.StartMatch || !e.KillSent || !e.Gone {
		return errors.New("remote identity/kill confirmation failed")
	}
	return nil
}

func main() { os.Exit(run()) }
func run() int {
	fs := flag.NewFlagSet("vpscheck", flag.ContinueOnError)
	config := fs.String("config", "", "credential-free controller JSON")
	output := fs.String("output", "crossmachine-vpscheck-report.json", "new report file (never overwritten)")
	var remote remoteConfig
	fs.StringVar(&remote.SSHExecutable, "ssh-executable", "", "required SSH executable path")
	fs.StringVar(&remote.IdentityFile, "identity-file", "", "required identity file path (not key contents)")
	fs.StringVar(&remote.Target, "target", "", "required SSH destination, [user@]hostname or IPv4")
	fs.StringVar(&remote.Container, "container", "", "required dedicated test runner container")
	fs.StringVar(&remote.ExpectedSHA, "expected-sha", "", "required lowercase SHA256 of test harness")
	fs.StringVar(&remote.HostLabel, "host-label", "", "required controller host label to kill")
	if fs.Parse(os.Args[1:]) != nil {
		return 2
	}
	if remote.validate() != nil {
		fmt.Fprintln(os.Stderr, "invalid or missing explicit remote configuration")
		return 2
	}
	f, err := os.Open(*config)
	if err != nil {
		fmt.Fprintln(os.Stderr, "configuration unavailable")
		return 2
	}
	defer f.Close()
	var c cm.Config
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	if dec.Decode(&c) != nil || c.Validate() != nil || c.Scenario != "checkpoint-kill" || c.KillHost != remote.HostLabel {
		fmt.Fprintln(os.Stderr, "invalid VPS kill configuration")
		return 2
	}
	// Reserve output before any side effects; do not discard a run over a collision.
	out, err := os.OpenFile(*output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Fprintln(os.Stderr, "report output unavailable")
		return 2
	}
	defer out.Close()
	ctx, cancel := context.WithTimeout(context.Background(), c.Timeout)
	defer cancel()
	report := cm.Report{Result: "NOT_RUN", Reason: "database_preparation_or_open_failed", BuildSHA: c.BuildSHA, Namespace: c.Namespace, Scenario: c.Scenario}
	db, err := cm.OpenPostgres(ctx, c)
	if err == nil {
		ctl := cm.Controller{Kill: func(ctx context.Context, w cm.WorkerEvidence) error { return killRemote(ctx, remote, w, c.Namespace) }}
		report = ctl.Run(ctx, c, db)
		db.Close()
	}
	if json.NewEncoder(out).Encode(report) != nil {
		fmt.Fprintln(os.Stderr, "report write failed")
		return 2
	}
	if out.Sync() != nil {
		fmt.Fprintln(os.Stderr, "report flush failed")
		return 2
	}
	fmt.Fprintln(os.Stdout, report.Result)
	if report.Result != "PASS" {
		return 1
	}
	return 0
}
