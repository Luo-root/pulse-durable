// Package networktest exercises Issue #98 against a dedicated real PostgreSQL
// instance. No mock SQL server, payload logging, schema drops or existing grants.
package networktest

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/Luo-root/pulse-durable/distributed/internal/faultproxy"
	"github.com/Luo-root/pulse-durable/distributed/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

const allowEnv = "PULSE_DISTRIBUTED_TEST_ALLOW_DESTRUCTIVE"

// The allowlist is deliberately not configurable: never point these tests at an
// ordinary development database. The caller provisions the disposable Docker DB.
func allowed(host string, port uint16, user, database string) bool {
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback() && port == 55432 && user == "durable_test" && database == "durable_dist_security_20261025"
}

func TestDSNAllowlist(t *testing.T) {
	if !allowed("127.0.0.1", 55432, "durable_test", "durable_dist_security_20261025") {
		t.Fatal("dedicated fixture rejected")
	}
	for _, bad := range []struct {
		host     string
		port     uint16
		user, db string
	}{
		{"localhost", 55432, "durable_test", "durable_dist_security_20261025"},
		{"192.0.2.1", 55432, "durable_test", "durable_dist_security_20261025"},
		{"127.0.0.1", 5432, "durable_test", "durable_dist_security_20261025"},
		{"127.0.0.1", 55432, "postgres", "durable_dist_security_20261025"},
		{"127.0.0.1", 55432, "durable_test", "postgres"},
	} {
		if allowed(bad.host, bad.port, bad.user, bad.db) {
			t.Fatal("unsafe fixture accepted")
		}
	}
}

type fixture struct {
	ctx            context.Context
	admin          *pgxpool.Pool
	direct, remote *postgres.Coordinator
	proxy          *faultproxy.Proxy
	ns             string
}

func opaqueID(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal("random identity unavailable")
	}
	return hex.EncodeToString(b[:])
}

func setup(t *testing.T) *fixture {
	t.Helper()
	dsn := os.Getenv("PULSE_DISTRIBUTED_TEST_DSN")
	if dsn == "" || os.Getenv(allowEnv) != "yes" {
		t.Skip("dedicated DSN and explicit ALLOW_DESTRUCTIVE=yes required")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid dedicated DSN")
	}
	cc := cfg.ConnConfig
	if !allowed(cc.Host, cc.Port, cc.User, cc.Database) || cc.TLSConfig != nil || len(cc.Fallbacks) != 0 {
		t.Fatal("DSN outside dedicated loopback Docker allowlist (disable TLS and fallbacks)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	t.Cleanup(cancel)
	cfg.MaxConns = 2
	cfg.MinConns = 0
	cfg.ConnConfig.ConnectTimeout = 2 * time.Second
	admin, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal("admin pool configuration failed")
	}
	t.Cleanup(admin.Close)
	if err = admin.Ping(ctx); err != nil {
		t.Fatal("dedicated admin connection failed")
	}
	id := opaqueID(t)
	role, ns := "pulse_net_"+id, "network-"+id
	// Identifiers and passwords below are generated hex, never supplied SQL.
	password := opaqueID(t)
	if _, err = admin.Exec(ctx, "CREATE ROLE "+role+" LOGIN PASSWORD '"+password+"'"); err != nil {
		t.Fatal("isolated worker role creation failed")
	}
	// Retain isolated namespace/role as evidence, never touch existing grants.
	if err = postgres.Migrate(ctx, dsn, postgres.Grant{Role: role, Namespace: ns, Permissions: []string{"query", "submit", "worker"}}); err != nil {
		t.Fatalf("schema verification/grant failed: %v", err)
	}
	p, err := faultproxy.Listen(ctx, faultproxy.Config{Upstream: net.JoinHostPort(cc.Host, strconv.Itoa(int(cc.Port))), MaxConnections: 4})
	if err != nil {
		t.Fatal("proxy listen failed")
	}
	t.Cleanup(func() { _ = p.Close() })
	makeDSN := func(host string) string {
		u := url.URL{Scheme: "postgres", Host: host, Path: "/" + cc.Database, User: url.UserPassword(role, password)}
		q := u.Query()
		q.Set("sslmode", "disable")
		q.Set("connect_timeout", "2")
		q.Set("pool_max_conns", "1")
		q.Set("pool_min_conns", "0")
		// Warm List below prepares the shared api(text,jsonb) statement before
		// fault injection. Simple protocol encodes []byte as bytea, not JSON.
		q.Set("default_query_exec_mode", "cache_statement")
		u.RawQuery = q.Encode()
		return u.String()
	}
	direct, err := postgres.New(ctx, makeDSN(net.JoinHostPort(cc.Host, strconv.Itoa(int(cc.Port)))))
	if err != nil {
		t.Fatalf("direct coordinator initialization failed: %v", err)
	}
	t.Cleanup(direct.Close)
	remote, err := postgres.New(ctx, makeDSN(p.Addr()))
	if err != nil {
		t.Fatalf("proxied coordinator initialization failed: %v", err)
	}
	t.Cleanup(remote.Close)
	f := &fixture{ctx: ctx, admin: admin, direct: direct, remote: remote, proxy: p, ns: ns}
	// New pings/checks the schema. List also warms the exact worker API connection.
	if _, err = remote.List(ctx, d.ListRequest{Namespace: ns, Limit: 1}); err != nil {
		t.Fatalf("worker connection warmup failed: %v", err)
	}
	return f
}

func (f *fixture) task(t *testing.T) d.Task {
	t.Helper()
	g, err := pulse.New("network-contract", "no external side effects")
	if err != nil {
		t.Fatal(err)
	}
	if err = g.Add(pulse.NewNode("one", "one", nil, nil, func(*pulse.RunCtx) error { return nil })); err != nil {
		t.Fatal(err)
	}
	task, err := d.NewTask(f.ctx, d.Identity{Namespace: f.ns, RunID: opaqueID(t)}, "network", "v1", g, pulse.Inputs{}, durable.NewCodecs())
	if err != nil {
		t.Fatal("task preparation failed")
	}
	if _, err = f.direct.Submit(f.ctx, task); err != nil {
		t.Fatalf("submit failed: %v", err)
	}
	return task
}

func claimRequest(t *testing.T, task d.Task, ttl time.Duration) d.ClaimRequest {
	return d.ClaimRequest{Namespace: task.Identity().Namespace, Worker: d.Worker{ID: "network-worker", BootID: opaqueID(t)}, Capabilities: []d.Route{task.Route()}, Limit: 1, TTL: ttl, LeaseIDs: []string{opaqueID(t)}}
}
func claim(t *testing.T, f *fixture, req d.ClaimRequest) d.Claim {
	t.Helper()
	cs, err := f.direct.Claim(f.ctx, req)
	if err != nil || len(cs) != 1 {
		t.Fatalf("claim count=%d error=%v", len(cs), err)
	}
	return cs[0]
}
func record(task d.Task, rev uint64) durable.RunRecord {
	return durable.RunRecord{Format: durable.RecordFormat, RunID: task.Identity().RunID, Revision: rev, Version: task.Route().BusinessVersion, Definition: task.Route().Definition, Inputs: task.InputsFingerprint(), RootScope: "root", Scopes: map[string]durable.ScopeRecord{"root": {Graph: "test", Inputs: task.InputsFingerprint(), Nodes: map[string]durable.NodeRecord{}}}, Attempts: map[string]durable.AttemptRecord{}}
}
func mutation(t *testing.T) d.Mutation {
	id := opaqueID(t)
	h := sha256.Sum256([]byte(id))
	return d.Mutation{ID: id, Digest: hex.EncodeToString(h[:])}
}

// await polls authoritative state with an independent admin connection. Ticks
// only pace queries: neither elapsed sleep nor proxy bytes prove a DB commit.
func (f *fixture) await(t *testing.T, task d.Task, predicate string, args ...any) {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 3*time.Second)
	defer cancel()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	params := []any{task.Identity().Namespace, task.Identity().RunID}
	params = append(params, args...)
	for {
		var ok bool
		err := f.admin.QueryRow(ctx, "SELECT "+predicate+" FROM pulse_durable_distributed.runs WHERE namespace=$1 AND run_id=$2", params...).Scan(&ok)
		if err != nil {
			t.Fatal("authoritative database barrier query failed")
		}
		if ok {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("authoritative database barrier timed out")
		case <-tick.C:
		}
	}
}
func waitUnknown(t *testing.T, f *fixture, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if !errors.Is(err, d.ErrUnknown) {
			t.Fatalf("lost write response must be ErrUnknown, got %v", err)
		}
	case <-f.ctx.Done():
		t.Fatal("write did not terminate within fixture deadline")
	}
}
func barrier(t *testing.T, f *fixture, after uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(f.ctx, 3*time.Second)
	defer cancel()
	if _, err := f.proxy.WaitBarrier(ctx, faultproxy.ServerToClient, after); err != nil {
		t.Fatal("response did not reach proxy gate")
	}
}
func restore(t *testing.T, f *fixture) {
	t.Helper()
	if err := f.proxy.Restore(); err != nil {
		t.Fatal(err)
	}
	if err := f.proxy.Resume(faultproxy.ServerToClient); err != nil {
		t.Fatal(err)
	}
}

func TestClaimLostResponse(t *testing.T) {
	f := setup(t)
	task := f.task(t)
	req := claimRequest(t, task, 15*time.Second)
	after, err := f.proxy.Pause(faultproxy.ServerToClient)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := f.remote.Claim(ctx, req); done <- err }()
	barrier(t, f, after)
	f.await(t, task, "status='running' AND epoch=1 AND lease_id=$3 AND worker_id=$4 AND boot_id=$5 AND lease_until>clock_timestamp()", req.LeaseIDs[0], req.Worker.ID, req.Worker.BootID)
	waitUnknown(t, f, done)
	restore(t, f)
	resolved, err := f.direct.ResolveClaim(f.ctx, f.ns, req.Worker, req.LeaseIDs[0])
	if err != nil || resolved.Closed || resolved.Claim == nil {
		t.Fatalf("original lease resolution failed: %v", err)
	}
	if resolved.Claim.Token.LeaseID != req.LeaseIDs[0] || resolved.Claim.Token.Epoch != 1 {
		t.Fatal("resolution changed original ownership")
	}
	if _, err = f.direct.Renew(f.ctx, resolved.Claim.Token, 15*time.Second); err != nil {
		t.Fatalf("ownership reconfirmation failed: %v", err)
	}
	again, err := f.direct.ResolveClaim(f.ctx, f.ns, req.Worker, req.LeaseIDs[0])
	if err != nil || again.Claim == nil || again.Claim.Token != resolved.Claim.Token {
		t.Fatal("repeat resolution was not idempotent")
	}
	var count int
	if err = f.admin.QueryRow(f.ctx, "SELECT count(*) FROM pulse_durable_distributed.events WHERE namespace=$1 AND run_id=$2 AND operation='claim'", f.ns, task.Identity().RunID).Scan(&count); err != nil || count != 1 {
		t.Fatal("resolution created a duplicate claim or event query failed")
	}
}

func TestSaveLostResponse(t *testing.T) {
	f := setup(t)
	task := f.task(t)
	owned := claim(t, f, claimRequest(t, task, 15*time.Second))
	after, err := f.proxy.Pause(faultproxy.ServerToClient)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	save := d.InitializeRequest{Token: owned.Token, Record: record(task, 1), Mutation: mutation(t)}
	go func() { done <- f.remote.InitializeOwned(ctx, save) }()
	barrier(t, f, after)
	f.await(t, task, "status='running' AND revision=1 AND record IS NOT NULL AND last_mutation_id=$3", save.Mutation.ID)
	waitUnknown(t, f, done)
	restore(t, f)
	loaded, err := f.direct.LoadOwned(f.ctx, owned.Token)
	if err != nil || loaded.Revision != 1 || loaded.RunID != task.Identity().RunID || loaded.Definition != save.Record.Definition || loaded.Inputs != save.Record.Inputs {
		t.Fatalf("authoritative checkpoint was not saved: %v", err)
	}
	s, err := f.direct.Get(f.ctx, task.Identity())
	if err != nil || s.Status != d.Running || s.Revision != 1 {
		t.Fatal("uncertain save must not manufacture business failure")
	}
}

// Coordinator-level transport and fencing checks complement the live Worker
// lifecycle and recovery test in worker_test.go.
func TestRenewFaultFencesOldOwner(t *testing.T) {
	for _, mode := range []string{"cut", "blackhole"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t)
			task := f.task(t)
			old := claim(t, f, claimRequest(t, task, time.Second))
			if mode == "cut" {
				f.proxy.Cut()
			} else {
				if _, err := f.proxy.Blackhole(faultproxy.ClientToServer, true); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithTimeout(f.ctx, 250*time.Millisecond)
			_, err := f.remote.Renew(ctx, old.Token, time.Second)
			cancel()
			if !errors.Is(err, d.ErrUnknown) {
				t.Fatalf("renew transport failure must be uncertain: %v", err)
			}
			// Expiration is sampled by PostgreSQL, not by the client clock.
			f.await(t, task, "status='running' AND lease_until<=clock_timestamp() AND epoch=1")
			if _, err = f.proxy.Blackhole(faultproxy.ClientToServer, false); err != nil {
				t.Fatal(err)
			}
			restore(t, f)
			newOwner := claim(t, f, claimRequest(t, task, 10*time.Second))
			if newOwner.Token.Epoch <= old.Token.Epoch || newOwner.Token.LeaseID == old.Token.LeaseID || newOwner.Token.Worker == old.Token.Worker {
				t.Fatal("reclaim did not fence ownership")
			}
			err = f.direct.InitializeOwned(f.ctx, d.InitializeRequest{Token: old.Token, Record: record(task, 1), Mutation: mutation(t)})
			if !errors.Is(err, d.ErrLeaseLost) {
				t.Fatalf("stale owner save was not fenced: %v", err)
			}
			f.await(t, task, "status='running' AND epoch=2 AND revision=0 AND lease_id=$3", newOwner.Token.LeaseID)
		})
	}
}

func TestDelayedDatabaseCalls(t *testing.T) {
	for _, delay := range []time.Duration{100 * time.Millisecond, 500 * time.Millisecond} {
		t.Run(delay.String(), func(t *testing.T) {
			f := setup(t)
			task := f.task(t)
			after, err := f.proxy.Delay(faultproxy.ServerToClient, delay)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(f.ctx, 3*time.Second)
			start := time.Now()
			s, err := f.remote.Get(ctx, task.Identity())
			elapsed := time.Since(start)
			cancel()
			if err != nil || s.Status != d.Queued {
				t.Fatalf("delayed real read failed: %v", err)
			}
			barrier(t, f, after)
			if elapsed < delay || elapsed > 4*time.Second {
				t.Fatal("delayed call outside bounded timing envelope")
			}
			// A stricter deadline must terminate a real database call, not hang.
			ctx, cancel = context.WithTimeout(f.ctx, delay/4)
			start = time.Now()
			_, err = f.remote.Get(ctx, task.Identity())
			elapsed = time.Since(start)
			cancel()
			if err == nil || elapsed > time.Second {
				t.Fatal("deadline did not bound delayed call")
			}
		})
	}
}
