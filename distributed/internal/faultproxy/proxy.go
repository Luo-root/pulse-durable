// Package faultproxy provides a bounded, loopback-only TCP fault injector for
// tests, not a production proxy. It does not parse SQL or identify commits.
// PostgreSQL tests may use sslmode=disable and coordinate byte barriers with
// application/database barriers. No payload bytes are logged or retained.
package faultproxy

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

// Direction is relative to the client that connects to the proxy.
type Direction uint8

const (
	ClientToServer Direction = iota
	ServerToClient
	MaxDelay   = 5 * time.Second
	bufferSize = 8192
)

var ErrClosed = errors.New("faultproxy: closed")

// Config requires a numeric loopback IP:port upstream; DNS and external
// connections are deliberately unsupported. MaxConnections defaults to 64.
type Config struct {
	Upstream       string
	MaxConnections int
}

// DirectionStats counts chunks at the proxy, not TCP packets or SQL messages.
// Interceptions increments once per chunk encountering any active fault.
type DirectionStats struct {
	ReadBytes        uint64
	ForwardedBytes   uint64
	DroppedBytes     uint64
	Interceptions    uint64
	InterceptedBytes uint64
}

type Stats struct {
	Accepted   uint64
	Rejected   uint64
	Active     int
	Directions [2]DirectionStats
}

type policy struct {
	paused    bool
	blackhole bool
	delay     time.Duration
}

type connection struct {
	client, server net.Conn
	ctx            context.Context
	cancel         context.CancelFunc
}

// Proxy controls existing and future connections. Controls affect bytes not yet
// past the gate: bytes already written to kernel buffers cannot be recalled.
// Each direction holds at most one fixed-size chunk per connection. Blackhole
// drains and discards chunks immediately; Pause applies TCP backpressure.
type Proxy struct {
	mu          sync.Mutex
	listener    net.Listener
	upstream    string
	limit       int
	ctx         context.Context
	cancel      context.CancelFunc
	changed     chan struct{}
	done        chan struct{}
	closed      bool
	cut         bool
	policies    [2]policy
	stats       Stats
	connections map[*connection]struct{}
	wg          sync.WaitGroup
}

// Listen binds 127.0.0.1:0. Canceling ctx has the same teardown effect as Close.
func Listen(ctx context.Context, cfg Config) (*Proxy, error) {
	if ctx == nil {
		return nil, errors.New("faultproxy: nil context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	host, port, err := net.SplitHostPort(cfg.Upstream)
	if err != nil {
		return nil, fmt.Errorf("faultproxy: upstream: %w", err)
	}
	ip := net.ParseIP(host)
	n, err := strconv.Atoi(port)
	if ip == nil || !ip.IsLoopback() || err != nil || n < 1 || n > 65535 {
		return nil, errors.New("faultproxy: upstream must be numeric loopback IP with nonzero port")
	}
	if cfg.MaxConnections == 0 {
		cfg.MaxConnections = 64
	}
	if cfg.MaxConnections < 1 || cfg.MaxConnections > 4096 {
		return nil, errors.New("faultproxy: MaxConnections must be 1..4096")
	}
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	child, cancel := context.WithCancel(ctx)
	p := &Proxy{listener: ln, upstream: cfg.Upstream, limit: cfg.MaxConnections, ctx: child, cancel: cancel, changed: make(chan struct{}), done: make(chan struct{}), connections: make(map[*connection]struct{})}
	p.wg.Add(2)
	go p.accept()
	go func() { defer p.wg.Done(); <-child.Done(); p.shutdown() }()
	go func() { p.wg.Wait(); close(p.done) }()
	return p, nil
}

func (p *Proxy) Addr() string          { return p.listener.Addr().String() }
func (p *Proxy) Done() <-chan struct{} { return p.done }
func (p *Proxy) Snapshot() Stats       { p.mu.Lock(); defer p.mu.Unlock(); return p.stats }
func (p *Proxy) notify()               { close(p.changed); p.changed = make(chan struct{}) }

func (p *Proxy) update(d Direction, f func(*policy)) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if d > ServerToClient {
		return 0, errors.New("faultproxy: invalid direction")
	}
	if p.closed {
		return 0, ErrClosed
	}
	f(&p.policies[d])
	p.notify()
	return p.stats.Directions[d].Interceptions, nil
}

// Pause returns a barrier baseline atomically with enabling the gate. Resume
// releases held chunks; blackhole, when also enabled, discards them on release.
func (p *Proxy) Pause(d Direction) (uint64, error) {
	return p.update(d, func(s *policy) { s.paused = true })
}
func (p *Proxy) Resume(d Direction) error {
	_, err := p.update(d, func(s *policy) { s.paused = false })
	return err
}
func (p *Proxy) Blackhole(d Direction, enabled bool) (uint64, error) {
	return p.update(d, func(s *policy) { s.blackhole = enabled })
}

// Delay is per read chunk, bounded by MaxDelay. Changing it wakes pending
// delays; elapsed time counts toward the new delay. Pause and delay overlap.
func (p *Proxy) Delay(d Direction, delay time.Duration) (uint64, error) {
	if delay < 0 || delay > MaxDelay {
		return 0, errors.New("faultproxy: delay outside 0..MaxDelay")
	}
	return p.update(d, func(s *policy) { s.delay = delay })
}

// WaitBarrier waits for an interception strictly newer than after. It proves
// bytes reached an active fault gate, not that an upstream transaction committed.
// Use the baseline returned by Pause/Blackhole/Delay, not timing sleeps.
func (p *Proxy) WaitBarrier(ctx context.Context, d Direction, after uint64) (DirectionStats, error) {
	if d > ServerToClient {
		return DirectionStats{}, errors.New("faultproxy: invalid direction")
	}
	for {
		p.mu.Lock()
		s := p.stats.Directions[d]
		ch := p.changed
		closed := p.closed
		p.mu.Unlock()
		if s.Interceptions > after {
			return s, nil
		}
		if closed {
			return s, ErrClosed
		}
		select {
		case <-ctx.Done():
			return s, ctx.Err()
		case <-ch:
		}
	}
}

// Cut closes both ends of all connections, including pending upstream dials,
// and rejects new connections until Restore. Other directional policies remain.
func (p *Proxy) Cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cut = true
	for c := range p.connections {
		c.cancel()
		c.client.Close()
		if c.server != nil {
			c.server.Close()
		}
	}
	p.notify()
}
func (p *Proxy) Restore() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrClosed
	}
	p.cut = false
	p.notify()
	return nil
}

func (p *Proxy) shutdown() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	p.cancel()
	p.listener.Close()
	for c := range p.connections {
		c.cancel()
		c.client.Close()
		if c.server != nil {
			c.server.Close()
		}
	}
	p.notify()
}

// Close is concurrent/idempotent and waits for all owned goroutines. Closing
// sockets interrupts blocked reads/writes; cancellation interrupts gates/dials.
func (p *Proxy) Close() error { p.shutdown(); <-p.done; return nil }

func (p *Proxy) accept() {
	defer p.wg.Done()
	for {
		client, err := p.listener.Accept()
		if err != nil {
			p.shutdown()
			return
		}
		p.mu.Lock()
		if p.closed || p.cut || len(p.connections) >= p.limit {
			p.stats.Rejected++
			p.mu.Unlock()
			client.Close()
			continue
		}
		ctx, cancel := context.WithCancel(p.ctx)
		c := &connection{client: client, ctx: ctx, cancel: cancel}
		p.connections[c] = struct{}{}
		p.stats.Accepted++
		p.stats.Active++
		p.wg.Add(1)
		p.mu.Unlock()
		go p.serve(c)
	}
}

func (p *Proxy) serve(c *connection) {
	defer p.wg.Done()
	defer c.cancel()
	defer func() {
		c.client.Close()
		if c.server != nil {
			c.server.Close()
		}
		p.mu.Lock()
		delete(p.connections, c)
		p.stats.Active--
		p.notify()
		p.mu.Unlock()
	}()
	server, err := (&net.Dialer{Timeout: time.Second}).DialContext(c.ctx, "tcp", p.upstream)
	if err != nil {
		return
	}
	p.mu.Lock()
	c.server = server
	if p.closed || p.cut || c.ctx.Err() != nil {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	// A connection has exactly two pumps, joined before it leaves the registry.
	done := make(chan struct{})
	go func() {
		p.pump(c.ctx, c.client, server, ClientToServer)
		c.cancel()
		c.client.Close()
		server.Close()
		close(done)
	}()
	p.pump(c.ctx, server, c.client, ServerToClient)
	c.cancel()
	c.client.Close()
	server.Close()
	<-done
}

func (p *Proxy) pump(ctx context.Context, src, dst net.Conn, d Direction) {
	buf := make([]byte, bufferSize)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			p.mu.Lock()
			p.stats.Directions[d].ReadBytes += uint64(n)
			p.mu.Unlock()
			drop, ok := p.gate(ctx, d, n)
			if !ok {
				return
			}
			if !drop {
				written := 0
				for written < n {
					m, werr := dst.Write(buf[written:n])
					written += m
					p.mu.Lock()
					p.stats.Directions[d].ForwardedBytes += uint64(m)
					p.mu.Unlock()
					if werr != nil || m == 0 {
						return
					}
				}
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *Proxy) gate(ctx context.Context, d Direction, n int) (drop, ok bool) {
	start := time.Now()
	counted := false
	for {
		p.mu.Lock()
		if p.closed || p.cut || ctx.Err() != nil {
			p.mu.Unlock()
			return false, false
		}
		s := p.policies[d]
		if !counted && (s.paused || s.blackhole || s.delay > 0) {
			p.stats.Directions[d].Interceptions++
			p.stats.Directions[d].InterceptedBytes += uint64(n)
			counted = true
			p.notify()
		}
		wait := s.delay - time.Since(start)
		if !s.paused && (s.blackhole || wait <= 0) {
			if s.blackhole {
				p.stats.Directions[d].DroppedBytes += uint64(n)
				p.notify()
			}
			p.mu.Unlock()
			return s.blackhole, true
		}
		ch := p.changed
		p.mu.Unlock()
		if s.paused {
			select {
			case <-ctx.Done():
				return false, false
			case <-ch:
			}
		} else {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return false, false
			case <-ch:
				timer.Stop()
			case <-timer.C:
			}
		}
	}
}
