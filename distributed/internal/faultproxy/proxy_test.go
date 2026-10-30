package faultproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// 所有测试套接字，包括合成上游，均使用回环地址的临时端口。
func upstream(t *testing.T, handler func(net.Conn)) string {
	t.Helper()
	ln, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	conns := make(map[net.Conn]bool)
	var wg sync.WaitGroup
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns[c] = true
			mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer c.Close()
				handler(c)
				mu.Lock()
				delete(conns, c)
				mu.Unlock()
			}()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		// 先等待 accept 结束，确保快照后不会再注册套接字。
		<-acceptDone
		mu.Lock()
		for c := range conns {
			c.Close()
		}
		mu.Unlock()
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("upstream shutdown blocked")
		}
	})
	return ln.Addr().String()
}
func echo(t *testing.T) string { return upstream(t, func(c net.Conn) { _, _ = io.Copy(c, c) }) }
func proxy(t *testing.T, addr string) *Proxy {
	t.Helper()
	p, err := Listen(context.Background(), Config{Upstream: addr})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}
func dial(t *testing.T, p *Proxy) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", p.Addr(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	c.SetDeadline(time.Now().Add(3 * time.Second))
	return c
}
func roundtrip(t *testing.T, c net.Conn, msg string) {
	t.Helper()
	if _, err := io.WriteString(c, msg); err != nil {
		t.Fatal(err)
	}
	b := make([]byte, len(msg))
	if _, err := io.ReadFull(c, b); err != nil {
		t.Fatal(err)
	}
	if string(b) != msg {
		t.Fatalf("unexpected echo length/content")
	}
}
func barrier(t *testing.T, p *Proxy, d Direction, base uint64) DirectionStats {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	s, err := p.WaitBarrier(ctx, d, base)
	if err != nil {
		t.Fatal(err)
	}
	if s.InterceptedBytes == 0 {
		t.Fatal("empty interception")
	}
	return s
}
func closed(t *testing.T, p *Proxy) {
	t.Helper()
	done := make(chan struct{})
	go func() { p.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close blocked")
	}
	if p.Snapshot().Active != 0 {
		t.Fatal("connections remain")
	}
}

// connectionClosed 检查两个方向：写入成功可能只是进入
// 本地 TCP 缓冲区，因此随后必须读取。超时不等于关闭。
func connectionClosed(c net.Conn) error {
	_, err := io.WriteString(c, "cut")
	if err == nil {
		var b [1]byte
		_, err = c.Read(b[:])
	}
	if err == nil {
		return errors.New("connection still delivered bytes")
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return fmt.Errorf("connection closure not observed: %w", err)
	}
	return nil
}

func TestConnectionClosedRejectsOpenBlackhole(t *testing.T) {
	p := proxy(t, echo(t))
	c := dial(t, p)
	roundtrip(t, c, "warm")
	base, err := p.Blackhole(ServerToClient, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SetReadDeadline(time.Now().Add(15 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	err = connectionClosed(c)
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		t.Fatalf("open blackhole must fail closure probe with timeout, got %v", err)
	}
	barrier(t, p, ServerToClient, base)
	if p.Snapshot().Active != 1 {
		t.Fatal("negative control connection was not open")
	}
	// 旧断言接受任意非 nil 的 Read 错误，包括此超时。
	if _, err := p.Blackhole(ServerToClient, false); err != nil {
		t.Fatal(err)
	}
	c.SetDeadline(time.Now().Add(3 * time.Second))
	roundtrip(t, c, "still open")
}

func eventually(t *testing.T, f func() bool) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for !f() {
		if time.Now().After(until) {
			t.Fatal("condition did not converge")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestNormalBidirectional(t *testing.T) {
	p := proxy(t, echo(t))
	c := dial(t, p)
	roundtrip(t, c, "request and response")
	eventually(t, func() bool {
		s := p.Snapshot()
		return s.Directions[0].ForwardedBytes == 20 && s.Directions[1].ForwardedBytes == 20
	})
	if s := p.Snapshot(); s.Accepted != 1 || s.Active != 1 {
		t.Fatalf("stats: %+v", s)
	}
}

// delayedWriteReturn 暴露数据交付与转发协程记账之间的间隔。
// 包装后的 Write 先交付真实字节，再等待测试放行。
type delayedWriteReturn struct {
	net.Conn
	release <-chan struct{}
}

func (c delayedWriteReturn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	<-c.release
	return n, err
}

func TestPauseDoesNotJoinDeliveredWriteAccounting(t *testing.T) {
	p := proxy(t, echo(t))
	src, sender := net.Pipe()
	dst, receiver := net.Pipe()
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		unblock()
		src.Close()
		sender.Close()
		dst.Close()
		receiver.Close()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("pump shutdown blocked")
		}
	})
	go func() {
		defer close(done)
		p.pump(ctx, src, delayedWriteReturn{Conn: dst, release: release}, ServerToClient)
	}()
	go func() { _, _ = io.WriteString(sender, "warm") }()
	receiver.SetReadDeadline(time.Now().Add(3 * time.Second))
	b := make([]byte, 4)
	if _, err := io.ReadFull(receiver, b); err != nil || string(b) != "warm" {
		t.Fatalf("warm delivery: %q, %v", b, err)
	}
	if _, err := p.Pause(ServerToClient); err != nil {
		t.Fatal(err)
	}
	before := p.Snapshot().Directions[ServerToClient]
	if before.ForwardedBytes != 0 {
		t.Fatalf("Write has not returned, got %+v", before)
	}
	unblock()
	eventually(t, func() bool {
		return p.Snapshot().Directions[ServerToClient].ForwardedBytes == 4
	})
	if s := p.Snapshot().Directions[ServerToClient]; s.Interceptions != 0 {
		t.Fatalf("only the pre-Pause warm bytes were sent, got %+v", s)
	}
}

func TestPauseResumeBothDirectionsExistingAndNew(t *testing.T) {
	for _, d := range []Direction{ClientToServer, ServerToClient} {
		t.Run(map[Direction]string{ClientToServer: "request", ServerToClient: "response"}[d], func(t *testing.T) {
			p := proxy(t, echo(t))
			old := dial(t, p)
			roundtrip(t, old, "warm")
			// 收到 warm 并不代表已等待转发协程完成 Write 后的记账。
			eventually(t, func() bool {
				s := p.Snapshot()
				return s.Directions[0].ForwardedBytes == 4 && s.Directions[1].ForwardedBytes == 4
			})
			base, err := p.Pause(d)
			if err != nil {
				t.Fatal(err)
			}
			conns := []net.Conn{old, dial(t, p)}
			for _, c := range conns {
				before := p.Snapshot().Directions[d].ForwardedBytes
				if _, err := io.WriteString(c, "held"); err != nil {
					t.Fatal(err)
				}
				s := barrier(t, p, d, base)
				base = s.Interceptions
				if p.Snapshot().Directions[d].ForwardedBytes != before {
					t.Fatal("gate forwarded held bytes")
				}
			}
			if err := p.Resume(d); err != nil {
				t.Fatal(err)
			}
			for i, c := range conns {
				b := make([]byte, 4)
				if _, err := io.ReadFull(c, b); err != nil {
					t.Fatalf("connection %d resume: %v", i, err)
				}
				if string(b) != "held" {
					t.Fatalf("connection %d resumed payload: %q", i, b)
				}
			}
		})
	}
}

func TestBlackholeResponseAndRequest(t *testing.T) {
	for _, d := range []Direction{ClientToServer, ServerToClient} {
		p := proxy(t, echo(t))
		c := dial(t, p)
		base, err := p.Blackhole(d, true)
		if err != nil {
			t.Fatal(err)
		}
		io.WriteString(c, "lost")
		barrier(t, p, d, base)
		eventually(t, func() bool { return p.Snapshot().Directions[d].DroppedBytes == 4 })
		c.SetReadDeadline(time.Now().Add(15 * time.Millisecond))
		b := make([]byte, 1)
		_, err = c.Read(b)
		var ne net.Error
		if !errors.As(err, &ne) || !ne.Timeout() {
			t.Fatalf("expected absent response, got %v", err)
		}
		if _, err = p.Blackhole(d, false); err != nil {
			t.Fatal(err)
		}
		c.SetDeadline(time.Now().Add(3 * time.Second))
		roundtrip(t, c, "next")
		closed(t, p)
	}
}

func TestBlackholeDoesNotBlockShutdown(t *testing.T) {
	p := proxy(t, upstream(t, func(c net.Conn) {
		b := make([]byte, bufferSize)
		for {
			if _, err := c.Write(b); err != nil {
				return
			}
		}
	}))
	base, _ := p.Blackhole(ServerToClient, true)
	_ = dial(t, p)
	barrier(t, p, ServerToClient, base)
	closed(t, p)
}

func TestDelayBoundAndCutRacesMultipleConnections(t *testing.T) {
	p := proxy(t, echo(t))
	if _, err := p.Delay(ClientToServer, MaxDelay+1); err == nil {
		t.Fatal("accepted excessive delay")
	}
	if _, err := p.Delay(ClientToServer, -1); err == nil {
		t.Fatal("accepted negative delay")
	}
	base, _ := p.Delay(ClientToServer, MaxDelay)
	for i := 0; i < 6; i++ {
		c := dial(t, p)
		io.WriteString(c, "delay")
	}
	barrier(t, p, ClientToServer, base)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				p.Delay(ClientToServer, time.Millisecond)
				p.Pause(ServerToClient)
				p.Resume(ServerToClient)
				p.Cut()
				p.Restore()
				p.Snapshot()
			}
		}()
	}
	wg.Wait()
	p.Cut()
	eventually(t, func() bool { return p.Snapshot().Active == 0 })
	c := dial(t, p)
	if err := connectionClosed(c); err != nil {
		t.Fatalf("cut did not close connection: %v", err)
	}
	p.Restore()
	p.Delay(ClientToServer, 0)
	roundtrip(t, dial(t, p), "restored")
	closed(t, p)
}

func TestDelayActuallyHoldsAndReleases(t *testing.T) {
	p := proxy(t, echo(t))
	c := dial(t, p)
	base, _ := p.Delay(ClientToServer, 40*time.Millisecond)
	start := time.Now()
	io.WriteString(c, "slow")
	barrier(t, p, ClientToServer, base)
	b := make([]byte, 4)
	if _, err := io.ReadFull(c, b); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 40*time.Millisecond {
		t.Fatal("delay returned early")
	}
}

func TestCancellationReleasesPauseReadAndWrite(t *testing.T) {
	for _, mode := range []string{"pause", "idle", "write"} {
		t.Run(mode, func(t *testing.T) {
			addr := echo(t)
			if mode == "write" {
				release := make(chan struct{})
				addr = upstream(t, func(c net.Conn) { c.(*net.TCPConn).SetReadBuffer(1024); <-release })
				t.Cleanup(func() { close(release) })
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			p, err := Listen(ctx, Config{Upstream: addr})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { p.Close() })
			c := dial(t, p)
			if mode == "pause" {
				base, _ := p.Pause(ClientToServer)
				io.WriteString(c, "held")
				barrier(t, p, ClientToServer, base)
			}
			var writer chan struct{}
			if mode == "write" {
				writer = make(chan struct{})
				go func() {
					defer close(writer)
					b := make([]byte, bufferSize)
					for {
						if _, err := c.Write(b); err != nil {
							return
						}
					}
				}()
				eventually(t, func() bool { return p.Snapshot().Directions[0].ReadBytes > 0 })
			}
			cancel()
			select {
			case <-p.Done():
			case <-time.After(3 * time.Second):
				t.Fatal("cancel did not release proxy")
			}
			if writer != nil {
				c.Close()
				select {
				case <-writer:
				case <-time.After(time.Second):
					t.Fatal("writer blocked")
				}
			}
			if p.Snapshot().Active != 0 {
				t.Fatal("active after cancel")
			}
		})
	}
}

func TestLimitsValidationAndBarrierCancellation(t *testing.T) {
	for _, addr := range []string{"example.com:80", "192.0.2.1:1", "127.0.0.1:0"} {
		if p, err := Listen(context.Background(), Config{Upstream: addr}); err == nil {
			p.Close()
			t.Fatal("accepted non-loopback or invalid upstream")
		}
	}
	p, err := Listen(context.Background(), Config{Upstream: echo(t), MaxConnections: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	c := dial(t, p)
	roundtrip(t, c, "one")
	_ = dial(t, p)
	eventually(t, func() bool { return p.Snapshot().Rejected > 0 })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.WaitBarrier(ctx, ClientToServer, 0); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := p.Pause(Direction(2)); err == nil {
		t.Fatal("invalid direction")
	}
	closed(t, p)
	if _, err := p.Pause(ClientToServer); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := p.WaitBarrier(context.Background(), ClientToServer, 0); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
