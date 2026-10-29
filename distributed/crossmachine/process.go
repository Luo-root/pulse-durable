// crossmachine 包控制显式配置的 systemtest 测试框架进程。
// 绝不发现主机、安装二进制文件或通过 argv 转发凭据。
package crossmachine

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/Luo-root/pulse-durable/distributed/systemtest"
)

// Command 是可执行文件及固定参数向量，而非 shell 片段。
// Env 将子进程变量名映射到父进程变量名。对于 SSH，这仅配置
// 本地 ssh 或包装进程：可信的父包装程序必须安排远端
// 注入（例如使用已有的受保护远端环境）；标准输入用于 JSON 控制。
// SSH 本身通过远端登录 shell 解释远端命令。只提供
// 可信的固定命令参数，不提供凭据或用户 shell 片段。
type Command struct {
	Path string            `json:"path"`
	Args []string          `json:"args"`
	Env  map[string]string `json:"command_env,omitempty"`
}

type Process interface {
	Send(string) error
	Events() []systemtest.Event
	Exited() (bool, error)
	Close() error
}
type Launcher func(context.Context, Command, []string, []string) (Process, error)

// ErrIncomplete 表示事件收集被中断；即使是预期终止，
// 也不能将不完整的事件流当成恢复通过的证据。
var ErrIncomplete = errors.New("event stream incomplete")
var ErrProcessFailed = errors.New("process failed")

type execProcess struct {
	cmd    *exec.Cmd
	input  io.WriteCloser
	output io.ReadCloser
	mu     sync.Mutex
	sendMu sync.Mutex
	events []systemtest.Event
	exited bool
	err    error
	done   chan struct{}
}

// Launch 直接使用 exec；有意丢弃标准错误，以免
// 报告或终端日志泄露含凭据的 ssh 或后端诊断。
func Launch(ctx context.Context, c Command, args, env []string) (Process, error) {
	if c.Path == "" {
		return nil, errors.New("empty executable")
	}
	cmd := exec.CommandContext(ctx, c.Path, append(append([]string(nil), c.Args...), args...)...)
	cmd.Env = env
	cmd.Stderr = io.Discard
	input, err := cmd.StdinPipe()
	if err != nil {
		return nil, errors.New("stdin pipe failed")
	}
	// 持有读取端：Wait 必须不依赖后代进程的 EOF 回收包装进程，
	// 且不得在扫描器读取完毕前关闭标准输出。
	output, writer, err := os.Pipe()
	if err != nil {
		_ = input.Close()
		return nil, errors.New("stdout pipe failed")
	}
	cmd.Stdout = writer
	cmd.WaitDelay = time.Second
	if err = cmd.Start(); err != nil {
		_ = input.Close()
		_ = output.Close()
		_ = writer.Close()
		return nil, errors.New("process start failed")
	}
	_ = writer.Close()
	p := &execProcess{cmd: cmd, input: input, output: output, done: make(chan struct{})}
	scanDone := make(chan struct{})
	go func() {
		defer close(scanDone)
		defer output.Close()
		s := bufio.NewScanner(output)
		s.Buffer(make([]byte, 4096), 64*1024)
		malformed := false
		for s.Scan() {
			var e systemtest.Event
			if json.Unmarshal(s.Bytes(), &e) != nil {
				malformed = true
				continue
			}
			if !eventKind(e.Kind) {
				continue
			}
			p.mu.Lock()
			// 即使可执行程序配置错误或输出过多，也限制内存用量。
			if len(p.events) < 100000 {
				p.events = append(p.events, e)
			} else {
				malformed = true
			}
			p.mu.Unlock()
		}
		p.mu.Lock()
		if (s.Err() != nil || malformed) && p.err == nil {
			p.err = errors.New("process/protocol failed")
		}
		p.mu.Unlock()
	}()
	go func() {
		err := cmd.Wait()
		// 包装进程退出后，其后代可能仍持有标准输出。为缓冲
		// 输出提供有界宽限期，随后报错，不虚构完整证据。
		select {
		case <-scanDone:
		case <-ctx.Done():
			_ = output.Close()
			<-scanDone
			p.mu.Lock()
			p.err = ErrIncomplete
			p.mu.Unlock()
		case <-time.After(time.Second):
			_ = output.Close()
			<-scanDone
			p.mu.Lock()
			p.err = ErrIncomplete
			p.mu.Unlock()
		}
		p.mu.Lock()
		if err != nil && p.err == nil {
			p.err = ErrProcessFailed
		}
		p.exited = true
		p.mu.Unlock()
		close(p.done)
	}()
	return p, nil
}
func eventKind(s string) bool {
	switch s {
	case "ready", "owned", "prefix", "suffix", "before_finish", "finish_held", "draining", "drained", "claim", "renew", "finish", "release", "blocked", "lost", "resolve", "save", "error":
		return true
	}
	// 工作进程观察事件名不是业务载荷。未知事件
	// 由小型允许列表在源头忽略，绝不写入报告。
	return false
}
func (p *execProcess) Send(s string) error {
	switch s {
	case "start", "release", "drain", "probe_finish":
	default:
		return errors.New("invalid control command")
	}
	p.sendMu.Lock()
	defer p.sendMu.Unlock()
	if json.NewEncoder(p.input).Encode(s) != nil {
		return errors.New("control write failed")
	}
	return nil
}
func (p *execProcess) Events() []systemtest.Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]systemtest.Event(nil), p.events...)
}
func (p *execProcess) Exited() (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.exited, p.err
}

// Close 先关闭标准输入（取消测试框架）。终止 SSH 客户端并不
// 证明远端工作进程已终止。成功运行要求排空且退出；
// 失败的远端运行可能需要父进程根据记录的 PID 和启动标识清理。
func (p *execProcess) Close() error {
	_ = p.input.Close()
	select {
	case <-p.done:
		return nil
	default:
	}
	// 即使后代继承了写句柄，也解除扫描器阻塞。
	p.mu.Lock()
	p.err = ErrIncomplete
	p.mu.Unlock()
	_ = p.output.Close()
	if p.cmd.Process != nil {
		_ = p.cmd.Process.Kill()
	}
	select {
	case <-p.done:
		return nil
	case <-time.After(2 * time.Second):
		return errors.New("local process reap timed out; remote state unknown")
	}
}

func commandEnvironment(c Command, forbidden []string) ([]string, error) {
	blocked := map[string]bool{"PULSE_DISTRIBUTED_TEST_DSN": true, "PULSE_DISTRIBUTED_TEST_ALLOW": true}
	for _, k := range forbidden {
		blocked[strings.ToUpper(k)] = true
	}
	for k := range c.Env {
		blocked[strings.ToUpper(k)] = true
	}
	var env []string
	for _, kv := range os.Environ() {
		key, _, _ := strings.Cut(kv, "=")
		if !blocked[strings.ToUpper(key)] {
			env = append(env, kv)
		}
	}
	for child, parent := range c.Env {
		if child == "" || strings.ContainsAny(child, "=\x00") || parent == "" {
			return nil, errors.New("invalid command environment mapping")
		}
		value, ok := os.LookupEnv(parent)
		if !ok {
			return nil, errors.New("command environment source missing")
		}
		env = append(env, child+"="+value)
	}
	return env, nil
}
