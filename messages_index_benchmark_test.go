package durable

import (
	"context"
	"fmt"
	"testing"

	"github.com/Luo-root/pulse"
)

// 使用完整受管消息执行路径；每轮保留全部已确认消息历史。
// 对照时统一公开 Pulse 依赖、GOMAXPROCS 和执行次数。
func BenchmarkMessageIndexRuntimeManaged(b *testing.B) {
	for _, n := range []int{128, 512, 2048} {
		b.Run(fmt.Sprintf("messages=%d", n), func(b *testing.B) {
			total := 0
			g := deltaManagedGraph(b, n, &total)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				o := &deltaTestStore{benchmark: true}
				r, err := New(o, NewCodecs())
				if err != nil {
					b.Fatal(err)
				}
				if _, err := r.Run(context.Background(), g, pulse.Inputs{}, RunOptions{RunID: "message-index-bench", Version: "1"}); err != nil {
					b.Fatal(err)
				}
				if total != n*(n+1)/2 || len(o.record.Messages) != n {
					b.Fatalf("sum=%d retained=%d messages=%d", total, len(o.record.Messages), n)
				}
			}
		})
	}
}
