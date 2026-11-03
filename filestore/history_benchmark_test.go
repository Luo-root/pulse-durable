package filestore

import (
	"fmt"
	"testing"

	durable "github.com/Luo-root/pulse-durable"
)

// 固定 origin/base 为 1，仅增加保留批数，区分必需快照大小与无关历史成本。
// 同一真实源码树、公开依赖和本地持久后端；准备写入不计入计时。
func BenchmarkHistoryPathCost(b *testing.B) {
	for _, count := range []int{128, 256, 512, 1024} {
		b.Run(fmt.Sprintf("H%d", count), func(b *testing.B) {
			st, err := New(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			ss, err := st.Acquire(b.Context(), "cost")
			if err != nil {
				b.Fatal(err)
			}
			r := incrementalSeed("cost")
			if err = ss.Initialize(b.Context(), r); err != nil {
				b.Fatal(err)
			}
			for i := 0; i < count; i++ {
				batch := revisionBatch(b, r)
				if _, err = ss.Apply(b.Context(), batch); err != nil {
					b.Fatal(err)
				}
				r.Revision++
			}
			if err = ss.Close(); err != nil {
				b.Fatal(err)
			}
			b.Run("LoadRevision1", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					image, err := st.LoadRevision(b.Context(), "cost", 1)
					if err != nil || image.Record.Revision != 1 {
						b.Fatalf("load: %v", err)
					}
				}
			})
			b.Run("CompactSameBase", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					state, err := st.CompactStorage(b.Context(), durable.CompactStorageRequest{RunID: "cost", ThroughRevision: 1, ExpectedGeneration: 1})
					if err != nil || state.Generation != 1 || state.HeadRevision != uint64(count+1) {
						b.Fatalf("compact: %v", err)
					}
				}
			})
		})
	}
}
