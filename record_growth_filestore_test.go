package durable_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/filestore"
)

func TestRecordGrowthHistorySurvivesDrainAndResume(t *testing.T) {
	const count, payloadBytes = 8, 32
	store := growthFileStore(t)
	g, output, calls := growthGraph(t, count, payloadBytes)
	rt := growthRuntime(t, store)
	first := growthRun(t, rt, g, "growth-run")
	growthCheckOutput(t, first, output, payloadBytes)
	before := growthLoad(t, store, "growth-run")
	if len(before.Messages) != count || len(before.Streams) != 1 {
		t.Fatalf("messages=%d streams=%d; want %d and 1", len(before.Messages), len(before.Streams), count)
	}
	seen := make(map[uint64]bool)
	for _, message := range before.Messages {
		if message.State != durable.AttemptConfirmed || message.Sequence < 1 || message.Sequence > count || seen[message.Sequence] {
			t.Fatalf("invalid retained attempt: %+v", message)
		}
		seen[message.Sequence] = true
	}
	for _, stream := range before.Streams {
		if len(stream.Pending) != 0 || stream.AckedThrough != count || stream.LastSequence != count {
			t.Fatalf("stream did not drain: %+v", stream)
		}
	}
	// A new Store and Runtime must read the file, rather than a MemoryStore copy.
	reopened, err := filestore.New(store.dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resumed, err := growthRuntime(t, reopened).Resume(ctx, g, pulse.Inputs{}, durable.RunOptions{RunID: "growth-run", Version: "growth/1"})
	if err != nil {
		t.Fatal(err)
	}
	growthCheckOutput(t, resumed, output, payloadBytes)
	after := growthLoad(t, reopened, "growth-run")
	if calls.Load() != 2 || !reflect.DeepEqual(after.Messages, before.Messages) {
		t.Fatalf("resume changed completed work: calls=%d messages=%d", calls.Load(), len(after.Messages))
	}
	for _, stream := range after.Streams {
		if len(stream.Pending) != 0 || stream.AckedThrough != count {
			t.Fatalf("resume changed drained frontier: %+v", stream)
		}
	}
	t.Logf("pending=0 confirmed=%d revision=%d resumed-revision=%d initial-saves=%d initial-written-B=%d", count, before.Revision, after.Revision, store.saves, store.bytes)
}

// Each fixture starts with a real, completed eight-message run. Larger history
// and backlog records are constructed for component costs, not resumed as runs.
func BenchmarkRecordGrowth(b *testing.B) {
	for _, payloadBytes := range []int{32, 4096} {
		g, _, _ := growthGraph(b, 8, payloadBytes)
		store := durable.NewMemoryStore()
		growthRun(b, growthRuntime(b, store), g, "growth-run")
		template := growthLoad(b, store, "growth-run")
		for _, count := range []int{8, 128, 1024} {
			for _, backlog := range []bool{false, true} {
				mode := "drained"
				if backlog {
					mode = "backlog"
				}
				name := fmt.Sprintf("%s/history=%d/payload=%d", mode, count, payloadBytes)
				record := growthFixture(b, template, count, payloadBytes, backlog)
				encoded := growthJSON(b, record)
				b.Run(name+"/Clone", func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						growthRecordSink = durable.CloneRecordForGrowthBenchmark(record)
					}
					b.StopTimer()
					b.ReportMetric(float64(len(encoded)), "record-B")
				})
				b.Run(name+"/JSON", func(b *testing.B) {
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						data, err := json.Marshal(record)
						if err != nil {
							b.Fatal(err)
						}
						growthBytesSink = data
					}
					b.StopTimer()
					b.ReportMetric(float64(len(encoded)), "record-B")
				})
				b.Run(name+"/FileApply", func(b *testing.B) {
					b.StopTimer()
					store := growthFileStore(b)
					session, err := store.Store.Acquire(context.Background(), record.RunID)
					if err != nil {
						b.Fatal(err)
					}
					b.Cleanup(func() {
						if err := session.Close(); err != nil {
							b.Error(err)
						}
					})
					seed := durable.CloneRecordForGrowthBenchmark(record)
					seed.Revision = 1
					seed.Attempts = nil
					seed.Streams = nil
					seed.Messages = nil
					seed.Managed = nil
					root := seed.Scopes[seed.RootScope]
					root.Nodes = nil
					seed.Scopes = map[string]durable.ScopeRecord{seed.RootScope: root}
					if err := session.Initialize(context.Background(), seed); err != nil {
						b.Fatal(err)
					}
					batches, err := durable.GrowthSeedBatchesForTest(seed, record)
					if err != nil {
						b.Fatal(err)
					}
					for _, batch := range batches {
						if _, err := session.Apply(context.Background(), batch); err != nil {
							b.Fatal(err)
						}
					}
					next, err := session.Load(context.Background())
					if err != nil {
						b.Fatal(err)
					}
					expected := durable.CloneRecord(record)
					expected.Revision = next.Revision
					if !reflect.DeepEqual(next, expected) {
						b.Fatal("seed background projection mismatch")
					}
					messageID := fmt.Sprintf("%032x", 1)
					var written, wireBytes int64
					b.ReportAllocs()
					b.ResetTimer()
					b.StartTimer()
					for i := 0; i < b.N; i++ {
						batch, err := durable.GrowthUpdateForTest(next, messageID)
						if err != nil {
							b.Fatal(err)
						}
						if _, err := session.Apply(context.Background(), batch); err != nil {
							b.Fatal(err)
						}
						b.StopTimer()
						// Count actual newly persisted batch envelope plus the replaced head. The
						// head's old bytes were overwritten, so directory size delta is insufficient.
						wire, err := durable.MarshalChangeBatch(batch)
						if err != nil {
							b.Fatal(err)
						}
						wireBytes += int64(len(wire))
						bytes, err := growthCommitBytes(store.dir, batch.Header().RunID, batch.Header().Revision)
						if err != nil {
							b.Fatal(err)
						}
						written += bytes
						if _, err := batch.ApplyTo(&next); err != nil {
							b.Fatal(err)
						}
						b.StartTimer()
					}
					b.StopTimer()
					b.ReportMetric(float64(written)/float64(b.N), "written-B/op")
					b.ReportMetric(float64(wireBytes)/float64(b.N), "wire-B/op")
					persisted, err := session.Load(context.Background())
					if err != nil {
						b.Fatal(err)
					}
					if !reflect.DeepEqual(persisted, next) {
						b.Fatal("incremental benchmark projection mismatch")
					}
					bytes, err := growthDiskBytes(store.dir)
					if err != nil {
						b.Fatal(err)
					}
					b.ReportMetric(float64(bytes), "stored-B")
				})
			}
		}
	}
}

// Keep real runs bounded: 1024-message costs are component fixtures above.
// Each successful Apply counts its immutable envelope and replaced head bytes;
// initialization counts its new snapshot and head. Measurement overhead is
// included in this instrumented pipeline benchmark only (not FileApply).
func BenchmarkRecordGrowthPipeline(b *testing.B) {
	for _, count := range []int{8, 128} {
		for _, payloadBytes := range []int{32, 4096} {
			b.Run(fmt.Sprintf("messages=%d/payload=%d", count, payloadBytes), func(b *testing.B) {
				store := growthFileStore(b)
				g, output, _ := growthGraph(b, count, payloadBytes)
				rt := growthRuntime(b, store)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					e := growthRun(b, rt, g, fmt.Sprintf("growth-%08d", i))
					growthCheckOutput(b, e, output, payloadBytes)
				}
				b.StopTimer()
				last := growthLoad(b, store, fmt.Sprintf("growth-%08d", b.N-1))
				b.ReportMetric(float64(len(growthJSON(b, last))), "record-B")
				b.ReportMetric(float64(store.saves)/float64(b.N), "saves/run")
				b.ReportMetric(float64(store.bytes)/float64(b.N), "written-B/run")
			})
		}
	}
}

var (
	growthRecordSink durable.RunRecord
	growthBytesSink  []byte
)

func growthFixture(tb testing.TB, template durable.RunRecord, count, payloadBytes int, backlog bool) durable.RunRecord {
	tb.Helper()
	record := durable.CloneRecordForGrowthBenchmark(template)
	record.Revision = 1
	var sample durable.MessageAttemptRecord
	for _, message := range record.Messages {
		sample = message
		break
	}
	record.Messages = make(map[string]durable.MessageAttemptRecord, count)
	for i := 1; i <= count; i++ {
		sample.Sequence = uint64(i)
		record.Messages[fmt.Sprintf("%032x", i)] = sample
	}
	for key, stream := range record.Streams {
		stream.LastSequence, stream.AckedThrough, stream.Pending = uint64(count), uint64(count), nil
		if backlog {
			stream.LastSequence = uint64(2 * count)
			stream.Pending = make([]durable.StreamMessageRecord, count)
			for i := range stream.Pending {
				stream.Pending[i] = durable.StreamMessageRecord{Sequence: uint64(count + i + 1), Value: growthValue(tb, "input", strings.Repeat("x", payloadBytes), "go/string")}
			}
		}
		cursor := growthValue(tb, "", int(stream.LastSequence), "go/int")
		stream.Cursor = &cursor
		record.Streams[key] = stream
	}
	for key, managed := range record.Managed {
		managed.State = []durable.ValueRecord{growthValue(tb, "bytes", count*payloadBytes, "go/int")}
		record.Managed[key] = managed
	}
	return record
}

func growthValue(tb testing.TB, name string, value any, typeID string) durable.ValueRecord {
	tb.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		tb.Fatal(err)
	}
	digest := sha256.Sum256(data)
	return durable.ValueRecord{Name: name, TypeID: typeID, Version: "1", State: "ready", Data: data, Digest: hex.EncodeToString(digest[:])}
}

func growthGraph(tb testing.TB, count, payloadBytes int) (*pulse.Graph, pulse.Key[string], *atomic.Int64) {
	tb.Helper()
	g, err := pulse.New("record-growth", "")
	if err != nil {
		tb.Fatal(err)
	}
	input, output := pulse.NewKey[pulse.Stream[string]]("input"), pulse.NewKey[string]("last")
	calls := new(atomic.Int64)
	payload := strings.Repeat("x", payloadBytes)
	err = g.Add(pulse.NewNode("source", "", nil, pulse.Provides(input), func(rc *pulse.RunCtx) error {
		calls.Add(1)
		writer, err := pulse.OpenStream(rc, input, pulse.WithBuffer(1), pulse.WithStreamSource("growth-source", true))
		if err != nil {
			return err
		}
		for i := int(writer.Position().Sequence) + 1; i <= count; i++ {
			if err := writer.SendAt(payload, i); err != nil {
				return err
			}
		}
		return nil
	}))
	if err != nil {
		tb.Fatal(err)
	}
	err = g.Add(pulse.NewNode("sink", "", pulse.Requires(input), pulse.Provides(output), func(rc *pulse.RunCtx) error {
		calls.Add(1)
		stream, err := pulse.Get(rc, input)
		if err != nil {
			return err
		}
		for {
			delivery, err := stream.Receive(rc)
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
			if err := pulse.HandleMessage(rc, delivery, func(rc *pulse.RunCtx, value string) error {
				previous, _, err := pulse.GetMessageState[int](rc, "bytes")
				if err != nil {
					return err
				}
				next := previous + len(value)
				if err := pulse.SetMessageState(rc, "bytes", next); err != nil {
					return err
				}
				if next == count*payloadBytes {
					return pulse.Set(rc, output, value)
				}
				return nil
			}); err != nil {
				return err
			}
		}
	}))
	if err != nil {
		tb.Fatal(err)
	}
	if err := g.DeclareOutput(pulse.NewPort(output)); err != nil {
		tb.Fatal(err)
	}
	return g, output, calls
}

func growthRuntime(tb testing.TB, store durable.Store) *durable.Runtime {
	tb.Helper()
	rt, err := durable.New(store, durable.NewCodecs())
	if err != nil {
		tb.Fatal(err)
	}
	return rt
}

func growthRun(tb testing.TB, rt *durable.Runtime, g *pulse.Graph, runID string) *pulse.Execution {
	tb.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	e, err := rt.Run(ctx, g, pulse.Inputs{}, durable.RunOptions{RunID: runID, Version: "growth/1"})
	if err != nil {
		tb.Fatal(err)
	}
	return e
}

func growthCheckOutput(tb testing.TB, e *pulse.Execution, output pulse.Key[string], payloadBytes int) {
	tb.Helper()
	value, state, err := pulse.ReadOutput(e, output)
	if err != nil || state != pulse.SlotStateReady || value != strings.Repeat("x", payloadBytes) {
		tb.Fatalf("output length=%d state=%s err=%v", len(value), state, err)
	}
}

func growthLoad(tb testing.TB, store durable.Store, runID string) durable.RunRecord {
	tb.Helper()
	session, err := store.Acquire(context.Background(), runID)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() {
		if err := session.Close(); err != nil {
			tb.Error(err)
		}
	}()
	record, err := session.Load(context.Background())
	if err != nil {
		tb.Fatal(err)
	}
	return record
}

func growthJSON(tb testing.TB, record durable.RunRecord) []byte {
	tb.Helper()
	data, err := json.Marshal(record)
	if err != nil {
		tb.Fatal(err)
	}
	return data
}

// Runtime serializes Save calls for this one-run-at-a-time measurement.
// Count only successful saves, reading the real file size after replacement.
// These are logical JSON bytes, not device writes, reads, or filesystem overhead.
type growthCountingStore struct {
	durable.Store
	dir   string
	saves int64
	bytes int64
}

func growthFileStore(tb testing.TB) *growthCountingStore {
	tb.Helper()
	dir := tb.TempDir()
	store, err := filestore.New(dir)
	if err != nil {
		tb.Fatal(err)
	}
	return &growthCountingStore{Store: store, dir: dir}
}

func (s *growthCountingStore) Acquire(ctx context.Context, runID string) (durable.Session, error) {
	session, err := s.Store.Acquire(ctx, runID)
	if err != nil {
		return nil, err
	}
	return &growthCountingSession{Session: session, store: s, path: growthRecordPath(s.dir, runID)}, nil
}

type growthCountingSession struct {
	durable.Session
	store *growthCountingStore
	path  string
}

func (s *growthCountingSession) Initialize(ctx context.Context, r durable.RunRecord) error {
	before, err := growthDiskBytes(s.store.dir)
	if err != nil {
		return err
	}
	if err := s.Session.Initialize(ctx, r); err != nil {
		return err
	}
	return s.count(before)
}
func (s *growthCountingSession) Apply(ctx context.Context, b durable.ChangeBatch) (durable.CommitAck, error) {
	ack, err := s.Session.Apply(ctx, b)
	if err != nil {
		return durable.CommitAck{}, err
	}
	written, err := growthCommitBytes(s.store.dir, b.Header().RunID, b.Header().Revision)
	if err != nil {
		return durable.CommitAck{}, err
	}
	s.store.saves++
	s.store.bytes += written
	return ack, nil
}
func (s *growthCountingSession) count(before int64) error {
	after, err := growthDiskBytes(s.store.dir)
	if err != nil {
		return err
	}
	s.store.saves++
	s.store.bytes += after - before
	return nil
}
func growthDiskBytes(dir string) (int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return 0, err
		}
		total += info.Size()
	}
	return total, nil
}

// growthCommitBytes counts both files written by one successful Apply, not
// cumulative directory size (which omits the replaced head's write cost).
func growthCommitBytes(dir, runID string, revision uint64) (int64, error) {
	sum := sha256.Sum256([]byte(runID))
	base := hex.EncodeToString(sum[:])
	path := filepath.Join(dir, base+".head")
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	var head struct {
		HeadID   string `json:"head_id"`
		Revision uint64 `json:"head_revision"`
	}
	if err := json.Unmarshal(data, &head); err != nil {
		return 0, err
	}
	if head.Revision != revision {
		return 0, fmt.Errorf("unexpected head revision %d", head.Revision)
	}
	info, err := os.Stat(filepath.Join(dir, base+".batch-"+head.HeadID))
	if err != nil {
		return 0, err
	}
	return int64(len(data)) + info.Size(), nil
}

func growthRecordPath(dir, runID string) string {
	digest := sha256.Sum256([]byte(runID))
	return filepath.Join(dir, hex.EncodeToString(digest[:])+".json")
}
