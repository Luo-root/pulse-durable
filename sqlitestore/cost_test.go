package sqlitestore

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/recordio"
)

// fullSnapshotObserver is an isolated test oracle for the former physical cost:
// it persists every actual Runtime projection in another SQLite database. It is
// not a Session implementation and cannot be selected as a product write path.
type fullSnapshotObserver struct {
	durable.Store
	legacy  *Store
	bytes   int64
	commits int
}
type observedSession struct {
	durable.Session
	owner      *fullSnapshotObserver
	projection durable.RunRecord
}

func (s *fullSnapshotObserver) Acquire(c context.Context, id string) (durable.Session, error) {
	ss, e := s.Store.Acquire(c, id)
	return &observedSession{Session: ss, owner: s}, e
}
func (s *observedSession) Initialize(c context.Context, r durable.RunRecord) error {
	if e := s.Session.Initialize(c, r); e != nil {
		return e
	}
	data, e := recordio.Encode(r)
	if e != nil {
		return e
	}
	s.projection, e = recordio.Decode(data)
	if e != nil {
		return e
	}
	return s.observe(c)
}
func (s *observedSession) observe(c context.Context) error {
	data, e := recordio.Encode(s.projection)
	if e != nil {
		return e
	}
	_, e = s.owner.legacy.db.ExecContext(c, `INSERT INTO legacy_cost VALUES (?,?) ON CONFLICT(run_id) DO UPDATE SET record=excluded.record`, s.projection.RunID, data)
	s.owner.bytes += int64(len(data))
	s.owner.commits++
	return e
}
func (s *observedSession) Apply(c context.Context, b durable.ChangeBatch) (durable.CommitAck, error) {
	ack, e := s.Session.Apply(c, b)
	if e != nil {
		return ack, e
	}
	if _, e = b.ApplyTo(&s.projection); e != nil {
		return durable.CommitAck{}, e
	}
	if e = s.observe(c); e != nil {
		return durable.CommitAck{}, e
	}
	return ack, nil
}

func TestSQLiteRuntime500AndStorageGrowth(t *testing.T) {
	for _, n := range []int{10, 100, 500} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "incremental.sqlite")
			s := openStore(t, path)
			legacy := openStore(t, filepath.Join(t.TempDir(), "legacy-cost.sqlite"))
			if _, e := legacy.db.Exec(`CREATE TABLE legacy_cost(run_id TEXT PRIMARY KEY,record BLOB)`); e != nil {
				t.Fatal(e)
			}
			observer := &fullSnapshotObserver{Store: s, legacy: legacy}
			rt, e := durable.New(observer, durable.NewCodecs())
			if e != nil {
				t.Fatal(e)
			}
			g, e := pulse.New("sum", "")
			if e != nil {
				t.Fatal(e)
			}
			input := pulse.NewKey[pulse.Stream[int]]("input")
			calls := 0
			if e = g.Add(pulse.NewNode("source", "", nil, pulse.Provides(input), func(rc *pulse.RunCtx) error {
				w, e := pulse.OpenStream(rc, input, pulse.WithBuffer(4), pulse.WithStreamSource("integers", true))
				if e != nil {
					return e
				}
				for v := int(w.Position().Sequence) + 1; v <= n; v++ {
					if e = w.SendAt(v, v); e != nil {
						return e
					}
				}
				return nil
			})); e != nil {
				t.Fatal(e)
			}
			if e = g.Add(pulse.NewNode("consumer", "", pulse.Requires(input), nil, func(rc *pulse.RunCtx) error {
				stream, e := pulse.Get(rc, input)
				if e != nil {
					return e
				}
				for {
					delivery, e := stream.Receive(rc)
					if e == io.EOF {
						return nil
					}
					if e != nil {
						return e
					}
					if e = pulse.HandleMessage(rc, delivery, func(rc *pulse.RunCtx, v int) error {
						calls++
						sum, _, e := pulse.GetMessageState[int](rc, "sum")
						if e != nil {
							return e
						}
						return pulse.SetMessageState(rc, "sum", sum+v)
					}); e != nil {
						return e
					}
				}
			})); e != nil {
				t.Fatal(e)
			}
			if _, e = s.db.Exec(`PRAGMA wal_autocheckpoint=0`); e != nil {
				t.Fatal(e)
			}
			if _, e = legacy.db.Exec(`PRAGMA wal_autocheckpoint=0`); e != nil {
				t.Fatal(e)
			}
			opts := durable.RunOptions{RunID: "sum", Version: "1"}
			if _, e = rt.Run(ctx, g, pulse.Inputs{}, opts); e != nil {
				t.Fatal(e)
			}
			ss := acquire(t, s, "sum")
			r, e := ss.Load(ctx)
			if e != nil {
				t.Fatal(e)
			}
			if calls != n {
				t.Fatalf("calls %d", calls)
			}
			for _, stream := range r.Streams {
				if stream.AckedThrough != uint64(n) || len(stream.Pending) != 0 {
					t.Fatal("ACK", stream)
				}
			}
			found := false
			for _, m := range r.Managed {
				for _, state := range m.State {
					if state.Name == "sum" {
						found = true
						if string(state.Data) != fmt.Sprint(n*(n+1)/2) {
							t.Fatal("sum", string(state.Data))
						}
					}
				}
			}
			if !found {
				t.Fatal("no sum")
			}
			deltaWAL, e := os.Stat(path + "-wal")
			if e != nil {
				t.Fatal(e)
			}
			legacyWAL, e := os.Stat(legacy.locks[:len(legacy.locks)-len(".runlocks")] + "-wal")
			if e != nil {
				t.Fatal(e)
			}
			t.Logf("actual SQLite WAL bytes incremental=%d legacy_full_snapshot=%d", deltaWAL.Size(), legacyWAL.Size())
			ss.Close()
			if e = s.Close(); e != nil {
				t.Fatal(e)
			}
			reopened := openStore(t, path)
			rt, e = durable.New(reopened, durable.NewCodecs())
			if e != nil {
				t.Fatal(e)
			}
			if _, e = rt.Resume(ctx, g, pulse.Inputs{}, opts); e != nil {
				t.Fatal(e)
			}
			if calls != n {
				t.Fatal("Resume replay")
			}
			var payload int64
			if e = reopened.db.QueryRow(`SELECT (SELECT sum(length(record)) FROM durable_snapshots)+(SELECT sum(length(wire)) FROM durable_batches)`).Scan(&payload); e != nil {
				t.Fatal(e)
			}
			// dbstat measures allocated physical B-tree bytes; WAL frame counts measure
			// real SQLite page writes, unlike multiplying the final record size.
			var incrementalBytes, legacyBytes int64
			if e = reopened.db.QueryRow(`SELECT sum(pgsize) FROM dbstat WHERE name IN ('durable_snapshots','durable_heads','durable_batches')`).Scan(&incrementalBytes); e != nil {
				t.Fatal(e)
			}
			if e = legacy.db.QueryRow(`SELECT sum(pgsize) FROM dbstat WHERE name='legacy_cost'`).Scan(&legacyBytes); e != nil {
				t.Fatal(e)
			}
			t.Logf("messages=%d commits=%d delta_payload=%d legacy_cumulative_payload=%d sqlite_allocated_incremental=%d sqlite_allocated_legacy_head=%d", n, observer.commits, payload, observer.bytes, incrementalBytes, legacyBytes)
			if payload >= observer.bytes {
				t.Fatal("incremental payload did not improve cumulative snapshot cost")
			}
		})
	}
}
