package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/filestore"
)

func TestActiveStreamAliasRecoveryAcrossProcessExit(t *testing.T) {
	dir := t.TempDir()
	err := activeStreamAliasProcess(t, dir, "run")
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != crashExit {
		t.Fatalf("confirmed pass-through Sub did not reach its active-source crash boundary: %v", err)
	}
	before := loadStreamRecord(t, dir)
	if err := checkActiveStreamAliasBoundary(before); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"source", "mount", "received-unacked", "saved-active-alias", "sent-1"} {
		if got := callCount(dir, name); got != 1 {
			t.Fatalf("%s calls before exit=%d, want 1", name, got)
		}
	}
	if err := activeStreamAliasProcess(t, dir, "resume"); err != nil {
		t.Fatalf("resume confirmed alias while its real source is active: %v", err)
	}
	after := loadStreamRecord(t, dir)
	if after.RootScope != before.RootScope || after.Revision <= before.Revision || len(after.Streams) != 1 {
		t.Fatal("recovery lost the run identity, progress, or single source ledger")
	}
	for _, stream := range after.Streams {
		if stream.LastSequence != 3 || stream.AckedThrough != 3 || len(stream.Pending) != 0 ||
			stream.Stream.Terminal != string(pulse.StreamCompleted) || stream.Stream.Stopped || stream.Stream.ID.Generation != 2 {
			t.Fatalf("source did not finish with exactly the pending tail and new messages confirmed: %+v", stream)
		}
		var cursor int
		if stream.Cursor == nil || json.Unmarshal(stream.Cursor.Data, &cursor) != nil || cursor != 3 {
			t.Fatalf("final source cursor=%+v, want 3", stream.Cursor)
		}
		source := after.Scopes[after.RootScope].Nodes["source"]
		if source.Attempt != stream.Stream.Attempt || after.Attempts[source.Attempt].State != durable.AttemptConfirmed {
			t.Fatal("completed stream is not owned by the confirmed real source")
		}
	}
	// A further fresh process must restore the completed nodes without repeating
	// their bodies or the now-confirmed deliveries. The call files are observations,
	// never recovery inputs or a claim of exactly-once external side effects.
	if err := activeStreamAliasProcess(t, dir, "completed"); err != nil {
		t.Fatalf("restore the final completed run: %v", err)
	}
	final := loadStreamRecord(t, dir)
	if final.RootScope != after.RootScope || len(final.Streams) != 1 {
		t.Fatal("completed restore changed the run or duplicated the source ledger")
	}
	for _, stream := range final.Streams {
		if stream.Stream.Terminal != string(pulse.StreamCompleted) || stream.Stream.Stopped ||
			stream.LastSequence != 3 || stream.AckedThrough != 3 || len(stream.Pending) != 0 {
			t.Fatalf("completed restore changed the final confirmation ledger: %+v", stream)
		}
	}
	for name, want := range map[string]int{
		"source": 2, "outer": 2, "mount": 1, "sink": 2,
		"received-unacked": 1, "saved-active-alias": 1,
		"resume-open-save": 1, "source-open-overlap": 1,
		"sent-1": 1, "sent-2": 1, "sent-3": 1,
		"consumed-1": 1, "consumed-2": 1, "consumed-3": 1,
	} {
		if got := callCount(dir, name); got != want {
			t.Errorf("%s calls=%d, want %d", name, got, want)
		}
	}
	t.Logf("real exit/restart: source body calls=2, Sub body calls=1, pending 1 + new 2,3 confirmed once; revision=%d→%d", before.Revision, after.Revision)
}

func activeStreamAliasProcess(t *testing.T, dir, phase string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestActiveStreamAliasProcessHelper$", "-test.count=1")
	cmd.Env = append(os.Environ(), "PULSE_ACTIVE_ALIAS_DIR="+dir, "PULSE_ACTIVE_ALIAS_PHASE="+phase)
	output, err := cmd.CombinedOutput()
	if len(output) > 0 {
		t.Logf("%s: %s", phase, output)
	}
	if ctx.Err() != nil {
		t.Fatalf("%s helper deadline: %v", phase, ctx.Err())
	}
	return err
}

type activeStreamAliasGates struct {
	received       chan struct{}
	sourceParked   chan struct{}
	mountSaved     chan struct{}
	aliasOpenSaved chan struct{}
	sourceReady    chan struct{}
	sourceOpening  chan struct{}
}

func TestActiveStreamAliasProcessHelper(t *testing.T) {
	dir := os.Getenv("PULSE_ACTIVE_ALIAS_DIR")
	if dir == "" {
		return
	}
	phase := os.Getenv("PULSE_ACTIVE_ALIAS_PHASE")
	if phase != "run" && phase != "resume" && phase != "completed" {
		t.Fatalf("unknown active alias phase %q", phase)
	}
	gates := &activeStreamAliasGates{
		received: make(chan struct{}), sourceParked: make(chan struct{}), mountSaved: make(chan struct{}),
		aliasOpenSaved: make(chan struct{}), sourceReady: make(chan struct{}), sourceOpening: make(chan struct{}),
	}
	store, err := filestore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	backend := &activeStreamAliasStore{Store: store, dir: dir, phase: phase, gates: gates}
	runtime, err := durable.New(backend, durable.NewCodecs())
	if err != nil {
		t.Fatal(err)
	}
	graph := activeStreamAliasGraph(t, dir, phase, gates)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	opts := durable.RunOptions{RunID: "stream-run", Version: "active-stream-alias/1"}
	var execution *pulse.Execution
	if phase == "run" {
		execution, err = runtime.Run(ctx, graph, pulse.Inputs{}, opts)
	} else {
		execution, err = runtime.Resume(ctx, graph, pulse.Inputs{}, opts)
	}
	if err != nil {
		t.Fatalf("%s: %v", phase, err)
	}
	if phase == "run" {
		t.Fatal("run returned without exiting at the confirmed active alias")
	}
	if execution.Snapshot().Graph.Status != pulse.GraphStatusCompleted {
		t.Fatalf("%s did not complete: %+v", phase, execution.Snapshot().Graph)
	}
}

func activeStreamAliasGraph(t *testing.T, dir, phase string, gates *activeStreamAliasGates) *pulse.Graph {
	t.Helper()
	graph, err := pulse.New("active-stream-alias", "先确认空透传子图，再续读活跃来源")
	if err != nil {
		t.Fatal(err)
	}
	child, err := pulse.New("empty-pass-through", "只绑定同一个流入口和出口")
	if err != nil {
		t.Fatal(err)
	}
	original, alias := pulse.NewKey[pulse.Stream[int]]("original"), pulse.NewKey[pulse.Stream[int]]("alias")
	inlet := pulse.NewKey[pulse.Stream[int]]("inlet")
	if err := child.DeclareInput(pulse.NewPort(inlet)); err != nil {
		t.Fatal(err)
	}
	if err := child.DeclareOutput(pulse.NewPort(inlet)); err != nil {
		t.Fatal(err)
	}
	if err := graph.Add(pulse.NewNode("source", "可续读的实际来源", nil, pulse.Provides(original), func(rc *pulse.RunCtx) error {
		logCall(dir, "source")
		if phase == "resume" {
			close(gates.sourceReady)
			if err := waitActiveStreamAlias(rc.Context(), gates.aliasOpenSaved, "alias Open save"); err != nil {
				return err
			}
			logCall(dir, "source-open-overlap")
			close(gates.sourceOpening)
		}
		writer, err := pulse.OpenStream(rc, original, pulse.WithBuffer(1), pulse.WithStreamSource("active-alias/integers", true))
		if err != nil {
			return err
		}
		position := writer.Position()
		if phase == "run" {
			if position.Sequence != 0 || position.Cursor != nil {
				return fmt.Errorf("new source has an unexpected position: %+v", position)
			}
		} else if position.Sequence != 1 || position.Cursor != 1 {
			return fmt.Errorf("active source did not recover Position(1,1): %+v", position)
		}
		for value := int(position.Sequence) + 1; value <= 3; value++ {
			if err := writer.SendAt(value, value); err != nil {
				return err
			}
			logCall(dir, fmt.Sprintf("sent-%d", value))
			if phase == "run" && value == 1 {
				close(gates.sourceParked)
				// Save exits the entire process before releasing this source. Its
				// complete node and successful stream terminal must still be absent.
				return waitActiveStreamAlias(rc.Context(), gates.mountSaved, "confirmed Sub save before process exit")
			}
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	observeSub := func(rc *pulse.RunCtx, next func(*pulse.RunCtx) error) error {
		logCall(dir, "mount")
		if err := next(rc); err != nil {
			return err
		}
		if err := waitActiveStreamAlias(rc.Context(), gates.received, "first Receive without Ack"); err != nil {
			return err
		}
		return waitActiveStreamAlias(rc.Context(), gates.sourceParked, "active source parked after SendAt(1)")
	}
	// The active outer mount delays child restoration until source's root
	// Attempt has opened. Otherwise the alias's blocked Save could prevent
	// source from entering its body through Durable's serialized Attempt hook.
	bridge, err := pulse.New("active-alias-bridge", "在根来源就绪后恢复子图别名")
	if err != nil {
		t.Fatal(err)
	}
	bridgeIn, bridgeOut := pulse.NewKey[pulse.Stream[int]]("bridge-in"), pulse.NewKey[pulse.Stream[int]]("bridge-out")
	if err := bridge.DeclareInput(pulse.NewPort(bridgeIn)); err != nil {
		t.Fatal(err)
	}
	if err := bridge.DeclareOutput(pulse.NewPort(bridgeOut)); err != nil {
		t.Fatal(err)
	}
	if err := pulse.Sub(bridge, "mount", "空子图纯流透传", []pulse.SubBind{
		pulse.In(bridgeIn, inlet), pulse.Out(inlet, bridgeOut),
	}, child, observeSub); err != nil {
		t.Fatal(err)
	}
	startBridge := func(rc *pulse.RunCtx, next func(*pulse.RunCtx) error) error {
		logCall(dir, "outer")
		if phase == "resume" {
			if err := waitActiveStreamAlias(rc.Context(), gates.sourceReady, "source body after root Attempt Open"); err != nil {
				return err
			}
		}
		return next(rc)
	}
	if err := pulse.Sub(graph, "outer", "恢复可暂停的子图作用域", []pulse.SubBind{
		pulse.In(original, bridgeIn), pulse.Out(bridgeOut, alias),
	}, bridge, startBridge); err != nil {
		t.Fatal(err)
	}
	if err := graph.Add(pulse.NewNode("sink", "别名与来源共用读取组", pulse.Requires(original, alias), nil, func(rc *pulse.RunCtx) error {
		logCall(dir, "sink")
		a, err := pulse.Get(rc, original)
		if err != nil {
			return err
		}
		b, err := pulse.Get(rc, alias)
		if err != nil {
			return err
		}
		if a != b {
			return errors.New("restored source and pass-through alias have different local endpoints")
		}
		for want := 1; ; want++ {
			// Alternate aliases so both names must advance the same queue and Ack ledger.
			stream := b
			if want%2 == 0 {
				stream = a
			}
			delivery, err := stream.Receive(rc)
			if err == io.EOF {
				if want != 4 {
					return fmt.Errorf("EOF after %d messages, want pending 1 and new 2,3", want-1)
				}
				for _, endpoint := range []pulse.Stream[int]{a, b} {
					snapshot := endpoint.Snapshot()
					if snapshot.Terminal != pulse.StreamCompleted || snapshot.LastSequence != 3 || snapshot.AckedThrough != 3 || snapshot.InFlight != 0 {
						return fmt.Errorf("alias lost the final shared confirmation ledger: %+v", snapshot)
					}
				}
				return nil
			}
			if err != nil {
				return err
			}
			if delivery.Value != want || delivery.Sequence != uint64(want) || want > 3 {
				return fmt.Errorf("received value=%d sequence=%d, want %d exactly once", delivery.Value, delivery.Sequence, want)
			}
			if phase == "run" {
				logCall(dir, "received-unacked")
				close(gates.received)
				return waitActiveStreamAlias(rc.Context(), gates.mountSaved, "process exit before Ack")
			}
			logCall(dir, fmt.Sprintf("consumed-%d", delivery.Value))
			if err := delivery.Ack(); err != nil {
				return err
			}
		}
	})); err != nil {
		t.Fatal(err)
	}
	return graph
}

func waitActiveStreamAlias(ctx context.Context, gate <-chan struct{}, boundary string) error {
	select {
	case <-gate:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("waiting for %s: %w", boundary, ctx.Err())
	}
}

func checkActiveStreamAliasBoundary(record durable.RunRecord) error {
	nodes := record.Scopes[record.RootScope].Nodes
	var mount durable.NodeRecord
	for _, scope := range record.Scopes {
		if node, ok := scope.Nodes["mount"]; ok {
			mount = node
		}
	}
	if mount.NodeID != "mount" || mount.Reason != string(pulse.NodeCompleted) || record.Attempts[mount.Attempt].State != durable.AttemptConfirmed || len(mount.Outputs) != 1 {
		return errors.New("pass-through Sub is not a complete confirmed node")
	}
	if _, ok := nodes["source"]; ok {
		return errors.New("real source completed before the alias crash boundary")
	}
	if _, ok := nodes["sink"]; ok {
		return errors.New("sink completed before the unacknowledged delivery crash boundary")
	}
	if len(record.Streams) != 1 {
		return fmt.Errorf("source ledgers=%d, want 1", len(record.Streams))
	}
	for _, stream := range record.Streams {
		ref := mount.Outputs[0].Stream
		if ref == nil || *ref != stream.Stream || ref.ID.NodeID != "source" || ref.Terminal != "" || ref.Stopped || ref.Capacity != 1 || !ref.Resumable {
			return fmt.Errorf("confirmed Sub does not alias the active resumable source: %+v", ref)
		}
		attempt := record.Attempts[ref.Attempt]
		if attempt.State != durable.AttemptActive || attempt.Parent != record.RootScope || attempt.NodeID != "source" || attempt.Boundary != "" {
			return fmt.Errorf("real source attempt is not still active: %+v", attempt)
		}
		if stream.LastSequence != 1 || stream.AckedThrough != 0 || len(stream.Pending) != 1 || stream.Pending[0].Sequence != 1 {
			return fmt.Errorf("crash boundary did not retain exactly the received, unacknowledged first message: %+v", stream)
		}
		var value, cursor int
		if err := json.Unmarshal(stream.Pending[0].Value.Data, &value); err != nil {
			return err
		}
		if stream.Cursor == nil {
			return errors.New("SendAt cursor was not saved")
		}
		if err := json.Unmarshal(stream.Cursor.Data, &cursor); err != nil {
			return err
		}
		if value != 1 || cursor != 1 {
			return fmt.Errorf("saved first value=%d cursor=%d, want 1,1", value, cursor)
		}
	}
	return nil
}

type activeStreamAliasStore struct {
	durable.Store
	dir   string
	phase string
	gates *activeStreamAliasGates
}

func (s *activeStreamAliasStore) Acquire(ctx context.Context, id string) (durable.Session, error) {
	session, err := s.Store.Acquire(ctx, id)
	if err != nil {
		return nil, err
	}
	return &activeStreamAliasSession{Session: session, dir: s.dir, phase: s.phase, gates: s.gates}, nil
}

type activeStreamAliasSession struct {
	durable.Session
	dir          string
	phase        string
	gates        *activeStreamAliasGates
	resumeOpened bool
}

func (s *activeStreamAliasSession) Apply(ctx context.Context, batch durable.ChangeBatch) (durable.CommitAck, error) {
	return observeApply(ctx, s.Session, batch, func(record durable.RunRecord, commit func() error) error { return s.observe(ctx, record, commit) })
}
func (s *activeStreamAliasSession) observe(ctx context.Context, record durable.RunRecord, commit func() error) error {
	if err := commit(); err != nil {
		return err
	}
	if s.phase == "run" {
		confirmedAlias := false
		for _, scope := range record.Scopes {
			if _, ok := scope.Nodes["mount"]; ok {
				confirmedAlias = true
			}
		}
		if !confirmedAlias {
			return nil
		}
		// Inspect the filestore result after its atomic Save, before admitting
		// either producer completion or Ack. No sleep or probabilistic crash point.
		saved, err := s.Session.Load(ctx)
		if err != nil {
			return err
		}
		if err := checkActiveStreamAliasBoundary(saved); err != nil {
			return err
		}
		for _, gate := range []<-chan struct{}{s.gates.received, s.gates.sourceParked} {
			select {
			case <-gate:
			default:
				return errors.New("Sub was saved before Receive and the active source were parked")
			}
		}
		logCall(s.dir, "saved-active-alias")
		os.Exit(crashExit)
	}
	if s.phase == "resume" && !s.resumeOpened {
		for _, stream := range record.Streams {
			if stream.Stream.ID.Generation != 2 {
				continue
			}
			select {
			case <-s.gates.sourceReady:
			default:
				return errors.New("alias Open started before source entered its body")
			}
			s.resumeOpened = true
			logCall(s.dir, "resume-open-save")
			// Source is held outside Open until the restored alias's real Open
			// has persisted generation 2. Hold that Open in Save until source is
			// entering OpenStream, exercising the concurrent acquisition window.
			close(s.gates.aliasOpenSaved)
			return waitActiveStreamAlias(ctx, s.gates.sourceOpening, "concurrent source Open")
		}
	}
	return nil
}
