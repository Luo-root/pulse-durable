package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
	"github.com/Luo-root/pulse-durable/filestore"
	"github.com/Luo-root/pulse/observe"
)

const observationRunID = "observed-process-run"

type observationLogRecord struct {
	Event, Status, HostID, TraceID string
	Err                            json.RawMessage
	Attrs                          map[string]json.RawMessage
}

// String 仅改善断言诊断；RawMessage 按原 JSON 展示，不输出十进制字节数组。
func (r observationLogRecord) String() string {
	data, err := json.Marshal(r)
	if err != nil {
		return fmt.Sprintf("observationLogRecord(invalid JSON: %v)", err)
	}
	return string(data)
}

func observationAttr[T any](t *testing.T, record observationLogRecord, key string) T {
	t.Helper()
	var value T
	if err := json.Unmarshal(record.Attrs[key], &value); err != nil {
		t.Fatalf("missing/invalid %s in %+v: %v", key, record, err)
	}
	return value
}

func TestRetryObservationAcrossProcessExit(t *testing.T) {
	dir := t.TempDir()
	run := func(role string, wantExit int) {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRetryObservationProcess$", "-test.timeout=10s")
		cmd.Env = append(os.Environ(), "PULSE_OBSERVATION_ROLE="+role, "PULSE_OBSERVATION_DIR="+dir, "GORACE=atexit_sleep_ms=0")
		output, err := cmd.CombinedOutput()
		if ctx.Err() != nil {
			t.Fatalf("%s process exceeded its deadline: %v\n%s", role, ctx.Err(), output)
		}
		if wantExit == 0 {
			if err != nil {
				t.Fatalf("%s process: %v\n%s", role, err, output)
			}
		} else {
			var exit *exec.ExitError
			if !errors.As(err, &exit) || exit.ExitCode() != wantExit {
				t.Fatalf("%s process must exit after logging attempt 2: %v\n%s", role, err, output)
			}
		}
	}
	run("run", crashExit)
	before := observationStoredRecord(t, dir)
	var firstScope, activeScope, parentScope string
	for scope, a := range before.Attempts {
		if a.NodeID == "work" && a.Boundary != "" {
			switch a.Index {
			case 1:
				firstScope = scope
				if a.State != durable.AttemptFailed {
					t.Fatal("first failure was not durably confirmed")
				}
			case 2:
				activeScope, parentScope = scope, a.Parent
				if a.State != durable.AttemptActive {
					t.Fatal("crashed attempt 2 must remain active")
				}
			}
		}
	}
	if firstScope == "" || activeScope == "" || parentScope == "" || len(before.Attempts) != 3 || len(before.Scopes[before.RootScope].Nodes) != 0 {
		t.Fatalf("unexpected crash frontier: %+v", before)
	}
	run("resume", 0)
	after := observationStoredRecord(t, dir)
	if after.RootScope != before.RootScope || len(after.Attempts) != 3 || after.Revision <= before.Revision ||
		after.Attempts[activeScope].Index != 2 || after.Scopes[after.RootScope].Nodes["work"].Attempt != parentScope ||
		after.Attempts[parentScope].State != durable.AttemptConfirmed {
		t.Fatalf("resume changed identity/budget or lost confirmation: %+v", after)
	}
	run("restored", 0)
	if final := observationStoredRecord(t, dir); final.Revision != after.Revision || callCount(dir, "observed-body") != 3 {
		t.Fatalf("complete restoration replayed business or saved history: revision=%d want=%d calls=%d", final.Revision, after.Revision, callCount(dir, "observed-body"))
	}
	for _, role := range []string{"run", "resume", "restored"} {
		records := observationRecords(t, dir, role)
		var phases []string
		var attempts []observationLogRecord
		for _, record := range records {
			if observationAttr[string](t, record, durable.AttrRunID) != observationRunID || record.HostID != "host-"+role || record.TraceID != "trace-"+role ||
				observationAttr[string](t, record, pulse.AttrExecution) == "" {
				t.Fatalf("lost stable or process attribution: %+v", record)
			}
			if record.Event != observe.EventAspect {
				continue
			}
			phase := observationAttr[string](t, record, pulse.AttrAspectPhase)
			phases = append(phases, phase)
			if observationAttr[int64](t, record, pulse.AttrBoundary) != 1 || observationAttr[int64](t, record, pulse.AttrAttemptLimit) != 3 {
				t.Fatalf("wrong boundary/budget: %+v", record)
			}
			if phase == string(pulse.AspectAttemptStarted) {
				attempts = append(attempts, record)
			}
			if phase == string(pulse.AspectAttemptFinished) && role == "run" && (record.Status != "failed" || string(record.Err) == "null") {
				t.Fatalf("failure was not observed: %+v", record)
			}
		}
		var want []string
		switch role {
		case "run":
			want = []string{"started", "attempt_started", "attempt_finished", "retry", "attempt_started"}
			if len(attempts) != 2 || observationAttr[int64](t, attempts[0], pulse.AttrAttempt) != 1 ||
				observationAttr[string](t, attempts[0], pulse.AttrAttemptOrigin) != "new" ||
				observationAttr[string](t, attempts[0], pulse.AttrAttemptScope) != firstScope {
				t.Fatalf("first attempt=%+v", attempts)
			}
		case "resume":
			want = []string{"started", "attempt_started", "attempt_finished", "finished"}
			if len(attempts) != 1 || observationAttr[string](t, attempts[0], pulse.AttrAttemptOrigin) != "resumed" {
				t.Fatalf("active recovery must report resumed, without a new retry: %+v", attempts)
			}
		case "restored":
			if len(records) != 2 || records[0].Event != observe.EventGraphStarted || records[1].Event != observe.EventGraphFinished {
				t.Fatalf("complete restoration fabricated business timing/history: %+v", records)
			}
		}
		if !reflect.DeepEqual(phases, want) {
			t.Fatalf("%s phases=%v want=%v", role, phases, want)
		}
		if len(attempts) > 0 {
			last := attempts[len(attempts)-1]
			if observationAttr[int64](t, last, pulse.AttrAttempt) != 2 || observationAttr[string](t, last, pulse.AttrAttemptScope) != activeScope ||
				observationAttr[string](t, last, pulse.AttrParentScope) != parentScope {
				t.Fatalf("%s cursor disagrees with filestore: %+v", role, last)
			}
			if role == "run" && observationAttr[string](t, last, pulse.AttrAttemptOrigin) != "new" {
				t.Fatal("fresh second attempt was not reported as new")
			}
		}
		t.Logf("%s: host=host-%s trace=trace-%s phases=%v", role, role, role, phases)
	}
	t.Logf("RunID=%s attempt=2 scope=%s parent=%s; revisions=%d→%d→%d; body calls=3", observationRunID, activeScope, parentScope, before.Revision, after.Revision, after.Revision)
}

func observationStoredRecord(t *testing.T, dir string) durable.RunRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	store, err := filestore.New(filepath.Join(dir, "checkpoints"))
	approvalOK(t, err)
	session, err := store.Acquire(ctx, observationRunID)
	approvalOK(t, err)
	defer func() { approvalOK(t, session.Close()) }()
	record, err := session.Load(ctx)
	approvalOK(t, err)
	return record
}

func observationRecords(t *testing.T, dir, role string) []observationLogRecord {
	t.Helper()
	file, err := os.Open(filepath.Join(dir, role+".jsonl"))
	approvalOK(t, err)
	defer file.Close()
	decoder := json.NewDecoder(file)
	var records []observationLogRecord
	for {
		var record observationLogRecord
		err := decoder.Decode(&record)
		if errors.Is(err, io.EOF) {
			return records
		}
		approvalOK(t, err)
		records = append(records, record)
	}
}

// 这个宿主出口复制 Attrs、一次补充 RunID，并同步写盘。业务体只负责业务，
// 不写观测；Sync 完成后才允许进程退出，排除日志缓冲导致的伪阴性。
type observationFileSink struct {
	mu        sync.Mutex
	file      *os.File
	err       error
	loggedTwo bool
}

func (s *observationFileSink) Write(record observe.Record) {
	var attrs observe.Attrs
	record.Attrs.Range(func(key string, value any) {
		switch value := value.(type) {
		case string:
			observe.Set(&attrs, key, value)
		case int64:
			observe.Set(&attrs, key, value)
		case float64:
			observe.Set(&attrs, key, value)
		case bool:
			observe.Set(&attrs, key, value)
		}
	})
	observe.Set(&attrs, durable.AttrRunID, observationRunID)
	record.Attrs = attrs
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return
	}
	s.err = json.NewEncoder(s.file).Encode(record)
	if s.err == nil {
		s.err = s.file.Sync()
	}
	phase, _ := observe.Get[string](record.Attrs, pulse.AttrAspectPhase)
	attempt, _ := observe.Get[int64](record.Attrs, pulse.AttrAttempt)
	if s.err == nil && record.Event == observe.EventAspect && phase == "attempt_started" && attempt == 2 {
		s.loggedTwo = true
	}
}

func TestRetryObservationProcess(t *testing.T) {
	role, dir := os.Getenv("PULSE_OBSERVATION_ROLE"), os.Getenv("PULSE_OBSERVATION_DIR")
	if role == "" {
		return
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	file, err := os.OpenFile(filepath.Join(dir, role+".jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	approvalOK(t, err)
	defer file.Close()
	sink := &observationFileSink{file: file}
	observer, err := observe.NewRecordObserver(observe.ObserveConfig{Sink: sink, HostID: "host-" + role, TraceID: "trace-" + role})
	approvalOK(t, err)
	g, err := pulse.New("observation-process", "真实退出后的切面归因")
	approvalOK(t, err)
	output := pulse.NewKey[int]("output")
	approvalOK(t, g.DeclareOutput(pulse.NewPort(output)))
	approvalOK(t, g.Add(pulse.NewNode("work", "第一次失败、第二次中断", nil, pulse.Provides(output), func(rc *pulse.RunCtx) error {
		calls := logCall(dir, "observed-body")
		if role == "run" && calls == 1 {
			return errors.New("first attempt failed")
		}
		if role == "run" && calls == 2 {
			sink.mu.Lock()
			ready, logErr := sink.loggedTwo, sink.err
			sink.mu.Unlock()
			if !ready || logErr != nil {
				return fmt.Errorf("attempt 2 record was not durably written: %v", logErr)
			}
			os.Exit(crashExit)
		}
		if role != "resume" || calls != 3 {
			return fmt.Errorf("unexpected body entry: role=%s calls=%d", role, calls)
		}
		return pulse.Set(rc, output, 42)
	}, pulse.Retry(3, 0))))
	store, err := filestore.New(filepath.Join(dir, "checkpoints"))
	approvalOK(t, err)
	runtime, err := durable.New(store, durable.NewCodecs())
	approvalOK(t, err)
	restored := 0
	opts := durable.RunOptions{RunID: observationRunID, Version: "observation/1", ExecOptions: []pulse.ExecOption{pulse.WithExecObserver(observer)},
		OnEvent: func(event durable.Event) error {
			if event.RunID != observationRunID || event.Kind != durable.EventRestored {
				return errors.New("unexpected recovery attribution")
			}
			restored++
			return nil
		}}
	var execution *pulse.Execution
	if role == "run" {
		execution, err = runtime.Run(ctx, g, pulse.Inputs{}, opts)
	} else {
		execution, err = runtime.Resume(ctx, g, pulse.Inputs{}, opts)
	}
	approvalOK(t, err)
	value, state, err := pulse.ReadOutput(execution, output)
	approvalOK(t, err)
	if value != 42 || state != pulse.SlotStateReady || (role == "restored" && restored != 1) || (role == "resume" && restored != 0) {
		t.Fatalf("result=%d/%s restored=%d", value, state, restored)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	approvalOK(t, sink.err)
}
