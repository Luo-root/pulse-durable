package postgres

import (
	"encoding/json"
	"errors"
	"strconv"

	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
)

// snapshotBudgetVersion 属于 Go/SQL 编码与精确计数确认协议。
// 版本 2 对应 snapshot2/wire2，不参与领域记录身份或业务路由。
const snapshotBudgetVersion = 2

var (
	// ErrSnapshotLimit 表示当前完整快照将超过 16 MiB；拒绝不会发布任何业务状态。
	ErrSnapshotLimit = errors.Join(errors.New("distributed postgres: current snapshot limit exceeded"), durable.ErrLimitExceeded)
	// ErrSnapshotBudgetUnsupported 在启动前拒绝缺少精确增量计数能力的根模块。
	ErrSnapshotBudgetUnsupported = errors.New("distributed postgres: root module lacks SnapshotSizeDelta; use the matching source workspace or a released compatible root version")
	ErrInvalidRecordBudget       = errors.New("postgres: invalid record snapshot budget")
)

type snapshotSizer interface {
	SnapshotSizeDelta(durable.RunRecord) (int64, error)
}

func requireSnapshotSizer() error {
	if _, ok := any(durable.ChangeBatch{}).(snapshotSizer); !ok {
		return ErrSnapshotBudgetUnsupported
	}
	return nil
}

func snapshotCandidate(batch durable.ChangeBatch, before durable.RunRecord, size int64) (int64, error) {
	sizer, ok := any(batch).(snapshotSizer)
	if !ok {
		return 0, ErrSnapshotBudgetUnsupported
	}
	delta, err := sizer.SnapshotSizeDelta(before)
	if err != nil {
		return 0, err
	}
	if size < 1 || size > MaxRecordBytes || delta < -size {
		return 0, invalidRecord(ErrInvalidRecordBudget)
	}
	if delta > MaxRecordBytes-size {
		return 0, ErrSnapshotLimit
	}
	return size + delta, nil
}

type snapshotBudgetAck struct {
	Version  int    `json:"snapshot_budget_version"`
	Bytes    string `json:"snapshot_bytes"`
	Revision string `json:"revision"`
	Chain    string `json:"head_chain"`
}

// 写入已可能提交，封装不匹配只能返回未知，不能确认或重试业务。
func validateSnapshotBudgetAck(b []byte, size int64, revision uint64, chain [32]byte) error {
	var ack snapshotBudgetAck
	if !receiptUnicode(b) || strictMaintenanceJSON(b, &ack) != nil || ack.Version != snapshotBudgetVersion || ack.Bytes != strconv.FormatInt(size, 10) || ack.Revision != strconv.FormatUint(revision, 10) || ack.Chain != chainText(chain) {
		return d.ErrUnknown
	}
	return nil
}

func validateLoadedSnapshotBudget(b, encoded []byte) error {
	expected := int64(len(encoded))
	var obj map[string]json.RawMessage
	if json.Unmarshal(b, &obj) != nil {
		return invalidRecord(ErrInvalidRecordBudget)
	}
	var version int
	var size, ledger string
	var valid bool
	if json.Unmarshal(obj["snapshot_budget_version"], &version) != nil || version != snapshotBudgetVersion || json.Unmarshal(obj["snapshot_bytes"], &size) != nil || size != strconv.FormatInt(expected, 10) || json.Unmarshal(obj["snapshot_ledger_bytes"], &ledger) != nil || ledger != size || json.Unmarshal(obj["snapshot_ledger_valid"], &valid) != nil || !valid {
		return invalidRecord(ErrInvalidRecordBudget)
	}
	return validateSnapshotLedger(obj["snapshot_ledger"], encoded)
}
