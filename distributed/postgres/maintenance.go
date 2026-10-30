package postgres

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"

	durable "github.com/Luo-root/pulse-durable"
	d "github.com/Luo-root/pulse-durable/distributed"
	"github.com/Luo-root/pulse-durable/recordio"
)

// StorageState 是受命名空间限定的物理状态。代际不是业务
// 修订号。身份由外层请求提供；状态不能在
// 不同身份间转移。此 API 要求 maintain 权限，而非工作进程所有权。
type StorageState struct {
	Generation   string `json:"generation"`
	BaseRevision string `json:"base_revision"`
	BaseChain    string `json:"base_chain"`
	HeadRevision string `json:"head_revision"`
	HeadChain    string `json:"head_chain"`
}

// CompactStorageRequest 永久预留 Mutation，即使构建失败也不释放。
// 结果未知时使用 FindMaintenance；不存在绝不能证明未发布。
// 不隐式提供保留、固定点续期、语义压缩或重放。
type CompactStorageRequest struct {
	Identity           d.Identity
	ExpectedGeneration uint64
	ThroughRevision    uint64
	Mutation           d.Mutation
}

type MaintenanceReceipt struct {
	MutationID     string       `json:"mutation_id"`
	MutationDigest string       `json:"mutation_digest"`
	Operation      string       `json:"operation"`
	State          StorageState `json:"state"`
	OccurredAt     time.Time    `json:"occurred_at"`
}

type MaintenanceResult struct {
	// 可为 absent、building、closed、expired 或 published。只有 published 是回执。
	Status string              `json:"status"`
	Result *MaintenanceReceipt `json:"result"`
}

func strictMaintenanceJSON(b []byte, out any) error {
	// 在解码结构体前，遍历 token 拒绝任意深度的重复键。
	dec := json.NewDecoder(bytes.NewReader(b))
	var walk func() error
	walk = func() error {
		tok, e := dec.Token()
		if e != nil {
			return d.ErrInvalid
		}
		if delim, ok := tok.(json.Delim); ok {
			switch delim {
			case '{':
				seen := map[string]bool{}
				for dec.More() {
					k, e := dec.Token()
					if e != nil {
						return d.ErrInvalid
					}
					s, ok := k.(string)
					if !ok || seen[s] {
						return d.ErrInvalid
					}
					seen[s] = true
					if e = walk(); e != nil {
						return e
					}
				}
				end, e := dec.Token()
				if e != nil || end != json.Delim('}') {
					return d.ErrInvalid
				}
			case '[':
				for dec.More() {
					if e := walk(); e != nil {
						return e
					}
				}
				end, e := dec.Token()
				if e != nil || end != json.Delim(']') {
					return d.ErrInvalid
				}
			default:
				return d.ErrInvalid
			}
		}
		return nil
	}
	if walk() != nil {
		return d.ErrInvalid
	}
	if _, e := dec.Token(); e != io.EOF {
		return d.ErrInvalid
	}
	var shape any
	if json.Unmarshal(b, &shape) != nil || !maintenanceShape(shape, reflect.TypeOf(out).Elem()) {
		return d.ErrInvalid
	}
	dec = json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if dec.Decode(out) != nil {
		return d.ErrInvalid
	}
	return nil
}
func maintenanceShape(v any, t reflect.Type) bool {
	if t == reflect.TypeOf(json.RawMessage{}) || t == reflect.TypeOf(time.Time{}) {
		return true
	}
	if t.Kind() == reflect.Pointer {
		if v == nil {
			return true
		}
		return maintenanceShape(v, t.Elem())
	}
	if t.Kind() != reflect.Struct {
		return true
	}
	obj, ok := v.(map[string]any)
	if !ok {
		return false
	}
	fields := map[string]reflect.Type{}
	for n := 0; n < t.NumField(); n++ {
		f := t.Field(n)
		name := strings.Split(f.Tag.Get("json"), ",")[0]
		fields[name] = f.Type
	}
	for k, val := range obj {
		ft, ok := fields[k]
		if !ok || !maintenanceShape(val, ft) {
			return false
		}
	}
	return true
}
func positiveDecimal(s string) (uint64, bool) {
	n, e := strconv.ParseUint(s, 10, 64)
	return n, e == nil && n > 0 && strconv.FormatUint(n, 10) == s
}
func validStorageState(s StorageState) bool {
	g, ok := positiveDecimal(s.Generation)
	if !ok || g > 9223372036854775807 {
		return false
	}
	base, ok := positiveDecimal(s.BaseRevision)
	if !ok {
		return false
	}
	head, ok := positiveDecimal(s.HeadRevision)
	return ok && base <= head && fingerprint(s.BaseChain) && fingerprint(s.HeadChain)
}

// CompactStorageDigest 将永久变更保护绑定到精确计划。
// 十进制长度避免分隔符歧义，且不暴露令牌凭据。
func CompactStorageDigest(q CompactStorageRequest) string {
	return digest([]byte(fmt.Sprintf("pulse-durable/maintenance/compact/v1\x00%d:%s%d:%s:%d:%d", len(q.Identity.Namespace), q.Identity.Namespace, len(q.Identity.RunID), q.Identity.RunID, q.ExpectedGeneration, q.ThroughRevision)))
}
func maintenanceArgs(q CompactStorageRequest) (map[string]any, error) {
	if !identity(q.Identity) || !mutation(q.Mutation) || q.ThroughRevision == 0 || q.ExpectedGeneration == 0 || q.ExpectedGeneration >= 9223372036854775807 || q.Mutation.Digest != CompactStorageDigest(q) {
		return nil, d.ErrInvalid
	}
	a := args(q.Identity)
	mutate(a, q.Mutation)
	a["generation"] = strconv.FormatUint(q.ExpectedGeneration, 10)
	a["through_revision"] = strconv.FormatUint(q.ThroughRevision, 10)
	return a, nil
}
func decodeMaintenanceResult(b []byte, m d.Mutation) (MaintenanceResult, error) {
	var out MaintenanceResult
	if strictMaintenanceJSON(b, &out) != nil {
		return out, d.ErrInvalid
	}
	switch out.Status {
	case "published":
		r := out.Result
		if r == nil || r.MutationID != m.ID || r.MutationDigest != m.Digest || r.Operation != "compact_storage" || r.OccurredAt.IsZero() || !validStorageState(r.State) {
			return MaintenanceResult{}, d.ErrInvalid
		}
	case "absent", "building", "closed", "expired":
		if out.Result != nil {
			return MaintenanceResult{}, d.ErrInvalid
		}
	default:
		return MaintenanceResult{}, d.ErrInvalid
	}
	return out, nil
}
func (c *Coordinator) maintenanceCall(ctx context.Context, op string, a map[string]any, write bool) ([]byte, error) {
	ctx, cancel, e := bounded(ctx)
	if e != nil {
		return nil, e
	}
	defer cancel()
	if c == nil || c.pool == nil {
		return nil, d.ErrInvalid
	}
	b, e := json.Marshal(a)
	if e != nil {
		return nil, d.ErrInvalid
	}
	var out []byte
	e = c.pool.QueryRow(ctx, "SELECT pulse_durable_distributed.maintenance($1,$2::jsonb)", op, b).Scan(&out)
	return out, scrub(e, write)
}
func (c *Coordinator) InspectStorage(ctx context.Context, i d.Identity) (StorageState, error) {
	if !identity(i) {
		return StorageState{}, d.ErrInvalid
	}
	b, e := c.maintenanceCall(ctx, "inspect", args(i), false)
	if e != nil {
		return StorageState{}, e
	}
	var s StorageState
	if strictMaintenanceJSON(b, &s) != nil || !validStorageState(s) {
		return StorageState{}, d.ErrInvalid
	}
	return s, nil
}
func (c *Coordinator) FindMaintenance(ctx context.Context, i d.Identity, m d.Mutation) (MaintenanceResult, error) {
	if !identity(i) || !mutation(m) {
		return MaintenanceResult{}, d.ErrInvalid
	}
	a := args(i)
	mutate(a, m)
	b, e := c.maintenanceCall(ctx, "find", a, false)
	if e != nil {
		return MaintenanceResult{}, e
	}
	return decodeMaintenanceResult(b, m)
}

// CompactStorage 在数据库锁外仅物化已验证批次。
// 与现有工作进程路径一样，验证信任经过认证的 Go 服务；
// maintain 凭据不是用于上传不可信快照的沙箱。
func (c *Coordinator) CompactStorage(ctx context.Context, q CompactStorageRequest) (MaintenanceResult, error) {
	a, e := maintenanceArgs(q)
	if e != nil {
		return MaintenanceResult{}, e
	}
	b, e := c.maintenanceCall(ctx, "begin", a, true)
	if e != nil {
		return MaintenanceResult{}, e
	}
	// 每次明确的本地失败都使用单独限时的上下文关闭固定点。
	// 发布结果未知时，绝不随后中止或自动重试。
	published := false
	defer func() {
		if !published {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, _ = c.maintenanceCall(cleanup, "abort", a, true)
		}
	}()
	var begin struct {
		Status string              `json:"status"`
		Image  json.RawMessage     `json:"image"`
		State  StorageState        `json:"state"`
		Result *MaintenanceReceipt `json:"result,omitempty"`
	}
	if strictMaintenanceJSON(b, &begin) != nil {
		return MaintenanceResult{}, d.ErrInvalid
	}
	if begin.Status == "published" {
		published = true
		return decodeMaintenanceResult(b, q.Mutation)
	}
	if begin.Status != "building" || !validStorageState(begin.State) || begin.State.Generation != strconv.FormatUint(q.ExpectedGeneration, 10) {
		return MaintenanceResult{}, d.ErrInvalid
	}
	r, chain, e := buildMaintenanceImage(ctx, q, begin.Image)
	if e != nil {
		return MaintenanceResult{}, e
	}
	wire, e := encodeMaintenanceRecord(r)
	if e != nil || len(wire) > MaxRecordBytes {
		return MaintenanceResult{}, d.ErrInvalid
	}
	if e = ctx.Err(); e != nil {
		return MaintenanceResult{}, e
	}
	a["record"] = wire
	a["snapshot_digest"] = digest(wire)
	a["chain"] = chain
	published = true // 从此处开始，发布结果可能未知
	b, e = c.maintenanceCall(ctx, "publish", a, true)
	if e != nil {
		return MaintenanceResult{}, e
	}
	return decodeMaintenanceResult(b, q.Mutation)
}

// 旧格式 2 的 omitempty 编码无法表示所有 nil 与空值状态。
// 拒绝有损物化，而不是发布细微不同的记录。
func encodeMaintenanceRecord(r durable.RunRecord) ([]byte, error) {
	b, e := recordio.EncodeSnapshot(r)
	if e != nil {
		return nil, d.ErrInvalidRecord
	}
	roundtrip, e := recordio.DecodeSnapshot(b)
	if e != nil || !reflect.DeepEqual(r, roundtrip) {
		return nil, d.ErrInvalidRecord
	}
	return b, nil
}
func buildMaintenanceImage(ctx context.Context, q CompactStorageRequest, b []byte) (durable.RunRecord, string, error) {
	if ctx == nil {
		return durable.RunRecord{}, "", d.ErrInvalid
	}
	s, _, snapshot, e := decodeSnapshot(b)
	if e != nil || s.Task.Identity() != q.Identity || s.Revision != q.ThroughRevision {
		return durable.RunRecord{}, "", d.ErrInvalid
	}
	var log storedLog
	if json.Unmarshal(b, &log) != nil || digest(snapshot) != log.Digest {
		return durable.RunRecord{}, "", d.ErrInvalidRecord
	}
	base, ok := positiveDecimal(log.BaseRevision)
	if !ok || !fingerprint(log.BaseChain) {
		return durable.RunRecord{}, "", d.ErrInvalidRecord
	}
	r, e := decodeRecord(s.Task, base, snapshot)
	if e != nil {
		return durable.RunRecord{}, "", e
	}
	chain := log.BaseChain
	for _, entry := range log.Batches {
		if e = ctx.Err(); e != nil {
			return durable.RunRecord{}, "", e
		}
		if r.Revision == ^uint64(0) || entry.Revision != strconv.FormatUint(r.Revision+1, 10) || entry.Parent != chain || digest(entry.Wire) != entry.Digest {
			return durable.RunRecord{}, "", d.ErrInvalidRecord
		}
		batch, err := durable.UnmarshalChangeBatch(entry.Wire)
		if err != nil {
			return durable.RunRecord{}, "", d.ErrInvalidRecord
		}
		if _, err = batch.ApplyTo(&r); err != nil {
			return durable.RunRecord{}, "", d.ErrInvalidRecord
		}
		raw, _ := hex.DecodeString(chain)
		var parent [32]byte
		copy(parent[:], raw)
		next, err := durable.NextChain(parent, sha256.Sum256(entry.Wire), r.Revision)
		if err != nil || chainText(next) != entry.Next {
			return durable.RunRecord{}, "", d.ErrInvalidRecord
		}
		chain = entry.Next
	}
	if r.Revision != q.ThroughRevision || chain != log.Chain {
		return durable.RunRecord{}, "", d.ErrInvalidRecord
	}
	return r, chain, nil
}
