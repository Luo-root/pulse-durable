// 本文件定义只读语义压缩计划，类似数据库 EXPLAIN 的确定性结果。
// 计划绑定当前逻辑记录和完整候选/保护集合；不写 Store，不授予删除权限。
package durable

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/Luo-root/pulse-durable/internal/snapshotfmt"
)

const (
	// HistoryCompressionMaxObjects 限制一份计划的删除与保护对象总数。
	// 这是初始规划资源预算，不能用于推断历史保留政策。
	HistoryCompressionMaxObjects = 100000
	HistoryCompressionMaxBytes   = 64 << 20
)

// HistoryCompressionRequest 选择一个 Run 当前 head 的候选范围。
// ThroughRevision 必须等于权威记录 revision；历史位置规划需要额外权威
// 历史投影，本入口不拿当前摘要套用到旧位置。零预算无效，失败返回零计划。
// 调用期间输入不得并发修改；结果不与记录共享可变存储。
type HistoryCompressionRequest struct {
	RunID           string `json:"run_id"`
	ThroughRevision uint64 `json:"through_revision"`
	MaxObjects      uint64 `json:"max_objects"`
	MaxBytes        uint64 `json:"max_bytes"`
}

func (q HistoryCompressionRequest) Validate() error {
	if !validSemanticText(q.RunID) || q.ThroughRevision == 0 || q.MaxObjects == 0 || q.MaxObjects > HistoryCompressionMaxObjects || q.MaxBytes == 0 || q.MaxBytes > HistoryCompressionMaxBytes {
		return ErrInvalid
	}
	return nil
}

func (q HistoryCompressionRequest) Digest() string {
	return historyCompressionDigest("request/v1", q.RunID, q.ThroughRevision, q.MaxObjects, q.MaxBytes)
}

// CompressionObjectRef 保存对象种类与完整持久键。两个字段分别编码，不能
// 用分隔符拼接后再散列，否则合法 ID 内的逗号等字符可能混淆集合边界。
type CompressionObjectRef struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

func (r CompressionObjectRef) valid() bool {
	return validSemanticText(r.Kind) && validSemanticText(r.ID)
}

// HistoryCompressionPlan 是不可执行的候选结果。DeleteObjects 与
// ProtectedObjects 必须互斥且有序，Rows 为删除对象数。Bytes 是源记录
// 无损快照与规范计划的预算字节之和，不代表磁盘/WAL IO 或可回收字节。
// HeadDigest 使用无损字段表示绑定 nil/empty；PlanDigest 是完整性检查，
// 不是授权。Executable=true 在执行契约完成之前一律拒绝。
type HistoryCompressionPlan struct {
	Request          HistoryCompressionRequest `json:"request"`
	RequestDigest    string                    `json:"request_digest"`
	PlanDigest       string                    `json:"plan_digest"`
	RunID            string                    `json:"run_id"`
	FromRevision     uint64                    `json:"from_revision"`
	ThroughRevision  uint64                    `json:"through_revision"`
	HeadRevision     uint64                    `json:"head_revision"`
	HeadDigest       string                    `json:"head_digest"`
	SummaryDigests   []string                  `json:"summary_digests,omitempty"`
	DeleteObjects    []CompressionObjectRef    `json:"delete_objects,omitempty"`
	ProtectedObjects []CompressionObjectRef    `json:"protected_objects,omitempty"`
	Rows             uint64                    `json:"rows"`
	Bytes            uint64                    `json:"bytes"`
	Executable       bool                      `json:"executable"`
}

func (p HistoryCompressionPlan) Digest() string {
	// 自身摘要和预算字段由外层绑定；只清除摘要本身以免自引用。
	p.PlanDigest = ""
	return historyCompressionDigest("plan/v1", p)
}

// Validate 只证明结构与摘要自洽。执行方仍须在权威锁内重新规划并比较，
// 不得因调用方能够重新计算 Digest 就信任其删除集合。
func (p HistoryCompressionPlan) Validate(q HistoryCompressionRequest) error {
	if q.Validate() != nil || p.Request != q || p.RequestDigest != q.Digest() || p.RunID != q.RunID || p.ThroughRevision != q.ThroughRevision || p.FromRevision != 1 || p.HeadRevision != p.ThroughRevision || !validDigest(p.HeadDigest) || !validDigest(p.PlanDigest) || p.Rows != uint64(len(p.DeleteObjects)) || p.Bytes == 0 || p.Executable {
		return ErrInvalid
	}
	if p.Bytes > q.MaxBytes || uint64(len(p.DeleteObjects))+uint64(len(p.ProtectedObjects)) > q.MaxObjects || len(p.SummaryDigests) > int(q.MaxObjects) {
		return ErrLimitExceeded
	}
	if p.PlanDigest != p.Digest() {
		return ErrInvalid
	}
	seen := make(map[CompressionObjectRef]bool, len(p.DeleteObjects)+len(p.ProtectedObjects))
	for _, objects := range [][]CompressionObjectRef{p.DeleteObjects, p.ProtectedObjects} {
		for i, object := range objects {
			if !object.valid() || seen[object] || i > 0 && !compressionObjectLess(objects[i-1], object) {
				return ErrInvalid
			}
			seen[object] = true
		}
	}
	for i, value := range p.SummaryDigests {
		if !validDigest(value) || i > 0 && p.SummaryDigests[i-1] > value {
			return ErrInvalid
		}
	}
	return nil
}

// PlanHistoryCompression 重新核对当前摘要与事实，生成候选和保护集合。
// 它不删除任何对象；恢复高水位与执行事务完成前始终 Executable=false。
func PlanHistoryCompression(record RunRecord, request HistoryCompressionRequest) (HistoryCompressionPlan, error) {
	if request.Validate() != nil || record.RunID != request.RunID || record.Revision == 0 || !hasCompleteSemanticEvidence(record) {
		return HistoryCompressionPlan{}, fmt.Errorf("%w: compression requires a complete semantic evidence component", ErrInvalid)
	}
	if request.ThroughRevision != record.Revision {
		return HistoryCompressionPlan{}, ErrConflict
	}
	objects := uint64(len(record.Attempts)) + uint64(len(record.Messages)) + uint64(len(record.Streams)) + uint64(len(record.Managed)) + uint64(len(record.Scopes)) + uint64(len(record.HistorySummaries)) + uint64(len(record.CommitIndex)) + 1
	for _, scope := range record.Scopes {
		objects += uint64(len(scope.Nodes))
		if objects > request.MaxObjects {
			return HistoryCompressionPlan{}, ErrLimitExceeded
		}
	}
	if objects > request.MaxObjects {
		return HistoryCompressionPlan{}, ErrLimitExceeded
	}
	// 只读计数先于全量摘要与候选分配，源记录本身也必须在本次预算内。
	sourceBytes, err := snapshotfmt.RecordSize(record)
	if err != nil || sourceBytes < 1 {
		return HistoryCompressionPlan{}, ErrInvalidRecord
	}
	if uint64(sourceBytes) > request.MaxBytes {
		return HistoryCompressionPlan{}, ErrLimitExceeded
	}
	if err := ValidateSemanticEvidence(record); err != nil {
		return HistoryCompressionPlan{}, err
	}
	plan := HistoryCompressionPlan{Request: request, RequestDigest: request.Digest(), RunID: record.RunID,
		FromRevision: 1, ThroughRevision: request.ThroughRevision, HeadRevision: record.Revision, HeadDigest: recordDigest(record)}
	deleted := make(map[CompressionObjectRef]bool)
	index := indexCompressionCandidates(record)
	for _, summary := range record.HistorySummaries {
		plan.SummaryDigests = append(plan.SummaryDigests, summary.Digest)
		objects, err := candidateObjectsFromVerifiedSource(record, summary, index)
		if err != nil {
			return HistoryCompressionPlan{}, err
		}
		for _, object := range objects {
			if deleted[object] {
				return HistoryCompressionPlan{}, fmt.Errorf("%w: overlapping compression summaries", ErrConflict)
			}
			deleted[object] = true
			plan.DeleteObjects = append(plan.DeleteObjects, object)
		}
		// 用结构编码表示复合键，避免 Kind/ID 本身含 ':' 时发生碰撞。
		key, _ := json.Marshal(summary.key())
		plan.ProtectedObjects = append(plan.ProtectedObjects, CompressionObjectRef{Kind: "history_summary", ID: string(key)})
	}
	for _, entry := range record.CommitIndex {
		plan.ProtectedObjects = append(plan.ProtectedObjects, CompressionObjectRef{Kind: "commit_index", ID: fmt.Sprint(entry.Revision)})
	}
	plan.ProtectedObjects = append(plan.ProtectedObjects, CompressionObjectRef{Kind: "provenance", ID: "singleton"})
	protect := func(kind, id string) {
		object := CompressionObjectRef{Kind: kind, ID: id}
		if !deleted[object] {
			plan.ProtectedObjects = append(plan.ProtectedObjects, object)
		}
	}
	for id := range record.Attempts {
		protect("attempt", id)
	}
	for id := range record.Messages {
		protect("message", id)
	}
	for id := range record.Streams {
		protect("stream", id)
	}
	for id, scope := range record.Scopes {
		protect("scope", id)
		for node := range scope.Nodes {
			key, _ := json.Marshal([]string{id, node})
			protect("node", string(key))
		}
	}
	for id := range record.Managed {
		protect("managed", id)
	}
	if uint64(len(plan.DeleteObjects))+uint64(len(plan.ProtectedObjects)) > request.MaxObjects {
		return HistoryCompressionPlan{}, ErrLimitExceeded
	}
	sort.Strings(plan.SummaryDigests)
	sortCompressionObjects(plan.DeleteObjects)
	sortCompressionObjects(plan.ProtectedObjects)
	plan.Rows = uint64(len(plan.DeleteObjects))
	// 摘要固定64字符；Bytes 的十进制宽度达到不动点后就是实际规范计划大小。
	plan.PlanDigest = "0000000000000000000000000000000000000000000000000000000000000000"
	plan.Bytes = uint64(sourceBytes)
	for {
		encoded, err := json.Marshal(plan)
		if err != nil {
			return HistoryCompressionPlan{}, ErrInvalid
		}
		next := uint64(sourceBytes) + uint64(len(encoded))
		if next == plan.Bytes {
			break
		}
		plan.Bytes = next
	}
	plan.PlanDigest = plan.Digest()
	if err := plan.Validate(request); err != nil {
		return HistoryCompressionPlan{}, err
	}
	return plan, nil
}

func recordDigest(record RunRecord) string {
	data, err := snapshotfmt.Encode(record)
	if err != nil {
		return ""
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// compressionSourceDigest 不包含相互引用的语义元数据。SourceDigest 表示
// 生成摘要时的业务事实投影，完整 HeadDigest 另行绑定摘要、索引及来源。
func compressionSourceDigest(record RunRecord) string {
	record.HistorySummaries, record.CommitIndex, record.Provenance = nil, nil, nil
	return recordDigest(record)
}

func historyCompressionDigest(domain string, values ...any) string {
	data, err := json.Marshal(struct {
		Domain string `json:"domain"`
		Values []any  `json:"values"`
	}{domain, values})
	if err != nil {
		return ""
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func compressionObjectLess(a, b CompressionObjectRef) bool {
	return a.Kind < b.Kind || a.Kind == b.Kind && a.ID < b.ID
}
