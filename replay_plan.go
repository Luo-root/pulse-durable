// 本文件定义指定位置派生回放的只读计划，类似数据库 EXPLAIN 的确定性结果。
//
// 与 Resume 的区别：Resume 保留原 RunID 并续跑；Replay 派生一个**新 RunID**，
// 从来源 Run 的某个已提交位置切断，目标节点及其实际依赖后代重新计算，
// 无关且仍然有效的兄弟分支成果可以复用。
//
// 本入口只做规划：它绑定来源位置、因果索引前缀与 cut 边界，不写 Store、
// 不创建派生 Run、不重映射身份，也不授予任何删除或回放权限。通用执行与
// 部分端口恢复原语属于 Pulse Core；本文件不臆造 Core 契约，因此计划始终
// Executable=false，执行片必须在 Core 原语与原子 CreateDerived 落地后另行实现。
package durable

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/Luo-root/pulse-durable/internal/snapshotfmt"
)

const (
	// ReplayMaxObjects 限制一份回放计划涉及的对象总数（保留成果 + 重算目标）。
	// 这是初始规划资源预算，不能用于推断保留政策或授权删除。
	ReplayMaxObjects = 100000
	// ReplayMaxBytes 限制来源无损快照与规范计划的预算字节之和。
	ReplayMaxBytes = 64 << 20
)

// ReplayRequest 选择来源 Run 的一个已提交位置，以及要重算的目标节点。
//
// ThroughRevision 必须等于权威来源记录的 revision：指定历史位置回放需要
// 额外的权威历史投影，本入口不拿当前摘要套用到旧位置。TargetNodeID 是
// 来源根图内要重算的节点身份；空值无效（整图重跑应使用普通 Run，而不是派生）。
// 调用期间输入不得并发修改；结果不与记录共享可变存储。
type ReplayRequest struct {
	SourceRunID     string `json:"source_run_id"`
	ThroughRevision uint64 `json:"through_revision"`
	TargetNodeID    string `json:"target_node_id"`
	MaxObjects      uint64 `json:"max_objects"`
	MaxBytes        uint64 `json:"max_bytes"`
}

func (q ReplayRequest) Validate() error {
	if !validSemanticText(q.SourceRunID) || q.ThroughRevision == 0 || !validSemanticText(q.TargetNodeID) ||
		q.MaxObjects == 0 || q.MaxObjects > ReplayMaxObjects || q.MaxBytes == 0 || q.MaxBytes > ReplayMaxBytes {
		return ErrInvalid
	}
	return nil
}

func (q ReplayRequest) Digest() string {
	return replayDigest("request/v1", q.SourceRunID, q.ThroughRevision, q.TargetNodeID, q.MaxObjects, q.MaxBytes)
}

// ReplayCut 描述派生 Run 从来源位置继承的因果前缀。
//
// CommitIndexDigest 绑定来源 CommitIndex 在 ThroughRevision 处的条目摘要，
// 使计划与「来源确实在该位置提交过」这一事实绑定；它不替代后端 head、
// 所有权或权限校验。ParentRevision 恒为 ThroughRevision-1。
type ReplayCut struct {
	ThroughRevision   uint64 `json:"through_revision"`
	ParentRevision    uint64 `json:"parent_revision"`
	BatchDigest       string `json:"batch_digest"`
	CommitIndexDigest string `json:"commit_index_digest"`
}

func (c ReplayCut) validate(through uint64) error {
	if through == 0 || c.ThroughRevision != through || c.ParentRevision+1 != c.ThroughRevision ||
		!validDigest(c.BatchDigest) || !validDigest(c.CommitIndexDigest) {
		return fmt.Errorf("%w: invalid replay cut", ErrInvalid)
	}
	// CommitIndexDigest 必须绑定同一条索引条目，而不只是「某个合法摘要」。
	// 否则重签计划就能把 cut 指向另一条批次，结构仍然合法。
	want := replayDigest("commit-index/v1", CommitIndexEntry{
		Revision: c.ThroughRevision, ParentRevision: c.ParentRevision, BatchDigest: c.BatchDigest,
	})
	if c.CommitIndexDigest != want {
		return fmt.Errorf("%w: replay cut index digest does not bind its entry", ErrInvalid)
	}
	return nil
}

// ReplayPlan 是不可执行的派生回放候选结果。
//
// RetainedNodes 是 cut 位置上仍然有效、可被派生 Run 复用的兄弟分支成果；
// RecomputeNodes 是目标节点及其实际依赖后代，必须重新计算。两个集合必须
// 互斥且有序，且并集恰好覆盖来源根图在该位置的全部已确认节点。
//
// HeadDigest 使用无损字段表示绑定 nil/empty；PlanDigest 是完整性检查，
// 不是授权。Executable=true 在执行契约完成之前一律拒绝。
type ReplayPlan struct {
	Request        ReplayRequest `json:"request"`
	RequestDigest  string        `json:"request_digest"`
	PlanDigest     string        `json:"plan_digest"`
	SourceRunID    string        `json:"source_run_id"`
	HeadRevision   uint64        `json:"head_revision"`
	HeadDigest     string        `json:"head_digest"`
	Cut            ReplayCut     `json:"cut"`
	TargetNodeID   string        `json:"target_node_id"`
	RetainedNodes  []string      `json:"retained_nodes,omitempty"`
	RecomputeNodes []string      `json:"recompute_nodes,omitempty"`
	Rows           uint64        `json:"rows"`
	Bytes          uint64        `json:"bytes"`
	Executable     bool          `json:"executable"`
}

func (p ReplayPlan) Digest() string {
	// 自身摘要由外层绑定；只清除摘要本身以免自引用。
	p.PlanDigest = ""
	return replayDigest("plan/v1", p)
}

// Validate 只证明结构与摘要自洽。执行方仍须在权威锁内重新规划并比较，
// 不得因调用方能够重新计算 Digest 就信任其保留/重算集合。
func (p ReplayPlan) Validate(q ReplayRequest) error {
	if q.Validate() != nil || p.Request != q || p.RequestDigest != q.Digest() ||
		p.SourceRunID != q.SourceRunID || p.HeadRevision != q.ThroughRevision ||
		p.TargetNodeID != q.TargetNodeID || !validDigest(p.HeadDigest) || !validDigest(p.PlanDigest) ||
		p.Rows == 0 || p.Bytes == 0 || p.Executable {
		return ErrInvalid
	}
	if err := p.Cut.validate(q.ThroughRevision); err != nil {
		return err
	}
	if p.Bytes > q.MaxBytes || uint64(len(p.RetainedNodes))+uint64(len(p.RecomputeNodes)) > q.MaxObjects {
		return ErrLimitExceeded
	}
	if p.PlanDigest != p.Digest() {
		return ErrInvalid
	}
	if p.Rows != uint64(len(p.RecomputeNodes)) {
		return ErrInvalid
	}
	// 目标必须在重算集合内，否则「派生回放目标节点」这一声明不成立。
	if !sortedContains(p.RecomputeNodes, q.TargetNodeID) {
		return ErrInvalid
	}
	seen := make(map[string]bool, len(p.RetainedNodes)+len(p.RecomputeNodes))
	for _, nodes := range [][]string{p.RetainedNodes, p.RecomputeNodes} {
		for i, id := range nodes {
			if !validSemanticText(id) || seen[id] || i > 0 && nodes[i-1] >= id {
				return ErrInvalid
			}
			seen[id] = true
		}
	}
	return nil
}

func sortedContains(values []string, target string) bool {
	i := sort.SearchStrings(values, target)
	return i < len(values) && values[i] == target
}

// PlanDerivedReplay 从权威来源记录重新推导 cut 前缀与保留/重算划分。
//
// 它不创建派生 Run、不重映射身份、不写 Store。来源必须携带完整语义证据：
// 没有因果 CommitIndex 就无法证明「来源在该位置提交过」，也就无法安全派生。
// 目标节点必须在来源根图该位置已确认；未确认节点没有可切断的成果。
func PlanDerivedReplay(record RunRecord, request ReplayRequest) (ReplayPlan, error) {
	if request.Validate() != nil || record.RunID != request.SourceRunID || record.Revision == 0 {
		return ReplayPlan{}, ErrInvalid
	}
	if request.ThroughRevision != record.Revision {
		return ReplayPlan{}, ErrConflict
	}
	if !hasCompleteSemanticEvidence(record) {
		return ReplayPlan{}, fmt.Errorf("%w: derived replay requires a complete semantic evidence component", ErrInvalid)
	}
	if err := ValidateSemanticEvidence(record); err != nil {
		return ReplayPlan{}, err
	}
	root, ok := record.Scopes[record.RootScope]
	if !ok {
		return ReplayPlan{}, fmt.Errorf("%w: replay source has no root scope", ErrInvalidRecord)
	}
	if _, confirmed := root.Nodes[request.TargetNodeID]; !confirmed {
		return ReplayPlan{}, fmt.Errorf("%w: replay target is not confirmed at the cut position", ErrConflict)
	}
	position := commitIndexPosition(record.CommitIndex, request.ThroughRevision)
	if position < 0 {
		return ReplayPlan{}, fmt.Errorf("%w: replay cut has no commit index entry", ErrInvalidRecord)
	}
	entry := record.CommitIndex[position]

	objects := uint64(len(root.Nodes)) + uint64(len(record.CommitIndex)) + 1
	if objects > request.MaxObjects {
		return ReplayPlan{}, ErrLimitExceeded
	}
	sourceBytes, err := snapshotfmt.RecordSize(record)
	if err != nil || sourceBytes < 1 {
		return ReplayPlan{}, ErrInvalidRecord
	}
	if uint64(sourceBytes) > request.MaxBytes {
		return ReplayPlan{}, ErrLimitExceeded
	}

	plan := ReplayPlan{
		Request: request, RequestDigest: request.Digest(), SourceRunID: record.RunID,
		HeadRevision: record.Revision, HeadDigest: recordDigest(record), TargetNodeID: request.TargetNodeID,
		Cut: ReplayCut{
			ThroughRevision: entry.Revision, ParentRevision: entry.ParentRevision,
			BatchDigest: entry.BatchDigest, CommitIndexDigest: replayDigest("commit-index/v1", entry),
		},
	}
	// 依赖闭包：目标节点及其实际依赖后代必须重算；其余已确认成果可保留。
	// 这里只按来源根图的显式依赖边划分，不臆造 Core 的部分端口恢复语义。
	recompute := replayDependencyClosure(root, request.TargetNodeID)
	for id := range root.Nodes {
		if recompute[id] {
			plan.RecomputeNodes = append(plan.RecomputeNodes, id)
		} else {
			plan.RetainedNodes = append(plan.RetainedNodes, id)
		}
	}
	sort.Strings(plan.RetainedNodes)
	sort.Strings(plan.RecomputeNodes)
	plan.Rows = uint64(len(plan.RecomputeNodes))
	if uint64(len(plan.RetainedNodes))+uint64(len(plan.RecomputeNodes)) > request.MaxObjects {
		return ReplayPlan{}, ErrLimitExceeded
	}
	// 摘要固定 64 字符；Bytes 的十进制宽度达到不动点后就是实际规范计划大小。
	plan.PlanDigest = "0000000000000000000000000000000000000000000000000000000000000000"
	plan.Bytes = uint64(sourceBytes)
	for {
		next, err := replayPlanBytes(sourceBytes, plan)
		if err != nil {
			return ReplayPlan{}, err
		}
		if next == plan.Bytes {
			break
		}
		plan.Bytes = next
	}
	plan.PlanDigest = plan.Digest()
	if err := plan.Validate(request); err != nil {
		return ReplayPlan{}, err
	}
	return plan, nil
}

// replayDependencyClosure 沿来源根图的成果引用计算目标的依赖后代闭包。
// 只使用记录里已持久化的流来源身份，不猜测运行时接线。
func replayDependencyClosure(root ScopeRecord, target string) map[string]bool {
	// producer 把「产出某个流来源的节点」映射到该来源身份。
	producer := make(map[string]string)
	for id, node := range root.Nodes {
		for _, value := range node.Outputs {
			if value.Stream != nil {
				producer[value.Stream.Attempt] = id
			}
		}
	}
	seen := map[string]bool{target: true}
	queue := []string{target}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		node, ok := root.Nodes[current]
		if !ok {
			continue
		}
		for _, value := range node.Outputs {
			if value.Stream == nil {
				continue
			}
			// 依赖该来源的节点必须重算；来源本身若由别的节点产出，也一并纳入。
			if owner, ok := producer[value.Stream.Attempt]; ok && !seen[owner] {
				seen[owner] = true
				queue = append(queue, owner)
			}
		}
	}
	return seen
}

func replayDigest(domain string, values ...any) string {
	return semanticDigest("replay/"+domain, values...)
}

// replayPlanBytes 计算「来源无损快照 + 规范计划」的预算字节。
// 计划自身含 Bytes 字段，其十进制宽度会影响总长度，因此由调用方迭代到不动点。
func replayPlanBytes(sourceBytes int64, plan ReplayPlan) (uint64, error) {
	encoded, err := json.Marshal(plan)
	if err != nil {
		return 0, ErrInvalid
	}
	return uint64(sourceBytes) + uint64(len(encoded)), nil
}
