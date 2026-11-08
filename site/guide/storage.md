# 存储

本页适用于选择检查点目录、检查已有运行，以及接入自定义存储后端。先完成[恢复教程](recovery.md)，可以用本页的程序读取教程生成的记录。

## 1. 选择存储方式

| 存储 | 适用场景 | 保证范围 |
| --- | --- | --- |
| `durable.NewMemoryStore()` | 示例、单元测试、进程内协议验证 | 进程内独占会话与 CAS；不保留进程退出后的记录 |
| `filestore.New(dir)` | 本机工作流的退出与重启恢复 | 同一 RunID 的本机跨进程文件锁、文件记录与原子替换 |
| `sqlitestore.New(path)` | 本机 SQLite 文件、首快照与增量批的事务存储 | 同一 RunID 的本机 OS 锁；SQLite `BEGIN IMMEDIATE` 短事务、FULL 同步、busy timeout 5000ms |
| `postgresstore.New(ctx, dsn)` | PostgreSQL 检查点数据库 | 同一 RunID 的会话级 advisory lock；首快照/批 BYTEA 与短事务 head CAS，无 fencing/派发保证 |
| 自定义 `durable.Store` | 由宿主明确存储及协调要求的环境 | 必须实现 Store/Session 契约，另行说明持久性和所有权保证 |

`filestore` 依赖文件系统的独占锁与同目录原子替换，不提供分布式租约、fencing 或跨机器派发，也不承诺断电时的持久性。不能仅凭把目录放到网络共享上，就将其当作分布式运行管理。

本页描述当前未发布 checkout 的强制 Initialize/Apply API，需要 Go **1.26+**、Pulse **v0.5.2**。没有兼容 Save、RequireDelta 或全量回退；业务使用 `durable.New` 与 Runtime Run/Resume，无需 opt-in。子模块 go.mod 已固定公开根开发提交 `v0.1.1-0.20261108162459-ccc90d4b53c3`，它可下载并包含本轮根包契约，是提交伪版本而非发布 tag；SQLite/独立 PostgreSQL 开发 pin 为 `v0.1.1-0.20261108164227-4babad07420f`，distributed 为 `v0.0.0-20261108164227-4babad07420f`，均已可下载并通过独立消费者验证。按[贡献指南](https://github.com/Luo-root/pulse-durable/blob/main/CONTRIBUTING.md)验证当前 checkout，不能将源码组合或根依赖下载结果等同于完整新版子模块发布；tag 状态见 [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases)。`sqlitestore` 与 `postgresstore` 是独立模块，完整程序和运维规则见 [SQLite 教程](sqlite.md)与 [PostgreSQL 教程](postgresql.md)。本轮四模块 `GOWORK=off` 下载/build/vet/race（含独立 PostgreSQL 实库）、根包/FileStore/integration race、SQLite 实库、中英文 SQLite/PG 教程三进程，以及 distributed PostgreSQL 新库全套 race 与空库 DDL 均已通过；这些结果不代替尚未运行的完整 CI。

`postgresstore` 使用 pgx/v5，初始化并严格检查专用 `pulse_durable` schema。同一 Store 已持有或 pending 的 RunID 立即返回 `ErrBusy`；跨 Store 须取得连接并完成 `pg_try_advisory_lock` 网络检测，检测到占用后返回 `ErrBusy`，不等待锁释放。池容量与网络检测的总等待默认最多 30 秒，caller 更短 deadline 优先；池满而无法判断远端占用时返回 `context.DeadlineExceeded`，不伪报 `ErrBusy`。Store.Close 取消 pending Acquire 并等待资源清理；解锁失败或结果不确定时销毁连接，清理最长 5 秒。DSN 由宿主管理，不能记录密码；真实数据库测试使用 `PULSE_POSTGRES_TEST_DSN`，会执行 `DROP SCHEMA IF EXISTS pulse_durable CASCADE`，**只能指向隔离测试数据库，不能使用业务库**。未设置 DSN 时数据库测试跳过，不代表通过。源码验证、使用片段与边界见 [`postgresstore/README.md`](https://github.com/Luo-root/pulse-durable/blob/main/postgresstore/README.md)。

`sqlitestore`：驱动 `modernc.org/sqlite v1.60.1` 内嵌 SQLite 3.53.4；`NewContext(ctx, path)` 支持取消数据库初始化。构造设置并读回验证 WAL、FULL (2) 与 busy_timeout=5000ms。数据库必须专用，物理 schema v3 严格匹配 DDL 并拒绝额外 schema 对象；旧 schema 明确拒绝，不自动删改或迁移，新版另用新库；旁边的 `.init.lock` 构造锁必须保留。`New(path)` 只接受数据库文件路径，会创建父目录并在旁边创建 `.runlocks/`；新目录/文件分别使用 0700/0600（Windows 由 ACL 管理）。首快照与不可变批保存于独立表，head 保存权威 `revision` 和 chain，`revision` 为十进制 TEXT；Apply 一事务插批并 CAS 推进 head，不重写完整记录。短事务使用 `BEGIN IMMEDIATE`，SQLite busy timeout 为 5000ms，运行期间不持有数据库事务。它只适用于支持 OS 文件锁的本机文件系统，不支持分布式协调、迁移或跨后端导入。

SQLite 初始化与示例见仓库 [`sqlitestore/README.md`](https://github.com/Luo-root/pulse-durable/blob/main/sqlitestore/README.md)。关闭时先停止业务运行并关闭 Session，再关闭 Store；Initialize/Apply 返回错误时按 Store 契约重新 Load，不推断是否已经提交。FULL 同步不等于已完成断电实测验收，备份、恢复与目录保留由宿主负责。

## 可组合历史证据（未发布）

`RunRecord` 不携带格式或能力版本号；`Version` 仍是恢复必须匹配的业务身份。可选的 `HistorySummaries`、`CommitIndex`、`Provenance` 全为 nil 表示普通记录，任一出现就必须三者齐全并通过完整证据闭包校验。正常 `Run` / `Initialize` 生成普通记录，普通 `Apply` 不能首次物化证据。`RunRecord.Format`、`RecordFormat`、`RecordFormat3`、`SemanticRecordFormat` 以及 `ChangeHeader.BatchFormat` / `LogicalFormat` 已删除。

`HistorySummaries` 以 `(Kind, ID)` 唯一标识摘要，Payload 有独立 SHA-256；`CommitIndex` 保存从 revision 1 到 head 的连续批次承诺；`Provenance` 保存来源身份与摘要。带证据的普通事务恰好追加下一条索引，禁止改写或删除旧索引。索引摘要绑定同一规范批次中去掉 `commit_index` 操作后的内容，避免自引用；提交前校验与字节预算仅处理变化对象，完整证据在加载边界校验。`CloneRecord` 隔离摘要 Payload 并保留 nil/空形状。自定义后端解码后应调用 `ValidateSemanticEvidence`，并继续执行身份、图、Codec 与引用校验。

摘要的解码、应用标记、候选规划、恢复与普通修改检查共用不可变的 `historySummaryRegistry`，内置 Retry 与 Message 前缀两个 handler。未知 Kind 可在底层只读路径保留为 opaque 证据；恢复或任何修改前必须拒绝未知必需语义，不能绕过检查。摘要与来源信息是完整性元数据，不是授权凭据，也不单凭自身证明历史来源真实。

`PlanHistoryCompression(record, request)` 是只读候选规划：要求完整证据组件且 ThroughRevision 等于当前 head，重新验证 Retry 失败前缀、消息完整重试组、ACK 与引用闭包；未知 Kind、缺口、活跃对象或来源变化均拒绝。请求显式限制删除与保护对象总数，Bytes 计源记录无损快照加规范计划的字节，不是可回收空间或 WAL。计划始终 `Executable=false`，自行改字段并重算 PlanDigest 不能授权执行。`ValidateHistoryCompressionEquivalence` 只复验候选删除与保留字段，不能替代完整图/Codec 恢复校验或锁后授权。

根包 `BuildHistoryCompressionBatch(record, plan)` 已实现：基于当前权威记录重新规划并核对计划，使用完整旧对象作精确 CAS 删除 Attempt/Message，在同一批次更新摘要的 `AppliedRevision`、追加 `CommitIndex`，并校验恢复闭包。构造器不写 Store、不修改输入记录；提交仍走现有 `Session.Apply` 与确认后发布路径。执行方必须在独占/有效所有权下重新 Load 权威状态，计划本身不授予权限。Runtime 恢复按已应用摘要继续 Retry 消费高水位与消息结算前沿，保留 Limit、耗尽身份、ACK 和 Managed State/Values 见证，拒绝前缀重现、后缀缺口及旧 Open/Fail/ACK；普通 Apply 不能创建、改写或删除已应用高水位。

上述根包事务不表示后端首次权威证据物化或 distributed 语义压缩已交付。首次物化、distributed 压缩 floor/永久 receipt、实际 retention 删除、GC 与指定位置派生回放仍未完成；当前 CommitIndex 也尚未包含该回放所需的输入、输出和 State 因果引用。物理 `CompactStorage`、GC、retention、语义压缩和 Replay 是不同能力。

编码版本仅留在边界：ChangeBatch wire 为 `2`，无损 snapshot envelope 为 `2`；Task 顶层为 `protocol=2` / `payload_format=1`，业务 Route 只含 TaskType、BusinessVersion、Definition、ManifestDigest。首版策略明确拒绝旧 wire、snapshot 和 Task，保留原字节，不自动迁移、重签或重编码；物理 backend schema/layout 另行校验，不能据此重写历史承诺。

## 2. 设置文件目录

以下是初始化片段，放在返回 `error` 的函数内；需要导入 `durable` 与 `filestore`，后续再调用 Runtime 的 `Run` 或 `Resume`：

```go
store, err := filestore.New("./checkpoints")
if err != nil {
	return err
}
runtime, err := durable.New(store, durable.NewCodecs())
if err != nil {
	return err
}
```

目录路径字符串不能为空；目录本身可以为空。`New` 会按需创建目录，并在创建 Store 时将相对路径转换为绝对路径。新进程创建新的 Store 时，仍须让路径指向原检查点目录。

支持 Unix 权限位的平台，新目录使用 `0700`，新文件使用 `0600`；已有目录权限不会被修改。Windows 使用目录 ACL。宿主负责目录、备份及数据保留策略，检查点不是加密容器。

每个 RunID 按其原文的 SHA-256 定位文件，RunID 不直接拼成路径。布局使用 `.snapshot-<id>` 快照、`.batch-<id>` 不可变批、`.history-<id>` 不可变快照索引和 `.head` 权威 manifest；所有权仍使用 `.lock`。每个批带固定 256 字节的路由头，包含 payload 摘要、父引用和跳指针；引用摘要逐级受 head 保护。每次合成只追加一个快照索引节点，不复制此前的快照目录。锁文件保留是正常情况，独占权来自 OS 锁，Close 或进程退出后释放。不要删除锁文件解除 `ErrBusy`，否则可能产生两个独占域。

Initialize 发布首快照和初始 head；普通 Apply 只写一个不可变批并同步，再原子替换同目录的小 head，不重写整份历史或快照索引。Load 沿 head 引用校验当前 base 快照和到 head 的连续批，不以 mtime 或目录排序推断提交。快照、批或索引已写但 head 未发布时属于未提交孤儿，不可认回为业务成果；head 发布后的确认失败按未知提交处理。显式快照合成通过独立的 `durable.Maintenance` 完成。`InspectStorage(ctx, runID)` 读取 generation、base/head revision 与 chain；`LoadRevision(ctx, runID, revision)` 通过受摘要保护的索引选择不晚于目标的最近已提交快照，只解码该快照到目标之间的连续批，返回精确投影及原链锚点，不执行业务代码，也不建立持久 pin。目标之外的归档或 suffix payload 不由本次读取审计；所需快照、路由头或批缺失、摘要/链/身份/前置条件损坏均返回 `ErrInvalidRecord`，不能冒充 `ErrHistoryExpired`。修订号 1 直接读取 origin 快照。`CompactStorage` 请求须携带 RunID、正数 `ThroughRevision` 与从 Inspect 取得的 `ExpectedGeneration`，目标须位于当前 base 与 head 之间；generation 过期返回 `ErrConflict`。

FileStore 合成从当前 base 快照重放到目标 R，保存新的完整物理快照，其 snapshot digest 独立计算，但保留原提交链锚点 `C(R)`，不由新快照摘要重新播种历史链。发布只推进表示 generation/base，使用累计物理批字节数保留完整后续 suffix，不改变业务 revision/head 或成果。目标等于当前 base 时是 no-op：仍校验请求范围、generation、权威 head、当前代快照索引及 base 快照的摘要与身份，但不读取任何批或旧归档，不写文件，也不消耗 generation。原始快照、全部批、全部已提交快照索引和历史修订仍保留，无 GC。

完整校验由可选 `durable.HistoryAuditor` 提供，FileStore 的 `AuditHistory(ctx, durable.AuditHistoryRequest{RunID: id})` 在独占锁内验证完整原始链、全部已提交快照的精确投影和索引跳指针，返回观察到的 `State`、`Snapshots`、`Batches` 与 `BatchBytes`；失败时这些计数不代表完成审计。未引用孤儿不属于审计对象，也不会被采纳。该能力不要求其他后端实现。设保留批数为 H、已提交合成次数为 G、所选快照到目标的批数为 K，FileStore 精确读取的寻址元数据最坏为 `O(log² H + log²(G+1))`，payload 重放为 K 个批及一份快照；每次 Apply 或合成的索引追加最多读 `O(log H)` 或 `O(log(G+1))` 元数据，摊销 `O(1)`，不复制历史目录。完整审计仍随全部保留历史增长。

MemoryStore 提供非耐久维护参考实现。FileStore 与独立 PostgreSQL 的维护读写使用原有独占运行锁，活跃 Session 导致 `ErrBusy`；SQLite 的 InspectStorage/LoadRevision 使用一致读事务、不需要 runlock，CompactStorage 仍排他并可能 `ErrBusy`。FileStore 维护入口不枚举目录清理暂存文件。发布后报错可能结果不确定，先重新 Inspect 核对 generation/base，再规划下一次操作。根包已提供语义压缩批构造器，首次权威证据物化、distributed 语义压缩、retention 删除与 `ReplayCreateDerived` 仍未完成；物理合成、GC 与这些能力分别验收，也不承诺无限保留。

当前 `.head` manifest 与批路由头为物理格式 `3`；旧 manifest `1`、`2` 和旧 `.json` 均明确拒绝，原数据及旧目录中的暂存文件保持不变。检测到旧 `.json` 返回明确旧格式错误，不自动清除、覆盖或迁移；需要旧数据时保留旧版和隔离目录，新版另用新目录。Acquire 取得锁后仍可清理本运行精确匹配 `<SHA-256>-<uint32十进制>.tmp` 的普通暂存文件（无前导零，`0` 除外），但不会把未引用的不可变文件认作提交。清理不递归、不触碰其他运行或 `.lock`；失败返回错误并释放锁。目录权限和保留由宿主管理。性能与逻辑 bytes 的测量边界见[检查点成本](checkpoint-cost.md)，增量不保证所有规模都更快。

## 3. 读取已有运行记录

先按[恢复教程](recovery.md)运行两个进程，保留 `durable-recovery/checkpoints`。然后在它的同级目录创建检查工具模块：

```sh
mkdir durable-storage
cd durable-storage
go mod init example.com/durable-storage
go get github.com/Luo-root/pulse-durable@v0.1.1-0.20261108162459-ccc90d4b53c3
```

需要 Go 1.26 或更高版本。保存以下完整程序为 `main.go`：

```go
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"

	"github.com/Luo-root/pulse-durable/filestore"
)

func main() {
	dir := flag.String("dir", "../durable-recovery/checkpoints", "checkpoint directory")
	runID := flag.String("run-id", "recovery-demo-001", "existing run ID")
	flag.Parse()
	if err := inspect(*dir, *runID); err != nil {
		log.Fatal(err)
	}
}

func inspect(dir, runID string) (err error) {
	store, err := filestore.New(dir)
	if err != nil {
		return err
	}
	session, err := store.Acquire(context.Background(), runID)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, session.Close()) }()
	record, err := session.Load(context.Background())
	if err != nil {
		return err
	}
	fmt.Println("run:", record.RunID)
	fmt.Println("business version:", record.Version)
	fmt.Println("revision:", record.Revision)
	fmt.Println("scopes:", len(record.Scopes))
	fmt.Println("attempts:", len(record.Attempts))
	return nil
}
```

执行：

```sh
go mod tidy
go run . -dir ../durable-recovery/checkpoints -run-id recovery-demo-001
```

输入须是当前 checkout 生成的新布局目录；不能把旧版恢复教程生成的 `.json` 或旧 snapshot/wire 当作兼容输入。对相同简单业务图，预期运行身份为 `recovery-demo-001`、业务版本为 `recovery/1`、修订号为正整数，完成后包含一个根作用域与两个节点根尝试。检查工具仅 Acquire/Load/Close，打印元数据，不打印编码业务值，不初始化或提交变更。记录没有格式字段；边界版本不匹配时明确拒绝并保留原字节，不自动升级目录或重编码历史。

读取也需要独占会话。如果业务进程仍持有同一 RunID，会立即返回 `ErrBusy`。`filestore.New` 会创建不存在的目录，所以路径拼错时可能得到空目录和 `ErrNotFound`；请核对路径，不能将记录不存在误判为业务已经完成。

## 4. 接入自定义 Store

通常业务只使用 Runtime；实现后端时，需要满足以下步骤：

1. `Acquire(ctx, runID)` 获得该 RunID 的独占会话。检测到同一 RunID 正被持有时返回 `ErrBusy`，不等待该运行释放所有权；连接容量与网络检测的等待由后端约定，不同 RunID 可以独立使用。
2. `Load(ctx)` 返回隔离副本，没有记录时返回 `ErrNotFound`。权限、损坏或读取故障不能伪装成不存在。
3. `Initialize(ctx, record)` 仅在无检查点时原子发布合法 Revision `1` 初始空执行状态，核对 RunID、业务身份与初始记录不变量；已有检查点返回 `ErrConflict`，不能导入业务历史。
4. `Apply(ctx, batch) (CommitAck, error)` 是唯一普通提交入口，原子核对 typed 变更的精确前置条件、身份与严格 +1 的修订号。耐久后端保存批并 CAS head/chain；失败不得发布半批，修订号溢出拒绝。新会话权威加载链，普通 Apply 不得重放整条历史或全量覆写。
5. `Close()` 幂等，等待在途操作并释放会话所有权；关闭后 Load/Initialize/Apply 必须报错。Store 须并发安全，不能通过可选接口探测后退回旧全量写入。

RunRecord 是可序列化的纯数据。作用域与尝试使用持久随机身份，不可按图路径或本地计数重新生成。字段约束与全部关联见[数据模型](../reference/data-model.md)。

## 5. 处理保存结果不确定

一次保存可能完成了写入，却在后续确认步骤报错。例如 head 已被替换，后续确认失败。此时“函数返回错误”不能证明“磁盘仍是旧记录”。

Runtime 会停止本次执行，不更新自己的已确认内存记录，不回滚或自动重试该写入。处理底层问题、等待原调用结束并确认所有权释放后，先核对原目录与 RunID，通过独占会话重新 `Load` 权威状态；检查会话关闭后再调用 Runtime。

已有有效且兼容的记录时调用 `Resume`：复用已确认成果，未确认节点按恢复规则处理，终态和原 Retry 预算仍然有效。只有确认本次新 `Run` 的首次记录尚未建立时，才按原输入、版本及 Codec 契约继续 `Run`。例如首次输入编码失败可能在Initialize 之前返回 `CheckpointError`；Initialize 报错则可能已经提交，不能据此猜测记录不存在，也不能将 `ErrNotFound` 自动改为 `Run`。权限、损坏或读取故障须先排查，不能作为不存在处理。完整分流见[错误处理](../reference/errors.md)。

自定义后端也必须允许并说明这种情况。不要在存储报错后自动拿旧修订号重试，或用旧内存快照覆盖真实记录。

## 错误如何处理

- `ErrBusy`：找出仍持有该运行的会话或进程；等待宿主的运行协调策略释放所有权，不删除锁文件。
- `ErrNotFound`：核对目录与 RunID，确认业务是否曾创建记录。
- `ErrConflict`：核对 CAS 修订号或运行身份；若通过 Runtime 的可靠保存边界返回，还会被包装为 `CheckpointError`。
- 权限、格式、Unicode 或 I/O 错误：保存现场并修正原因。读取失败不能通过创建新记录覆盖原文件。

文件 Store 写前检查所有元数据字符串和 map 键的 UTF-8；非法元数据不会改变旧文件或 Revision。读取拒绝非法原始 UTF-8 与未配对的 JSON Unicode 代理项。编码后的 `Data []byte` 是不透明字节，不受元数据 UTF-8 限制。

## 下一步

核对[数据模型](../reference/data-model.md)中的 Store/Session 协议，和[错误处理](../reference/errors.md)中的可靠错误分类。需要接入具体业务运行时，返回[恢复教程](recovery.md)。
