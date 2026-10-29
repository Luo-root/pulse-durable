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

本页描述当前开发分支的强制 Initialize/Apply API，需要 Go **1.26+**、Pulse **v0.5.0**，不是旧 v0.1.0 的接口说明或新 API 已发布声明。没有兼容 Save、RequireDelta 或全量回退；业务使用 `durable.New` 与 Runtime Run/Resume，无需 opt-in。安装 pin 留待跨模块统一更新，请按[贡献指南](https://github.com/Luo-root/pulse-durable/blob/main/CONTRIBUTING.md)验证本地 checkout，发布状态见 [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases)。`sqlitestore` 与 `postgresstore` 是独立模块；完整程序和运维规则见 [SQLite 教程](sqlite.md)与 [PostgreSQL 教程](postgresql.md)。独立 PostgreSQL 仍在开发，本页不声明本轮实库验收通过。

`postgresstore` 使用 pgx/v5，初始化并严格检查专用 `pulse_durable` schema。同一 Store 已持有或 pending 的 RunID 立即返回 `ErrBusy`；跨 Store 须取得连接并完成 `pg_try_advisory_lock` 网络检测，检测到占用后返回 `ErrBusy`，不等待锁释放。池容量与网络检测的总等待默认最多 30 秒，caller 更短 deadline 优先；池满而无法判断远端占用时返回 `context.DeadlineExceeded`，不伪报 `ErrBusy`。Store.Close 取消 pending Acquire 并等待资源清理；解锁失败或结果不确定时销毁连接，清理最长 5 秒。DSN 由宿主管理，不能记录密码；真实数据库测试使用 `PULSE_POSTGRES_TEST_DSN`，会执行 `DROP SCHEMA IF EXISTS pulse_durable CASCADE`，**只能指向隔离测试数据库，不能使用业务库**。未设置 DSN 时数据库测试跳过，不代表通过。源码验证、使用片段与边界见 [`postgresstore/README.md`](https://github.com/Luo-root/pulse-durable/blob/main/postgresstore/README.md)。

`sqlitestore`：驱动 `modernc.org/sqlite v1.60.1` 内嵌 SQLite 3.53.4；`NewContext(ctx, path)` 支持取消数据库初始化。构造设置并读回验证 WAL、FULL (2) 与 busy_timeout=5000ms。数据库必须专用，物理 schema v3 严格匹配 DDL 并拒绝额外 schema 对象；旧 schema 明确拒绝，不自动删改或迁移，新版另用新库；旁边的 `.init.lock` 构造锁必须保留。`New(path)` 只接受数据库文件路径，会创建父目录并在旁边创建 `.runlocks/`；新目录/文件分别使用 0700/0600（Windows 由 ACL 管理）。首快照与不可变批保存于独立表，head 保存权威 `revision` 和 chain，`revision` 为十进制 TEXT；Apply 一事务插批并 CAS 推进 head，不重写完整记录。短事务使用 `BEGIN IMMEDIATE`，SQLite busy timeout 为 5000ms，运行期间不持有数据库事务。它只适用于支持 OS 文件锁的本机文件系统，不支持分布式协调、迁移或跨后端导入。

SQLite 初始化与示例见仓库 [`sqlitestore/README.md`](https://github.com/Luo-root/pulse-durable/blob/main/sqlitestore/README.md)。关闭时先停止业务运行并关闭 Session，再关闭 Store；Initialize/Apply 返回错误时按 Store 契约重新 Load，不推断是否已经提交。FULL 同步不等于已完成断电实测验收，备份、恢复与目录保留由宿主负责。

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

每个 RunID 按其原文的 SHA-256 定位文件，RunID 不直接拼成路径。新布局使用 `.snapshot-<id>` 首快照、`.batch-<id>` 不可变批和 `.head` 权威 manifest；所有权仍使用 `.lock`。锁文件保留是正常情况，独占权来自 OS 锁，Close 或进程退出后释放。不要删除锁文件解除 `ErrBusy`，否则可能产生两个独占域。

Initialize 发布首快照和初始 head；普通 Apply 写不可变批并同步，再原子替换同目录的小 head，不重写整份历史。Load 只沿 head 引用校验快照摘要、连续修订号、批摘要和父链，不以 mtime 或目录排序推断提交。批已写但 head 未发布时属于未提交孤儿，不可认回为业务成果；head 发布后的确认失败按未知提交处理。显式快照合成通过独立的 `durable.Maintenance` 完成，不是普通 Apply 的全量写入回退。`InspectStorage(ctx, runID)` 读取 generation、base/head revision 与 chain；`LoadRevision(ctx, runID, revision)` 返回精确历史投影及原链锚点，不执行业务代码，也不建立持久 pin。`CompactStorage` 请求须携带 RunID、正数 `ThroughRevision` 与从 Inspect 取得的 `ExpectedGeneration`，目标须位于当前 base 与 head 之间；generation 过期返回 `ErrConflict`。

合成在修订号 R 保存新的完整物理快照，其 snapshot digest 独立计算，但必须保留原提交链锚点 `C(R)`，不能由新快照摘要重新播种历史链。发布只推进表示 generation/base，不改变业务 revision/head 或成果；原始快照、全部批和历史修订仍保留，无 GC。MemoryStore 提供同语义的非耐久参考实现。FileStore 与独立 PostgreSQL 的维护读写使用原有独占运行锁，活跃 Session 导致 `ErrBusy`；SQLite 的 InspectStorage/LoadRevision 使用一致读事务、不需要 runlock，CompactStorage 仍排他并可能 `ErrBusy`。发布后报错可能结果不确定，先重新 Inspect 核对 generation/base，再规划下一次操作。业务语义历史压缩、retention 与 `ReplayCreateDerived` 尚未完成；物理合成不是这些能力，也不是无限保留承诺。

当前 `.head` manifest 为物理格式 `2`；旧 manifest `1` 和旧 `.json` 均拒绝，原数据保持不变。检测到旧 `.json` 返回明确旧格式错误，不自动清除、覆盖或迁移；需要旧数据时保留旧版和隔离目录，新版另用新目录。Acquire 取得锁后仍可清理本运行精确匹配 `<SHA-256>-<uint32十进制>.tmp` 的普通暂存文件（无前导零，`0` 除外），但不会把未引用的不可变文件认作提交。清理不递归、不触碰其他运行或 `.lock`；失败返回错误并释放锁。目录权限和保留由宿主管理。性能与逻辑 bytes 的测量边界见[检查点成本](checkpoint-cost.md)，增量不保证所有规模都更快。

## 3. 读取已有运行记录

先按[恢复教程](recovery.md)运行两个进程，保留 `durable-recovery/checkpoints`。然后在它的同级目录创建检查工具模块：

```sh
mkdir durable-storage
cd durable-storage
go mod init example.com/durable-storage
# 新 API 的安装 pin 留待统一更新；先按贡献指南配置当前 checkout 工作区。
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
	fmt.Println("record format:", record.Format)
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

输入须是当前开发分支生成的新布局目录；不能把旧版恢复教程生成的 `.json` 当作兼容输入。对相同简单业务图，预期运行身份为 `recovery-demo-001`、逻辑格式为 `2`、修订号为正整数，完成后包含一个根作用域与两个节点根尝试。检查工具仅 Acquire/Load/Close，打印元数据，不打印编码业务值，不初始化或提交变更。逻辑 RecordFormat 与后端物理格式是不同边界，旧物理目录不会因逻辑格式可识别就自动升级。

读取也需要独占会话。如果业务进程仍持有同一 RunID，会立即返回 `ErrBusy`。`filestore.New` 会创建不存在的目录，所以路径拼错时可能得到空目录和 `ErrNotFound`；请核对路径，不能将记录不存在误判为业务已经完成。

## 4. 接入自定义 Store

通常业务只使用 Runtime；实现后端时，需要满足以下步骤：

1. `Acquire(ctx, runID)` 获得该 RunID 的独占会话。检测到同一 RunID 正被持有时返回 `ErrBusy`，不等待该运行释放所有权；连接容量与网络检测的等待由后端约定，不同 RunID 可以独立使用。
2. `Load(ctx)` 返回隔离副本，没有记录时返回 `ErrNotFound`。权限、损坏或读取故障不能伪装成不存在。
3. `Initialize(ctx, record)` 仅在无检查点时原子发布合法 Revision `1` 初始空执行状态，核对当前格式、RunID 与业务身份；已有检查点返回 `ErrConflict`，不能导入业务历史。
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
