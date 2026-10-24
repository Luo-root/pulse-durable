# Durable 整工作流分布式执行设计

状态：待评审、待实现。本文没有将设计能力描述为已完成能力，也没有分布式实测结果。

关联：[Issue #6：领取、租约与故障接管契约](https://github.com/Luo-root/pulse-durable/issues/6)，含 2026-10-15 的补充验收要求。本轮进一步选定 PostgreSQL 为首个实施后端；这是对原票“后端待定”的设计决策，不回写首次 v0.1.0 的范围。

## 1. 范围和保证

一共三个地方：Durable 根包继续负责检查点执行；新增独立协调模块负责提交、领取和 worker 生命周期；新增 PostgreSQL 协调表保存任务、租约和受保护的检查点。数据库驱动不进入 Pulse 或 Durable 根包，依赖方向仍为协调模块 → Durable → Pulse。

- 按整份 workflow 分工，不把同一张图的节点派到不同机器。每个有效租约由一个 worker 的本地 Pulse 内核推进；不同 RunID 并行。
- 接管从权威检查点恢复，不保存调用栈；已确认成果复用，未确认业务体可能重做，既有 Retry、子图、受管流及确认后发布语义保持。
- 保证失效 owner 无法提交检查点、最终结果或改变任务状态；不保证任意时刻只有一个进程在执行业务代码。暂停、分区、不可取消的业务代码可能导致执行重叠。
- 不承诺外部副作用 exactly-once。业务幂等键应使用稳定 RunID 与业务操作身份，不能使用每次变化的 lease ID。业务修改与去重记录须同事务提交；outbox 还需要稳定事件 ID、可靠投递与消费去重。外部系统接受 fencing 时才可将 epoch 传给它验证。
- 第一版不实现跨库原子事务、节点 RPC、管理控制台、自动代码下载或跨后端迁移。

## 2. 已核对的现有能力与缺口

| 证据 | 现状 | 本设计的增量 |
| --- | --- | --- |
| `store.go:21-39` | Acquire/Load/Save/Close；Save 依赖 Revision CAS；没有 lease 参数和终态操作 | 新协调接口，不把旧 Store 视为天然支持租约 |
| `store.go:45-58` | RunRecord 有 Format、RunID、Version、Definition、Inputs、作用域和尝试；Inputs 是指纹 | 另存可跨机器恢复的输入载荷和路由描述，另存运行终态 |
| `runtime.go:60-74,89-180` | Run/Resume 阻塞 Wait；自行 Acquire 并 defer Close；恢复要求重新给图、输入和业务版本 | 显式的已领取执行入口，复用校验与执行逻辑，不重复 Acquire |
| `runtime.go:109-160` | Start 前核对定义、版本、输入和已有记录；最终失败没有新预算 | 分布式 worker 保留这些校验，并增加注册路由、Codec 与载荷校验 |
| `postgresstore/store.go:23-25,94,129-180,243-271,380-427` | 固定 schema 的严格校验；连接级 advisory lock；Save 仅校验身份与 Revision | 新 schema，不改旧表，不在长时间业务执行期间占一条锁连接 |
| `README.md:142-151`、`postgresstore/README.md:18-20` | 尚不承诺调度、租约、fencing 或实库故障验收 | 本文定义后续能力，不改写现有发布状态 |

不能只在 worker 开始时校验 token，然后调用原 `postgresstore.Session.Save`。旧 Save 不知道 epoch，Revision 也不是所有权；接管后检查点尚未变化时，旧 Revision 仍可能匹配。

## 3. 身份、版本和提交载荷

### 3.1 四种身份分别使用

- `RunID`：宿主显式提交的非空 UTF-8 稳定运行身份，恢复不变。实际主键为 `(namespace, run_id)`；namespace 是分布式租户/应用隔离边界，不是业务版本。
- `lease_id`：每次领取前生成的新随机 128 位以上身份，不复用；同一次 Claim 结果查询仍用原 ID。标识本次实际分布式执行，与 Pulse 进程内 Execution ID 分开记录。
- `worker_id`：运维可读实例名，加每次进程启动的随机 `worker_boot_id`。同名 worker 重启不能继承旧租约。
- `epoch`：数据库内每 RunID 严格递增的所有权代数，首次为 1，每次 Claim/Takeover 增加，释放后不归零；禁止溢出回绕。lease_id 和 epoch 必须同时匹配。

不新增 WorkflowID。节点原有 Scope/Attempt 随 RunRecord 保留，不从 lease_id/epoch 重建，不因接管获得新 Retry 预算。观测同时携带 RunID、lease_id、epoch、worker_boot_id、Pulse 本地 Execution ID。

### 3.2 任务描述和注册表

Submit 提交不可变描述：`task_type`、业务 `Version`、`Definition` 指纹、协议版本、RunRecord 格式能力、输入封装格式版本、完整 Codec 清单（键名、TypeID、Version）、规范化输入载荷、载荷 SHA-256、输入语义指纹及路由摘要。另有调用方访问控制后的 namespace。

输入载荷必须包含能够在另一进程构造 `pulse.Inputs` 的实际值，不能只存现有 `RunRecord.Inputs` 哈希。第一版仅支持显式、有限大小的编码载荷，不支持函数、channel、进程内指针或直接传流端点。需要外部流的任务持久提交来源 ID、恢复游标所需配置引用及兼容版本，worker 通过注册构造器重建来源；无法重建的输入在 Submit 拒绝。载荷限额、Codec 清单顺序与确定性封装必须在实现 PR 中作为格式规范固定。

宿主注册 `(task_type, Version, Definition, Codec 清单摘要, 协议能力)` → 图/输入解码器/纯装配构造器；worker 只领取完整匹配的路由。不解析版本大小关系，不以“最新版本”替代精确匹配，也不认为结构指纹能识别闭包、函数体或切面语义变化。业务 Version 变化必须显式发布新的注册项。

执行顺序：检查封装与长度 → 校验载荷摘要 → 匹配注册项及 Codec → 重算本地图 Definition → 解码输入并重算输入指纹 → 校验已有 RunRecord 格式、全部引用和值 → 续租仍有效确认 → 装配恢复计划 → BeforeStart → Start。拒绝发生在业务节点和宿主 BeforeStart 前；装配构造器、Codec 也可能运行用户代码，契约要求其无业务副作用。路由条件只用于筛选，不代替领取后的逐项验证。

混合版本滚动升级期间保留旧注册项的 worker 排空旧任务。无兼容 worker 的任务继续 queued，暴露阻塞路由和等待时间，不默默改版本。不支持格式的 worker 不领取；已领取后发现载荷损坏或不可兼容，将其受保护地置为 `blocked` 并记录脱敏原因，不循环抢领。修复后经显式管理操作验证并重排，不能修改原任务身份描述；业务定义变化须用新 RunID。

### 3.3 Submit 去重

唯一约束为 `(namespace, run_id)`。对规范化不可变描述求 `submission_digest`，包括路由、版本、输入载荷、Codec 和所有影响执行的选项；不包括创建时间、追踪信息或随机请求 ID。

同主键、同 digest 且逐字段确认相等：返回原任务与状态，标记 deduplicated，不重置租约、检查点或终态。同主键但描述不同：返回 `ErrSubmitConflict`，不得覆盖输入。并发 Submit 用 INSERT ON CONFLICT DO NOTHING 后读取权威行比较，事务竞争结束再查询；不能将“本语句未读到冲突行”当不存在而创建第二条。digest 不是访问凭证。

终态和去重记录第一版不自动删除，RunID 不可重用；后续保留策略需保留同命名空间的身份墓碑，否则去重期限必须作为破坏性契约另行评审。

## 4. 模块、表和访问边界

建议新增独立 Go 模块 `distributed/`，其中 `postgres/` 实现协调存储；这是拟议布局而非当前已有接口。协调接口提供 Submit、Claim、Renew、LoadOwned、SaveOwned、Finish、Release、Cancel、Get/List。根包不导入它或 pgx。

数据库使用独立 schema `pulse_durable_distributed`，独立 schema 版本和迁移，不向旧 `pulse_durable` 加表（旧初始化严格检查对象数量）。首版一张权威 `runs` 表将租约和 record 放在同一行，避免跨表检查与写入遗漏：

| 列 | 约束/用途 |
| --- | --- |
| namespace、run_id | 联合主键；不可变 |
| 不可变提交描述、submission_digest、input_payload | 校验与版本路由；载荷 bytea |
| status | queued / running / blocked / succeeded / failed / canceled |
| epoch bigint | 初始 0；非负；溢出拒绝；不使用 uint64 隐式截断 |
| lease_id、worker_id、worker_boot_id、lease_until | running 全部非空；其他状态全部清空 |
| revision numeric(20,0)、record bytea | 初始 revision=0、record NULL；后续与 RunRecord.Revision 一致，范围 0..MaxUint64 |
| last_mutation_id、last_mutation_digest | 最近可靠变更的确定身份，用于识别丢失提交响应 |
| created_at、updated_at、finished_at、reason_code | DB 时间与脱敏查询信息，不持久化任意业务错误正文 |

另有同事务追加的 `events` 表，按 `(namespace, run_id, event_seq)` 唯一，记录领取、续租异常、释放、接管、终态及拒写事实；正常每次续租可用指标替代事件，避免无限放大。事件与结果不可出现“终态提交了但历史说未完成”的双写窗口。拒写事件另开短事务记录观测，不让记录失败改变拒写结果。队列建立匹配路由/queued 的索引和 running/lease_until 的部分索引；Get 从写主库读，第一版不使用副本做接管或不确定提交裁决。

### 4.1 排除旧写路径

旧模式与新模式不得共用同一个逻辑 RunID 命名空间。部署配置显式区分 legacy 与 distributed namespace，禁止把旧表当新任务检查点。新 worker 不对新任务构造旧 `postgresstore.Store`，新任务的 record 只存在新表。旧记录不自动迁移；迁移需停旧 owner、核对终态/输入载荷/版本、使用新 namespace，独立实施和验收。

数据库授权分为迁移角色、协调执行角色、查询角色；业务进程不持迁移权限。新表撤销任意普通 UPDATE 权限，通过版本化数据库函数执行下节短事务，函数固定 search_path、校验调用范围；worker 只获所需 EXECUTE 权限。旧 Store 凭据无新 schema 写权限。否则任何通用 SQL/旧写适配器都可绕过 fencing，协议保证不成立。第一版可信宿主负责 namespace 鉴权；多租户强隔离不能只靠传入字符串。

## 5. 原子操作和数据库时间

### 5.1 共同前提（实现不得省略）

所有权操作只在单个权威 PostgreSQL 主库执行。要求无丢失已确认事务的持久配置；异步主从强制切换丢提交、旧主继续写入不在保证内。数据库恢复到旧备份后必须隔离旧 worker 并切换新 namespace，不能让 epoch 倒退后继续对外宣称 fencing。数据库自身的单主隔离和持久性由部署保障。

每个操作使用 READ COMMITTED 的短事务/数据库函数：先 `SELECT ... FOR UPDATE` 锁定权威 runs 行，获得锁后再执行 `SELECT clock_timestamp()` 得到 `t`；在同一事务内以当前行条件 UPDATE，并核对恰好一行。锁超时、语句超时、连接 deadline 和 idle-in-transaction 超时必须有界；业务代码、网络副作用、编码大对象和人为等待不放在此事务内。领取批次小且可配置。

禁止在加锁前采样时间，禁止用事务开始时冻结的 `now()` / `CURRENT_TIMESTAMP` 判断到期：事务可能在锁上等待到租约已过期。也禁止应用“先 SELECT token，再无条件 UPDATE”。所有操作都拿同一行锁；下列 UPDATE 条件即使有锁仍保留，便于审计和变异测试。

定义 `owner_ok(t)` 为：`status='running' AND epoch=:epoch AND lease_id=:lease AND worker_boot_id=:boot AND lease_until > :t`。到期边界为 `lease_until <= t`，无客户端宽限写入。

有效写的线性化点是持锁后、使用 t 进行条件校验和变更的时刻，不声称以 TCP 回包或 COMMIT 回包时刻为准。若写入在有效期内完成条件判定，提交响应可能晚于 expiry，但 Takeover 必须等行锁释放；它不能先接管再被这笔旧写覆盖。长事务会延迟接管，因此超时是可用性验收项；不承诺在数据库不可用或存在无界锁等待时按 TTL 准时恢复。

以下为需要实现的原子 SQL 轮廓，不是可直接执行的迁移脚本。`:t` 均指本事务锁后取到的数据库时间，参数已通过格式、长度和范围校验。校验分两层：SQL 负责权限、所有权、expiry、Revision、不可变身份和状态机；可信 Go 执行器负责完整 RunRecord 图语义、Codec、节点输出和最终执行结论。数据库权限不是防御恶意受信 worker 伪造业务成功的沙箱。

首版固定 record 为 UTF-8 JSON 字节，并限制编码长度。SQL 使用 `convert_from(record, 'UTF8')::jsonb` 解析实际写入字节，核对 run_id、revision、version、definition、inputs 与提交描述及 expected revision 一致；缺字段、错误类型、越界和解析错误均拒绝，不能只校验调用者另传的 metadata。Finish(succeeded/failed) 的业务终态证明由共用 Runtime 执行结果分类产生，协调器在校验权威记录后调用。SQL 只验证合法状态转换、有效 owner 与最终 revision，不宣称能验证任意 Go 图的业务语义。直接调用数据库函数的受信身份仍须服从此契约；权限撤销禁止绕过函数进行普通 UPDATE。

### 5.2 Submit 与 Claim/Takeover

Submit 在同一事务插入完整描述，`status='queued', epoch=0, revision=0`；冲突按 3.3 处理。Submit 的成功不代表领取或业务成功，只有 Commit 确认后返回 accepted；响应丢失可重发相同描述查询去重结果。

Claim 先按 worker 精确支持的路由查候选，用 `FOR UPDATE SKIP LOCKED LIMIT :batch` 取得行。候选筛选的时间只用于减少扫描，取得行锁后必须重新采样和判断。对每个候选执行：

```sql
UPDATE pulse_durable_distributed.runs
SET status='running', epoch=epoch+1, lease_id=:new_random_lease,
    worker_id=:worker, worker_boot_id=:boot,
    lease_until=:t + :ttl, updated_at=:t
WHERE namespace=:ns AND run_id=:run AND epoch=:observed_epoch
  AND epoch < 9223372036854775807
  AND (status='queued' OR (status='running' AND lease_until <= :t))
RETURNING epoch, lease_until, revision, record;
```

queued → running 为 Claim；expired running → running 为 Takeover，必须增加 epoch 并换 lease_id，保留检查点和预算。blocked/所有终态不可领取。续租与接管抢同一锁：先成功续租者使接管条件不成立；先接管者使旧续租条件不成立。不存在后台无条件“把过期 running 改 queued”的竞态窗口。

Claim Commit 响应不确定时不 Start。用原 lease_id 查询主库：仅在仍是本 boot/epoch、仍有效且 Renew 成功后才能执行；否则丢弃候选。不用新的 lease_id 重放同一次不确定领取，也不启动多个本地 Execution。

### 5.3 Renew

```sql
UPDATE pulse_durable_distributed.runs
SET lease_until=:t + :ttl, updated_at=:t
WHERE namespace=:ns AND run_id=:run
  AND status='running' AND epoch=:epoch AND lease_id=:lease
  AND worker_boot_id=:boot AND lease_until > :t;
```

TTL 来自有界配置，必须正数；禁止 expired owner 原地复活。初始测试配置可用 TTL=15s、续租周期=5s、单次 DB deadline=2s，它们是待测参数而非生产推荐。worker 使用单调时钟调度续租和本地安全截止时间；本地 deadline 仅用于提前停止，不赋予提交权。每次确认 Renew 后更新本地预算；不得以响应收到时刻加 TTL 高估服务端租约，可保守使用请求发出时刻加 TTL 再扣安全余量。

### 5.4 LoadOwned 与 fenced Save

LoadOwned 在同一锁后时间规则下验证 owner_ok，读取 record/revision；读取失败不是 ErrNotFound。Get 可以不持租约查询状态，但 Get 结果不构成继续执行许可。

Save 在编码完成后发起短事务，以 owner_ok(t) 和 Revision 同时保护：

```sql
UPDATE pulse_durable_distributed.runs
SET revision=:expected+1, record=:encoded_record,
    last_mutation_id=:mutation, last_mutation_digest=:digest, updated_at=:t
WHERE namespace=:ns AND run_id=:run
  AND status='running' AND epoch=:epoch AND lease_id=:lease
  AND worker_boot_id=:boot AND lease_until > :t
  AND revision=:expected;
```

首次 expected=0 只能写 Revision=1，之后严格 +1，溢出拒绝。函数核对 record 的 RunID、Revision 及描述一致性，防止把别的运行记录写入。流 append、消息 ack、显式状态、Retry、完整节点等所有可靠保存均走这一路径，没有备用旧 Store 写入口。成功 Commit 前不发布成果；0 行时由持锁当前行区分 `ErrLeaseLost`、`ErrRevisionConflict`、`ErrTerminal`，不能报告 skipped。

Revision 与 epoch 分工：前者识别同 owner 的检查点顺序，后者拒绝接管前 owner，即使接管后 revision 没变也能拒绝 A。

### 5.5 Finish、Release、Cancel

Finish 只由有效 owner 执行。持锁后以 owner_ok(t) 和 expected revision 条件，在**同一事务**写入最后需要的 record（若有则 Revision +1），保存终态结果摘要、设置 succeeded/failed、finished_at、清空租约，并追加终态事件。结果以已确认 RunRecord 为权威，不另写一个无 fencing 的结果表。无新检查点时仍比较最终 revision。可信 Go 执行器必须先核对根图完成与输出完整性再请求 succeeded；failed 仅用于 Runtime 已权威确认、预算耗尽的最终业务失败，不能把 CheckpointError、网络超时或宿主退出当 failed。数据库函数验证的是 owner、最终 revision 和终态转换，不独立证明 Go 图语义。终态不可再 Save/Renew/Release；相同 Finish 的重试只能查询原 mutation 确认，不能重开运行。

Release 用于 worker 停机、基础设施中断或可重试执行错误：先取消本地、等待可协作业务与保存任务退出，再以 owner_ok(t) 将 running → queued、清空租约，保留 revision、record、epoch，不做 canceled。如果代码不响应取消，不得提前 Release 后放任它继续有效写；让租约自然到期，服务端 fencing 仍是最终防线。Close/析构不执行无条件 Release，不得解除新 owner 的租约。

Cancel 是经授权的业务操作，不需要当前 owner token，但必须在同一行锁下执行 `WHERE status IN ('queued','running','blocked')`，置 canceled、清空租约、记录 finished_at 与取消事件。取消前最后已提交检查点保留，不回滚外部副作用。worker 在续租/轮询发现 canceled 时取消本地。查询必须区分 canceled 已生效与业务 goroutine 已退出，不能保证对不协作代码即时停止。

Finish 与 Cancel 并发由同一行锁决定：Finish 先提交则 Cancel 返回 already_terminal 和最终状态；Cancel 先提交则 Finish 返回 ErrTerminal，结果不进入终态记录。重复 Cancel 返回已有状态。终态没有自动 Resume；需要重做业务时显式创建新 RunID。

## 6. Worker 执行和 Run/Resume 兼容

worker 循环为：登记版本能力 → 按剩余并发容量 Claim → 确认提交 → 校验载荷与恢复记录 → 启动独立续租任务 → 显式执行已领取会话 → 根据权威执行结论 Finish 或 Release。空队列使用有上限的抖动退避；续租有独立连接池预算，不能被 Save 或大量 Claim 耗尽。每个 lease 至多一个本地执行，进程内登记表只用于防自身重复，不替代数据库原子性。

现有 `Runtime.Run/Resume` 和 `Store/Session` 的签名及行为保持：Run 仍要求新 RunID、Resume 仍要求调用方提供图和相同输入、调用仍阻塞。不将现有 Run 静默改成 Submit，也不把现有 Close 静默解释成业务取消。旧模式也不自动进入 distributed namespace。

现有 Runtime 不能直接接收已领取 session，因此必须实施一个根包的显式桥接入口，并抽取共用执行逻辑：

1. 旧 Run/Resume 继续负责 Acquire/Close，并调用共用执行逻辑。
2. 新入口接收已领取的、只允许 fenced Load/Save 的会话、已验证描述、图和输入；不再调用 Store.Acquire，也不自行宣告终态。入口只依赖标准库、Pulse 与根包数据类型。接口名称和公开性在桥接 PR 评审，不在本文假称 API 已存在。
3. 协调模块掌握 Renew/Finish/Release。session 的 Close 只结束本地资源，不隐式撤销另一 owner；必须让其本地 Save 串行并在失租时关闭。
4. record 缺失且 revision=0 时使用现有新建逻辑；record 存在时使用恢复逻辑，包括格式、输入、作用域、流、最终业务失败与预算校验。不能仅凭 status=running 决定调用现有 Run。
5. 若根检查点已经成功但 Finish 前进程死亡，接管者校验并复用成果完成 Finish，不重做已确认节点；若根最终失败已确认，则 Finish(failed)，不分配新预算。执行异常分类必须复用现有错误类型，不能将所有 Wait 错误统一为失败终态。

不能靠每个租约临时包一层旧 Store.Adapter 然后让 Acquire 再领取，也不能旁路现有 validateLoaded。BeforeStart、订阅和恢复回调在一次新执行中仍按原顺序发生，不从历史伪造 Pulse 节点 Running/Completed 生命周期。

## 7. 故障处理与时间边界

| 情况 | 行为与最终裁决 |
| --- | --- |
| Renew 返回 token/expiry 不匹配 | 立即标记失租，取消本地执行，拒绝本地后续 Save；仍靠服务端条件拒写兜底 |
| Renew 超时、连接断开、返回不确定 | 封闭旧 session，拒绝新 Save，并取消旧 Execution。只有旧 Execution 已完全退出、全部在途 Save 已落定且结果完成权威重读后，才允许重新确认并 Renew 原租约、创建新的恢复执行。不能完成静止确认时，不续租、不复用原 token；待到期后以新 lease_id/epoch 领取。禁止两次 Execution 共用仍可写的同一令牌 |
| Save/Finish COMMIT 返回错误或响应丢失 | 不自动重放、不向下游发布本次未确认结果，取消执行。恢复时从主库重新 Load；比较 mutation ID、digest、revision、epoch 与终态确认原请求是否提交，否则保留 unknown，不能凭网络错误报告业务失败 |
| 读取错误或 DB 不可用 | 不当作空历史、不新建同名任务；停止领取，取消受影响执行，使用有界退避。DB 恢复后重新确认/领取；不能因客户端缓存认为自己仍有权 |
| Claim 成功后进程强杀 | 到期可接管；record=0 的新任务仍在表中，不静默漏任务 |
| 业务完成、检查点未提交 | 允许重做业务；业务幂等负责副作用窗口 |
| 检查点已提交、发布前死亡 | 由持久成果恢复并重新提供本地数据流输出；不等于外部消息已投递一次 |
| 进程长暂停后恢复 | 在任何新业务启动点和可靠保存前确认本地 lease 状态；过期 token 的所有存储写都被拒绝；不能假设取消能撤回已发请求 |
| DB 重启 | 已提交行和 epoch 保留；短事务连接报错按未知提交处理；恢复后按 DB 时间重新竞争，不能把 advisory lock 消失当作租约释放 |

每个可靠变更先生成 mutation_id 和编码 digest，同一次不确定请求不换身份。Save 每个 lease 内串行；如果 last_mutation 已被新 owner 后续保存覆盖，则不能仅靠该字段断言旧请求是否提交，查询最终权威记录并走恢复，不向调用方伪造确定的原调用成功。需要永久逐次回执时可用同事务 events 的 mutation 唯一键保留，但不将“至少一次重试”变成 exactly-once 业务承诺。

数据库时间是租约唯一权威。首版安全前提是权威 PostgreSQL 的租约时钟不回拨；数据库持久性、时间源与单主保障属于部署条件。墙钟回拨不仅影响接管延迟：在尚无新 owner 时，也可能使已到期旧令牌重新满足 expiry 条件，因此不宣称协议能够容忍任意回拨。监控不能替代这一前提。

所有权操作在检测到时间异常时 fail closed。恢复流程必须先停用全部旧 worker 的数据库访问、终止其遗留事务，再使用新 namespace 和新 worker 启动恢复；不得在原 token 下直接恢复服务。没有检测到的回拨仍属于首版保证之外，不能将“尚未观测异常”当作不回拨证明。向前跳会提前过期，epoch/fencing 仍隔离接管后的旧写。worker 单调计时仅用于主动停工，不赋予数据库写权限。

测试用隔离时钟验证“已观察过期→回拨→原 token Save/Renew”的反例，以及触发异常隔离后旧凭据不可访问、新 namespace 可恢复的操作流程。该测试用于证明前提与失败边界，不伪称任意回拨下仍安全。不得修改 Windows 或 VPS 的真实主机时间。

停止 worker 与业务取消分开：优雅停机停止 Claim，给既有执行有限 drain 时间；超时取消本地并按 Release 规则处理。强杀没有机会 Release，不将所有运行设 canceled。服务管理员清理 worker 注册信息也不能无条件删除租约。

## 8. 必须可复现的两 worker 交错

测试使用屏障/数据库会话锁/故障代理控制顺序，不靠 sleep 猜时机。所有步骤记录同 RunID 的身份、revision、epoch 和 DB 时间。

### 8.1 同时领取

1. Submit R，queued/epoch=0。
2. A、B 同时 Claim，控制二者到达行锁点。
3. 只有一方获得 epoch=1 的有效租约；另一方跳过或无候选，不启动该 R。
4. 获胜者 Save 成功；败方伪造自己 lease 的 Save 被拒绝。

### 8.2 A 失联，B 接管，A 恢复

1. A 持 lease=a/epoch=1，revision=7；A 已在客户端读到旧所有权和 revision，随后断开到 DB 的网络并暂停在 Save 前。
2. 数据库确认 expiry；B Takeover 获 lease=b/epoch=2。先不让 B Save，使 revision 仍为 7，专门排除“Revision 恰好挡住旧写”的假阳性。
3. 恢复 A 网络，A 用 expected=7、epoch=1 尝试 Save/Finish/Renew/Release，全部被拒绝且字节、revision、状态不变。
4. B Save 至 revision=8 并 Finish；A 的 Close 不能影响 B 或最终状态。允许业务副作用计数大于一次，但持久成果必须来自有效 owner。

变异对照 A：删除所有权条件、保留 Revision CAS，同一交错中 A 的 expected=7 会错误成功，用例必须变红。变异对照 B：实现为先读 token 校验，暂停 A，B 接管，再让 A 普通写，必须检测到旧结果覆盖。两项变异均只在隔离测试构建中执行，不能合入生产路径。去掉 expiry 或终态条件也应有针对性负例。

### 8.3 锁等待跨 expiry 与取消竞态

- A 的 Renew/Save 事务开始于 expiry 前，被行锁阻塞；放行时已过 expiry，使用锁后 clock_timestamp 必须拒绝。变异使用 now() 必须被测试捕获。
- A 已在有效期持锁完成条件更新，B 在 expiry 后等待 Takeover：B 只能在 A 结束后判断最新行，不能先接管再被 A 覆盖。人为制造慢提交用于测恢复延迟与超时，不将其计入正常吞吐。
- Finish 与 Cancel 两种提交顺序均做屏障测试：唯一终态，取消先成功则无最终结果写入，完成先成功则取消不改变成功；重复请求不能产生第二个终态。
- Renew 丢响应时，屏障挂住忽略取消的业务体及在途 Save；确认同一 token 下无法启动第二次 Execution。旧执行及 Save 完全退出后才能复用仍有效的租约，否则只能等待新 epoch 接管。
- SQL 函数直接提交 RunID/Revision/Version/Definition/Inputs 不符、非法 UTF-8/JSON、超限记录必须拒绝；Go 执行器不得对未完成根图调用 Finish(succeeded)。SQL 不承担防御恶意受信 worker 的业务语义验证。

## 9. 系统测试矩阵和验收

本节全部是待执行计划。当前未运行真实数据库、多 worker、跨机器或性能验收，不填造通过率或吞吐数字。

### 9.1 环境、负载与执行方法

- 协议单测可用可控时钟/内存模型；原子性、锁等待、提交不确定必须使用真实 PostgreSQL。正常测试和变异测试使用隔离、可丢弃库、新 schema，不复用旧后端会 DROP schema 的测试 DSN。
- 单机：真实独立 OS 进程，分别 1（基线）、2、4、8 worker；不是一个测试进程中的 goroutine。测试控制器记录 PID、boot ID、构建 SHA、版本和每个 worker 实际接单数量，强杀后必须出现新 PID/boot。
- 跨机器：至少两台独立主机，共享同一授权测试 DB；总量 2/4/8 worker 分别分配 1+1、2+2、4+4，资源不足时如实记录未测单元。分别测 worker 间不连通（应不影响 DB 协调）、单个 worker 到 DB 断连、全部 worker 到 DB 断连。不能把同机不同端口称作跨机器。
- 负载固定随机种子、输入、并发上限、图定义和 Codec：短无副作用图测协调开销；CPU/IO 混合整图测扩展；100/1000 节点与不同记录大小测 Save 放大；带 Retry/子图/流/消息状态的图测恢复语义。业务副作用使用可查询的测试去重表和稳定操作键。
- 性能各单元预热 1 分钟、稳态 10 分钟、重复 3 次；相同 DB/硬件/连接预算下对照 1/2/4/8 worker。故障单元每个交错至少 100 次，独立进程每个故障至少 20 次；另做本机 8 worker 连续 2 小时稳定性测试。时间、次数是验收计划，未经批准资源不足可以标未测，不可缩减后仍称全部通过。

### 9.2 覆盖矩阵

| 方向 | 场景（本机与跨机均覆盖，注明不适用项） | 方法与观测 | 验收条件 |
| --- | --- | --- | --- |
| 功能 | Submit 重复/冲突/并发；同 RunID 多领取；不同 RunID 分工 | 对照提交描述和任务行、业务启动计数 | 相同提交仅一任务，不同描述冲突；有效持久提交唯一；2/4/8 各进程均有实际处理 |
| 功能 | Run/Resume 回归、初始化前崩溃、完成但 Finish 前崩溃、预算耗尽 | 运行根模块及各独立模块回归；按屏障杀进程恢复 | 已确认节点不重做、预算不重置、最终失败不复活；确认后发布不变 |
| 功能 | Codec/Definition/Version/协议/载荷摘要不符；滚动升级混合版本 | v1/v2 进程混跑、损坏载荷；业务入口计数器 | 不匹配业务入口及 BeforeStart 计数均为 0；无兼容者可查询 queued/blocked，不无限抢领 |
| 稳定性 | 网络断开、单向黑洞、丢响应、100/500/2000ms 延迟与抖动 | 授权故障代理；分别切 Claim/Save/Renew/Finish 的提交前后 | 旧写成功数为 0；unknown 不伪造失败/成功；恢复后无非终态任务永久失踪 |
| 稳定性 | A 断网/长暂停、B 接管、A 恢复；旧读再写 | 第 8 节屏障及真实进程暂停/恢复；记录接管前后 revision | CAS 未变时仍拒 A；所有 fencing 变异确定失败，不只观察最终结果 |
| 稳定性 | 强杀、优雅停止、忽略取消的业务 | 在领取、续租、业务、保存、发布、Finish 各边界终止进程 | 强杀任务可恢复、停机不变 canceled；忽略取消也无法失效提交 |
| 稳定性 | DB 重启、池耗尽、慢事务、锁等待跨 expiry、时钟跳变 | 隔离 DB/代理/测试时钟；记录 SQLSTATE、DB 时间及锁等待 | 不把 DB 错误当空历史；过期 Renew 不复活；持锁恢复界限与偏移影响符合第 5/7 节 |
| 稳定性 | Cancel/Finish/Release/Takeover 并发，取消后旧 owner 恢复 | 运行两种有序交错及随机重复 | 终态最多一次且不可变；Cancel 胜出后 Save/Finish 成功数为 0 |
| 性能 | 1/2/4/8 worker，相同任务集/总资源与每 worker 资源两种口径 | 记录总吞吐、尾延迟、DB CPU/IO、连接、锁等待 | 所有安全性验收仍通过；公布各次数据、离散度与饱和点，不以更多 worker 必须线性加速为前提 |
| 扩展性 | 批量 queued、无匹配版本、大记录、高 Save 频率、续租抢占 | backlog 增长/排空曲线，调度/续租池隔离对照 | 队列最终排空或有明确 blocked 原因；无饥饿性漏任务；续租不被 Claim/Save 无界阻塞 |
| 观测 | 跨进程/跨机恢复、拒写、未知提交、终态查询 | 将事件与 DB 行、PID、Mutation ID 对账 | 每条接管/终态可关联 RunID、lease_id、epoch、boot；无密钥/业务载荷泄漏，不伪造节点事件 |

### 9.3 度量与判定

安全性硬门槛：失效 owner 有效写入数=0；重复/冲突 Submit 破坏数=0；不可变终态被改写数=0；版本不匹配业务启动数=0；无法解释的丢任务数=0；两种去 fencing 变异和冻结时间变异必须被确定性用例捕获。不能用 99.9% 掩盖安全性错误。

可用性度量：Submit→Claim、Claim→Start、expiry→Takeover、故障解除→完成的 p50/p95/p99/max；Renew 延迟与失败率；各 worker 实际工作量、backlog、blocked 路由时长、重复业务次数、恢复复用节点数。正常 DB 健康且无注入、无 backlog、路由匹配时，expiry→Takeover 的初始验收预算为 `poll_max + DB_operation_deadline + 2s`；故障恢复后 2 倍 TTL 内应观察到候选重新领取或明确记录阻塞原因。超预算保留为失败/待调查，不能删除离群值；数据库不可用时间不计算成协议保证内的自动恢复时间。

性能度量：成功运行/秒、节点/秒、每运行 Save 数与字节、p95/p99 总耗时、协调 SQL/秒、worker CPU/RSS/GC、DB CPU/IO/WAL、池等待、锁等待、续租裕量、无效 Claim 比例；报告加速比 Tn/T1 与效率 Tn/(n*T1)。绝对吞吐与跨机网络预算须等硬件参数明确后，在测试前冻结，不事后挑阈值。第一轮用于建立基线和瓶颈归因，不把“测过 8 worker”写成“可线性扩展”。后续同环境回归的吞吐下降 >10% 或 p99 增长 >20% 标为回归待解释，不自动改变安全性门槛。

观测输出：结构化事件包含 RunID/namespace、worker/boot、lease/epoch、revision、mutation、操作、原因码、DB 时间和本地耗时；指标避免 RunID 高基数标签，详细身份放日志/trace。记录 rejected、unknown、timeout、blocked、failed、canceled、succeeded，不能把 fencing 拒绝统计为普通 skipped。

每次验收保存构建 SHA、Go/Pulse/DB/协议版本、OS/CPU/内存、网络拓扑和 RTT、连接池、TTL/续租/轮询参数、故障脚本与种子、原始事件、数据库最终对账与实际命令。报告分别标记 PASS / FAIL / SKIP / 未验证；混合版本和网络中断未跑不得以单元测试替代。根模块 `./...` 不覆盖独立模块，后续必须分别 build/vet/race，并用显式源码工作区验证桥接联调；不能拿旧 postgresstore 的跳过测试充当新协调模块实库通过。

## 10. 实施 PR 切分与交付顺序

以下仅是拆分建议，本轮不创建 Issue/PR、不提交推送。实施前由主任务按已有 Issue 关联并确认范围；所有发现的 Bug 按先 Issue 后 PR 处理，合并、发版和部署由维护者决定。

| 顺序 | 单一交付 | 前置/验收 |
| --- | --- | --- |
| PR 1 | 根包显式已领取执行桥接、共享校验提取、可靠错误分类；旧 API 不变 | 现有 Run/Resume/Retry/流/race 回归；测试伪 session 证明不重复 Acquire/无旁路 Save |
| PR 2 | 独立协调模块、描述/输入封装规范、注册和路由、Submit 去重、状态查询 | 固定协议/载荷限额/Codec 规范；版本不符业务启动=0；不先引入分布式宣传 |
| PR 3 | PostgreSQL 新 schema/权限/函数；Claim/Renew/Load/Save/Finish/Release/Cancel | 实库原子性、锁后时间、同 revision 旧 owner 拒写、去 fencing 变异、终态竞态；无此验证不启用 worker |
| PR 4 | Worker loop、续租隔离、停机与取消、提交不确定恢复及观测 | 真实 2/4/8 独立进程；各边界强杀/暂停/恢复、路由混跑；新旧 namespace 隔离 |
| PR 5 | 故障驱动器和单机稳定/性能测试、原始结果对账 | 本机系统矩阵、DB 重启/故障代理、2 小时稳定性；结果如实保留失败/跳过 |
| PR 6 | 经授权的跨机器测试与同页最终验收/迁移说明 | 先确认第 11 节参数与故障权限；两主机 2/4/8、网络矩阵、性能比较；只更新实测结论 |

每个 PR 都须先读本设计相应不变量，保持规模可评审；SQL、权限和相关测试同 PR 交付，不以“稍后加 fencing”运行未经保护的版本。完整系统验收之前只能称实验性能力，不改变当前 v0.1.0 已有能力说明。

## 11. 未决参数、假设与授权边界

已决定：整 workflow 分工；PostgreSQL 首后端；独立模块/schema；DB 锁后时间；每次随机 lease 身份加递增 epoch；检查点与租约同行原子条件写；旧 Store 写入口隔离；worker 停止不等于业务取消；旧 Run/Resume 保持。

实施 PR 需明确而未冻结的 API 细节：根包桥接入口名称/公开性、输入封装的精确规范和默认字节限额、支持的 PostgreSQL 最低版本、DDL/函数迁移与权限安装方式、生产 TTL/续租/连接池默认值、永久历史保留策略。它们不改变本文原子性与兼容边界；若实现需要放宽，应先重新评审。

跨机器环境已选定：本机 Windows + 用户自有 VPS。两端 worker 连接同一套隔离 PostgreSQL，分别运行 1+1、2+2、4+4 个进程。网络故障优先在专用测试代理中注入，不修改 VPS 全局网络规则或主机时间，不影响既有服务。

部署前仍需核对：VPS 的 OS/CPU/RAM、授权连接方式、可用 Go 和 PostgreSQL、独立测试库/DSN 的安全注入方式、DB 所在机器与持久配置、资源配额、允许的端口/网络路径、测试进程暂停/强杀及隔离 DB 重启权限、测试时间窗、结果保存位置。选择测试拓扑不等于授权安装、部署或故障注入。凭据不写进设计文件或测试输出。

本轮仅创建此设计文档，未连接任何服务器、未安装软件、未部署、未运行破坏性数据库测试，也未承诺跨机器验收已通过。在环境与授权未明确前，跨机器实测为阻塞项，不推测或复用用户服务器配置。
