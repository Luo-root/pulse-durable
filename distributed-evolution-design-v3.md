# Durable 存储演进 v3：全后端统一增量契约

日期：2026-10-28。状态：破坏性接口修订，待复审；不是实现或性能验收。
源码基线：3139e3a024d849a347d3ad06fa6b91abc934ed7a。
承接 distributed-evolution-design-v2.md 的回放、历史压缩、pin、retention 及业务不变量；本稿仅替换其旧 Save 兼容、后端范围与升级章节。v2 原文件保留在 distributed-contracts-20261024 工作树。本稿与 v2 冲突时以本稿为准，不保留两套写入政策。

## 1. 已确认的范围和边界

用户决定：所有官方存储都必须解决正常 checkpoint 全量写问题；当前早期版本允许破坏性修改，不要求兼容旧 Session.Save、旧 schema 或旧存储文件。
官方实现全集：MemoryStore、filestore、sqlitestore、postgresstore、distributed/postgres。MemoryStore 是语义参考，不提供持久性；独立 postgresstore 是会话锁语义，不提供分布式租约；只有 distributed/postgres 提供 token/epoch/lease fencing，不能互相冒称。
不再维护 RequireDelta 选择、DeltaSession 可选探测或自动全量回退。普通业务提交只用增量；初始化及显式快照合成可以写完整快照，但不能每次 Apply 后都重写完整快照。
现有 ChangeBatch wire 1 / logical format2 的不变量继续有效；logical format3 仅在加入历史摘要/回放证据时切换，不能把物理日志格式与逻辑记录格式混为一谈。
拒绝旧存储不等于获准删除旧数据：检测到旧格式必须返回 UnsupportedFormat/明确升级错误，不能自动清库、覆盖旧目录或默默重新初始化。本轮不交付旧数据转换器；需保留历史的用户先继续用旧版和隔离旧存储，新版采用新目录/新库。备份恢复的源数据也不能自动执行。

## 2. 接口与唯一业务状态转换

目标根接口（Go 具体命名实施时保持以下语义）：

```go
type Store interface { Acquire(context.Context, string) (Session, error) }
type Session interface {
    Load(context.Context) (RunRecord, error)
    Initialize(context.Context, RunRecord) error
    Apply(context.Context, ChangeBatch) (CommitAck, error)
    Close() error
}
```

- Initialize 的不存在条件指“检查点不存在”，不是调度运行身份不存在。本地会话要求该RunID无检查点/manifest/墓碑；分布式InitializeOwned要求Submit创建的task/run已存在、status=running、token有效、checkpoint revision=0且无manifest，在同一事务核对task identity并发布revision1。记录必须Revision=1、当前逻辑格式、RunID/版本/定义/输入与会话task匹配，且是业务启动前的初始空执行状态。已有检查点、已种子的CreateDerived运行或墓碑均Conflict，不覆盖。派生初始快照由专门CreateDerived入口原子发布来源信息，不通过Initialize导入。
- Initialize 响应丢失也可能已提交，Runtime 停止并从 Load 裁决，不盲重试或以“已存在”推断本请求已成功。Initialize 不赋执行权；分布式 InitializeOwned 必须有有效token。
- Load 只返回通过摘要、连续 revision、identity、typed precondition校验的完整隔离投影；无记录与损坏/旧格式必须可区分。不为查询重新写快照或推进业务revision。
- Apply 唯一普通写入口，batch expected/next revision 严格+1，身份与业务版本固定，uint64耗尽拒绝。批内State、ACK、输出、确认等全部原子。原子失败/未知提交、panic封闭、确认后发布继续沿用当前Runtime行为。
- Close 幂等，关闭后拒绝新操作，等待在途操作退出；不隐式Release、不修改新epoch执行权。
- 删除旧 Save、DeltaSession 与 RunOptions.RequireDelta，普通/claimed执行同走强制Apply。第三方后端需实现新版Session，不能运行中探测失败再回退。
- 内部recordTransaction、ChangeBatch、wire、ApplyTo继续为单源。各后端不重写ACK、Retry、子图或消息业务规则。后端管理物理提交和独占/CAS；graph/codec校验归Runtime。
- Root runtime 的 initial save 改Initialize；其他hooks全走Apply。节点最终完整校验暂允许物化只读proposal；不得把该物化结果写成完整checkpoint，后续按测量优化校验本身。
- 维护合成是单独显式能力，不放回普通全量Save接口；合成不增加业务revision、不得覆盖并发后缀。

分布式Coordinator同步去掉SaveOwned，改为InitializeOwned(Token,Mutation,RunRecord)，保留LoadOwned并强制ApplyOwned；worker只暴露一个claimed Session，不再有legacy/delta两个wrapper。保留租约、epoch、LeaseID、boot、database clock、CAS、receipt未知核对规则。

## 3. 物理表示与读写成本

所有耐久后端采用一个基础快照、连续不可变批与head；每个持久batch信封存canonical wire、内容SHA256、parent revision/chain和next chain，head原子指向已提交批。物理链由Session/Coordinator独占管理和注入，不属于Runtime的ChangeHeader；明确替代v2中Runtime批必带ParentChainDigest的要求。Load仍返回RunRecord，但Session必须同时权威读取并保存在内部的HeadRevision、HeadChain和表示代次；Initialize成功也初始化同一内部锚点。Apply要求batch.ExpectedRevision同时匹配会话锚点与数据库head，提交时CAS匹配revision和head chain；成功后同时更新Session锚点和返回业务CommitAck，未知时封闭该会话、不凭缓存继续写。新会话/epoch必须重新读取链锚点，不能从业务投影重新推导，也不能仅因revision相同信任旧缓存。Runtime不维护第二份chain，CommitAck仍精确匹配业务ChangeHeader。

链算法放根包（根包不得反向依赖recordio）：摘要均用原始32字节，revision用8字节无符号big-endian。设S=SHA256(基础快照实际持久bytes)，W=SHA256(canonical batch wire)。初始链C1=SHA256(ASCII `pulse-durable/chain/seed/v1` + 单个0字节 + S + BE64(1))；后续Cr=SHA256(ASCII `pulse-durable/chain/batch/v1` + 单个0字节 + C(r-1) + W + BE64(r))。拼接各段长度固定或域前缀固定，无JSON/base64字符串参与链运算。实现必须提供固定输入的独立测试向量及跨后端同值测试，未运行前不宣称验证通过。

物理合成到revision R时：新快照内容摘要Snew独立保存，chain anchor必须仍是已提交C(R)，不得以Snew生成新业务链。head的业务revision/chain不变，仅表示代次推进；Apply不得因合成将链重置。基础快照与manifest分别保存snapshot_digest和chain_anchor；Load验证两者后重放后缀。源RunID/版本/定义/输入身份来自快照及batch头；摘要只证明完整性不授予权限。

- MemoryStore：独占session、record私有投影，初始化复制一次、Apply按变更对象更新；Load隔离复制。实现全部CAS和失败语义，但明确无进程退出持久性。
- filestore：每运行独占OS锁；Initialize原子发布快照及首manifest；Apply写不可变批文件、Sync、再原子替换小head。批已写但head未发布属于孤儿，不可作为已提交成果认回；head发布后错误按未知提交。head/批校验不依赖mtime或文件名排序。批/manifest同目录原子替换及Windows/Linux进程强杀分别验收，继续不承诺未经测量的断电安全。旧JSON文件检测并拒绝，新布局不得误用旧文件作为新运行。
- SQLite：一事务插batch并CAS推进head，初始化单独首快照。会话OS锁保持现有跨进程独占；使用事务一致视图Load，不能分两次不一致查询。旧表版本明确拒绝而非修改旧数据。
- 独立Postgres：会话级advisory锁保持现有独占；一事务插batch/CAS head。Load一致快照，故障释放连接/锁与Close遵守原约束；不宣称worker fencing。
- 分布式Postgres：同一事务持权威run锁，最终提交前同时重验token/租约/clock/revision，插batch/receipt/guard/event并更新head；worker无表级DML权限，不以一次Go端检查代替数据库提交条件。

读压力不能被转移放大：session首次Load重放链建立私有投影；正常Apply只检查当前head及受影响对象，不在每次提交时读取/重放整条历史，也不每次扫描完整快照计算摘要。后端如不能在本片实现局部权威索引，应明确停止放行而非声称性能问题已解决。
会话缓存仅在独占或有效owner下使用；新会话/新epoch必须权威加载。数据库CAS保证投影绑定同一head，失去所有权后缓存不能授权提交。
显式快照合成缩短重放链；快照合成、语义历史压缩、retention仍是不同能力，沿用v2契约。活动对象总大小/批大小/未合成链需有界并可观察，超过预算明确停止，不丢消息/不拆原子批。不以无限增长换取提交快的基准。

## 4. 跨后端验收矩阵

| 面 | 全部官方后端 | 耐久后端额外 | 分布式PG额外 |
|---|---|---|---|
| 初始化 | 空库成功、重复/错identity/错误revision拒绝 | 提交前后退出/丢响应 | 旧token和过期租约拒绝 |
| 增量 | 同一业务投影、精确preconditions、错header/溢出/批故障零发布 | batch/head原子性，故障后只见完整旧或新版 | epoch接管、提交最终租约复核、mutation/receipt |
| 流/消息 | 500条真实Runtime、State删除、ACK/多路输出、Retry/Sub恢复 | 独立进程重启读取、不重复已确认消费 | 两worker强杀接管，旧owner迟到写拒绝 |
| 完整性 | 错摘要、缺批、乱序、重复不同内容failclosed | 损坏文件/SQL行、并发读合成 | ACL/签名/跨namespace、fencing变异 |
| 生命周期 | Load副本隔离、Close/inflight/取消/未知封闭 | OS锁/连接退出释放 | 不确定操作不Finish/Release |
| 破坏性升级 | 旧格式拒绝且不写不删 | 新目录/新库与旧数据并存 | 旧client不能写新版schema |
| 成本 | 同图10/100/500轮、每提交bytes/allocs/Load恢复 | SQLite/PG实际数据库写量、文件实际写量 | WAL/锁等待/网络提交bytes |

旧/新算法对照可保留在测试基准或历史commit，不能保留产品双写路线。测试oracle不能只比较同一apply算法的两份结果，还要断言具体业务输出、持久提交边界及强杀后的调用次数。性能提升依实测按后端分别报告，不承诺所有图都显著加速，不混称Pulse Core自身吞吐提升。

## 5. 执行与覆盖账本

- C1 根契约/Runtime/Memory及测试适配：必须先实现强制Initialize+Apply、去掉可选模式、未知提交不变。不能合并一个使主线全部后端编译失败的半成品；跨文件接口更新可用同一集成PR，组件在隔离分支验证。
- C2 文件/SQLite/独立PG/分布式PG：各自独占目录并行实现，统一共享C1契约和batch链算法。子模块固定真实可下载根版本，无release replace。接口变更跨模块发布顺序由维护者最后统一；不伪造已发布tag。
- C3 各后端真实测试矩阵、深审、CI、双语文档与成本报告；所有官方后端缺一项就不能宣称“统一增量存储完成”。
- C4 v2历史压缩、pin/retention及回放继续属于本次总体目标，不因本接口修订推迟或删减。

职责、输入输出、排除与交接见§2/§3；执行授权分别为本地session独占和分布式R-OWN；生命周期与失败见§2/§3及v2公共规则；数值预算沿用v2§7，若实测要求修改需注明依据；验收见§4。原设计D01–D06账本继续跟踪，旧兼容门槛D06由“旧格式拒绝且不破坏数据”替代。
尚未实现、未实测的项目均不能标PASS。用户许可破坏性API，不许可删除已有库/目录、代发版、自动部署或泄露凭据。
