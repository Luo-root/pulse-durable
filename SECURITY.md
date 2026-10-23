# Security Policy

This policy covers Pulse Durable, including the root runtime and the `filestore`, `sqlitestore`, and `postgresstore` backends. English and Chinese sections are equivalent.

## Supported versions

Pulse Durable currently has no formal release. Security fixes are maintained on `main`; older pinned development commits are not maintained as separate patch branches. Use the remediation revision specified in an advisory when a fix is available.

The support table will be updated when formal versions are published. The pinned revisions in tutorials make examples reproducible; they do not establish a long-term support branch.

## Reporting a vulnerability

**Report privately rather than opening a public issue or reproduction PR.** Email **3029295957@qq.com**, including:

- the affected Durable commit or version, Pulse dependency version, Go version and OS;
- the impact and conditions needed to reproduce it;
- a minimal program or exact steps, with sensitive business data removed;
- the Store/Codec involved and the interruption/recovery sequence, where relevant;
- a proposed mitigation, if any, and whether you want to be credited.

GitHub private vulnerability reporting is currently disabled for this repository; email is the private reporting channel. Never send credentials or production checkpoint records containing unredacted business data.

The maintainer will assess the report, coordinate a fix and disclosure, and respect the reporter's privacy. Reporting a suspected vulnerability does not require a working fix.

## In scope

Security defects in this repository, for example:

- unintended access to or modification of checkpoint records through the built-in file backend;
- path handling or record decoding that exposes files or data beyond the intended storage location;
- incorrect validation of run identity, business version, scopes or record ownership that mixes results between runs;
- bypassing confirmed-before-publish or turning a reliable storage/restore failure into a successful result;
- code or documentation that exposes credentials or checkpoint payloads unexpectedly.

Defects in Pulse or another dependency should also be reported to the corresponding upstream project. Include the dependency version when notifying this repository of an integration impact.

## Expected boundaries

Durable is a library running with its host process's privileges. Checkpoints contain encoded business inputs and outputs; they are not an encrypted container. Storage permissions, encryption, backups and retention are the host's responsibility.

`filestore` targets one local host and does not provide distributed leases or a power-loss durability guarantee. Unconfirmed node bodies can execute again after interruption; applications must make external effects idempotent or transactional. These documented limits are not themselves vulnerabilities; a defect that violates a stated library guarantee remains in scope.

Application-specific authorization, user-supplied business functions and custom Store/Codec implementations are maintained by their authors. Do not treat a custom Codec or a checkpoint from an untrusted source as a sandboxed execution boundary.

---

## 中文

本政策适用于 Pulse Durable 根运行时与 `filestore`、`sqlitestore`、`postgresstore` 后端。

### 支持范围

Durable 当前尚无正式 release。安全修复维护在 `main`，不为更早的固定开发提交另开补丁分支。有安全公告时，使用公告指定的修复版本或提交。

正式版本发布后会更新支持范围。教程中的固定提交用于复现示例，不代表长期支持分支。

### 私密上报

**不要开公开 Issue，也不要先提交包含漏洞复现的公开 PR。** 请发送邮件到 **3029295957@qq.com**，尽量提供：

- 受影响的 Durable 版本或提交、Pulse 依赖版本、Go 版本和操作系统；
- 问题的影响以及触发条件；
- 删除敏感业务数据后的最小程序或确切步骤；
- 相关 Store/Codec，以及中断和恢复的具体过程；
- 已想到的缓解方式，以及是否愿意被致谢。

本仓库当前未启用 GitHub 私密漏洞报告，邮箱是私密渠道。不要发送凭据或含未脱敏业务数据的生产检查点记录。

维护者会评估报告、协调修复与披露，并尊重举报人的隐私。没有现成修复方案也可以报告疑似漏洞。

### 在范围内

本仓库中的安全缺陷，例如：

- 内置文件后端导致非预期的检查点读取或修改；
- 路径处理或记录解码使文件、数据超出预期存储位置暴露；
- RunID、业务版本、作用域或记录归属校验错误，导致不同运行的成果混用；
- 绕过确认后发布，或把可靠存储/恢复失败变成成功结果；
- 代码或文档意外泄露凭据、检查点载荷。

Pulse 或其他依赖的缺陷还应上报对应上游；向本仓库说明联动影响时，请附依赖版本。

### 已说明的边界

Durable 是在宿主进程权限内工作的库。检查点包含编码后的业务输入和输出，不是加密容器；存储权限、加密、备份与保留策略由宿主管理。

`filestore` 面向本机单宿主，不提供分布式租约或断电持久性保证。中断后未确认的节点体可能重新执行，外部副作用需要业务提供幂等或事务。这些已说明的限制本身不属于漏洞；破坏库已承诺保证的缺陷仍在报告范围内。

应用鉴权、业务函数、自定义 Store/Codec 由其作者维护。自定义 Codec 或来自不可信来源的检查点，不能当作沙箱执行边界。
