# Contributing to Pulse Durable

Pulse Durable provides checkpoint storage and recovery for Pulse workflows. Documentation targets the v0.1.0 API baseline (Go 1.26+, Pulse v0.5.0). This is release preparation, not a claim of publication: check [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases) for tag status. Versioned installation commands are unavailable before the corresponding tags are published. Contributions are welcome.

English and Chinese sections are equivalent. If they disagree, the English text is the reference. Participation follows the [Code of Conduct](CODE_OF_CONDUCT.md); contributions are made under the repository's [MIT License](LICENSE).

## English

### Before you start

- Open an issue before a non-trivial change to behavior, an API, a storage format or the repository layout. Explain the use case, proposed approach, compatibility impact and acceptance criteria. Typo fixes and small patches for an existing issue may go directly to a PR.
- For a bug, include a minimal reproduction, expected and actual behavior, Go version, OS, Durable commit and Pulse dependency version. For recovery problems, include the interruption point and a sanitized record or log when useful.
- Keep PRs focused and reviewable, aiming for roughly 1000 changed lines. Split larger changes into dependent PRs with a clear merge order.
- Work on a branch and open a PR; do not push directly to `main`.
- Report vulnerabilities privately using [SECURITY.md](SECURITY.md), rather than a public issue.

### Local development

Use **Go 1.26.0 or later**. The module pins its public Pulse dependency in `go.mod`; normal validation must use that version rather than a local replacement.

Disable workspace overrides for the current shell:

```sh
# Bash
export GOWORK=off
```

```powershell
# PowerShell
$env:GOWORK = 'off'
```

From the repository root, run:

```sh
go mod download
go build ./...
go vet ./...
go test -race -count=1 -timeout=180s ./...
```

Optional `sqlitestore` and `postgresstore` are independent modules, excluded from root `./...`. First validate each module independently against its public root dependency, with `GOWORK=off`:

```powershell
foreach ($module in @('sqlitestore', 'postgresstore')) {
    Push-Location $module
    go mod download
    go build ./...
    go vet ./...
    go test -race -count=1 ./...
    Pop-Location
}
```

To validate the current root and both backend sources together, from the checkout root create a temporary workspace. If `go.work` already exists, stop and preserve it; do not overwrite it. This is source-checkout validation, not proof of public backend installation. The target installation versions are `@v0.1.0`; root `v0.1.0`, `sqlitestore/v0.1.0`, and `postgresstore/v0.1.0` are separate tags. Their publication status is determined by GitHub Releases; source validation does not publish them.

```powershell
$previousGowork = $env:GOWORK
$env:GOWORK = 'off'
go work init . ./sqlitestore ./postgresstore
$env:GOWORK = (Join-Path (Get-Location) 'go.work')
go build ./... ./sqlitestore/... ./postgresstore/...
go vet ./... ./sqlitestore/... ./postgresstore/...
go test -race -count=1 ./... ./sqlitestore/... ./postgresstore/...
$env:GOWORK = $previousGowork
```

Remove the temporary `go.work`/`go.work.sum` after validation; never commit them or add release dependency `replace` directives. PostgreSQL integration tests require a separate opt-in: set `PULSE_POSTGRES_TEST_DSN` to an isolated disposable database, then run `go test -race -count=1 ./postgresstore/...` in that workspace. Tests execute **`DROP SCHEMA IF EXISTS pulse_durable CASCADE`** and recreate it. **Never use a business database.** Without a DSN, database tests skip; that is not database acceptance. See [`postgresstore/README.md`](postgresstore/README.md).

This validates the current source tree only; it is not a release-install check. Do not claim power-loss, network-filesystem, backup/restore, migration, or distributed-coordination acceptance without a separate test.

Use the active Go toolchain's formatter. In Bash:

```sh
"$(go env GOROOT)/bin/gofmt" -l $(git ls-files '*.go')
```

In PowerShell:

```powershell
$durableGofmt = Join-Path (go env GOROOT) 'bin/gofmt.exe'
& $durableGofmt -l (git ls-files '*.go')
```

The formatter's output must be empty; `gofmt -l` can list files while returning exit code 0. Preserve UTF-8 and LF line endings.

CI runs build, vet, formatting and race tests on **Linux and Windows**, including `filestore` and real process-exit/recovery integration tests. Root and SQLite tests use local temporary data and require no API credentials. PostgreSQL database tests are separate, opt-in tests requiring an isolated test DSN. Initial module downloads need access to a Go module source or an existing cache.

### Repository layout and design boundaries

- The root package owns `Runtime`, Run/Resume, Store records and Codec contracts; it imports Pulse.
- `filestore/` implements the local persistent backend; `sqlitestore/` and `postgresstore/` are independent Go 1.26 SQLite and PostgreSQL backends; `integration/` checks real process interruption and recovery.
- `example_test.go` collects executable node, subgraph and managed-stream examples.
- `site/` holds the bilingual documentation and VitePress site; `.github/assets/` holds the brand sources.

Keep the dependency direction **Durable → Pulse**. Changes to graph execution primitives belong in Pulse and require their own review; do not duplicate the engine inside Durable.

Recovery changes must preserve confirmed-before-publish, distinguish normal skips from reliable failures, and keep run identity, version, scope and Retry budget checks intact. Do not describe durable waits, distributed ownership or exactly-once external effects as implemented capabilities.

For new behavior, add a focused regression that demonstrates the observable contract. A claim about surviving process exit needs a real persistent backend and an exit/restart scenario; an in-memory test alone is not enough. Use isolated temporary directories and do not commit checkpoint records, secrets or build outputs.

### Documentation

Update `README.md` and `README_en.md` together. Guides live in `site/guide/` and `site/en/guide/`; references live in `site/reference/` and `site/en/reference/`. Keep paired pages consistent. Current tutorials target v0.1.0, Go 1.26+ and Pulse v0.5.0; distinguish the target API baseline from published tags and keep historical measurement versions unchanged. Development pins belong only in the installation alternative.

For documentation or site changes, with Node.js 22 and npm available:

```sh
cd site
npm ci
npm run build
```

The build checks dead links. Compile complete example programs after changing their APIs or commands. Site builds and preview artifacts run on PRs; GitHub Pages publishes only from `main`.

### What a PR should contain

State the linked issue, problem, resulting behavior, new public surface, compatibility or storage-format impact, exact validation commands and review focus. Use a closing keyword only when the PR completes the issue.

Read your own diff before submitting it. Changes to persisted data, Codec behavior or recovery must explain how existing records are handled. Update both languages and the affected examples when documented behavior changes.

The maintainer decides merging and releases. Passing CI does not by itself authorize a merge or a version release.

---

## 中文

Pulse Durable 为 Pulse 工作流提供检查点存储与恢复。文档以 v0.1.0 为目标 API 基线（Go 1.26+、Pulse v0.5.0）。这是发版准备，不代表已发布；tag 状态以 [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases) 为准，相应 tag 发布前版本安装命令尚不可用。参与项目须遵守[行为准则](CODE_OF_CONDUCT.md)，贡献采用仓库的 [MIT 许可](LICENSE)。

### 开工之前

- 改变行为、公开 API、存储格式或仓库布局的非小改动，先开 Issue，写明使用场景、方案、兼容性影响与验收标准。错字修正和已有 Issue 的小补丁可以直接提 PR。
- 报告 bug 时提供最小复现、预期与实际结果、Go 版本、操作系统、Durable 提交和 Pulse 依赖版本。恢复问题请说明在哪一步中断，必要时附脱敏记录或日志。
- PR 保持单一主题，有效改动按约 1000 行控制；更大的改动拆成前后依赖的 PR，并写清合并顺序。
- 在分支上开发并提交 PR，不直接推送 `main`。
- 安全漏洞按[安全政策](SECURITY.md)私密上报，不开公开 Issue。

### 本地开发

需要 **Go 1.26.0 或更新版本**。`go.mod` 固定公开的 Pulse 依赖；正常验证使用该版本，不依赖本地替换。

先关闭当前 shell 的工作区覆盖：

```sh
# Bash
export GOWORK=off
```

```powershell
# PowerShell
$env:GOWORK = 'off'
```

在仓库根目录执行：

```sh
go mod download
go build ./...
go vet ./...
go test -race -count=1 -timeout=180s ./...
```

可选的 `sqlitestore` 和 `postgresstore` 是独立模块，根模块 `./...` 不包含它们。先保持 `GOWORK=off`，分别核验公开根依赖：

```powershell
foreach ($module in @('sqlitestore', 'postgresstore')) {
    Push-Location $module
    go mod download
    go build ./...
    go vet ./...
    go test -race -count=1 ./...
    Pop-Location
}
```

若要一起验证当前根模块与两个后端源码，在 checkout 根目录创建临时工作区。已有 `go.work` 时停止并保留，不覆盖。此步骤验证源码 checkout，不证明公开后端可安装。目标安装版本为 `@v0.1.0`；根 `v0.1.0`、`sqlitestore/v0.1.0`、`postgresstore/v0.1.0` 是独立 tag。发布状态以 GitHub Releases 为准；源码验证不会发布它们。

```powershell
$previousGowork = $env:GOWORK
$env:GOWORK = 'off'
go work init . ./sqlitestore ./postgresstore
$env:GOWORK = (Join-Path (Get-Location) 'go.work')
go build ./... ./sqlitestore/... ./postgresstore/...
go vet ./... ./sqlitestore/... ./postgresstore/...
go test -race -count=1 ./... ./sqlitestore/... ./postgresstore/...
$env:GOWORK = $previousGowork
```

验证后移除临时 `go.work`/`go.work.sum`，不提交这些文件，也不加入发布依赖 `replace`。PostgreSQL 集成测试需另外显式启用：设置 `PULSE_POSTGRES_TEST_DSN` 指向隔离、可丢弃的测试数据库，再在该工作区执行 `go test -race -count=1 ./postgresstore/...`。测试执行 **`DROP SCHEMA IF EXISTS pulse_durable CASCADE`** 并重建，**绝不能使用业务库**。未设置 DSN 时数据库测试跳过，不代表数据库验收通过。详见 [`postgresstore/README.md`](postgresstore/README.md)。

这只验证当前源码树，不是发布安装检查。没有单独实测时，不要宣称已验收断电、网络文件系统、备份/恢复、迁移或分布式协调。

格式检查使用当前 Go 工具链。Bash：

```sh
"$(go env GOROOT)/bin/gofmt" -l $(git ls-files '*.go')
```

PowerShell：

```powershell
$durableGofmt = Join-Path (go env GOROOT) 'bin/gofmt.exe'
& $durableGofmt -l (git ls-files '*.go')
```

判据是**输出为空**；`gofmt -l` 即使列出了文件也可能返回 0。文件保持 UTF-8 与 LF。

CI 在 **Linux 和 Windows** 上执行 build、vet、格式与 race 测试，包含 `filestore` 和真实进程退出/恢复集成测试。根模块与 SQLite 测试只使用本地临时数据，不需要 API 凭据；PostgreSQL 真实数据库测试独立、显式启用，需要隔离测试 DSN；首次下载模块需要能访问模块源或已有缓存。

### 仓库结构与设计边界

- 根包维护 `Runtime`、Run/Resume、Store 记录与 Codec 契约，依赖 Pulse。
- `filestore/` 是本地持久化后端；`sqlitestore/` 和 `postgresstore/` 是独立 Go 1.26 SQLite 与 PostgreSQL 后端；`integration/` 验证真实进程中断与恢复。
- `example_test.go` 集中提供节点、子图和受管流的可执行示例。
- `site/` 保存双语文档与 VitePress 站点；`.github/assets/` 保存品牌源文件。

依赖方向保持 **Durable → Pulse**。图执行原语应在 Pulse 中单独评审，不能在 Durable 中复制一套引擎。

恢复相关修改须保留确认后发布，区分正常跳过与可靠失败，并维护运行身份、版本、作用域和 Retry 预算校验。持久等待、分布式运行所有权与外部副作用 exactly-once 不能写成已有能力。

新行为应有能观察到契约的针对性回归。声称可跨进程恢复时，必须使用真实持久化后端和退出/重启场景；仅用内存存储不足以证明。测试使用隔离临时目录，不提交检查点记录、凭据或构建产物。

### 文档

`README.md` 与 `README_en.md` 同步维护。指南位于 `site/guide/` 与 `site/en/guide/`，参考位于 `site/reference/` 与 `site/en/reference/`。双语事实和示例对应。现行教程以 v0.1.0、Go 1.26+、Pulse v0.5.0 为目标，区分目标 API 基线与已发布 tag，历史测量版本保持不动。开发 pin 只放安装页备选。

修改文档或站点时，准备 Node.js 22 与 npm，执行：

```sh
cd site
npm ci
npm run build
```

构建会检查死链。修改示例 API 或运行命令后，编译完整程序。PR 执行站点构建并上传预览产物；GitHub Pages 只从 `main` 发布。

### PR 必须说明什么

写清关联 Issue、问题与修改后的行为、新增公开接口、兼容性或存储格式影响、实际执行的验证命令，以及评审关注点。只有真正完成 Issue 时才使用自动关闭关键字。

提交前先自审 diff。涉及持久化数据、Codec 或恢复的修改须说明如何处理已有记录；行为文档变化时同步两种语言与对应示例。

合并与发版由维护者决定；CI 通过本身不等于合并或发版授权。
