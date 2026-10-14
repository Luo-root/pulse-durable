# Contributing to Pulse Durable

Pulse Durable provides checkpoint storage and recovery for Pulse workflows. It is currently a development project with no formal release. Contributions are welcome.

English and Chinese sections are equivalent. If they disagree, the English text is the reference. Participation follows the [Code of Conduct](CODE_OF_CONDUCT.md); contributions are made under the repository's [MIT License](LICENSE).

## English

### Before you start

- Open an issue before a non-trivial change to behavior, an API, a storage format or the repository layout. Explain the use case, proposed approach, compatibility impact and acceptance criteria. Typo fixes and small patches for an existing issue may go directly to a PR.
- For a bug, include a minimal reproduction, expected and actual behavior, Go version, OS, Durable commit and Pulse dependency version. For recovery problems, include the interruption point and a sanitized record or log when useful.
- Keep PRs focused and reviewable, aiming for roughly 1000 changed lines. Split larger changes into dependent PRs with a clear merge order.
- Work on a branch and open a PR; do not push directly to `main`.
- Report vulnerabilities privately using [SECURITY.md](SECURITY.md), rather than a public issue.

### Local development

Use **Go 1.25.0 or later**. The module pins its public Pulse dependency in `go.mod`; normal validation must use that version rather than a local replacement.

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

CI runs build, vet, formatting and race tests on **Linux and Windows**, including `filestore` and real process-exit/recovery integration tests. Tests use local temporary data and require no API credentials. Initial module downloads need access to a Go module source or an existing cache.

### Repository layout and design boundaries

- The root package owns `Runtime`, Run/Resume, Store records and Codec contracts; it imports Pulse.
- `filestore/` implements the local persistent backend; `integration/` checks real process interruption and recovery.
- `example_test.go` and `subgraph_example_test.go` provide executable examples.
- `site/` holds the bilingual documentation and VitePress site; `.github/assets/` holds the brand sources.

Keep the dependency direction **Durable → Pulse**. Changes to graph execution primitives belong in Pulse and require their own review; do not duplicate the engine inside Durable.

Recovery changes must preserve confirmed-before-publish, distinguish normal skips from reliable failures, and keep run identity, version, scope and Retry budget checks intact. Do not describe durable waits, distributed ownership or exactly-once external effects as implemented capabilities.

For new behavior, add a focused regression that demonstrates the observable contract. A claim about surviving process exit needs a real persistent backend and an exit/restart scenario; an in-memory test alone is not enough. Use isolated temporary directories and do not commit checkpoint records, secrets or build outputs.

### Documentation

Update `README.md` and `README_en.md` together. Guides live in `site/guide/` and `site/en/guide/`; references live in `site/reference/` and `site/en/reference/`. Keep paired pages consistent, and distinguish public pinned development revisions from formal releases.

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

Pulse Durable 为 Pulse 工作流提供检查点存储与恢复，目前是开发阶段项目，尚无正式 release。参与项目须遵守[行为准则](CODE_OF_CONDUCT.md)，贡献采用仓库的 [MIT 许可](LICENSE)。

### 开工之前

- 改变行为、公开 API、存储格式或仓库布局的非小改动，先开 Issue，写明使用场景、方案、兼容性影响与验收标准。错字修正和已有 Issue 的小补丁可以直接提 PR。
- 报告 bug 时提供最小复现、预期与实际结果、Go 版本、操作系统、Durable 提交和 Pulse 依赖版本。恢复问题请说明在哪一步中断，必要时附脱敏记录或日志。
- PR 保持单一主题，有效改动按约 1000 行控制；更大的改动拆成前后依赖的 PR，并写清合并顺序。
- 在分支上开发并提交 PR，不直接推送 `main`。
- 安全漏洞按[安全政策](SECURITY.md)私密上报，不开公开 Issue。

### 本地开发

需要 **Go 1.25.0 或更新版本**。`go.mod` 固定公开的 Pulse 依赖；正常验证使用该版本，不依赖本地替换。

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

CI 在 **Linux 和 Windows** 上执行 build、vet、格式与 race 测试，包含 `filestore` 和真实进程退出/恢复集成测试。测试只使用本地临时数据，不需要 API 凭据；首次下载模块需要能访问模块源或已有缓存。

### 仓库结构与设计边界

- 根包维护 `Runtime`、Run/Resume、Store 记录与 Codec 契约，依赖 Pulse。
- `filestore/` 是本地持久化后端；`integration/` 验证真实进程中断与恢复。
- `example_test.go` 与 `subgraph_example_test.go` 提供可执行示例。
- `site/` 保存双语文档与 VitePress 站点；`.github/assets/` 保存品牌源文件。

依赖方向保持 **Durable → Pulse**。图执行原语应在 Pulse 中单独评审，不能在 Durable 中复制一套引擎。

恢复相关修改须保留确认后发布，区分正常跳过与可靠失败，并维护运行身份、版本、作用域和 Retry 预算校验。持久等待、分布式运行所有权与外部副作用 exactly-once 不能写成已有能力。

新行为应有能观察到契约的针对性回归。声称可跨进程恢复时，必须使用真实持久化后端和退出/重启场景；仅用内存存储不足以证明。测试使用隔离临时目录，不提交检查点记录、凭据或构建产物。

### 文档

`README.md` 与 `README_en.md` 同步维护。指南位于 `site/guide/` 与 `site/en/guide/`，参考位于 `site/reference/` 与 `site/en/reference/`。双语事实和示例对应，并区分固定的公开开发提交与正式 release。

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
