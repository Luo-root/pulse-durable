# Installation

This page and current tutorials target the **Durable v0.1.0** API, require **Go 1.26+**, and depend on **Pulse v0.5.0**. This is release preparation, not a claim of publication: check [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases) for tag status. The v0.1.0 commands below are unavailable until the corresponding tags are published. Historical performance pages retain their measured versions.

## 1. Check prerequisites

```sh
go version
```

Expect `go1.26.0` or later. Downloads need a Go module proxy or GitHub; check connectivity and `GOPROXY` first if they fail.

## 2. Create a module and install

```sh
mkdir durable-quickstart
cd durable-quickstart
go mod init example.com/durable-quickstart
go get github.com/Luo-root/pulse-durable@v0.1.0
```

Install optional backends only as needed. The root module does not pull in database drivers when you do not use them:

```sh
go get github.com/Luo-root/pulse-durable/sqlitestore@v0.1.0
go get github.com/Luo-root/pulse-durable/postgresstore@v0.1.0
```

The repository contains three independent modules, requiring three separate release tags:

| Module | Repository tag | go get version |
| --- | --- | --- |
| Root | `v0.1.0` | `@v0.1.0` |
| SQLite | `sqlitestore/v0.1.0` | `@v0.1.0` |
| PostgreSQL | `postgresstore/v0.1.0` | `@v0.1.0` |

The root tag does not publish submodules automatically. This describes naming only; it does not create or publish tags. See [SQLite](sqlite.md) and [PostgreSQL](postgresql.md) for complete usage.

### Pre-release development pin alternative

If target tags are not yet published, these public development pins were previously verified to resolve. They are older development snapshots, not v0.1.0 release-install acceptance, and may lack fixes in the current source:

```sh
go get github.com/Luo-root/pulse-durable@v0.0.0-20261023040750-1b397e91c878
go get github.com/Luo-root/pulse-durable/sqlitestore@v0.0.0-20261023051404-bc57abd1d7c3
go get github.com/Luo-root/pulse-durable/postgresstore@v0.0.0-20261023051404-bc57abd1d7c3
```

Validate the current checkout with the temporary `go.work` in [Contributing](https://github.com/Luo-root/pulse-durable/blob/main/CONTRIBUTING.md), without local `replace` directives in release dependencies.

## 3. Inspect dependencies

```sh
go list -m github.com/Luo-root/pulse-durable github.com/Luo-root/pulse
```

A new module using target release installation should resolve Durable `v0.1.0` and Pulse `v0.5.0`; development pins resolve to their pseudo-versions. Existing projects may retain higher dependencies through Go module version selection: inspect and validate the actual combination. Do not immediately run `go mod tidy` without source imports, as it removes unused dependencies.

Temporarily disable workspaces for an independent check. PowerShell:

```powershell
$env:GOWORK = 'off'
go list -m github.com/Luo-root/pulse-durable github.com/Luo-root/pulse
```

POSIX shell:

```sh
GOWORK=off go list -m github.com/Luo-root/pulse-durable github.com/Luo-root/pulse
```

Restore previous environment settings afterward.

## 4. Choose imports

This is an import fragment, not a complete program:

```go
import (
    "github.com/Luo-root/pulse"
    durable "github.com/Luo-root/pulse-durable"
)
```

Use `durable.NewMemoryStore()` for in-process examples, `filestore` or [SQLite](sqlite.md) for local persistence, and [PostgreSQL](postgresql.md) for database storage. Add complete source before running `go mod tidy`.

## Common problems

| Symptom | Action |
| --- | --- |
| `unknown revision v0.1.0` | Check Releases and the module-specific tag; before publication use development pins or source validation, not an API-error diagnosis |
| Go is too old | Use Go 1.26 or later |
| Module download fails | Check connectivity, proxy, and module address |
| `go list` reports another version | Check `go.mod`, workspaces, and higher existing dependencies |
| Local `replace` interferes | Check public versions in an independent module |

## Next steps

Continue with [Quick start](quickstart.md) or [Recovery after process exit](recovery.md). Before changing business definitions or codecs, check the [Execution contract](../reference/execution.md).
