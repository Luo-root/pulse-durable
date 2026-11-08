# Installation

This page explains the relationship between the **current untagged development source** and public installation versions. It requires **Go 1.26+**; the current root module depends on **Pulse v0.5.2**. The storage contract uses domain records without format numbers, wire `2`, snapshot envelope `2`, and Task top-level protocol `2` / payload_format `1`. Root development commit `v0.1.1-0.20261108162459-ccc90d4b53c3` has been resolved by `go list -m` and is downloadable, at full Hash `ccc90d4b53c318696328cf0fbed7f876e996b7a8`. It is a public commit pseudo-version, not a release tag. Submodule go.mod files pin that root dependency. SQLite/standalone PostgreSQL resolve to `v0.1.1-0.20261108164227-4babad07420f`; distributed resolves to `v0.0.0-20261108164227-4babad07420f`, at full Hash `4babad07420f7114729455eaf7fcff9ee5818002`. These are also development commits rather than tags; earlier source tests do not establish validation of the complete downloaded packages. Check [GitHub Releases](https://github.com/Luo-root/pulse-durable/releases) for tags. Historical performance pages retain their measured versions.

## 1. Check prerequisites

```sh
go version
```

Expect `go1.26.0` or later. Downloads need a Go module proxy or GitHub; check connectivity and `GOPROXY` first if they fail.

## 2. Install the root development commit and validate submodule source

The root module can use the resolved public development pin:

```sh
mkdir durable-quickstart
cd durable-quickstart
go mod init example.com/durable-quickstart
go get github.com/Luo-root/pulse-durable@v0.1.1-0.20261108162459-ccc90d4b53c3
```

Install optional modules as needed using complete development pins resolved by `go list -m`:

```sh
go get github.com/Luo-root/pulse-durable/sqlitestore@v0.1.1-0.20261108164227-4babad07420f
go get github.com/Luo-root/pulse-durable/postgresstore@v0.1.1-0.20261108164227-4babad07420f
go get github.com/Luo-root/pulse-durable/distributed@v0.0.0-20261108164227-4babad07420f
```

A fresh consumer with `GOWORK=off` and no replace downloaded the modules, checked all four versions, assembled Store/Coordinator/worker interfaces, checked snapshot2, and passed `go test -race .` (4.858s). This validates independent consumption of public development pins, not a formal tag release or all backend real-database tests on the consumer package. See [Contributing](https://github.com/Luo-root/pulse-durable/blob/main/CONTRIBUTING.md) for source validation.

Root and submodules publish separately; a root pin does not automatically publish submodules. See [SQLite](sqlite.md), [PostgreSQL](postgresql.md), and the [distributed entry point](https://github.com/Luo-root/pulse-durable/blob/main/distributed/README.md) for current usage.

### Historical development pins

Older development pins do not provide the current contract and are not for current examples. Historical performance pages retain their measured versions; use the downloadable root development commit above for this contract.

## 3. Inspect dependencies

```sh
go list -m github.com/Luo-root/pulse-durable github.com/Luo-root/pulse
```

Root development installation should resolve `v0.1.1-0.20261108162459-ccc90d4b53c3` and Pulse `v0.5.2`; a source workspace may point root to its local checkout. Submodule go.mod files pin this root version; complete submodules resolve to the development versions above. Existing projects may retain higher dependencies through Go module version selection: inspect and validate the actual combination. Do not immediately run `go mod tidy` without source imports, as it removes unused dependencies.

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
