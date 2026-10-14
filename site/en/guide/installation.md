# Installation

Use this page to add Pulse Durable to a Go program for the first time. You will create a module with a pinned dependency, ready to run the complete program in [Quick start](quickstart.md).

## 1. Check prerequisites

Requires Go 1.25 or later. Run:

```sh
go version
```

Expect `go1.25.0` or later. You also need access to a Go module proxy or GitHub. If downloads fail, check connectivity and your current `GOPROXY` setting first.

## 2. Create a module and install

Run these commands where you want to keep the example. You may choose a different directory name:

```sh
mkdir durable-quickstart
cd durable-quickstart
go mod init example.com/durable-quickstart
go get github.com/Luo-root/pulse-durable@d9415114b739
```

Durable has no formal release yet. This command pins a public development revision. Go resolves it to a pseudo-version and writes that version to `go.mod`. It is not a released `v0.1`.

The revision pins Pulse:

```text
v0.4.2-0.20261014103213-1f67045d6c42
```

You normally do not need to install Pulse separately. Go module version selection can retain a higher Pulse dependency already present in your project. For an existing application, inspect the resolved version and validate that combination.

## 3. Inspect dependencies

Run:

```sh
go list -m github.com/Luo-root/pulse-durable github.com/Luo-root/pulse
```

In the new module created above, expect a development pseudo-version for Durable and the Pulse version listed above. Do not run `go mod tidy` immediately after installation: without source imports, it removes unused dependencies.

If your machine uses `go.work` and you want to check this module independently, temporarily disable the workspace. In PowerShell:

```powershell
$env:GOWORK = 'off'
go list -m github.com/Luo-root/pulse-durable github.com/Luo-root/pulse
```

In a POSIX shell:

```sh
GOWORK=off go list -m github.com/Luo-root/pulse-durable github.com/Luo-root/pulse
```

## 4. Choose imports

Application code uses:

```go
import (
	"github.com/Luo-root/pulse"
	durable "github.com/Luo-root/pulse-durable"
)
```

This is an import fragment, not a complete program. For local persistent storage, also import `github.com/Luo-root/pulse-durable/filestore`. Use `durable.NewMemoryStore()` for examples and tests in one process, and `filestore.New(dir)` for recovery after local process exit.

## Common problems

| Symptom | Action |
| --- | --- |
| Go is too old | Use Go 1.25 or later, then install again |
| Module download fails | Check connectivity, proxy settings, and the module address in the error, then retry the download |
| `go list` reports a different Pulse version | Check the existing `go.mod` and workspace; a higher existing version participates in Go module version selection |
| Dependencies are missing after adding code | Repeat the pinned `go get` in the module directory, then run `go mod tidy` |
| Local `replace` makes results hard to reproduce | Validate public revisions in an independent module before choosing development workspace references |

## Next steps

Continue with [Quick start](quickstart.md): add a complete `main.go`, run it, and read a declared outlet. If you already completed the minimal run, continue with [Recovery after process exit](recovery.md). Before upgrading code or codecs, check the compatibility rules in the [Execution contract](../reference/execution.md).
