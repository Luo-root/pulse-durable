# 存储

本页适用于选择检查点目录、检查已有运行，以及接入自定义存储后端。先完成[恢复教程](recovery.md)，可以用本页的程序读取教程生成的记录。

## 1. 选择存储方式

| 存储 | 适用场景 | 保证范围 |
| --- | --- | --- |
| `durable.NewMemoryStore()` | 示例、单元测试、进程内协议验证 | 进程内独占会话与 CAS；不保留进程退出后的记录 |
| `filestore.New(dir)` | 本机工作流的退出与重启恢复 | 同一 RunID 的本机跨进程文件锁、文件记录与原子替换 |
| 自定义 `durable.Store` | 由宿主明确存储及协调要求的环境 | 必须实现 Store/Session 契约，另行说明持久性和所有权保证 |

`filestore` 依赖文件系统的独占锁与同目录原子替换，不提供分布式租约、fencing 或跨机器派发，也不承诺断电时的持久性。不能仅凭把目录放到网络共享上，就将其当作分布式运行管理。

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

目录不能为空。`New` 会按需创建目录，并在创建 Store 时将相对路径转换为绝对路径。新进程创建新的 Store 时，仍须让路径指向原检查点目录。

支持 Unix 权限位的平台，新目录使用 `0700`，新文件使用 `0600`；已有目录权限不会被修改。Windows 使用目录 ACL。宿主负责目录、备份及数据保留策略，检查点不是加密容器。

每个 RunID 按其原文的 SHA-256 定位文件，RunID 不直接拼成路径。记录使用 `.json`，所有权使用 `.lock`。锁文件保留是正常情况；独占权来自操作系统锁，关闭会话或退出进程后释放。删除锁文件可能产生两个不同的独占域，不应作为解除 `ErrBusy` 的办法。

保存会先将完整新记录写入同目录暂存文件，再同步并替换 `.json`。若进程在替换前退出，下次 `Acquire` 取得该 RunID 的锁后会删除遗留暂存文件，始终以原 `.json` 为权威，不会认回未提交的新记录。清理仅匹配本运行的 `<SHA-256>-<uint32十进制>.tmp` 普通文件（随机数无前导零，`0` 除外），不递归、不处理其他运行、近似名称或 `.lock`；读取目录或删除失败会返回错误并释放锁。暂存文件仍可能保留到该运行下次成功取得锁，目录权限和保留策略由宿主管理。

## 3. 读取已有运行记录

先按[恢复教程](recovery.md)运行两个进程，保留 `durable-recovery/checkpoints`。然后在它的同级目录创建检查工具模块：

```sh
mkdir durable-storage
cd durable-storage
go mod init example.com/durable-storage
go get github.com/Luo-root/pulse-durable@f46adb637745d864649685cf481675d5dc0ca904
```

需要 Go 1.25 或更高版本。保存以下完整程序为 `main.go`：

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

预期输出中的运行身份是 `recovery-demo-001`，格式为 `2`，修订号是正整数。新建普通与流式运行都使用格式 `2`；普通格式 `1` 的已有记录仍可读取，并在下一次保存时升级为 `2`。对于恢复教程的简单图，完成后包含一个根作用域与两个节点根尝试。检查工具只打印元数据，不打印编码业务值，也不调用 `Save`。

读取也需要独占会话。如果业务进程仍持有同一 RunID，会立即返回 `ErrBusy`。`filestore.New` 会创建不存在的目录，所以路径拼错时可能得到空目录和 `ErrNotFound`；请核对路径，不能将记录不存在误判为业务已经完成。

## 4. 接入自定义 Store

通常业务只使用 Runtime；实现后端时，需要满足以下步骤：

1. `Acquire(ctx, runID)` 获得该 RunID 的独占会话。同一 RunID 正被持有时立即返回 `ErrBusy`，不同 RunID 可以独立使用。
2. `Load(ctx)` 返回隔离副本，没有记录时返回 `ErrNotFound`。权限、损坏或读取故障不能伪装成不存在。
3. `Save(ctx, record, expectedRevision)` 原子比较并交换记录。首次 expected 为 `0`、新 Revision 为 `1`；后续 Revision 必须为 expected 加一，且记录 RunID 与会话一致。
4. CAS 条件不符时返回 `ErrConflict`，旧记录不变。成功后记录完整可读，不能暴露半份更新。
5. `Close()` 释放所有权，允许重复调用；关闭后 `Load` 和 `Save` 必须返回错误。Store 须并发安全。

RunRecord 是可序列化的纯数据。作用域与尝试使用持久随机身份，不可按图路径或本地计数重新生成。字段约束与全部关联见[数据模型](../reference/data-model.md)。

## 5. 处理保存结果不确定

一次保存可能完成了写入，却在后续确认步骤报错。例如文件已被替换，目录同步失败。此时“函数返回错误”不能证明“磁盘仍是旧记录”。

Runtime 会停止本次执行，不更新自己的已确认内存记录，不回滚或自动重试该写入。处理底层问题后，调用新的 `Resume`，让它重新 `Load` 权威记录。记录若已确认该节点，就复用成果；若仍未确认，就按恢复规则重新进入节点。

自定义后端也必须允许并说明这种情况。不要在存储报错后自动拿旧修订号重试，或用旧内存快照覆盖真实记录。

## 错误如何处理

- `ErrBusy`：找出仍持有该运行的会话或进程；等待宿主的运行协调策略释放所有权，不删除锁文件。
- `ErrNotFound`：核对目录与 RunID，确认业务是否曾创建记录。
- `ErrConflict`：核对 CAS 修订号或运行身份；若通过 Runtime 的可靠保存边界返回，还会被包装为 `CheckpointError`。
- 权限、格式、Unicode 或 I/O 错误：保存现场并修正原因。读取失败不能通过创建新记录覆盖原文件。

文件 Store 写前检查所有元数据字符串和 map 键的 UTF-8；非法元数据不会改变旧文件或 Revision。读取拒绝非法原始 UTF-8 与未配对的 JSON Unicode 代理项。编码后的 `Data []byte` 是不透明字节，不受元数据 UTF-8 限制。

## 下一步

核对[数据模型](../reference/data-model.md)中的 Store/Session 协议，和[错误处理](../reference/errors.md)中的可靠错误分类。需要接入具体业务运行时，返回[恢复教程](recovery.md)。
