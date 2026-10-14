---
layout: page
title: 检查点执行与恢复文档
description: 保存 Pulse 工作流的已确认成果，在进程重启后继续执行。
home:
  eyebrow: PULSE DURABLE · CHECKPOINT RECOVERY
  title: 保存已确认成果，继续未完成步骤
  lead: 为 Pulse 工作流提供检查点编码、存储和恢复。重建相同定义，使用原 RunID 与输入，让未确认步骤从头重新执行。
  primary: { text: 完成首次运行, link: /guide/quickstart }
  secondary: { text: 演练退出与恢复, link: /guide/recovery }
  version: 开发版文档 · Go 1.25+ · 尚未发布正式 tag
  flowTitle: 从持久检查点恢复
  flowLabel: 已确认的第一步恢复成果，未确认的第二步重新执行，产生最终结果
  flow:
    - { name: prepare, detail: 从检查点恢复已确认成果, state: restored }
    - { name: finish, detail: 未确认步骤从函数开头进入, state: executed }
    - { name: result, detail: 从新 Execution 读取结果, state: '42 · ready' }
  flowCaption: 确认后发布输出。未确认业务体可能重复执行。
  pathsLabel: DOCUMENTATION
  pathsTitle: 从首次运行到可靠恢复
  paths:
    - { title: 入门与恢复演练, description: 安装开发版，运行完整程序，实际退出进程再恢复。, action: 阅读入门指南, link: /guide/ }
    - { title: 接入业务与存储, description: 配置业务编码，使用文件后端，理解子图和重试恢复。, action: 选择使用任务, link: /guide/subgraphs }
    - { title: 核对恢复契约, description: 查阅版本、输入、所有权、记录修订与故障处理。, action: 打开参考索引, link: /reference/ }
  companion:
    title: 工作流定义继续使用 Pulse
    description: 节点、类型化数据、声明端口和子图仍由 Pulse 定义。Durable 负责跨执行保留已确认成果；持久等待和分布式管理尚未实现。
    action: 阅读 Pulse 文档
    link: https://luo-root.github.io/pulse/
---

<Home />
