---
layout: page
title: Checkpoint execution and recovery
description: Save confirmed Pulse workflow results and continue after a process restart.
home:
  eyebrow: PULSE DURABLE · CHECKPOINT RECOVERY
  title: Keep confirmed results. Continue unfinished work.
  lead: Checkpoint encoding, storage, and recovery for Pulse workflows. Rebuild the same definition with the original RunID and inputs, then re-enter unconfirmed functions from the beginning.
  primary: { text: Complete a first run, link: /en/guide/quickstart }
  secondary: { text: Practice exit and recovery, link: /en/guide/recovery }
  version: Development docs · Go 1.25+ · No released tag yet
  flowTitle: Recover from a durable checkpoint
  flowLabel: Restore the confirmed first step, execute the unconfirmed second step, and read the final result
  flow:
    - { name: prepare, detail: Restore the confirmed result, state: restored }
    - { name: finish, detail: Re-enter the unconfirmed function, state: executed }
    - { name: result, detail: Read from a new Execution, state: '42 · ready' }
  flowCaption: Publish after confirmation. Unconfirmed business code can run again.
  pathsLabel: DOCUMENTATION
  pathsTitle: From a first run to reliable recovery
  paths:
    - { title: Start and recover, description: Install the development version and exit a real process before resuming., action: Read the introduction, link: /en/guide/ }
    - { title: Integrate your application, description: Encode business values and configure storage, subgraphs, and retries., action: Choose a guide, link: /en/guide/subgraphs }
    - { title: Check recovery rules, description: Look up versions, inputs, ownership, revisions, and failure handling., action: Open the reference, link: /en/reference/ }
  companion:
    title: Define your workflow with Pulse
    description: Pulse still defines nodes, typed data, ports, and subgraphs. Durable retains confirmed results across executions. Durable waits and distributed management are not implemented.
    action: Read Pulse documentation
    link: https://luo-root.github.io/pulse/en/
---

<Home />
