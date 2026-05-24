# Squid Mesh Manual

This manual is organized as numbered lessons. Read them in order if you are new
to Squid Mesh, or jump to the reference sections when you already know the
runtime model.

## Lessons

### 1. Understand The Model

Start with the product shape and the three boundaries: workflow definition,
journal runtime, and host execution.

- [Learning path](learning_path.md)
- [Positioning](positioning.md)

### 2. Install Squid Mesh In A Host App

Add the dependency, install the migration, configure the repo and queue, and
start a worker loop that drains journal attempts.

- [Host app integration](host_app_integration.md)
- [Compatibility matrix](compatibility.md)

### 3. Write Your First Workflow

Define manual triggers, payload fields, steps, transitions, and custom
`SquidMesh.Step` modules.

- [Workflow authoring](workflow_authoring.md#define-a-workflow)
- [Workflow authoring: triggers](workflow_authoring.md#triggers)

### 4. Run, Drain, Inspect, And Explain

Start a workflow through the public API, execute visible work with
`SquidMesh.execute_next/1`, then inspect the run history and graph.

- [Learning path: start and drain](learning_path.md#3-start-and-drain-a-run)
- [Architecture: execution flow](architecture.md#execution-flow)

### 5. Add Reliability

Add bounded retries, waits, idempotent external side effects, replay safety, and
explicit recovery routes.

- [Operations: retries and backoff](operations.md#retries-and-backoff)
- [Operations: replay after irreversible side effects](operations.md#replay-after-irreversible-side-effects)
- [Tool adapters](tool_adapters.md)

### 6. Add Human Review

Use pause and approval steps when a run needs operator input, then expose the
public resume, approve, and reject APIs through your host app boundary.

- [Learning path: human boundaries](learning_path.md#6-add-human-boundaries)
- [Host app integration: audit history](host_app_integration.md#minimal-otp-host-skeleton)

### 7. Add Cron Activation

Declare cron triggers in workflow modules while keeping recurring scheduling in
the host app.

- [Workflow authoring: cron](workflow_authoring.md#triggers)
- [Host app integration: cron payload contract](host_app_integration.md#cron-payload-contract)

### 8. Add Backend-Owned Leases When Needed

Keep basic hosts simple with an `execute_next/1` worker loop. Use Bedrock when a
host wants durable backend delivery, delayed visibility, heartbeats, retry
requeue, dead-letter handling, and lease ownership.

- [Host app integration: Bedrock lease backend setup](host_app_integration.md#bedrock-lease-backend-setup)
- [Bedrock minimal host app](../examples/bedrock_minimal_host_app/README.md)

### 9. Operate The Runtime

Size worker pools, watch visible attempt depth, capture telemetry, and understand
what Squid Mesh does and does not guarantee.

- [Operations guide](operations.md)
- [Observability](observability.md)
- [Production readiness](production_readiness.md)

### 10. Read The Internals

Use these when contributing to the runtime or building tooling such as
SquidSonar.

- [Architecture](architecture.md)
- [Jido runtime architecture](jido_runtime_architecture.md)
- [Durable dispatch protocol](durable_dispatch_protocol.md)

### 11. Give AI Agents The Right Rules

Use package-style usage rules when an AI coding agent is changing Squid Mesh or
building with it. The main file captures the common contract, and the topic
files split runtime, host app, workflow authoring, testing, docs, and tooling
guidance.

- [Squid Mesh usage rules](../usage-rules.md)
- [Runtime usage rules](../usage-rules/runtime.md)
- [Testing usage rules](../usage-rules/testing.md)
- [Documentation usage rules](../usage-rules/documentation.md)

## Reference

- [Workflow authoring](workflow_authoring.md) - DSL, payloads, transitions,
  conditions, dependencies, retries, cron, and examples
- [Host app integration](host_app_integration.md) - install, config, worker
  loops, cron payloads, Bedrock setup, and Phoenix/OTP host shapes
- [Compatibility matrix](compatibility.md) - supported toolchain and runtime
  assumptions
- [Positioning](positioning.md) - product lane and adjacent project comparison
- [Production readiness](production_readiness.md) - current release bar
- [Usage rules](../usage-rules.md) - condensed rules for AI agents and tooling

## Example Workflow Shapes

- scheduled RSS digest delivery
- issue triage or planning workflows
- recovery, approval, and back-office workflows inside Phoenix apps

## Example Apps

- [Minimal host app](../examples/minimal_host_app/README.md) for a standalone
  development harness
- [Bedrock minimal host app](../examples/bedrock_minimal_host_app/README.md)
  for backend-owned delivery and lease coverage
