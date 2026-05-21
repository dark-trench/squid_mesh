# Documentation

This index is the starting point for Squid Mesh setup, integration, and runtime
reference material.

## Start Here

- [Host app integration](host_app_integration.md) for install, config, and the
  host-app contract
- [Workflow authoring](workflow_authoring.md) for the workflow DSL, payloads,
  steps, transitions, and cron triggers
- [Positioning](positioning.md) for Squid Mesh's product lane relative to Jido,
  Runic, Reactor, Ash Reactor, Sage, and FlowStone
- [Compatibility matrix](compatibility.md) for the supported baseline

## Example Workflow Shapes

- scheduled RSS digest delivery
- issue triage or planning workflows
- recovery, approval, and back-office workflows inside Phoenix apps

## Operations

- [Operations guide](operations.md) for queue sizing, retries, waits, and cron
  activation
- [Observability](observability.md) for telemetry and runtime visibility
- [Production readiness](production_readiness.md) for the current release bar

## Architecture

- [Architecture](architecture.md) for runtime responsibilities and execution
  flow
- [Jido runtime architecture](jido_runtime_architecture.md) for the intended
  journal, agent, heartbeat, and executor shape
- [Positioning](positioning.md) for supported, planned, and out-of-scope
  workflow capabilities
- [Durable dispatch protocol](durable_dispatch_protocol.md) for the Jido-native
  dispatch journal contract
- [Tool adapters](tool_adapters.md) for integration boundaries

## Examples

- [Workflow authoring](workflow_authoring.md) for manual, cron, and
  dependency-based workflow examples
- [Minimal host app](../examples/minimal_host_app/README.md) for a standalone
  development harness
