# Positioning

Squid Mesh is an embedded durable workflow runtime for Elixir applications.
It is for teams that want business workflows to live inside their existing
Phoenix or OTP app, share that app's repo and deployment model, and still have
durable run history, retries, approvals, replay, cancellation, and operator
inspection.

The public authoring surface is intentionally Squid Mesh-native: workflow
modules, triggers, payload contracts, steps, transitions, dependency edges, and
retry policy. Jido, Runic, and Spark are important foundations, but workflow
authors should not have to think in raw agent, planner, or storage primitives
for the common path.

## Product Lane

Squid Mesh sits between a job backend and a standalone workflow service.

- It is more than a job queue: Squid Mesh owns workflow structure, step state,
  attempts, retries, waits, approvals, replay policy, and inspection.
- It is less than a separate workflow platform: Squid Mesh runs inside the host
  app and delegates queueing, delayed scheduling, redelivery, and cron
  activation to the host's chosen executor.
- It is not a generic replacement for Jido, Runic, Reactor, Sage, or FlowStone:
  those projects solve adjacent problems at different abstraction layers.

That boundary is deliberate. The host application keeps its domain contexts,
database, supervision tree, observability stack, and job infrastructure. Squid
Mesh adds the durable workflow contract above those pieces.

## Runtime Direction

The long-term runtime shape is:

1. Spark defines the authoring DSL and normalized workflow spec.
2. Runic plans dependency readiness and runnable workflow work.
3. Squid Mesh records durable workflow and dispatch facts.
4. Jido provides the runtime foundation for actions, signals, agents, thread
   journals, checkpoints, storage, and supervised execution.
5. Executor backends remain responsible for concrete delivery mechanics such as
   queues, scheduled work, redelivery, and worker infrastructure. IntentLedger is
   the preferred durable executor direction, while Squid Mesh keeps the core
   dispatch contract backend-neutral so host applications can provide their own
   executor when needed.

The current implementation is partway through that transition. Squid Mesh
already uses Spark and Jido-compatible step execution, and it now has a durable
dispatch protocol plus rebuildable workflow and dispatch agents for the
Jido-native core. The live runtime still uses the current Postgres tables and
host-executor path until the journal-backed execution path is wired through.

## Status Terms

- Supported: available in the current runtime and covered by repository docs
  and tests.
- In progress: implemented as a protocol or foundation, but not wired through
  the full runtime path yet.
- Planned: accepted roadmap direction linked to an issue, but not a runtime
  guarantee today.
- Out of scope: intentionally not part of Squid Mesh's product surface.

## Capability Map

| Capability | Status | Notes |
| --- | --- | --- |
| Spark-backed workflow DSL | Supported | Triggers, payload contracts, steps, transitions, retries, dependency edges, and formatter support. |
| Native step contract | Supported | `SquidMesh.Step` is the preferred authoring path. Raw `Jido.Action` modules remain an explicit interop path. |
| Durable run history | Supported | Runs, step runs, attempts, and audit events are persisted in the host app's Postgres database. |
| Host executor boundary | Supported | Squid Mesh delegates queueing, delayed scheduling, redelivery, and cron activation to `SquidMesh.Executor`. |
| Human approval workflows | Supported | Pause and approval flows are durable for transition-based workflows. |
| Replay and cancellation | Supported | Replay respects irreversible and non-compensatable steps; cancellation converges through persisted run state. |
| Inspection and explanation | Supported, evolving | Current inspection reads persisted runtime tables. The new core will rebuild views from durable journals and checkpoints in [#163](https://github.com/ccarvalho-eng/squid_mesh/issues/163). |
| Durable dispatch protocol | In progress | The pure protocol, projection, and `Jido.Storage` journal boundary define runnable intent, claims, leases, heartbeats, completion, failure, retries, terminal-run fencing, and checkpoint pointers. It is implemented as a foundation, but not yet the full live execution path. |
| Jido.Storage-backed core | In progress | Protocol entries and projection checkpoints can be persisted through `Jido.Storage`; live runtime adoption remains follow-up work after [#162](https://github.com/ccarvalho-eng/squid_mesh/issues/162). |
| Jido-native runtime agents | In progress | Workflow and dispatch agents can rebuild from durable journals and checkpoints; [#164](https://github.com/ccarvalho-eng/squid_mesh/issues/164) covers the completed agent foundation. |
| IntentLedger executor | Planned | IntentLedger is the preferred durable executor direction for leases, retries, queue delivery, and worker recovery while Squid Mesh keeps custom executor support. |
| Scheduled-start metadata | Supported | Intended schedule windows are stored in durable run context for cron starts. Cron triggers can opt into duplicate-start protection with stable scheduler signal ids or complete intended windows. |
| Conditional and deferred continuation | Planned | Durable planner facts and deferred wakeups are tracked in [#140](https://github.com/ccarvalho-eng/squid_mesh/issues/140). |
| Fan-out and fan-in contract | Planned | Runic-backed join and sibling behavior are tracked in [#142](https://github.com/ccarvalho-eng/squid_mesh/issues/142). |
| Dynamic graph expansion | Planned | Runtime-safe dynamic subflows are deferred until after the core runtime and tracked in [#141](https://github.com/ccarvalho-eng/squid_mesh/issues/141). |
| Oban-specific core | Out of scope | Host apps may choose Oban behind the executor boundary, but Squid Mesh core is not Oban-centric. |
| Exactly-once external side effects | Out of scope | Squid Mesh can provide durable workflow state and fencing semantics, but external systems still require idempotency. |
| Bundled workflow dashboard | Out of scope | Squid Mesh exposes inspection data; host apps own their operator UI. |

## Why Squid Mesh Exists Above Jido

[Jido](https://hex.pm/packages/jido) is the runtime foundation: agents,
actions, signals, directives, supervision, persistence primitives, and
operational runtime structure. Squid Mesh uses that foundation, but it is not
trying to make users write Jido-native workflow applications by hand.

Squid Mesh adds the workflow product layer:

- workflow definitions and validation
- trigger and payload contracts
- step input and output mapping
- retry, replay, cancellation, approval, and failure-routing policy
- durable dispatch semantics
- workflow inspection and explanation projections
- host-app integration around an existing Ecto repo and executor
- a backend-neutral dispatch contract that can use IntentLedger as the default
  durable executor path without making every workflow definition depend on
  IntentLedger-specific concepts

Use Jido directly when the main abstraction is an autonomous or supervised
agent. Use Squid Mesh when the main abstraction is a durable workflow run that
operators need to inspect, resume, retry, replay, or cancel.

## Adjacent Projects

| Project | Primary fit | Relationship to Squid Mesh |
| --- | --- | --- |
| [Jido](https://hex.pm/packages/jido) | OTP-native agents, actions, signals, directives, and supervised autonomous systems. | Runtime foundation and interop layer. Squid Mesh keeps raw Jido primitives out of the common workflow authoring path. |
| [Runic](https://hex.pm/packages/runic) | Data-driven workflow graphs, dependency planning, and runnable extraction. | Planner foundation. Squid Mesh maps declared workflow structure and readiness into durable runnable intent. |
| [Reactor](https://hex.pm/packages/reactor) | Concurrent dependency-resolving saga orchestration for Elixir applications, with Ash integration available through Ash Reactor. | Adjacent orchestrator. Squid Mesh emphasizes durable host-app workflow state, operator inspection, approvals, replay, and the Jido-native runtime direction. |
| [Sage](https://hex.pm/packages/sage) | Dependency-free saga composition with transaction and compensation callbacks. | Good fit for local saga execution. Squid Mesh targets longer-lived inspectable workflow runs with persisted step and attempt history. |
| [FlowStone](https://hex.pm/packages/flowstone) | Asset-first data orchestration and dependency-aware pipelines. | Adjacent data-pipeline tool. Squid Mesh focuses on application workflows, approvals, recovery policy, dispatch semantics, and inspection. |

## How To Choose

Choose Squid Mesh when:

- workflow state belongs inside an existing Phoenix or OTP application;
- runs must survive restarts, deploys, retries, and worker redelivery;
- operators need to know why work is waiting, retrying, paused, failed,
  cancelled, or complete;
- approvals, manual review, replay, cancellation, and recovery policy are part
  of the business process;
- workflow authors should use domain-level workflow concepts instead of raw
  process, job, agent, or planner primitives.

Choose another layer when:

- a short-lived in-memory saga is enough;
- the main abstraction is a long-running autonomous agent;
- the main abstraction is an asset graph or data materialization pipeline;
- the app only needs a job queue, scheduler, or background worker backend;
- a separate workflow service is a better operational boundary than embedding
  workflow state in the host application.

## Reading The Roadmap

The roadmap separates what users can rely on today from the foundations that
are being prepared for the next runtime generation.

Use the current runtime when you need the supported workflow DSL, persisted run
history, host-executor integration, retries, approvals, replay, cancellation,
and inspection backed by the existing Postgres tables.

Treat the durable dispatch protocol as an architectural foundation. It defines
the vocabulary for runnable intent, claim fencing, leases, heartbeats, retries,
and terminal-run behavior. The workflow and dispatch agents can rebuild that
state from durable journals, but the live runtime has not fully switched to that
path yet.

Track the linked issues for the larger runtime transition:

- [#170](https://github.com/ccarvalho-eng/squid_mesh/issues/170) covers the
  lease, heartbeat, and fencing guarantees expected from the durable executor
  integration.
- [#163](https://github.com/ccarvalho-eng/squid_mesh/issues/163) covers
  journal-backed inspection and explanation projections.
Oban can still be a practical executor choice in a host application. It is an
executor implementation detail, not the core Squid Mesh runtime model.
