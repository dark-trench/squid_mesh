<div align="center">

# SquidMesh - Durable workflow runtime for Elixir

<img width="350" alt="Squid Mesh logo" src="https://github.com/user-attachments/assets/37bdd955-aacf-448e-b050-4d3305020c32" />

<p>
  <a href="https://github.com/dark-trench/squid_mesh/actions/workflows/ci.yml">
    <img alt="CI" src="https://github.com/dark-trench/squid_mesh/actions/workflows/ci.yml/badge.svg" />
  </a>
  <a href="https://codecov.io/gh/dark-trench/squid_mesh">
    <img alt="Codecov" src="https://codecov.io/gh/dark-trench/squid_mesh/branch/main/graph/badge.svg" />
  </a>
  <a href="https://hex.pm/packages/squid_mesh">
    <img alt="Hex.pm" src="https://img.shields.io/hexpm/v/squid_mesh" />
  </a>
  <a href="https://hexdocs.pm/squid_mesh">
    <img alt="HexDocs" src="https://img.shields.io/badge/docs-hexdocs-purple" />
  </a>
  <a href="https://github.com/dark-trench/squid_mesh/blob/main/LICENSE">
    <img alt="License: Apache 2.0" src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" />
  </a>
</p>

</div>

---

Squid Mesh is an embedded durable workflow runtime for Elixir applications.

Workflows are declared as Elixir modules through a DSL, persisted through
Jido journals, and executed by host-owned workers calling
`SquidMesh.execute_next/1`.

The runtime stores workflow state, step attempts, retries, approvals,
transitions, audit events, and recovery history inside the host application's
database through `Jido.Storage` and the default Ecto adapter.

Execution remains host-owned.

Squid Mesh does not run as a separate service, broker, or orchestration
cluster. The host application keeps its existing supervision tree, deployment
model, repository, schedulers, and queue backend.

At runtime, Squid Mesh owns:

- workflow progression
- transition routing
- retry semantics
- pause and approval handling
- replay and recovery policy
- durable execution history
- graph and runtime inspection

Queue delivery, worker supervision, and backend leasing remain host-owned
concerns handled by the application or adapter layer.

A typical setup looks like this:

```text
Phoenix / OTP Application
│
├── Squid Mesh Workflow DSL
│
├── Jido Runtime + Journals
│
├── Jido.Storage Adapter
│   └── Ecto / Postgres
│
├── Host-Owned Workers
│   └── SquidMesh.execute_next/1
│
└── Optional Queue Backend
    ├── Bedrock
    ├── database-backed delivery
    └── host-specific delivery systems
```

This keeps workflow decisions in the library while deployment, queueing, and
worker lifecycle policy stay inside the host application.

Internally, the runtime builds on:

- **Jido** for actions, execution, and journaling
- **Runic** for workflow planning
- **Spark** for the DSL authoring surface

For architecture details and runtime boundaries, see:

- [Architecture](docs/architecture.md)
- [Positioning Guide](docs/positioning.md)

---

## Table of Contents

- [Runtime Capabilities](#runtime-capabilities)
- [Getting Started](#getting-started)
- [Optional Dashboard](#optional-dashboard)
- [Example Applications](#example-applications)
- [When to Use Squid Mesh](#when-to-use-squid-mesh)
- [Runtime Shape](#runtime-shape)
- [Execution Boundary](#execution-boundary)
- [Quick Start](#quick-start)
- [Example: The Ring Errand](#example-the-ring-errand)
- [Documentation](#documentation)
- [Community](#community)
- [Contributing](#contributing)

---

## Runtime Capabilities

### Workflow Authoring

- workflow DSL with manual and cron triggers
- transition-based and dependency-based workflow models
- explicit step input selection and output mapping
- conditional transitions with persisted routing decisions
- retries, waits, failure routes, dependency joins, and approval gates
- compensation and irreversible-step declarations for recovery policy

### Durable Runtime

- Postgres-backed Jido journal history for runs, steps, attempts, and manual
  decisions
- durable workflow state across restarts, deploys, retries, and worker
  redelivery
- replay, cancellation, retry exhaustion, and recovery semantics
- pause/resume and human-in-the-loop approval flows
- audit events for operational visibility

### Execution Model

- pulled execution through `SquidMesh.execute_next/1`
- optional cron payload delivery through host-owned schedulers
- host-owned worker supervision and capacity management
- same-process host repo transactions for small local step groups
- raw `Jido.Action` interoperability when needed

### Inspection and Operations

- runtime inspection through declared step state
- chronological step history and audit events
- graph output for workflow visualization
- structured diagnostics through `SquidMesh.explain_run/2`
- visibility into waiting, retrying, paused, failed, cancelled, and
  completed runs

### Built-in Step Support

- native `SquidMesh.Step` modules
- built-in steps including `:log`, `:wait`, `:pause`, and `:approval`
- raw `Jido.Action` interop for lower-level integration

---

## Getting Started

Choose the path that matches how you want to learn or integrate Squid Mesh.

| Path | Start with | Use when |
| --- | --- | --- |
| First concepts | [Getting Started](docs/getting_started.md) | You want the model, install path, worker loop, inspection, reliability, and operations in order. |
| Interactive learning | [![Run in Livebook](https://livebook.dev/badge/v1/pink.svg)](https://livebook.dev/run?url=https%3A%2F%2Fgithub.com%2Fdark-trench%2Fsquid_mesh%2Fblob%2Fmain%2Fdocs%2Fgetting_started.livemd) | You want to run a small workflow and inspect attempts, wakeups, graph output, and approval state. |
| Workflow DSL | [Workflow Authoring](docs/workflow_authoring.md) and [Workflow Authoring Livebook](docs/workflow_authoring.livemd) | You want to understand triggers, payloads, steps, transitions, dependencies, input mapping, retries, and compensation. |
| Host integration | [Host App Integration](docs/host_app_integration.md) | You are adding Squid Mesh to a Phoenix or OTP app. |
| Executable examples | [Reference Workflows](docs/reference_workflows.md) and the [Minimal Host App](examples/minimal_host_app/README.md) | You want approval, recovery, dependency, saga, cron, restart, and soak examples with runnable coverage. |
| Backend leases | [Bedrock Minimal Host App](examples/bedrock_minimal_host_app/README.md) | You want backend-owned delivery, leases, delayed visibility, retry requeue, dead-letter handling, and cron payload mapping. |

The documentation index is [docs/index.md](docs/index.md).

---

## Optional Dashboard

[SquidSonar](https://github.com/dark-trench/squid_sonar) is the optional
read-only Phoenix LiveView dashboard for Squid Mesh.

Mount it inside a Phoenix host application to inspect recent workflow runs,
filter by status, search runtime metadata, and view run detail pages with
diagnosis, history counts, last error information, and workflow graph
visualization.

---

## Example Applications

### Minimal Host App

[Minimal Host App](examples/minimal_host_app/README.md) is the reference
standalone harness for:

- recovery
- approvals
- cron activation
- local transactions
- replay
- smoke verification
- resilience verification
- soak verification

### Bedrock Minimal Host App

[Bedrock Minimal Host App](examples/bedrock_minimal_host_app/README.md) is the
delivery-and-leasing harness for:

- Bedrock-backed queueing
- lease ownership
- delayed jobs
- retry requeue
- dead-letter behavior

---

## When to Use Squid Mesh

Use Squid Mesh when a Phoenix or OTP application needs a durable workflow run as
the main abstraction, not just a background job.

It fits flows where:

- state should stay inside the host application and survive restarts, deploys,
  retries, and worker redelivery
- operators need to inspect why work is waiting, retrying, paused, failed,
  cancelled, or complete
- approvals, manual review, replay, cancellation, and recovery policy belong to
  the business process
- step history and manual decisions need to remain available after execution

If you are new to the project, start with
[Getting Started](docs/getting_started.md). It teaches the model in order:
installation, writing one workflow, draining journal attempts, inspecting the
run, then adding retries, manual gates, cron, and Bedrock-backed leases when
those pieces are needed.

For the full runtime direction and comparison with adjacent projects, see the
[Positioning Guide](docs/positioning.md).

> [!WARNING]
> Squid Mesh is still in early development.
>
> The runtime is suitable for evaluation, local development, and integration
> work, but it is not yet documented as production-ready.
>
> See [Production Readiness](docs/production_readiness.md) for the current
> checklist and remaining production-readiness items.

---

## Runtime Shape

Squid Mesh owns:

- workflow structure
- payload validation
- runtime state
- retry policy
- recovery policy
- durable workflow history

Your host application keeps:

- its existing `Repo`
- supervision tree
- application boundaries
- deployment model
- queue and scheduler infrastructure
- delivery leases and worker fencing when the chosen backend requires them

The Jido-native runtime persists workflow and dispatch facts through
`Jido.Storage`. The default Ecto adapter stores those journals in the host
application's repository.

Workers execute visible attempts by calling:

```elixir
SquidMesh.execute_next/1
```

Cron schedulers can deliver:

```elixir
SquidMesh.Executor.Payload.cron/3
```

payloads to:

```elixir
SquidMesh.Runtime.Runner.perform/2
```

---

## Execution Boundary

The journal-backed runtime is Jido-native.

Squid Mesh records workflow facts in Jido journals while host-owned workers
provide process supervision and capacity by calling `SquidMesh.execute_next/1`.

External schedulers may enqueue cron activation payloads, but step execution is
claimed through the journal-backed worker loop.

Host apps are responsible for leasing and worker fencing at any delivery
backend they add around the journal worker loop. `SquidMesh.Executor.Leases` is
the public adapter contract for backends that expose claim, heartbeat,
completion, failure, retry, and dead-letter behavior.

Bedrock is the recommended reference backend for that shape because its public
Job Queue and Store APIs already expose leasing and delayed-delivery operations.
The Bedrock minimal host app shows this through
`BedrockMinimalHostApp.SquidMeshLeaseAdapter`.

Other delivery systems can be used when the host app provides equivalent queue
visibility, ownership, heartbeat, retry, and stale-worker recovery semantics.

For example, a Bedrock-backed adapter uses Bedrock for:

- job delivery
- lease ownership and extension
- stale-worker recovery
- delivery metadata

Another host-specific adapter can use its own storage and delivery mechanism if
it also implements the lease and recovery behavior required by the deployment
model.

The key boundary is:

- Squid Mesh owns workflow decisions and journaled facts.
- Host adapters own the concrete queue, lease, and worker-lifecycle mechanics
  required by their backend.

See [Architecture](docs/architecture.md#execution-flow) for the runtime flow
diagram and component boundaries.

---

## Quick Start

### Requirements

- an existing Elixir application
- an existing Ecto `Repo`
- Postgres for persisted runtime state
- a worker process that calls `SquidMesh.execute_next/1`
- a lease or fencing strategy for distributed or backend-owned delivery

---

### 1. Install from Hex.pm

```elixir
defp deps do
  [
    {:squid_mesh, "~> 0.1.0-beta.3"}
  ]
end
```

For the common authoring path, define custom steps with `use SquidMesh.Step`.

Raw `Jido.Action` modules remain supported as an explicit interop path. If the
host application defines raw Jido actions directly, add `:jido` explicitly as
well:

```elixir
defp deps do
  [
    {:jido, "~> 2.0"},
    {:squid_mesh, "~> 0.1.0-beta.3"}
  ]
end
```

---

### 2. Configure Squid Mesh

```elixir
config :squid_mesh,
  repo: MiddleEarth.Repo,
  queue: "default"
```

Start one supervised worker loop that calls `SquidMesh.execute_next/1`.

See [Host App Integration](docs/host_app_integration.md) for a minimal worker
shape.

---

### 3. Install Migrations

```sh
mix deps.get
mix squid_mesh.install
mix ecto.migrate
```

`mix squid_mesh.install` creates one current-schema Squid Mesh migration in the
host application's `priv/repo/migrations`.

The host application still owns migrations for its chosen job system.

---

### 4. Import Formatter Rules

To keep workflow modules formatted consistently as DSL-style declarations,
import Squid Mesh formatter rules from the host application.

```elixir
# .formatter.exs

[
  import_deps: [:squid_mesh],
  inputs: [
    "{mix,.formatter}.exs",
    "{config,lib,test}/**/*.{ex,exs}"
  ]
]
```

This allows workflow definitions to retain readable multi-line formatting for:

- triggers
- payload declarations
- transitions
- dependency graphs
- retry configuration
- compensation policies
- conditional routing

## Example: The Ring Errand

Before the larger workflow example, here is the workflow API in smaller pieces.

Manual triggers declare an entrypoint and a payload contract. Payload fields are
validated before Squid Mesh persists the run, and defaults are resolved during
run creation.

```elixir
defmodule MiddleEarth.Workflows.RingErrand do
  use SquidMesh.Workflow

  workflow do
    trigger :leave_shire do
      manual()

      payload do
        field :bearer, :string, default: "Frodo"
        field :ring_id, :string
        field :snack_count, :integer, default: 11
        field :panic_level, :float, required: false
        field :eagle_backup?, :boolean, default: false
        field :fellowship, :list, default: ["Sam"]
        field :map_marks, :map, default: %{}

        field :route_preferences, :map,
          default: %{
            preferred_route: "moria",
            risk_tolerance: "heroic"
          }

        field :mood, :atom, default: :peckish
        field :started_on, :string, default: {:today, :iso8601}
      end
    end

    step :pack_lembas, Hobbiton.Steps.PackLembas,
      input: [:snack_count],
      output: :provisions,
      transaction: :repo

    step :announce_departure, :log,
      message: "Leaving the Shire with suspicious jewelry",
      level: :info

    step :wait_for_gandalf, :wait,
      duration: 5_000

    step :hide_at_prancing_pony, :pause

    approval_step :council_vote,
      output: :council

    step :choose_path, Rivendell.Steps.ChoosePath,
      input: [
        bearer: [:bearer],
        council_decision: [:council, :decision],
        preferred_route: [:route_preferences, :preferred_route],
        risk_tolerance: [:route_preferences, :risk_tolerance]
      ],
      output: :route

    step :cross_moria, Fellowship.Steps.CrossMoria,
      input: [:bearer, :provisions, :council, :route],
      output: :moria,
      retry: [
        max_attempts: 3,
        backoff: [
          type: :exponential,
          min: 1_000,
          max: 10_000
        ]
      ]

    step :reserve_eagle, Eagles.Steps.ReserveRide,
      compensate: Eagles.Steps.CancelRide

    step :insult_sauron, Gondor.Steps.InsultSauron,
      compensatable: false

    step :toss_ring, Mordor.Steps.TossRing,
      irreversible: true

    step :walk_home_awkwardly,
      Hobbiton.Steps.WalkHomeAwkwardly

    transition :pack_lembas,
      on: :ok,
      to: :announce_departure

    transition :announce_departure,
      on: :ok,
      to: :wait_for_gandalf

    transition :wait_for_gandalf,
      on: :ok,
      to: :hide_at_prancing_pony

    transition :hide_at_prancing_pony,
      on: :ok,
      to: :council_vote

    transition :council_vote,
      on: :ok,
      to: :choose_path

    transition :council_vote,
      on: :error,
      to: :walk_home_awkwardly

    transition :choose_path,
      on: :ok,
      to: :reserve_eagle,
      condition: [
        path: [:route, :decision],
        equals: "eagle"
      ]

    transition :choose_path,
      on: :ok,
      to: :cross_moria

    transition :cross_moria,
      on: :ok,
      to: :reserve_eagle

    transition :cross_moria,
      on: :error,
      to: :walk_home_awkwardly,
      recovery: :undo

    transition :reserve_eagle,
      on: :ok,
      to: :insult_sauron

    transition :insult_sauron,
      on: :ok,
      to: :toss_ring

    transition :toss_ring,
      on: :ok,
      to: :complete

    transition :walk_home_awkwardly,
      on: :ok,
      to: :complete
  end
end
```

Cron-triggered workflows use the same workflow model, while recurring
scheduling and activation remain owned by the host application.

```elixir
defmodule Gondor.Workflows.BeaconWatch do
  use SquidMesh.Workflow

  workflow do
    trigger :nightly_beacon_check do
      cron "0 21 * * *", timezone: "Etc/UTC"

      payload do
        field :steward_mood, :string, default: "dramatic"
        field :orc_count, :integer, default: 9001
      end
    end

    step :inspect_hilltops,
      Gondor.Steps.InspectHilltops,
      retry: [max_attempts: 5]

    step :light_first_beacon,
      Gondor.Steps.LightBeacon,
      compensate: Gondor.Steps.ExtinguishBeacon

    step :log_call_for_aid, :log,
      message: "Gondor calls for aid",
      level: :info

    transition :inspect_hilltops,
      on: :ok,
      to: :light_first_beacon

    transition :light_first_beacon,
      on: :ok,
      to: :log_call_for_aid

    transition :log_call_for_aid,
      on: :ok,
      to: :complete
  end
end
```

Dependency-based workflows use `after: [...]` instead of explicit transitions.
A step becomes runnable only after all declared dependencies complete.

```elixir
defmodule Mordor.Workflows.FinalDistraction do
  use SquidMesh.Workflow

  workflow do
    trigger :start_distraction do
      manual()

      payload do
        field :speech, :string, default: "For Frodo."
      end
    end

    step :march_to_gate,
      Gondor.Steps.MarchToGate

    step :look_very_brave,
      Gondor.Steps.LookBrave

    step :sneak_up_volcano,
      Hobbiton.Steps.SneakUpVolcano

    step :declare_victory,
      Gondor.Steps.DeclareVictory,
      after: [
        :march_to_gate,
        :look_very_brave,
        :sneak_up_volcano
      ],
      irreversible: true
  end
end
```

Step modules implement the domain work while Squid Mesh manages:

- durable runtime state
- retries and retry exhaustion
- recovery semantics
- pause and approval flows
- inspection and graph state

For approval or manual-review gates, use `approval_step/2` in
transition-oriented workflows and resume execution through the explicit
decision APIs.

```elixir
SquidMesh.approve_run/3
SquidMesh.reject_run/3
```

Approval steps persist their resolved `:ok` and `:error` targets together with
their output-mapping metadata, allowing paused review flows to survive deploys
and restarts without semantic drift.

Generic `SquidMesh.unblock_run/2` remains available for lower-level `:pause`
steps when manual intervention is needed without a formal approve/reject
contract.

When a step requires a narrower contract than the accumulated workflow context,
use `input: [...]` to select fields and `output: :key` to namespace returned
results for downstream steps.

Conditional transitions keep routing logic inside workflow progression rather
than hiding it inside action code.

Squid Mesh evaluates transitions in declaration order. The first matching
condition wins, while unconditional transitions may act as fallbacks.
Selected edges are persisted and exposed through graph inspection.

When a custom step needs several local repo writes to commit or roll back
together, declare:

```elixir
transaction: :repo
```

This wraps only that step callback in the configured Ecto repository
transaction. Workflow durability, successor dispatch, external side effects,
and compensation boundaries remain explicit.

For external side effects that cannot be honestly reversed, mark the step with:

```elixir
irreversible: true
```

or:

```elixir
compensatable: false
```

Squid Mesh exposes these recovery boundaries during inspection and blocks replay
by default after irreversible execution unless explicitly overridden.

In the Ring Errand example, the `:error` transition on `:cross_moria` acts as a
same-step fallback after retry exhaustion.

The compensation callback behaves differently. Compensation executes only if
`:reserve_eagle` completed successfully and a later step causes the workflow to
fail.

For reversible saga-style steps, compensation callbacks are declared directly on
the step definition.

```elixir
step :borrow_elven_rope,
  Lothlorien.Steps.BorrowRope,
  compensate: Lothlorien.Steps.ReturnRope

step :reserve_eagle,
  Eagles.Steps.ReserveRide,
  compensate: Eagles.Steps.CancelRide

step :cross_moria,
  Fellowship.Steps.CrossMoria,
  retry: [max_attempts: 2]

transition :borrow_elven_rope,
  on: :ok,
  to: :reserve_eagle

transition :reserve_eagle,
  on: :ok,
  to: :cross_moria

transition :cross_moria,
  on: :ok,
  to: :complete
```

When a downstream step fails after retries and the workflow has no forward
`:error` route, Squid Mesh executes completed compensation callbacks in reverse
completion order.

In the example above, a failed `:cross_moria` step cancels the eagle
reservation before returning the rope.

Each compensation result is persisted under the originating step’s
`recovery.compensation` history.

Start workflows through the public runtime API:

```elixir
{:ok, run} =
  SquidMesh.start_run(
    MiddleEarth.Workflows.RingErrand,
    :leave_shire,
    %{
      ring_id: "one-ring"
    }
  )

SquidMesh.inspect_run(
  run.run_id,
  include_history: true
)
```

With history enabled, inspection includes:

- chronological `step_runs`
- declared workflow state
- audit events
- pause and resume actions
- approval and rejection history

For workflows paused at a generic `:pause` step, resume with:

```elixir
SquidMesh.unblock_run/2
```

For approval steps, resume through the explicit decision APIs:

```elixir
{:ok, paused_run} =
  SquidMesh.inspect_run(
    run.run_id,
    include_history: true
  )

{:ok, resumed_run} =
  SquidMesh.unblock_run(
    paused_run.run_id,
    %{
      actor: "strider",
      reason: "pipeweed restocked"
    }
  )

{:ok, approved_run} =
  SquidMesh.approve_run(
    resumed_run.run_id,
    %{
      actor: "elrond",
      note: "approved by council"
    }
  )

{:ok, rejected_run} =
  SquidMesh.reject_run(
    resumed_run.run_id,
    %{
      actor: "elrond",
      note: "too much singing"
    }
  )
```

Runs can also be listed, cancelled, and replayed.

Replay requires an explicit override after irreversible or
non-compensatable execution.

```elixir
{:ok, running_runs} =
  SquidMesh.list_runs(status: :running)

{:ok, cancelling_run} =
  SquidMesh.cancel_run(run.run_id)

{:ok, replayed_run} =
  SquidMesh.replay_run(run.run_id)

{:ok, reviewed_replay} =
  SquidMesh.replay_run(
    run.run_id,
    allow_irreversible: true
  )
```

Use `SquidMesh.explain_run/2` when a host application needs structured
operator-facing diagnostics.

```elixir
{:ok, explanation} =
  SquidMesh.explain_run(run.run_id)

explanation.reason
#=> :waiting_for_retry
```

`inspect_run/2` returns persisted runtime facts.

`explain_run/2` summarizes:

- the current runtime reason
- valid next actions
- supporting evidence

This structure is designed for dashboards, CLIs, and operational tooling.

Graph inspection exposes workflows as UI-friendly nodes and edges.

```elixir
{:ok, graph} =
  SquidMesh.inspect_run_graph(run.run_id)

graph
|> SquidMesh.Runs.GraphInspection.to_map()
|> Map.take([
  :status,
  :current_node_ids,
  :nodes,
  :edges
])
```

For conditional paths, the selected transition edge is marked separately from
skipped sibling edges, allowing dashboards to visualize execution flow directly
from persisted runtime state.

---

## Documentation

Use the documentation index for setup, workflow authoring, runtime operations,
inspection, and architecture details.

### Core Guides

- [Docs Index](docs/index.md)
- [Getting Started](docs/getting_started.md)
- [Workflow Authoring Guide](docs/workflow_authoring.md)
- [Host App Integration](docs/host_app_integration.md)
- [Architecture](docs/architecture.md)
- [Positioning Guide](docs/positioning.md)
- [Production Readiness](docs/production_readiness.md)

### Runtime & Inspection

- [Reference Workflows](docs/reference_workflows.md)
- [Execution Flow](docs/architecture.md#execution-flow)
- [Graph Inspection](docs/architecture.md#inspection)
- [Recovery & Replay](docs/workflow_authoring.md#recovery-and-replay)
- [Approval Flows](docs/workflow_authoring.md#approval-steps)

### Example Applications

- [Minimal Host App](examples/minimal_host_app/README.md)
- [Bedrock Minimal Host App](examples/bedrock_minimal_host_app/README.md)

---

## Community

### Discussion

Use the
[Elixir Forum thread](https://elixirforum.com/t/squid-mesh-workflow-automation-runtime-for-elixir-applications/75162)
for public discussion, architectural context, and workflow-runtime design
conversations.

### Issues & Feature Requests

Use
[GitHub Issues](https://github.com/dark-trench/squid_mesh/issues)
for:

- bug reports
- feature requests
- release-tracked work
- runtime regressions
- documentation improvements

### Discord

For informal discussion around Squid Mesh, Jido, runtime internals, and related
orchestration tooling, use the
[Squid Mesh channel on the Jido Discord](https://discord.com/channels/1323353012235796550/1504122798027571331).

---

## Contributing

Please review the existing runtime model and workflow semantics before proposing
substantial changes.

### Start Here

- [Contributing Guide](CONTRIBUTING.md)
- [Code of Conduct](CODE_OF_CONDUCT.md)

### Areas Where Contributions Help Most

- runtime reliability
- workflow ergonomics
- inspection tooling
- recovery semantics
- documentation improvements
- backend integrations
- executable examples and soak coverage

### Development Principles

When contributing, please aim to:

- preserve deterministic runtime behavior
- keep execution semantics explicit
- maintain operational clarity
- avoid hidden orchestration behavior
- support embedded host-application deployment models
