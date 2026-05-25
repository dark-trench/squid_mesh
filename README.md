<div align="center">
  <h2>SquidMesh—Durable workflows for Elixir apps</h2>
  <img width="350" alt="sm-logo" src="https://github.com/user-attachments/assets/37bdd955-aacf-448e-b050-4d3305020c32" />
  <p>
    <a href="https://github.com/dark-trench/squid_mesh/actions/workflows/ci.yml">
      <img alt="CI" src="https://github.com/dark-trench/squid_mesh/actions/workflows/ci.yml/badge.svg" />
    </a>
    <a href="https://codecov.io/gh/dark-trench/squid_mesh">
      <img alt="Codecov" src="https://codecov.io/gh/dark-trench/squid_mesh/branch/main/graph/badge.svg" />
    </a>
    <a href="https://hex.pm/packages/squid_mesh">
      <img alt="Hex" src="https://img.shields.io/hexpm/v/squid_mesh" />
    </a>
    <a href="https://hexdocs.pm/squid_mesh">
      <img alt="HexDocs" src="https://img.shields.io/badge/docs-hexdocs-purple" />
    </a>
    <a href="https://github.com/dark-trench/squid_mesh/blob/main/LICENSE">
      <img alt="License: Apache 2.0" src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" />
    </a>
  </p>
</div>

Squid Mesh is an embedded durable workflow runtime for Elixir applications. It
is for teams that want business workflows to live inside an existing Phoenix or
OTP app, share that app's repo and deployment model, and still have durable run
history, retries, approvals, replay, cancellation, and operator inspection.

It sits between a job backend and a standalone workflow service: more
structured and inspectable than a job queue, but still embedded in the host app
instead of running as a separate platform.

Internally, Squid Mesh builds on Jido for the action/runtime foundation, Runic
for workflow planning, and Spark for the DSL authoring surface. For comparison
with adjacent orchestration tools, see the [Positioning guide](docs/positioning.md).

## Getting Started

Choose the path that matches how you want to learn:

| Path | Start with | Use when |
| --- | --- | --- |
| First concepts | [Getting Started](docs/getting_started.md) | You want the model, install path, worker loop, inspection, reliability, and operations in order. |
| Interactive learning | [![Run in Livebook](https://livebook.dev/badge/v1/pink.svg)](https://livebook.dev/run?url=https%3A%2F%2Fgithub.com%2Fdark-trench%2Fsquid_mesh%2Fblob%2Fmain%2Fdocs%2Fgetting_started.livemd) | You want to run a small workflow and inspect attempts, wakeups, graph output, and approval state. |
| Workflow DSL | [Workflow authoring](docs/workflow_authoring.md) and [Workflow authoring Livebook](docs/workflow_authoring.livemd) | You want to understand triggers, payloads, steps, transitions, dependencies, input mapping, retries, and compensation. |
| Host integration | [Host app integration](docs/host_app_integration.md) | You are adding Squid Mesh to a Phoenix or OTP app. |
| Executable examples | [Reference Workflows](docs/reference_workflows.md) and the [Minimal Host App](examples/minimal_host_app/README.md) | You want approval, recovery, dependency, saga, cron, restart, and soak examples that actually run. |
| Backend leases | [Bedrock Minimal Host App](examples/bedrock_minimal_host_app/README.md) | You want backend-owned delivery, leases, delayed visibility, retry requeue, dead-letter handling, and cron payload mapping. |

The full documentation home is [docs/index.md](docs/index.md).

## What It Does

- workflow DSL with manual and cron triggers
- Postgres-backed Jido journal history for runs, steps, attempts, and manual
  decisions
- pulled execution through `SquidMesh.execute_next/1`, with optional cron
  payload delivery through host schedulers
- retries, waits, failure routes, dependency joins, and HITL approval gates
- explicit step input selection and output mapping
- same-process host repo transactions for small local step groups
- runtime inspection through declared step state, audit events, graph output,
  and `SquidMesh.explain_run/2`
- native `SquidMesh.Step` modules, built-in steps like `:log`, `:wait`,
  `:pause`, and `:approval`, plus raw `Jido.Action` interop

## Companion Dashboard

[SquidSonar](https://github.com/dark-trench/squid_sonar) is the optional
read-only Phoenix LiveView dashboard for Squid Mesh. Mount it inside a Phoenix
host app to inspect recent workflow runs, filter by status, search runtime
metadata, and view run detail pages with diagnosis, history counts, last error
information, and workflow graph visualization.

## Example Apps

- [Minimal host app](examples/minimal_host_app/README.md): the reference
  standalone harness for recovery, approvals, cron, local transactions, replay,
  and smoke/resilience/soak verification.
- [Bedrock minimal host app](examples/bedrock_minimal_host_app/README.md): the
  delivery-and-leasing harness for Bedrock-backed queueing, lease ownership,
  delayed jobs, retry requeue, and dead-letter behavior.

## When To Use It

Use Squid Mesh when a Phoenix or OTP app needs a durable workflow run as the
main abstraction, not just a background job. It fits flows where:

- state should stay inside the host app and survive restarts, deploys, retries,
  and worker redelivery
- operators need to inspect why work is waiting, retrying, paused, failed,
  cancelled, or complete
- approvals, manual review, replay, cancellation, and recovery policy belong to
  the business process
- step history and manual decisions need to remain available after execution

For the full runtime direction and comparison with adjacent projects, see the
[Positioning guide](docs/positioning.md).

If you are new to the project, start with
[Getting Started](docs/getting_started.md). It teaches the model in order:
install, write one workflow, drain journal attempts, inspect the run, then add
retries, manual gates, cron, and Bedrock-backed leases when those pieces are
needed.

> [!WARNING]
> Squid Mesh is still in early development. The runtime is suitable for evaluation, local development, and integration work, but it is not yet documented as production-ready.
> See [Production Readiness](docs/production_readiness.md) for the current checklist and remaining bar.

## Runtime Shape

- Squid Mesh owns workflow structure, payload validation, runtime state, and
  retry policy
- your host app keeps its existing `Repo`, supervision tree, and application
  boundaries
- the Jido-native runtime persists workflow and dispatch facts through
  `Jido.Storage`; the default Ecto adapter stores those journals in the host repo
- workers execute visible attempts by calling `SquidMesh.execute_next/1`
- cron schedulers can deliver `SquidMesh.Executor.Payload.cron/3` payloads to
  `SquidMesh.Runtime.Runner.perform/2`

## Execution Boundary

The journal-backed runtime is Jido-native. Squid Mesh records workflow facts in Jido
journals while host-owned workers provide process supervision and capacity by
calling `SquidMesh.execute_next/1`. External schedulers may enqueue cron
activation payloads, but step delivery now runs through the journal-backed
worker loop.

For example, a Bedrock adapter could use Bedrock/FDB for job delivery, lease
extension, stale-worker recovery, and delivery metadata. A Postgres or Oban
adapter could keep using relational storage for delivery. The key boundary is
that Squid Mesh owns workflow decisions and journaled facts, while adapters own
the concrete queue and lease mechanics required by their backend.

See [Architecture](docs/architecture.md#execution-flow) for the runtime flow
diagram and component boundaries.

## Quick Start

Requirements:

- an existing Elixir application
- an existing Ecto `Repo`
- Postgres for persisted runtime state
- a worker process that calls `SquidMesh.execute_next/1`

### 1. Install from Hex.pm

```elixir
defp deps do
  [
    {:squid_mesh, "~> 0.1.0-beta.2"}
  ]
end
```

For the common authoring path, define custom steps with `use SquidMesh.Step`.
Raw `Jido.Action` modules remain supported as an explicit interop path; if the
host app defines raw Jido actions directly, add `:jido` explicitly as well:

```elixir
defp deps do
  [
    {:jido, "~> 2.0"},
    {:squid_mesh, "~> 0.1.0-beta.2"}
  ]
end
```

### 2. Configure Squid Mesh

```elixir
config :squid_mesh,
  repo: MiddleEarth.Repo,
  queue: "default"
```

Start one supervised worker loop that calls `SquidMesh.execute_next/1`. See
[Host App Integration](docs/host_app_integration.md) for a minimal worker
shape.

### 3. Install migrations

```sh
mix deps.get
mix squid_mesh.install
mix ecto.migrate
```

`mix squid_mesh.install` creates one current-schema Squid Mesh migration in the
host app's `priv/repo/migrations`. The host app still owns migrations for its
chosen job system.

### 4. Import formatter rules

To keep workflow modules formatted as DSL-style calls, import Squid Mesh's
formatter configuration from the host app:

```elixir
# .formatter.exs
[
  import_deps: [:squid_mesh],
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"]
]
```

## Example: The Ring Errand

Before the longer example, here is the workflow API in small pieces.

Manual triggers declare an entrypoint and a payload contract. Payload fields are
validated before Squid Mesh persists the run, and defaults are resolved at run
creation time:

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
          default: %{preferred_route: "moria", risk_tolerance: "heroic"}
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

    step :wait_for_gandalf, :wait, duration: 5_000
    step :hide_at_prancing_pony, :pause

    approval_step :council_vote, output: :council

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
        backoff: [type: :exponential, min: 1_000, max: 10_000]
      ]

    step :reserve_eagle, Eagles.Steps.ReserveRide,
      compensate: Eagles.Steps.CancelRide

    step :insult_sauron, Gondor.Steps.InsultSauron,
      compensatable: false

    step :toss_ring, Mordor.Steps.TossRing,
      irreversible: true

    step :walk_home_awkwardly, Hobbiton.Steps.WalkHomeAwkwardly

    transition :pack_lembas, on: :ok, to: :announce_departure
    transition :announce_departure, on: :ok, to: :wait_for_gandalf
    transition :wait_for_gandalf, on: :ok, to: :hide_at_prancing_pony
    transition :hide_at_prancing_pony, on: :ok, to: :council_vote
    transition :council_vote, on: :ok, to: :choose_path
    transition :council_vote, on: :error, to: :walk_home_awkwardly
    transition :choose_path,
      on: :ok,
      to: :reserve_eagle,
      condition: [path: [:route, :decision], equals: "eagle"]
    transition :choose_path, on: :ok, to: :cross_moria
    transition :cross_moria, on: :ok, to: :reserve_eagle
    transition :cross_moria, on: :error, to: :walk_home_awkwardly, recovery: :undo
    transition :reserve_eagle, on: :ok, to: :insult_sauron
    transition :insult_sauron, on: :ok, to: :toss_ring
    transition :toss_ring, on: :ok, to: :complete
    transition :walk_home_awkwardly, on: :ok, to: :complete
  end
end
```

Cron triggers use the same workflow shape, but the host app owns recurring
scheduling and activation:

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

    step :inspect_hilltops, Gondor.Steps.InspectHilltops,
      retry: [max_attempts: 5]

    step :light_first_beacon, Gondor.Steps.LightBeacon,
      compensate: Gondor.Steps.ExtinguishBeacon

    step :log_call_for_aid, :log,
      message: "Gondor calls for aid",
      level: :info

    transition :inspect_hilltops, on: :ok, to: :light_first_beacon
    transition :light_first_beacon, on: :ok, to: :log_call_for_aid
    transition :log_call_for_aid, on: :ok, to: :complete
  end
end
```

Dependency-based workflows use `after: [...]` instead of transitions. A step is
runnable only after all of its declared dependencies complete:

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

    step :march_to_gate, Gondor.Steps.MarchToGate
    step :look_very_brave, Gondor.Steps.LookBrave
    step :sneak_up_volcano, Hobbiton.Steps.SneakUpVolcano

    step :declare_victory, Gondor.Steps.DeclareVictory,
      after: [:march_to_gate, :look_very_brave, :sneak_up_volcano],
      irreversible: true
  end
end
```

Step modules implement domain work. Squid Mesh records durable journal state,
makes runnable attempts visible to `SquidMesh.execute_next/1`, applies retry
policy, routes failures after retry exhaustion, and exposes run inspection.

For approval or manual-review gates, use `approval_step/2` in transition-based
workflows and resume the paused run through `SquidMesh.approve_run/3` or
`SquidMesh.reject_run/3`. Approval steps persist their resolved `:ok` and
`:error` targets plus output-mapping metadata, so already-paused review runs keep
the same decision semantics across restarts and deploys. Generic
`SquidMesh.unblock_run/2` remains available for lower-level `:pause` steps when
you need manual intervention without an explicit approve/reject contract.

When a step needs a narrower contract than the whole payload plus accumulated
context, use `input: [...]` to select keys and `output: :key` to namespace the
returned map for downstream steps. Keyword input mappings can read nested paths
from the durable run context. In the example, `:choose_path` reads nested
`route_preferences` payload values and the approval output under `:council`,
then stores its result under `:route`.

Conditional transitions keep routing decisions in workflow progression instead
of burying them inside the next step. Squid Mesh evaluates matching transitions
in declaration order; the first equality condition that matches the durable
context wins, and an unconditional transition can act as the fallback. The
selected edge is persisted and appears in graph inspection.

When a custom step needs several local repo writes to commit or roll back
together, declare `transaction: :repo`. This wraps only that action callback in
the configured Ecto repo transaction; workflow durability, successor dispatch,
external side effects, and saga compensation remain explicit Squid Mesh
boundaries.

For external side effects that cannot be honestly undone, mark the step with
`irreversible: true` or `compensatable: false`. Squid Mesh exposes that recovery
policy in inspection and blocks replay by default after such a step completes;
council members can still replay with `allow_irreversible: true` after
reviewing the side effect.

In the Ring Errand example, the `:error` transition on `:cross_moria` is a
same-step fallback after retries are exhausted. The compensation callback is
different: it is used only if `:reserve_eagle` completes, stores reversible
reservation output, and a later step causes the run to fail.

For other reversible saga steps, declare compensation callbacks the same way:

```elixir
step :borrow_elven_rope, Lothlorien.Steps.BorrowRope,
  compensate: Lothlorien.Steps.ReturnRope

step :reserve_eagle, Eagles.Steps.ReserveRide,
  compensate: Eagles.Steps.CancelRide

step :cross_moria, Fellowship.Steps.CrossMoria,
  retry: [max_attempts: 2]

transition :borrow_elven_rope, on: :ok, to: :reserve_eagle
transition :reserve_eagle, on: :ok, to: :cross_moria
transition :cross_moria, on: :ok, to: :complete
```

When a downstream step fails after retries and the workflow has no forward
`:error` path, Squid Mesh runs completed compensation callbacks in reverse
completion order. In the example above, a failed `:cross_moria` step cancels the
eagle reservation before returning the rope, and each result is persisted under
the original step's `recovery.compensation` history.

Start the workflow through the public API and inspect the result with history:

```elixir
{:ok, run} =
  SquidMesh.start_run(MiddleEarth.Workflows.RingErrand, :leave_shire, %{
    ring_id: "one-ring"
  })

SquidMesh.inspect_run(run.run_id, include_history: true)
```

With history enabled, the inspected run includes chronological `step_runs`,
declared `steps` state, and durable `audit_events` for pause, resume, approval,
and rejection actions.

For workflows paused at a generic `:pause` step, resume with `unblock_run/2`.
For approval steps, resume through the explicit decision APIs:

```elixir
{:ok, paused_run} = SquidMesh.inspect_run(run.run_id, include_history: true)

{:ok, resumed_run} =
  SquidMesh.unblock_run(paused_run.run_id, %{
    actor: "strider",
    reason: "pipeweed restocked"
  })

# Once the run pauses at an approval step, choose one path:
{:ok, approved_run} =
  SquidMesh.approve_run(resumed_run.run_id, %{
    actor: "elrond",
    note: "approved by council"
  })

# Or reject it instead:
{:ok, rejected_run} =
  SquidMesh.reject_run(resumed_run.run_id, %{
    actor: "elrond",
    note: "too much singing"
  })
```

Runs can also be listed, cancelled, or replayed. Replay requires an explicit
override after irreversible or non-compensatable steps:

```elixir
{:ok, running_runs} = SquidMesh.list_runs(status: :running)
{:ok, cancelling_run} = SquidMesh.cancel_run(run.run_id)

{:ok, replayed_run} = SquidMesh.replay_run(run.run_id)
{:ok, reviewed_replay} = SquidMesh.replay_run(run.run_id, allow_irreversible: true)
```

Use `SquidMesh.explain_run/2` when a host app needs council-facing diagnostics:

```elixir
{:ok, explanation} = SquidMesh.explain_run(run.run_id)

explanation.reason
#=> :waiting_for_retry
```

`inspect_run/2` returns the persisted runtime facts. `explain_run/2` summarizes
the current reason, valid next actions, and evidence in a structured shape that
dashboards and CLIs can render themselves.

Graph inspection exposes the same run as UI-friendly nodes and edges:

```elixir
{:ok, graph} = SquidMesh.inspect_run_graph(run.run_id)

graph
|> SquidMesh.Runs.GraphInspection.to_map()
|> Map.take([:status, :current_node_ids, :nodes, :edges])
```

For conditional paths, the selected transition edge is marked separately from
skipped sibling edges, so a dashboard can show whether the fellowship took the
eagle branch or the Moria fallback without replaying step code.

## Documentation

Use the docs index for setup, workflow authoring, operations, and architecture:

- [Docs index](docs/index.md)
- [Host app integration](docs/host_app_integration.md)
- [Workflow authoring guide](docs/workflow_authoring.md)
- [Positioning guide](docs/positioning.md)
- [Example host app](https://github.com/dark-trench/squid_mesh/tree/main/examples/minimal_host_app)

## Community

Use the [Elixir Forum thread](https://elixirforum.com/t/squid-mesh-workflow-automation-runtime-for-elixir-applications/75162)
for public discussion and design context. Use
[GitHub issues](https://github.com/dark-trench/squid_mesh/issues) for bug
reports, feature requests, and release-tracked work. For informal runtime and
Jido-adjacent chat, use the
[Squid Mesh channel on the Jido Discord](https://discord.com/channels/1323353012235796550/1504122798027571331).

## Contributing

- [Contributing guide](CONTRIBUTING.md)
- [Code of conduct](CODE_OF_CONDUCT.md)
