<div align="center">

# SquidMesh — Durable workflow runtime for Elixir

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

Squid Mesh is an embedded durable workflow runtime for Elixir applications. Workflows are declared as Elixir modules through a DSL, persisted through Jido journals, and executed by host-owned workers calling `SquidMesh.execute_next/1`.

The runtime stores workflow state, step attempts, retries, approvals, transitions, audit events, and recovery history inside the host application's database through `Jido.Storage` and the default Ecto adapter. Squid Mesh does not run as a separate service, broker, or orchestration cluster — the host application keeps its existing supervision tree, deployment model, repository, schedulers, and queue backend.

Squid Mesh owns workflow progression, transition routing, retry semantics, pause and approval handling, replay and recovery policy, durable execution history, and graph inspection. Queue delivery, worker supervision, and backend leasing remain host-owned concerns.

Internally, the runtime builds on [Jido](https://github.com/agentjido/jido) for actions, execution, and journaling; [Runic](https://github.com/dark-trench/runic) for workflow planning; and [Spark](https://github.com/ash-project/spark) for the DSL authoring surface.

> **Warning**
> Squid Mesh is still in early development. The runtime is suitable for evaluation, local development, and integration work, but it is not yet documented as production-ready. See [Production Readiness](docs/production_readiness.md) for the current checklist and remaining items.

## Start Here

The quickest first run is the guided Livebook. It creates a small workflow, starts a journal-backed run, drains visible work with `SquidMesh.execute_next/1`, and inspects the durable result without making you read the full workflow reference first.

[![Run in Livebook](https://livebook.dev/badge/v1/pink.svg)](https://livebook.dev/run?url=https%3A%2F%2Fgithub.com%2Fdark-trench%2Fsquid_mesh%2Fblob%2Fmain%2Fdocs%2Fgetting_started.livemd)

| If you want to... | Start with |
| --- | --- |
| Run the smallest guided example | [Getting Started Livebook](docs/getting_started.livemd) |
| Add Squid Mesh to a host app | [Getting Started guide](docs/getting_started.md) |
| Inspect a working app harness | [Minimal host app](examples/minimal_host_app/README.md) |

The written guide follows the same path in a little more detail: installation, one workflow, draining journal attempts, inspecting the run, then adding retries, manual gates, cron, and Bedrock-backed leases only when those pieces are useful.

## Jido Primitive Boundary

Squid Mesh uses Jido as an internal runtime foundation while keeping the public workflow API focused on Squid Mesh concepts. Production runtime code uses five main Jido primitive families:

| Jido primitive | Squid Mesh use |
| --- | --- |
| `Jido.Agent` | Rebuildable workflow and dispatch coordination state. |
| `Jido.Action` | Step execution interop, including raw Jido action modules and the native `SquidMesh.Step` adapter. |
| `Jido.Storage` | Journal and checkpoint persistence boundary. |
| `Jido.Thread` / `Jido.Thread.Entry` | Durable journal facts for run, dispatch, index, and catalog threads. |
| `Jido.Exec` | Action execution inside the journal executor. |
| `Jido.Signal` | Optional boundary envelope for internal Squid Mesh runtime command signals. |

Support code also touches lower-level details such as `Jido.Thread.EntryNormalizer` and validates built-in storage adapters like `Jido.Storage.File` and `Jido.Storage.Redis`. Workflow authors normally do not need to use those primitives directly.

Runtime command signals use `SquidMesh.Runtime.Signal` as the stable Squid Mesh contract. `SquidMesh.Runtime.Signal.JidoAdapter` can convert those structs to and from `Jido.Signal` envelopes for advanced runtime integration, while public callers stay on Squid Mesh APIs.

Journal-backed runtime commands are also persisted as run-thread command
receipts before their lifecycle facts. `SquidMesh.inspect_run/2` exposes those
receipts through `snapshot.command_history`, including the command signal type,
payload, actor/comment when supplied, redacted metadata, idempotency key when
relevant, and occurrence time.

## Getting Started

After the first run, use these references to go deeper:

| Reference | Use when |
| --- | --- |
| [Getting Started](docs/getting_started.md) | You want the shortest written setup and run path. |
| [Workflow Authoring](docs/workflow_authoring.md) | You want to understand triggers, payloads, steps, transitions, dependencies, input mapping, retries, and compensation. |
| [Host App Integration](docs/host_app_integration.md) | You are adding Squid Mesh to a Phoenix or OTP app. |
| [Reference Workflows](docs/reference_workflows.md) | You want approval, recovery, dependency, saga, cron, restart, and soak examples. |
| [Minimal Host App](examples/minimal_host_app/README.md) | You want the executable example app used for smoke testing. |
| [Bedrock Minimal Host App](examples/bedrock_minimal_host_app/README.md) | You want backend-owned delivery, leases, delayed visibility, retry requeue, and dead-letter handling. |
| [Architecture](docs/architecture.md) | You want the runtime flow diagram and component boundaries. |
| [Positioning Guide](docs/positioning.md) | You want to understand how Squid Mesh compares to adjacent projects. |

## Installation

Add Squid Mesh to your dependencies:

```elixir
defp deps do
  [
    {:squid_mesh, "~> 0.1.0-beta.3"}
  ]
end
```

If your host application defines raw `Jido.Action` modules directly, add `:jido` explicitly as well:

```elixir
defp deps do
  [
    {:jido, "~> 2.0"},
    {:squid_mesh, "~> 0.1.0-beta.3"}
  ]
end
```

Configure the repo and default queue:

```elixir
config :squid_mesh,
  repo: MiddleEarth.Repo,
  queue: "default"
```

Install and run the migration:

```sh
mix deps.get
mix squid_mesh.install
mix ecto.migrate
```

To keep workflow modules formatted consistently as DSL-style declarations, import Squid Mesh formatter rules in `.formatter.exs`:

```elixir
[
  import_deps: [:squid_mesh],
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"]
]
```

Finally, start one supervised worker loop. See [Host App Integration](docs/host_app_integration.md) for a minimal worker shape.

## Workflows

Workflows are Elixir modules. A trigger declares the entrypoint and validates the payload before the run is persisted. Steps declare their inputs, outputs, retry policy, and compensation behaviour. Transitions wire them together.

The Ring Errand below is the canonical example — a quest with manual gates, approval flows, conditional routing, retries, saga compensation, and irreversible steps:

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

Cron-triggered workflows follow the same shape, with scheduling and activation remaining host-owned:

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

Dependency-based workflows use `after: [...]` instead of explicit transitions. A step becomes runnable only after all declared dependencies complete:

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

## Running Workflows

Start a run through the public API:

```elixir
{:ok, run} =
  SquidMesh.start_run(
    MiddleEarth.Workflows.RingErrand,
    :leave_shire,
    %{ring_id: "one-ring"}
  )
```

Inspect a run with its full step history, audit events, and approval history:

```elixir
SquidMesh.inspect_run(run.run_id, include_history: true)
```

When a run needs an operator-facing explanation of its current state:

```elixir
{:ok, explanation} = SquidMesh.explain_run(run.run_id)
explanation.reason #=> :waiting_for_retry
```

`explain_run/2` summarizes the current runtime reason, valid next actions, and supporting evidence. It is designed for dashboards, CLIs, and operational tooling.
When command receipt facts are present, `explanation.details.latest_command`
shows the most recent runtime command and `explanation.evidence.command_history`
keeps the redacted command audit trail. `explanation.evidence.command_counts`
summarizes redacted command deliveries per signal type, while
`explanation.evidence.duplicate_commands` highlights duplicate command
evidence:

```elixir
explanation.evidence.command_counts
#=> %{"start_run" => 1, "cancel_run" => 2}
```

## Approvals and Manual Gates

Approval steps and pause steps block forward progression until explicitly resolved. For generic pause steps:

```elixir
SquidMesh.unblock_run(run.run_id, %{actor: "strider", reason: "pipeweed restocked"})
```

For formal approval gates:

```elixir
SquidMesh.approve_run(run.run_id, %{actor: "elrond", note: "approved by council"})
SquidMesh.reject_run(run.run_id, %{actor: "elrond", note: "too much singing"})
```

Host apps that already normalize operator commands at their own boundary can
build explicit runtime signals and apply them through the same journal
interpreter used by the public wrappers:

```elixir
alias SquidMesh.Runtime.Signal

{:ok, signal} =
  Signal.approve_run(run.run_id, %{actor: "elrond", note: "approved by council"},
    metadata: %{source: "middle_earth.workflow_runs"},
    idempotency_key: "council-approval-#{run.run_id}"
  )

{:ok, approved_run} = SquidMesh.apply_signal(signal)
```

The same pattern applies to `Signal.resume_run/3`, `Signal.reject_run/3`, and
`Signal.cancel_run/2`. Reusing an idempotency key makes duplicate command
delivery return the already-applied run state without appending another command
receipt.

Approval steps persist their resolved `:ok` and `:error` targets along with output-mapping metadata, so paused review flows survive deploys and restarts without semantic drift.

## Compensation and Recovery

When a downstream step fails after retries and the workflow has no forward `:error` route, Squid Mesh executes completed compensation callbacks in reverse completion order. For saga-style steps, declare the callback directly on the step:

```elixir
step :borrow_elven_rope, Lothlorien.Steps.BorrowRope,
  compensate: Lothlorien.Steps.ReturnRope

step :reserve_eagle, Eagles.Steps.ReserveRide,
  compensate: Eagles.Steps.CancelRide

step :cross_moria, Fellowship.Steps.CrossMoria,
  retry: [max_attempts: 2]
```

A failed `:cross_moria` after retry exhaustion cancels the eagle reservation before returning the rope. Each compensation result is persisted under the originating step's `recovery.compensation` history.

For external side effects that cannot be honestly reversed, mark the step `irreversible: true` or `compensatable: false`. Squid Mesh exposes these recovery boundaries during inspection and blocks replay by default after irreversible execution unless explicitly overridden.

## Child Workflows

Native steps can start durable child workflow runs when runtime data expands the scope of work. The parent step must pass its `SquidMesh.Step.Context` and a stable `child_key`:

```elixir
defmodule Hobbiton.Steps.SendPartyInvites do
  use SquidMesh.Step, name: :send_party_invites

  @impl true
  def run(%{party_id: party_id, guests: guests}, %SquidMesh.Step.Context{} = context) do
    children =
      for guest <- guests do
        {:ok, child} =
          SquidMesh.start_child_run(
            context,
            Hobbiton.Workflows.DeliverInvite,
            %{party_id: party_id, guest_id: guest.id},
            child_key: "invite_#{guest.id}"
          )

        child.run_id
      end

    {:ok, %{invite_run_ids: children}}
  end
end
```

Each child is a normal journal run with its own inspection, retry, replay, and cancellation boundary. Repeating the same parent step and `child_key` returns the existing child run instead of creating duplicate lineage.

## Cancellation, Replay, and Listing

```elixir
{:ok, running_runs} = SquidMesh.list_runs(status: :running)
{:ok, _} = SquidMesh.cancel_run(run.run_id)
{:ok, _} = SquidMesh.replay_run(run.run_id)

# Replay past irreversible steps requires an explicit override
{:ok, _} = SquidMesh.replay_run(run.run_id, allow_irreversible: true)
```

## Graph Inspection

Graph inspection exposes the workflow as UI-friendly nodes and edges. For conditional paths, the selected transition edge is marked separately from sibling edges, allowing dashboards to visualize execution flow directly from persisted runtime state:

```elixir
{:ok, graph} = SquidMesh.inspect_run_graph(run.run_id)

graph
|> SquidMesh.Runs.GraphInspection.to_map()
|> Map.take([:status, :current_node_ids, :nodes, :edges])
```

## Optional Dashboard

[SquidSonar](https://github.com/dark-trench/squid_sonar) is the optional read-only Phoenix LiveView dashboard for Squid Mesh. Mount it inside a Phoenix host application to inspect recent runs, filter by status, search runtime metadata, and view run detail pages with diagnosis, history counts, last error information, and workflow graph visualization.

## Contributing

Please review the existing runtime model and workflow semantics before proposing substantial changes. Contributions are most welcome in: runtime reliability, workflow ergonomics, inspection tooling, recovery semantics, documentation improvements, backend integrations, and executable examples.

- [Contributing Guide](CONTRIBUTING.md)
- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Elixir Forum discussion thread](https://elixirforum.com/t/squid-mesh-workflow-automation-runtime-for-elixir-applications/75162)
- [GitHub Issues](https://github.com/dark-trench/squid_mesh/issues)
- [Squid Mesh channel on the Jido Discord](https://discord.com/channels/1323353012235796550/1504122798027571331)

## License

Copyright 2024, released under the [Apache 2.0 License](LICENSE).
