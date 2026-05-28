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

Storage portability comes from the journal storage adapter contract, not from
arbitrary database compatibility. The bundled production relational path is a
Postgres-compatible Ecto adapter; see the
[storage strategy](docs/storage_strategy.md) for the required adapter
guarantees.

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

Squid Mesh uses Jido as an internal runtime foundation while keeping the public workflow API focused on Squid Mesh concepts. Production runtime code uses these main Jido primitive families:

| Jido primitive | Squid Mesh use |
| --- | --- |
| `Jido.Agent` | Rebuildable workflow and dispatch coordination state. |
| `Jido.Action` | Step execution interop, including raw Jido action modules and the native `SquidMesh.Step` adapter. |
| `Jido.Storage` | Journal and checkpoint persistence boundary. |
| `Jido.Thread` / `Jido.Thread.Entry` | Durable journal facts for run, dispatch, index, and catalog threads. |
| `Jido.Exec` | Action execution inside the journal executor. |
| `Jido.Signal` | Interop envelope for Squid Mesh runtime signals when agents or other Jido primitives need to exchange commands/events. |

Support code also touches lower-level details such as `Jido.Thread.EntryNormalizer` and validates built-in storage adapters like `Jido.Storage.File` and `Jido.Storage.Redis`. Workflow authors normally do not need to use those primitives directly.

Runtime command signals use `SquidMesh.Runtime.Signal` as the stable Squid Mesh contract. Signals are the natural internal command/event shape for runtime control; `SquidMesh.Runtime.Signal.JidoAdapter` converts those structs to and from `Jido.Signal` envelopes when agents or other Jido primitives need to participate, while public callers can stay on Squid Mesh APIs.

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
  repo: Acme.Repo,
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

A compact order fulfillment workflow combines manual gates, approval flows, conditional routing, retries, saga compensation, irreversible steps, and built-in adapters:

```elixir
defmodule Acme.Workflows.OrderFulfillment do
  use SquidMesh.Workflow

  workflow do
    trigger :checkout_submitted do
      manual()

      payload do
        field :order_id, :string
        field :customer_id, :string
        field :total_cents, :integer
        field :expedite, :boolean, default: false
      end
    end

    step :reserve_inventory, Warehouse.Steps.ReserveInventory,
      input: [:order_id],
      output: :inventory_hold,
      transaction: :repo,
      compensate: Warehouse.Steps.ReleaseInventory

    step :screen_payment, Risk.Steps.ScreenPayment,
      input: [:customer_id, :total_cents],
      output: :risk,
      retry: [
        max_attempts: 3,
        backoff: [type: :exponential, min: 1_000, max: 10_000]
      ]

    step :manual_review, :pause

    approval_step :capture_approval,
      output: :approval

    step :capture_payment, Payments.Steps.Capture,
      input: [:order_id, :total_cents, :inventory_hold, approval: [:approval, :decision]],
      output: :payment,
      compensate: Payments.Steps.Refund

    step :book_shipment, Shipping.Steps.BookShipment,
      input: [:order_id, :inventory_hold, expedite: [:expedite]],
      output: :shipment,
      retry: [max_attempts: 2],
      compensate: Shipping.Steps.CancelShipment

    step :send_receipt, Notifications.Steps.SendReceipt,
      input: [:order_id, :payment, :shipment],
      compensatable: false

    step :handoff_to_carrier, Shipping.Steps.HandoffToCarrier,
      input: [:shipment],
      irreversible: true

    step :cancel_order, Orders.Steps.CancelOrder

    transition :reserve_inventory, on: :ok, to: :screen_payment
    transition :reserve_inventory, on: :error, to: :cancel_order

    transition :screen_payment,
      on: :ok,
      to: :capture_payment,
      condition: [path: [:risk, :decision], equals: "approve"]

    transition :screen_payment,
      on: :ok,
      to: :manual_review,
      condition: [path: [:risk, :decision], equals: "review"]

    transition :screen_payment, on: :error, to: :cancel_order
    transition :manual_review, on: :ok, to: :capture_approval
    transition :manual_review, on: :error, to: :cancel_order
    transition :capture_approval, on: :ok, to: :capture_payment
    transition :capture_approval, on: :error, to: :cancel_order
    transition :capture_payment, on: :ok, to: :book_shipment
    transition :capture_payment, on: :error, to: :cancel_order, recovery: :undo
    transition :book_shipment, on: :ok, to: :send_receipt
    transition :book_shipment, on: :error, to: :cancel_order, recovery: :undo
    transition :send_receipt, on: :ok, to: :handoff_to_carrier
    transition :handoff_to_carrier, on: :ok, to: :complete
    transition :cancel_order, on: :ok, to: :complete
  end
end
```

Cron-triggered workflows follow the same shape, with scheduling and activation remaining host-owned:

```elixir
defmodule Acme.Workflows.InventoryAudit do
  use SquidMesh.Workflow

  workflow do
    trigger :nightly_inventory_audit do
      cron "0 21 * * *", timezone: "Etc/UTC"

      payload do
        field :warehouse_id, :string, default: "primary"
        field :low_stock_threshold, :integer, default: 10
      end
    end

    step :scan_stock, Warehouse.Steps.ScanStock,
      retry: [max_attempts: 5]

    step :open_restock_orders, Warehouse.Steps.OpenRestockOrders,
      compensate: Warehouse.Steps.CloseRestockOrders

    step :log_audit, :log,
      message: "nightly inventory audit completed",
      level: :info

    transition :scan_stock, on: :ok, to: :open_restock_orders
    transition :open_restock_orders, on: :ok, to: :log_audit
    transition :log_audit, on: :ok, to: :complete
  end
end
```

Dependency-based workflows use `after: [...]` instead of explicit transitions. A step becomes runnable only after all declared dependencies complete:

```elixir
defmodule Acme.Workflows.FulfillmentFanout do
  use SquidMesh.Workflow

  workflow do
    trigger :prepare_fulfillment do
      manual()

      payload do
        field :order_id, :string
      end
    end

    step :print_pick_list, Warehouse.Steps.PrintPickList
    step :reserve_packaging, Warehouse.Steps.ReservePackaging
    step :rate_shipments, Shipping.Steps.RateShipments

    step :release_to_floor, Warehouse.Steps.ReleaseToFloor,
      after: [:print_pick_list, :reserve_packaging, :rate_shipments],
      irreversible: true
  end
end
```

## Running Workflows

Runs start through the public API:

```elixir
{:ok, run} =
  SquidMesh.start(
    Acme.Workflows.OrderFulfillment,
    :checkout_submitted,
    %{order_id: "ord_123", customer_id: "cus_456", total_cents: 12_500}
  )
```

Inspection APIs keep explicit names such as `inspect_run/2`,
`inspect_run_graph/2`, and `explain_run/2` to avoid confusion with Elixir's
`inspect/2`.

Public start, replay, and control helpers use concise names: `start/3`,
`resume/3`, `approve/3`, `reject/3`, `cancel/2`, and `replay/2`. Runtime signal
constructors such as `Signal.approve_run/3` keep run-suffixed names because
those names describe persisted command intent.

Run inspection includes full step history, audit events, and approval history:

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
SquidMesh.resume(run.run_id, %{actor: "ops", reason: "manual review complete"})
```

For formal approval gates:

```elixir
SquidMesh.approve(run.run_id, %{actor: "fraud_analyst", note: "capture approved"})
SquidMesh.reject(run.run_id, %{actor: "fraud_analyst", note: "suspected fraud"})
```

Host apps that already normalize operator commands at their own boundary can
build explicit runtime signals and apply them through the same journal
interpreter used by the public control functions:

```elixir
alias SquidMesh.Runtime.Signal

{:ok, signal} =
  Signal.approve_run(run.run_id, %{actor: "fraud_analyst", note: "capture approved"},
    metadata: %{source: "acme.workflow_runs"},
    idempotency_key: "capture-approval-#{run.run_id}"
  )

{:ok, approved_run} = SquidMesh.apply_signal(signal)
```

The same pattern applies to `Signal.start_run/4`, `Signal.start_cron/4`,
`Signal.replay_run/2`, `Signal.resume_run/3`, `Signal.reject_run/3`, and
`Signal.cancel_run/2`. Keep using the named helpers such as `SquidMesh.start/3`
and `SquidMesh.cancel/2` for ordinary application calls; use
`SquidMesh.apply_signal/2` when an agent, router, webhook, scheduler, or Jido
interop boundary has already produced a signal envelope. Reusing an idempotency
key makes duplicate command delivery return the already-applied run state
without appending another command receipt.

Workflow definitions are authored with the Squid Mesh DSL. Runtime signals
start, replay, cancel, or resolve runs of those definitions.

Approval steps persist their resolved `:ok` and `:error` targets along with output-mapping metadata, so paused review flows survive deploys and restarts without semantic drift.

## Compensation and Recovery

When a downstream step fails after retries and the workflow has no forward `:error` route, Squid Mesh executes completed compensation callbacks in reverse completion order. Saga-style steps attach the callback directly on the step:

```elixir
step :reserve_inventory, Warehouse.Steps.ReserveInventory,
  compensate: Warehouse.Steps.ReleaseInventory

step :capture_payment, Payments.Steps.Capture,
  compensate: Payments.Steps.Refund

step :book_shipment, Shipping.Steps.BookShipment,
  retry: [max_attempts: 2]
```

A failed `:book_shipment` after retry exhaustion refunds the payment before releasing the inventory hold. Each compensation result is persisted under the originating step's `recovery.compensation` history.

External side effects that cannot be honestly reversed use `irreversible: true` or `compensatable: false`. Squid Mesh exposes these recovery boundaries during inspection and blocks replay by default after irreversible execution unless explicitly overridden.

## Child Workflows

Native steps can start durable child workflow runs when runtime data expands the scope of work. The parent step passes its `SquidMesh.Step.Context` and a stable `child_key`:

```elixir
defmodule Acme.Steps.StartSupplierOrders do
  use SquidMesh.Step, name: :start_supplier_orders

  @impl true
  def run(%{order_id: order_id, suppliers: suppliers}, %SquidMesh.Step.Context{} = context) do
    children =
      for supplier <- suppliers do
        {:ok, child} =
          SquidMesh.start_child_run(
            context,
            Acme.Workflows.SupplierOrder,
            %{order_id: order_id, supplier_id: supplier.id},
            child_key: "supplier_#{supplier.id}"
          )

        child.run_id
      end

    {:ok, %{supplier_run_ids: children}}
  end
end
```

Each child is a normal journal run with its own inspection, retry, replay, and cancellation boundary. Repeating the same parent step and `child_key` returns the existing child run instead of creating duplicate lineage.

## Cancellation, Replay, and Listing

```elixir
{:ok, running_runs} = SquidMesh.list_runs(status: :running)
{:ok, _} = SquidMesh.cancel(run.run_id)
{:ok, _} = SquidMesh.replay(run.run_id)

# Replay past irreversible steps requires an explicit override
{:ok, _} = SquidMesh.replay(run.run_id, allow_irreversible: true)
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
