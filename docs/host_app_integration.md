# Host App Integration

This document defines the initial integration contract for:

- Phoenix applications
- OTP applications with an existing `Repo`
- existing installations that already run background jobs

## Tested Toolchain

Current CI and onboarding smoke tests run with:

- Erlang/OTP `28.4.1`
- Elixir `1.19.5-otp-28`
- `Jido 2.0+`

## Installation

Add `:squid_mesh` to the host application's dependencies and fetch dependencies
as usual with Mix.

Preferred Hex dependency:

```elixir
defp deps do
  [
    {:squid_mesh, "~> 0.1.0-alpha.7"}
  ]
end
```

If the host app defines custom steps with `use Jido.Action`, add `:jido`
explicitly to the host app as well rather than relying on a transitive
dependency:

```elixir
defp deps do
  [
    {:jido, "~> 2.0"},
    {:squid_mesh, "~> 0.1.0-alpha.7"}
  ]
end
```

Then install Squid Mesh's library-owned migrations into the host app:

```sh
mix squid_mesh.install
mix ecto.migrate
```

`mix squid_mesh.install` creates one current-schema Squid Mesh migration in the
host application's `priv/repo/migrations` directory. It does not install or run
migrations for the host application's job backend.

## Configuration

Start with three pieces:

1. Squid Mesh config points at the host repo and runtime boundary.
2. The journal runtime owns its dispatch queue through Squid Mesh config; the
   executor config only matters for hosts still exercising the table runtime.
3. Journal workers call `SquidMesh.execute_next/1` to claim and execute visible
   attempts.

The host application configures Squid Mesh under the `:squid_mesh` application:

```elixir
config :squid_mesh,
  repo: MyApp.Repo,
  queue: "default"
```

Required keys:

- `:repo` - the Ecto repo Squid Mesh uses for persisted runtime state

Optional keys:

- `:runtime` - `:journal` by default; routes public start, execution, and
  manual-control APIs through the Jido-native journal runtime
- `:read_model` - `:read_model` by default; routes inspection, graph
  inspection, and explanation through journal projections
- `:journal_storage` - optional for the default Ecto-backed setup; when omitted,
  Squid Mesh uses `{SquidMesh.Runtime.Journal.Storage.Ecto, repo: MyApp.Repo}`.
  Set it only to override the storage adapter. Explicit `nil` is rejected for
  journal-backed runtime or read-model paths.
- `:queue` - `"default"` by default; selects the journal dispatch queue used by
  the configured journal runtime and read model
- `:executor` - required only when `:runtime` is explicitly
  `:runtime_tables`; this host module implements `SquidMesh.Executor`
- `:stale_step_timeout` - table-runtime-only; `:disabled` by default. Set a
  non-negative millisecond timeout only when using legacy runtime tables and
  redelivered jobs need to reclaim stale `running` steps after worker
  interruption

For most host apps, the inferred Ecto storage is the recommended starting point
when `MyApp.Repo` uses Postgres or a Postgres-compatible Ecto adapter. It
persists Jido threads and checkpoints in Squid Mesh's installed tables and keeps
journal storage in the same transactional database boundary as the host app. The
boundary remains adapter-shaped, so other Jido-compatible stores can be used
later, but production stores must still provide ordered per-thread appends,
durable checkpoint reads, and conflict detection for `:expected_rev`.

The current journal default covers start, cancellation, global and
workflow-filtered `list_runs/2`, inspect, explain, graph inspection, manual
resume/approval controls, and `SquidMesh.execute_next/1`. Journal listing is
backed by a durable run catalog fact rather than a storage-adapter scan, and
returns redacted summaries; use `inspect_run/2` for one run when a caller needs
inputs, outputs, attempts, or claim metadata. Dashboards can call `list_runs([])`
for the index view, then pass the selected summary's `run_id` and `queue` to
`inspect_run(run_id, queue: queue, include_history: true)` or
`inspect_run_graph(run_id, queue: queue)` for detail views. The remaining
table-only APIs return explicit
`{:unsupported_runtime, {:journal, operation}}` errors under the journal default
until their journal implementations land: `replay_run/2` and cron starts
delivered through `SquidMesh.Runtime.Runner`.

## Executor Contract

For the runtime-table path, the host executor is the queue boundary Squid Mesh
calls. Copy this module, replace `MyApp.JobQueue.enqueue/1` with the host app's
job backend, and keep the queued job generic:

```elixir
defmodule MyApp.SquidMeshExecutor do
  @behaviour SquidMesh.Executor

  alias SquidMesh.Executor.Payload

  def enqueue_step(_config, run, step, opts) do
    run
    |> Payload.step(step)
    |> enqueue(opts)
  end

  def enqueue_steps(config, run, steps, opts) do
    steps
    |> Enum.reduce_while({:ok, []}, fn step, {:ok, metadata} ->
      case enqueue_step(config, run, step, opts) do
        {:ok, job_metadata} -> {:cont, {:ok, [job_metadata | metadata]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, metadata} -> {:ok, Enum.reverse(metadata)}
      {:error, reason} -> {:error, reason}
    end
  end

  def enqueue_compensation(_config, run, opts) do
    run
    |> Payload.compensation()
    |> enqueue(opts)
  end

  def enqueue_cron(_config, workflow, trigger, opts) do
    workflow
    |> Payload.cron(trigger, Keyword.take(opts, [:signal_id, :intended_window]))
    |> enqueue(opts)
  end

  defp enqueue(payload, opts) do
    job = %{payload: payload, queue: queue(), schedule_in: opts[:schedule_in]}

    case MyApp.JobQueue.enqueue(job) do
      {:ok, job} ->
        {:ok, %{job_id: job.id, queue: job.queue, schedule_in: opts[:schedule_in]}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp queue do
    :my_app
    |> Application.get_env(__MODULE__, [])
    |> Keyword.get(:queue, :squid_mesh)
  end
end
```

The executor callbacks receive:

- `run` - the persisted Squid Mesh run
- `step` - the next step name, for step jobs
- `workflow` and `trigger` - the cron workflow activation target
- `opts[:schedule_in]` - seconds to delay a retry or wait continuation
- `opts[:signal_id]` - optional stable scheduler signal id for a cron activation
- `opts[:intended_window]` - optional logical schedule window for a cron activation

Return `{:ok, metadata}` after enqueueing. Metadata is included in dispatch
telemetry, so useful values are `:job_id`, `:queue`, `:worker`, and
`:schedule_in`.

The queued job should deliver the stored payload back to Squid Mesh without
knowing workflow details:

```elixir
defmodule MyApp.SquidMeshJob do
  def perform(%{payload: payload}) do
    SquidMesh.Runtime.Runner.perform(payload)
  end
end
```

`MyApp.JobQueue` is intentionally a placeholder. In a real host app, replace it
with the app's durable job backend and make sure delayed jobs honor
`:schedule_in`. Cron activation is also host-owned; the host scheduler should
call `enqueue_cron/4` or enqueue `SquidMesh.Executor.Payload.cron/3`.

When a scheduler can provide deterministic schedule metadata, pass it with the
cron payload instead of adding it to workflow input:

```elixir
Payload.cron(MyApp.Workflows.DailyStandup, :daily_standup,
  signal_id: "daily-standup:2026-05-15T09:00:00Z",
  intended_window: %{
    start_at: "2026-05-15T09:00:00Z",
    end_at: "2026-05-15T10:00:00Z"
  }
)
```

Squid Mesh persists this under `run.context.schedule` before workflow
processing. Steps can read it from `context.state.schedule`, and inspection or
explanation surfaces can show the intended window separately from actual worker
receive time.

If the workflow declares `cron ..., idempotency: :return_existing_run` or
`idempotency: :skip_duplicate`, the scheduler identity also becomes the start
idempotency key. Duplicate delivery of the same workflow, trigger, and key will
not insert a second run. Idempotent cron starts must include `signal_id` or a
complete `intended_window`; otherwise Squid Mesh returns
`{:error, {:missing_schedule_idempotency_key, trigger_name}}`.

That is the whole execution contract for the current runtime path. Workflow
modules, context modules, and controllers should not need to know which job
backend the executor uses.

## Optional Lease Executor Contract

Backends that expose worker leases can also implement
`SquidMesh.Executor.Leases`. This is separate from the queue executor: it claims
visible work, heartbeats active claims, completes delivered work, and returns
failed work to the backend's retry or dead-letter policy.

The current runtime does not require a lease executor. The behavior exists so
Bedrock, IntentLedger, or another durable backend can expose lease semantics
through a stable Squid Mesh boundary while the Jido-native dispatch path evolves.

## Bedrock Lease Backend Setup

Squid Mesh stays executor-agnostic: workflow modules and runtime state do not
depend on Bedrock APIs. For hosts that want backend-owned leasing today, Bedrock
is the recommended reference backend because it already owns durable delivery,
delayed visibility, leases, heartbeats, retry timing, and recovery. That same
ownership model is also a better foundation for distributed workflows, where
multiple workers may claim, heartbeat, fail, or recover work across process and
node boundaries.

Use `examples/bedrock_minimal_host_app` as the concrete setup guide. The example
keeps the storage boundary explicit:

- `BedrockMinimalHostApp.Repo` stores Squid Mesh workflow and attempt state.
- `BedrockMinimalHostApp.JobQueue` stores queue items, delayed visibility,
  leases, retries, and queue metadata.
- `BedrockMinimalHostApp.SquidMeshExecutor` adapts Squid Mesh enqueue calls to
  Bedrock Job Queue.
- `BedrockMinimalHostApp.SquidMeshLeaseExecutor` adapts Bedrock claims,
  heartbeats, completion, and failure to `SquidMesh.Executor.Leases`.

A host app using the same shape should:

1. Configure `:squid_mesh` with the host repo and Squid Mesh executor module.
2. Configure the executor's Bedrock queue id and topic.
3. Start the host repo, Bedrock cluster, and Bedrock job queue under
   supervision.
4. Keep `:stale_step_timeout` disabled so Bedrock owns stale-worker recovery.
5. Keep workflow definitions backend-neutral; only the host executor modules
   should know Bedrock exists.

The example config shape is:

```elixir
config :my_app, MyApp.SquidMeshExecutor,
  queue_id: "tenant_a",
  topic: "squid_mesh:payload"

config :squid_mesh,
  repo: MyApp.Repo,
  executor: MyApp.SquidMeshExecutor
```

To verify the reference path locally:

```sh
cd examples/bedrock_minimal_host_app
mix setup
MIX_ENV=test mix test test/bedrock_job_queue_stress_test.exs test/bedrock_minimal_host_app/squid_mesh_lease_executor_test.exs
```

That test path covers Bedrock queue behavior plus the lease executor contract.
It does not make Bedrock a required Squid Mesh dependency; another durable
executor can use the same Squid Mesh boundaries if it provides equivalent lease,
heartbeat, retry, and recovery semantics.

For background on why durable workflow systems often benefit from queueing close
to the data and tenancy model they serve, see Apple's
[QuiCK: A Queuing System in CloudKit](https://www.foundationdb.org/files/QuiCK.pdf)
paper.

## First Run Checklist

For a new integration, the shortest path to a successful first run is:

1. Add `:squid_mesh` to the host app's dependencies.
2. Add or confirm a working Postgres-backed `Repo`.
3. Run `mix squid_mesh.install`.
4. Run `mix ecto.migrate`.
5. Configure `:squid_mesh` with the host app's `Repo`.
6. Start the host app's `Repo` under supervision.
7. Start one workflow through the public API, execute visible attempts with
   `SquidMesh.execute_next/1`, and inspect it with history enabled.

Add a host executor and job system only when explicitly using
`runtime: :runtime_tables` or validating a legacy executor scenario.

## Existing Application Setup

For an existing Phoenix or OTP application:

1. Add the `:squid_mesh` dependency.
2. Configure `:repo` to point at the app's existing repo.
3. Call `SquidMesh.config!/0` during boot or integration setup to verify the
   required contract is present.
4. Integrate Squid Mesh from the host application's contexts, services,
   controllers, or internal APIs.

The host application is responsible for:

- database setup and migrations
- journal worker lifecycle for `SquidMesh.execute_next/1`
- any HTTP or internal API endpoints exposed to end users

That means the embedded install path assumes:

- the host app already owns its `Repo`
- the host app starts workers that call `SquidMesh.execute_next/1`
- the host app adds executor and job-system tables only for explicit table-runtime
  integrations

## Minimal OTP Host Skeleton

For a plain OTP application, the minimum moving pieces are:

- a `Repo` module
- `Repo` in the application supervision tree
- a supervised worker that periodically calls `SquidMesh.execute_next/1`
- `:squid_mesh` configuration pointing at that `Repo`
- one host-facing module that calls `SquidMesh`

Dependency shape:

```elixir
defp deps do
  [
    {:ecto_sql, "~> 3.13"},
    {:postgrex, "~> 0.20"},
    {:jido, "~> 2.0"},
    {:squid_mesh, "~> 0.1.0-alpha.7"}
  ]
end
```
Add the host job backend separately.

Application supervision shape:

```elixir
children = [
  MyApp.Repo,
  MyApp.JobQueue
]
```

Host-facing boundary:

```elixir
defmodule MyApp.WorkflowRuns do
  def start_payment_recovery(payload) do
    SquidMesh.start_run(MyApp.Workflows.PaymentRecovery, :payment_recovery, payload)
  end

  def inspect_run(run_id) do
    SquidMesh.inspect_run(run_id, include_history: true)
  end

  def unblock_run(run_id, attrs \\ %{}) do
    SquidMesh.unblock_run(run_id, attrs)
  end

  def approve_run(run_id, attrs) do
    SquidMesh.approve_run(run_id, attrs)
  end

  def reject_run(run_id, attrs) do
    SquidMesh.reject_run(run_id, attrs)
  end
end
```

If the host app exposes pause-resume or approval workflows, keep the latest
Squid Mesh migrations applied before deploying the feature. Paused step runs
now persist internal resume metadata so `unblock_run/2`, `approve_run/3`, and
`reject_run/3` can continue with stable output and transition semantics after
restarts or code changes.

Operational review shape:

```elixir
{:ok, paused_run} = MyApp.WorkflowRuns.inspect_run(run_id)

Enum.map(paused_run.audit_events, &{&1.type, &1.step})
#=> [{:paused, :wait_for_review}]

{:ok, _run} =
  MyApp.WorkflowRuns.approve_run(run_id, %{
    actor: "ops_123",
    comment: "customer verified",
    metadata: %{ticket: "SUP-42"}
  })

{:ok, completed_run} = MyApp.WorkflowRuns.inspect_run(run_id)

Enum.map(completed_run.audit_events, &{&1.type, &1.actor, &1.comment})
#=> [{:paused, nil, nil}, {:approved, "ops_123", "customer verified"}]
```

`include_history: true` is the public audit boundary. With history enabled, the
run includes chronological `step_runs`, declared `steps` state, and durable
`audit_events` for pause, resume, approval, and rejection actions.

## Minimal Phoenix Host Skeleton

A Phoenix application uses the same runtime contract. The main difference is
that Squid Mesh usually sits behind a context or controller boundary.

Typical shape:

- add `:squid_mesh` and `:jido` to the Phoenix app
- keep using the Phoenix app's existing `Repo`
- start a supervised worker that calls `SquidMesh.execute_next/1`
- configure `:squid_mesh` to use that `Repo`
- expose workflow operations through a context or controller

Context boundary:

```elixir
defmodule MyApp.WorkflowRuns do
  def start_payment_recovery(attrs) do
    SquidMesh.start_run(MyApp.Workflows.PaymentRecovery, :payment_recovery, attrs)
  end

  def inspect_run(run_id) do
    SquidMesh.inspect_run(run_id, include_history: true)
  end

  def unblock_run(run_id, attrs \\ %{}) do
    SquidMesh.unblock_run(run_id, attrs)
  end

  def approve_run(run_id, attrs) do
    SquidMesh.approve_run(run_id, attrs)
  end

  def reject_run(run_id, attrs) do
    SquidMesh.reject_run(run_id, attrs)
  end
end
```

Controller shape:

```elixir
def create(conn, params) do
  with {:ok, run} <- MyApp.WorkflowRuns.start_payment_recovery(params) do
    json(conn, %{id: run.run_id, status: run.status})
  end
end
```

## Development Setup

For local development and examples, a minimal host app can provide:

- a local Postgres-backed repo
- a local background job setup
- direct application code calls into Squid Mesh

This uses the same configuration contract as an existing application setup.
In that mode, the example app may also own its job-backend migrations because
it is acting as a standalone development harness rather than an embedded
install.

## Validation

Host applications can validate the contract directly:

```elixir
{:ok, config} = SquidMesh.config()
```

Or raise on missing required keys:

```elixir
config = SquidMesh.config!()
```

## Example Development Harness

The example host app smoke-test harness builds on this same contract and is the
reference setup for end-to-end development and verification.

Path:

- `examples/minimal_host_app`

Suggested workflow:

1. Start Postgres for the example app.
2. Run `mix setup` inside `examples/minimal_host_app`.
3. Run `mix example.smoke` to exercise the host app boundary.

Fast verification path:

- run `MIX_ENV=test mix example.smoke` inside `examples/minimal_host_app`

The example app wires:

- its own `MinimalHostApp.Repo`
- journal runtime smoke paths that use inferred Ecto storage and
  `SquidMesh.execute_next/1`
- explicit table-runtime smoke paths that use `MinimalHostApp.SquidMeshExecutor`
  and one generic worker calling `SquidMesh.Runtime.Runner.perform/1`
- Squid Mesh through `MinimalHostApp.WorkflowRuns`

## Inspecting History

For real host apps, `inspect_run/2` is most useful with history enabled:

```elixir
SquidMesh.inspect_run(run_id, include_history: true)
```

That returns the top-level run plus:

- `steps`: logical per-step state in workflow order, including dependency edges
- `step_runs`: persisted execution history
- `attempts`: persisted retry history for each step run

This split gives host apps both declared per-step state and the raw execution
timeline from one inspection call.

Use `explain_run/2` when an operator surface needs the current reason and safe
next actions instead of the full inspection snapshot:

```elixir
{:ok, explanation} = SquidMesh.explain_run(run_id)

%{
  status: explanation.status,
  reason: explanation.reason,
  step: explanation.step,
  next_actions: explanation.next_actions
}
```

`inspect_run/2` answers "what persisted state exists?". `explain_run/2` answers
"why is this run here, what evidence supports that, and what can an operator do
next?". The explanation keeps `details` and `evidence` structured so Phoenix
apps, CLIs, and dashboards can render their own messages.
