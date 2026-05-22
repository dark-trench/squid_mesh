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

1. Squid Mesh config points at the host repo and executor.
2. The executor config owns the queue name.
3. The host job calls `SquidMesh.Runtime.Runner.perform/1`.

The host application configures Squid Mesh under the `:squid_mesh` application:

```elixir
config :squid_mesh,
  repo: MyApp.Repo,
  executor: MyApp.SquidMeshExecutor

config :my_app, MyApp.SquidMeshExecutor,
  queue: :squid_mesh
```

Required keys:

- `:repo` - the Ecto repo Squid Mesh uses for persisted runtime state
- `:executor` - the host module that implements `SquidMesh.Executor`

Optional keys:

- `:stale_step_timeout` - `:disabled` by default; set a non-negative
  millisecond timeout to let redelivered jobs reclaim stale `running` steps
  after worker interruption

## Executor Contract

The host executor is the only queue boundary Squid Mesh calls. Copy this module,
replace `MyApp.JobQueue.enqueue/1` with the host app's job backend, and keep the
queued job generic:

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

## First Run Checklist

For a new integration, the shortest path to a successful first run is:

1. Add `:squid_mesh` to the host app's dependencies.
2. Add or confirm a working Postgres-backed `Repo`.
3. Add or confirm a working job system for the host executor.
4. Add the host app's job-system migrations, if needed.
5. Run `mix squid_mesh.install`.
6. Run `mix ecto.migrate`.
7. Configure `:squid_mesh` with the host app's `Repo` and executor module.
8. Configure the host executor's queue, worker, and scheduler.
9. Start the host app's `Repo` and job system under supervision.
10. Start one workflow through the public API and inspect it with history enabled.

## Existing Application Setup

For an existing Phoenix or OTP application:

1. Add the `:squid_mesh` dependency.
2. Configure `:repo` to point at the app's existing repo.
3. Configure `:executor` to point at the app's executor module.
4. Call `SquidMesh.config!/0` during boot or integration setup to verify the
   required contract is present.
5. Integrate Squid Mesh from the host application's contexts, services,
   controllers, or internal APIs.

The host application is responsible for:

- database setup and migrations
- executor and background job infrastructure lifecycle
- any HTTP or internal API endpoints exposed to end users

That means the embedded install path assumes:

- the host app already owns its `Repo`
- the host app already owns its executor and job-system configuration
- the host app already manages its job-system tables, if any

## Minimal OTP Host Skeleton

For a plain OTP application, the minimum moving pieces are:

- a `Repo` module
- an executor module implementing `SquidMesh.Executor`
- a worker or equivalent delivery adapter that calls `SquidMesh.Runtime.Runner.perform/1`
- `Repo` and the chosen job system in the application supervision tree
- `:squid_mesh` configuration pointing at that `Repo` and executor
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

- add `:squid_mesh`, `:jido`, and the chosen job backend to the Phoenix app
- keep using the Phoenix app's existing `Repo`
- start the job backend in the application supervision tree
- configure `:squid_mesh` to use that `Repo` and executor
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

  def list_runs(opts \\ []) do
    SquidMesh.list_runs(opts)
  end
end
```

Controller shape:

```elixir
def create(conn, params) do
  with {:ok, run} <- MyApp.WorkflowRuns.start_payment_recovery(params) do
    json(conn, %{id: run.id, status: run.status})
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
- `MinimalHostApp.SquidMeshExecutor` as the host-owned executor
- one generic worker that calls `SquidMesh.Runtime.Runner.perform/1`
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
