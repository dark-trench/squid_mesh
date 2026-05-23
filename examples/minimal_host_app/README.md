# Minimal Host App

Reference host-app harness for Squid Mesh.

This example shows how an application can:

- configure Squid Mesh with its own `Repo` and `Oban`
- expose workflow operations through an application-facing module
- pause and resume a human-in-the-loop workflow through that boundary
- activate cron workflows through the host app's Oban plugins
- run repeatable smoke, resilience, and bounded soak paths during development

## Setup

Start a local Postgres instance and point `DATABASE_URL` at it. The default is:

```sh
ecto://postgres:postgres@localhost/minimal_host_app_dev
```

Then set up the example app:

```sh
mix setup
```

This will:

- create the example app database
- install Squid Mesh migrations into the example app with `mix squid_mesh.install`
- run the example app's `Oban` migration
- run both the example app and Squid Mesh migrations through `mix ecto.migrate`

This example is the standalone development harness. Unlike the embedded host-app
install path, it owns its own `Oban` migration so the runtime can be exercised
without depending on another application.

Run the verification tasks one at a time. They share the same test database,
manual Oban instance, and local gateway stubs, so parallel runs can interfere
with each other's polling windows.

## Smoke Path

Run the test-mode smoke path:

```sh
MIX_ENV=test mix example.smoke
```

This command creates the test database if needed, runs migrations, starts the
repo and Oban, starts a local HTTP gateway stub, then runs the example smoke
path to completion.

Run the development-like path after `mix setup`:

```sh
mix example.smoke
```

The smoke task:

- starts a manual payment recovery workflow through
  `MinimalHostApp.WorkflowRuns.start_payment_recovery/1`
- starts the dependency-based recovery workflow through
  `MinimalHostApp.WorkflowRuns.start_dependency_recovery/1`
- starts a manual approval workflow through
  `MinimalHostApp.WorkflowRuns.start_manual_approval/1`
- explains the paused approval run through `MinimalHostApp.WorkflowRuns.explain_run/1`
- approves the paused run through `MinimalHostApp.WorkflowRuns.approve_run/2`
- starts a manual digest run through
  `MinimalHostApp.WorkflowRuns.start_manual_digest/1`
- starts the local ledger checkout workflow through
  `MinimalHostApp.WorkflowRuns.start_local_ledger_checkout/1`
- starts a saga checkout run through
  `MinimalHostApp.WorkflowRuns.start_saga_checkout/1`
- waits for execution, inspects all completed manual workflows, and
  verifies the paused approval run's durable audit history, local transaction
  rollback, and saga rollback compensation history
- activates the same digest workflow through the host app's Oban-backed cron plugin
- verifies both digest triggers complete through the same workflow graph

## Restart Resilience

Run the restart resilience verification:

```sh
MIX_ENV=test mix example.resilience
```

This path verifies:

- queued work survives an Oban restart boundary
- delayed work survives an Oban restart boundary
- retrying work survives an Oban restart boundary
- a paused manual-approval run survives restart and still approves through the host boundary with the same resume semantics

## Bounded Soak And Load

Run the bounded soak and load verification:

```sh
MIX_ENV=test mix example.soak
```

This path is intentionally not a benchmark. It drives a bounded mix of:

- concurrent successful workflow runs
- retried workflow runs
- replayed workflow runs
- cancelled workflow runs

## Example Boundary

The host-facing boundary is:

```elixir
MinimalHostApp.WorkflowRuns.start_payment_recovery(%{
  account_id: "acct_123",
  invoice_id: "inv_456",
  attempt_id: "attempt_789",
  gateway_url: "http://127.0.0.1:4010/gateway"
})
```

That map is the workflow payload for the `:payment_recovery` trigger declared
in the example workflow.

The payment recovery workflow marks its customer notification step as
non-compensatable:

```elixir
step(:notify_customer, MinimalHostApp.Steps.NotifyCustomer, compensatable: false)
```

That marker makes replay require explicit operator approval after the
notification has completed, instead of silently treating the side effect as
reversible.

The saga checkout workflow demonstrates reversible side effects:

```elixir
step :reserve_inventory, MinimalHostApp.Steps.ReserveInventory,
  compensate: MinimalHostApp.Steps.ReleaseInventory

step :authorize_payment, MinimalHostApp.Steps.AuthorizePayment,
  compensate: MinimalHostApp.Steps.VoidPaymentAuthorization

step :capture_payment, MinimalHostApp.Steps.CapturePayment, retry: [max_attempts: 2]
```

The capture step fails after its retry policy is exhausted, then Squid Mesh
voids the payment authorization and releases inventory in reverse completion
order. The smoke task verifies those compensation results through
`inspect_run(..., include_history: true)`.

The same workflow routes exhausted gateway failures to a compensation step:

```elixir
transition(:check_gateway_status,
  on: :error,
  to: :issue_gateway_credit,
  recovery: :compensation
)
```

When that path runs, `inspect_run(run_id, include_history: true)` exposes a
`:compensation_routed` audit event and the failed step's
`recovery.failure.strategy`.

The dependency recovery workflow demonstrates named path input mapping on a
join step. `:load_account` and `:load_invoice` keep their outputs in the run
context, while `:prepare_notification` receives only the nested values it needs:

```elixir
step :prepare_notification, MinimalHostApp.Steps.PrepareNotification,
  after: [:load_account, :load_invoice],
  input: [
    account_id: [:account, :id],
    invoice_id: [:invoice, :id],
    account_tier: [:account, :tier]
  ]
```

The smoke task verifies that persisted step history records that mapped input.

The local ledger checkout workflow demonstrates a same-process host repo
transaction group:

```elixir
step :post_local_ledger_entries, MinimalHostApp.Steps.PostLocalLedgerEntries,
  transaction: :repo
```

The step writes two local ledger rows through `MinimalHostApp.Repo`. When the
step returns `{:ok, output}`, both rows commit before Squid Mesh records the
completed step. When the step returns `{:error, reason}`, both rows roll back
and Squid Mesh records the durable step failure. This is a local database
boundary only; saga compensation and later workflow steps remain explicit
workflow concerns.

Host apps can expose diagnostics through the same boundary:

```elixir
{:ok, explanation} = MinimalHostApp.WorkflowRuns.explain_run(run_id)

explanation.reason
```

The reference workflow and step modules live in:

- `lib/minimal_host_app/workflows/payment_recovery.ex`
- `lib/minimal_host_app/workflows/dependency_recovery.ex`
- `lib/minimal_host_app/workflows/manual_approval.ex`
- `lib/minimal_host_app/workflows/local_ledger_checkout.ex`
- `lib/minimal_host_app/workflows/saga_checkout.ex`
- `lib/minimal_host_app/workflows/daily_digest.ex`
- `lib/minimal_host_app/steps/`

## Multi-Trigger Workflow Example

`MinimalHostApp.Workflows.DailyDigest` demonstrates one workflow graph with two
entrypoints:

```elixir
trigger :manual_digest do
  manual()

  payload do
    field :channel, :string
    field :digest_date, :string
  end
end

trigger :daily_digest do
  cron "@reboot", timezone: "Etc/UTC", idempotency: :return_existing_run

  payload do
    field :channel, :string, default: "ops"
    field :digest_date, :string, default: {:today, :iso8601}
  end
end
```

Both triggers run `:announce_digest` and `:record_digest_delivery`. The host app
can start the manual entrypoint through `WorkflowRuns.start_manual_digest/1`,
while the cron plugin starts the same workflow through `:daily_digest`.

The cron trigger opts into scheduled-start idempotency. Because this example
uses Oban's static `@reboot` cron args, the host plugin supplies one signal id
per plugin boot; duplicate delivery of that same boot activation returns the
first run instead of creating a second one. Normal recurring schedules should
provide a per-window `signal_id` or `intended_window` from the host scheduler.

## Dependency Workflow Example

The example app also includes a dependency-based workflow with two roots and a
join step:

```elixir
defmodule MinimalHostApp.Workflows.DependencyRecovery do
  use SquidMesh.Workflow

  workflow do
    trigger :dependency_recovery do
      manual()

      payload do
        field :account_id, :string
        field :invoice_id, :string
        field :attempt_id, :string
      end
    end

    step :load_account, MinimalHostApp.Steps.LoadAccount
    step :load_invoice, MinimalHostApp.Steps.LoadInvoice

    step :prepare_notification, MinimalHostApp.Steps.PrepareNotification,
      after: [:load_account, :load_invoice],
      input: [
        account_id: [:account, :id],
        invoice_id: [:invoice, :id],
        account_tier: [:account, :tier]
      ]
  end
end
```

This workflow is exercised through `MinimalHostApp.WorkflowRuns.start_dependency_recovery/1`
and the example app test suite. Ready dependency roots still execute one at a
time today; `after: [...]` guarantees that the join step waits for both inputs.
