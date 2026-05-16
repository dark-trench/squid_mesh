defmodule MinimalHostApp.WorkflowRunsTest do
  use MinimalHostApp.DataCase

  alias MinimalHostApp.CronPlugin
  alias MinimalHostApp.Smoke
  alias MinimalHostApp.Workers.SquidMeshWorker
  alias MinimalHostApp.WorkflowRuns
  alias MinimalHostApp.Workflows.DailyDigest
  alias Oban.Job

  defmodule InvalidRecurringIdempotentCronWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :daily_digest do
        cron "0 9 * * *", timezone: "Etc/UTC", idempotency: :return_existing_run

        payload do
          field :channel, :string, default: "ops"
          field :digest_date, :string, default: {:today, :iso8601}
        end
      end

      step :announce_digest, :log, message: "posting daily digest"
      step :record_digest_delivery, MinimalHostApp.Steps.RecordDigestDelivery

      transition :announce_digest, on: :ok, to: :record_digest_delivery
      transition :record_digest_delivery, on: :ok, to: :complete
    end
  end

  test "starts the example payment recovery workflow through the host boundary" do
    bypass = Bypass.open()

    attrs = %{
      account_id: "acct_123",
      invoice_id: "inv_456",
      attempt_id: "attempt_789",
      gateway_url: endpoint_url(bypass.port, "/gateway")
    }

    assert {:ok, run} = WorkflowRuns.start_payment_recovery(attrs)

    assert_enqueued(
      worker: MinimalHostApp.Workers.SquidMeshWorker,
      queue: "squid_mesh",
      args: %{"kind" => "step", "run_id" => run.id, "step" => "load_invoice"}
    )

    assert run.workflow == MinimalHostApp.Workflows.PaymentRecovery
    assert run.trigger == :payment_recovery
    assert run.status == :pending
    assert run.payload == attrs
    assert run.current_step == :load_invoice
  end

  test "inspects a started run through the host boundary" do
    assert {:ok, run} =
             WorkflowRuns.start_payment_recovery(%{
               account_id: "acct_123",
               invoice_id: "inv_456",
               attempt_id: "attempt_789",
               gateway_url: "http://127.0.0.1:4010/gateway"
             })

    assert {:ok, inspected_run} = WorkflowRuns.inspect_payment_recovery(run.id)
    assert inspected_run == run
  end

  test "surfaces payment recovery compensation through host inspection history" do
    {server_pid, port} =
      MinimalHostApp.RuntimeHarness.start_gateway_server(
        fn _attempt -> MinimalHostApp.RuntimeHarness.failure_gateway_response(503, "down") end,
        10
      )

    on_exit(fn -> MinimalHostApp.RuntimeHarness.stop_gateway_server(server_pid) end)

    attrs = %{
      account_id: "acct_123",
      invoice_id: "inv_456",
      attempt_id: "attempt_789",
      gateway_url: endpoint_url(port, "/gateway")
    }

    assert {:ok, run} = WorkflowRuns.start_payment_recovery(attrs)

    assert :ok = MinimalHostApp.RuntimeHarness.wait_for_execution()

    assert :ok =
             MinimalHostApp.RuntimeHarness.perform_scheduled_step!(run.id, "check_gateway_status")

    assert :ok =
             MinimalHostApp.RuntimeHarness.perform_scheduled_step!(run.id, "check_gateway_status")

    assert :ok =
             MinimalHostApp.RuntimeHarness.perform_scheduled_step!(run.id, "check_gateway_status")

    assert :ok =
             MinimalHostApp.RuntimeHarness.perform_scheduled_step!(run.id, "check_gateway_status")

    assert :ok = MinimalHostApp.RuntimeHarness.wait_for_execution()

    assert {:ok, completed_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.id)
    assert {:ok, history_run} = WorkflowRuns.inspect_run(run.id, include_history: true)

    assert completed_run.status == :completed

    assert completed_run.context.compensation == %{
             account_id: "acct_123",
             invoice_id: "inv_456",
             status: "credit_issued"
           }

    assert [
             %{
               type: :compensation_routed,
               step: :check_gateway_status,
               metadata: %{target: :issue_gateway_credit}
             }
           ] = history_run.audit_events

    assert [
             %{step: :load_invoice, status: :completed},
             %{
               step: :check_gateway_status,
               status: :failed,
               recovery: %{failure: %{strategy: :compensation, target: :issue_gateway_credit}}
             },
             %{step: :issue_gateway_credit, status: :completed}
           ] = history_run.step_runs
  end

  test "executes a dependency-based workflow through the host boundary" do
    attrs = %{
      account_id: "acct_123",
      invoice_id: "inv_456",
      attempt_id: "attempt_789"
    }

    assert {:ok, run} = WorkflowRuns.start_dependency_recovery(attrs)
    assert run.current_step == nil

    assert :ok = MinimalHostApp.RuntimeHarness.wait_for_execution()
    assert {:ok, completed_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.id)
    assert {:ok, history_run} = WorkflowRuns.inspect_run(run.id, include_history: true)

    assert completed_run.status == :completed
    assert completed_run.context.account == %{id: "acct_123", tier: "standard"}

    assert completed_run.context.invoice == %{
             id: "inv_456",
             account_id: "acct_123",
             attempt_id: "attempt_789"
           }

    assert completed_run.context.notification == %{
             channel: "email",
             account_id: "acct_123",
             invoice_id: "inv_456",
             account_tier: "standard"
           }

    assert Enum.map(history_run.steps, &{&1.step, &1.status, &1.depends_on}) == [
             {:load_account, :completed, []},
             {:load_invoice, :completed, []},
             {:prepare_notification, :completed, [:load_account, :load_invoice]}
           ]
  end

  test "commits local repo transaction groups through the host boundary" do
    assert {:ok, run} =
             WorkflowRuns.start_local_ledger_checkout(%{
               account_id: "acct_local_123",
               fail_after_reserve: false
             })

    assert :ok = MinimalHostApp.RuntimeHarness.wait_for_execution()
    assert {:ok, completed_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.id)

    assert completed_run.status == :completed
    assert completed_run.context.local_ledger == %{status: "committed", entries: 2}
    assert local_ledger_entries(run.id) == ["reserve", "capture"]
  end

  test "rolls back local repo transaction groups when the step fails" do
    assert {:ok, run} =
             WorkflowRuns.start_local_ledger_checkout(%{
               account_id: "acct_local_456",
               fail_after_reserve: true
             })

    assert :ok = MinimalHostApp.RuntimeHarness.wait_for_execution()
    assert {:ok, failed_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.id)

    assert failed_run.status == :failed
    assert failed_run.current_step == :post_local_ledger_entries

    assert failed_run.last_error == %{
             message: "local ledger capture failed",
             retryable?: false
           }

    assert local_ledger_entries(run.id) == []
  end

  test "approves a manual approval workflow through the host boundary" do
    assert {:ok, run} = WorkflowRuns.start_manual_approval(%{account_id: "acct_approval_123"})

    assert :ok = MinimalHostApp.RuntimeHarness.wait_for_execution()
    assert {:ok, paused_run} = WorkflowRuns.inspect_run(run.id, include_history: true)

    assert paused_run.status == :paused
    assert paused_run.current_step == :wait_for_approval

    assert Enum.map(paused_run.audit_events, &{&1.type, &1.step}) == [
             {:paused, :wait_for_approval}
           ]

    assert Enum.map(paused_run.steps, &{&1.step, &1.status}) == [
             {:wait_for_approval, :running},
             {:record_approval, :waiting},
             {:record_rejection, :waiting}
           ]

    assert {:ok, resumed_run} =
             WorkflowRuns.approve_run(
               run.id,
               %{actor: "ops_123", comment: "approved", metadata: %{ticket: "SUP-123"}}
             )

    assert resumed_run.status == :running
    assert resumed_run.current_step == :record_approval

    assert :ok = MinimalHostApp.RuntimeHarness.wait_for_execution()
    assert {:ok, completed_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.id)
    assert {:ok, completed_history} = WorkflowRuns.inspect_run(run.id, include_history: true)

    assert completed_run.status == :completed

    assert completed_run.context.approval.account_id == "acct_approval_123"
    assert completed_run.context.approval.status == "approved"
    assert completed_run.context.approval.decision == "approved"
    assert completed_run.context.approval.actor == "ops_123"
    assert completed_run.context.approval.comment == "approved"

    assert Enum.map(completed_history.audit_events, &{&1.type, &1.step, &1.actor}) == [
             {:paused, :wait_for_approval, nil},
             {:approved, :wait_for_approval, "ops_123"}
           ]

    assert Enum.map(completed_history.audit_events, & &1.metadata) == [
             nil,
             %{ticket: "SUP-123"}
           ]
  end

  test "runs the daily digest workflow through its manual trigger" do
    attrs = %{channel: "ops-manual", digest_date: "2026-05-10"}

    assert {:ok, run} = WorkflowRuns.start_manual_digest(attrs)

    assert run.workflow == MinimalHostApp.Workflows.DailyDigest
    assert run.trigger == :manual_digest
    assert run.payload == attrs

    assert_enqueued(
      worker: MinimalHostApp.Workers.SquidMeshWorker,
      queue: "squid_mesh",
      args: %{"kind" => "step", "run_id" => run.id, "step" => "announce_digest"}
    )

    assert :ok = MinimalHostApp.RuntimeHarness.wait_for_execution()
    assert {:ok, completed_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.id)

    assert completed_run.status == :completed
    assert completed_run.context.digest_delivery.channel == "ops-manual"
    assert completed_run.context.digest_delivery.digest_date == "2026-05-10"

    assert {:ok, inspected_run} = WorkflowRuns.inspect_run(run.id)
    assert inspected_run.payload == attrs
  end

  test "runs the daily digest workflow through its cron trigger" do
    signal_id = unique_reboot_signal_id()

    existing_run_ids =
      case WorkflowRuns.list_daily_digest_runs() do
        {:ok, runs} -> MapSet.new(runs, & &1.id)
        {:error, _reason} -> MapSet.new()
      end

    job = %Oban.Job{
      args: %{
        "kind" => "cron",
        "workflow" => "Elixir.MinimalHostApp.Workflows.DailyDigest",
        "trigger" => "daily_digest",
        "signal_id" => signal_id
      }
    }

    assert :ok = MinimalHostApp.Workers.SquidMeshWorker.perform(job)

    assert_enqueued(
      worker: MinimalHostApp.Workers.SquidMeshWorker,
      queue: "squid_mesh",
      args: %{"kind" => "step", "step" => "announce_digest"}
    )

    assert :ok = MinimalHostApp.RuntimeHarness.wait_for_execution()

    assert {:ok, runs} = WorkflowRuns.list_daily_digest_runs()
    run = Enum.find(runs, fn run -> not MapSet.member?(existing_run_ids, run.id) end)

    assert %SquidMesh.Run{} = run
    assert run.trigger == :daily_digest
    assert is_binary(run.payload.digest_date)
    assert run.payload.channel == "ops"
    assert run.context.schedule.idempotency == "return_existing_run"
    assert run.context.schedule.idempotency_key == signal_id
  end

  test "generates a new reboot signal id for each cron plugin boot" do
    first_signal_id = plugin_reboot_signal_id()
    second_signal_id = plugin_reboot_signal_id()

    assert first_signal_id != second_signal_id
    assert String.starts_with?(first_signal_id, "minimal-host-app:reboot:")

    assert String.ends_with?(
             first_signal_id,
             ":Elixir.MinimalHostApp.Workflows.DailyDigest:daily_digest"
           )
  end

  test "skips duplicate daily digest cron activation in the host example" do
    signal_id = "minimal-host-app:test:daily_digest:duplicate"

    payload = %{
      "kind" => "cron",
      "workflow" => "Elixir.MinimalHostApp.Workflows.DailyDigest",
      "trigger" => "daily_digest",
      "signal_id" => signal_id
    }

    assert :ok = SquidMeshWorker.perform(%Job{args: payload})
    assert :ok = SquidMeshWorker.perform(%Job{args: payload})
    assert :ok = MinimalHostApp.RuntimeHarness.wait_for_execution()

    assert {:ok, runs} = WorkflowRuns.list_daily_digest_runs()

    runs_with_signal =
      Enum.filter(runs, fn run ->
        get_in(run.context, [:schedule, :idempotency_key]) == signal_id
      end)

    assert [_run] = runs_with_signal
  end

  test "rejects idempotent recurring cron workflows without dynamic schedule identity" do
    assert {:error, reason} =
             CronPlugin.validate(workflows: [InvalidRecurringIdempotentCronWorkflow])

    assert reason =~ "must provide dynamic schedule identity"
  end

  test "rejects a manual approval workflow through the host boundary" do
    assert {:ok, run} = WorkflowRuns.start_manual_approval(%{account_id: "acct_review_123"})

    assert :ok = MinimalHostApp.RuntimeHarness.wait_for_execution()
    assert {:ok, paused_run} = WorkflowRuns.inspect_run(run.id, include_history: true)

    assert paused_run.status == :paused
    assert paused_run.current_step == :wait_for_approval

    assert {:ok, resumed_run} =
             WorkflowRuns.reject_run(run.id, %{actor: "ops_456", comment: "rejected"})

    assert resumed_run.status == :running
    assert resumed_run.current_step == :record_rejection

    assert :ok = MinimalHostApp.RuntimeHarness.wait_for_execution()
    assert {:ok, completed_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.id)
    assert {:ok, completed_history} = WorkflowRuns.inspect_run(run.id, include_history: true)

    assert completed_run.status == :completed
    assert completed_run.context.approval.account_id == "acct_review_123"
    assert completed_run.context.approval.status == "rejected"
    assert completed_run.context.approval.decision == "rejected"
    assert completed_run.context.approval.actor == "ops_456"
    assert completed_run.context.approval.comment == "rejected"

    assert Enum.map(completed_history.audit_events, &{&1.type, &1.step, &1.actor}) == [
             {:paused, :wait_for_approval, nil},
             {:rejected, :wait_for_approval, "ops_456"}
           ]
  end

  test "runs the documented smoke path" do
    assert %{
             payment_recovery: payment_recovery,
             dependency_recovery: dependency_recovery,
             manual_approval: manual_approval,
             manual_digest: manual_digest,
             local_ledger_checkout: local_ledger_checkout,
             local_ledger_rollback: local_ledger_rollback,
             daily_digest: daily_digest
           } =
             Smoke.run_all!()

    assert payment_recovery.status == :completed
    assert payment_recovery.context.notification.channel == "email"
    assert payment_recovery.context.gateway_check.status == "retry_required"

    assert dependency_recovery.status == :completed
    assert dependency_recovery.context.notification.channel == "email"

    assert manual_approval.status == :completed
    assert manual_approval.context.approval.status == "approved"

    assert manual_digest.status == :completed
    assert manual_digest.trigger == :manual_digest

    assert local_ledger_checkout.status == :completed
    assert local_ledger_checkout.context.local_ledger.entries == 2

    assert local_ledger_rollback.status == :failed
    assert local_ledger_rollback.current_step == :post_local_ledger_entries

    assert daily_digest.status == :completed
    assert daily_digest.trigger == :daily_digest
  end

  test "runs the cancellation smoke path" do
    assert %SquidMesh.Run{} = run = Smoke.run_cancellation!()
    assert run.status == :cancelled
  end

  defp endpoint_url(port, path) do
    "http://127.0.0.1:#{port}#{path}"
  end

  defp unique_reboot_signal_id do
    "minimal-host-app:test:daily_digest:#{System.unique_integer([:positive])}"
  end

  defp plugin_reboot_signal_id do
    assert {:ok, {_supervisor_flags, [child_spec]}} =
             CronPlugin.init(conf: oban_config(), workflows: [DailyDigest])

    %{start: {Oban.Plugins.Cron, :start_link, [opts]}} = child_spec
    [{"@reboot", SquidMeshWorker, entry_opts}] = Keyword.fetch!(opts, :crontab)
    payload = Keyword.fetch!(entry_opts, :args)
    Map.fetch!(payload, "signal_id")
  end

  defp oban_config do
    :minimal_host_app
    |> Application.fetch_env!(Oban)
    |> Keyword.put(:testing, :disabled)
    |> Keyword.put(:plugins, false)
    |> Keyword.put(:queues, false)
    |> Keyword.put(:peer, {Oban.Peers.Isolated, [leader?: true]})
    |> Oban.Config.new()
  end

  defp local_ledger_entries(run_id) do
    Repo.all(
      from(entry in "local_ledger_entries",
        where: entry.run_id == ^run_id,
        order_by: [asc: entry.id],
        select: entry.entry
      )
    )
  end
end
