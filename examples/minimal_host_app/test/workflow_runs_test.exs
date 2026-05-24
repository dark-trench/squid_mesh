defmodule MinimalHostApp.WorkflowRunsTest do
  use MinimalHostApp.DataCase

  alias MinimalHostApp.CronPlugin
  alias MinimalHostApp.Smoke
  alias MinimalHostApp.Workers.SquidMeshWorker
  alias MinimalHostApp.WorkflowRuns
  alias MinimalHostApp.Workflows.DailyDigest
  alias Oban.Job
  alias SquidMesh.ReadModel.Inspection.Snapshot
  alias SquidMesh.ReadModel.Listing.Summary

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

    assert run.workflow == "Elixir.MinimalHostApp.Workflows.PaymentRecovery"
    assert run.trigger == "payment_recovery"
    assert run.status == :running
    assert run.input == attrs
    assert [%{step: "load_invoice", status: :available}] = run.visible_attempts
  end

  test "inspects a started run through the host boundary" do
    assert {:ok, run} =
             WorkflowRuns.start_payment_recovery(%{
               account_id: "acct_123",
               invoice_id: "inv_456",
               attempt_id: "attempt_789",
               gateway_url: "http://127.0.0.1:4010/gateway"
             })

    assert {:ok, inspected_run} = WorkflowRuns.inspect_payment_recovery(run.run_id)
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

    assert {:ok, completed_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.run_id)
    assert {:ok, history_run} = WorkflowRuns.inspect_run(run.run_id, include_history: true)

    assert completed_run.status == :completed

    assert completed_run.context.compensation == %{
             account_id: "acct_123",
             invoice_id: "inv_456",
             status: "credit_issued"
           }

    assert [
             {"load_invoice", :completed, true, 1},
             {"check_gateway_status", :failed, false, 1},
             {"check_gateway_status", :failed, false, 2},
             {"check_gateway_status", :failed, false, 3},
             {"check_gateway_status", :failed, false, 4},
             {"check_gateway_status", :failed, true, 5},
             {"issue_gateway_credit", :completed, true, 1}
           ] =
             Enum.map(history_run.attempts, &{&1.step, &1.status, &1.applied?, &1.attempt_number})
  end

  test "executes a dependency-based workflow through the host boundary" do
    attrs = %{
      account_id: "acct_123",
      invoice_id: "inv_456",
      attempt_id: "attempt_789"
    }

    assert {:ok, run} = WorkflowRuns.start_dependency_recovery(attrs)
    assert Enum.map(run.visible_attempts, & &1.step) == ["load_account", "load_invoice"]

    assert :ok = MinimalHostApp.RuntimeHarness.wait_for_execution()
    assert {:ok, completed_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.run_id)
    assert {:ok, history_run} = WorkflowRuns.inspect_run(run.run_id, include_history: true)

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

    assert [
             _load_account,
             _load_invoice,
             %{step: "prepare_notification", input: prepare_notification_input}
           ] = history_run.attempts

    assert prepare_notification_input == %{
             account_id: "acct_123",
             invoice_id: "inv_456",
             account_tier: "standard"
           }
  end

  test "executes a dependency workflow through the supervised journal executor" do
    attrs = %{
      account_id: "acct_supervised_executor",
      invoice_id: "inv_supervised_executor",
      attempt_id: "attempt_supervised_executor"
    }

    assert {:ok, run} = WorkflowRuns.start_dependency_recovery(attrs)

    executor_name = :"minimal_host_app_journal_executor_#{System.unique_integer([:positive])}"

    start_supervised!(
      {MinimalHostApp.JournalExecutor,
       name: executor_name,
       owner_id: "minimal-host-app-supervised-test",
       idle_interval_ms: 10,
       error_interval_ms: 10}
    )

    assert {:ok, completed_run} = await_terminal_without_harness(run.run_id)
    assert completed_run.status == :completed
    assert completed_run.context.notification.account_id == "acct_supervised_executor"
  end

  test "executes a dependency workflow through inferred Ecto journal defaults" do
    queue = "minimal-host-app-default-journal-#{System.unique_integer([:positive])}"

    with_squid_mesh_env(
      [
        repo: Repo,
        queue: queue
      ],
      fn ->
        assert {:ok, config} = SquidMesh.config()
        assert config.runtime == :journal
        assert config.read_model == :read_model
        assert config.journal_storage.adapter == SquidMesh.Runtime.Journal.Storage.Ecto
        assert config.journal_storage.opts == [repo: Repo]

        attrs = %{
          account_id: "acct_default_journal",
          invoice_id: "inv_default_journal",
          attempt_id: "attempt_default_journal"
        }

        assert {:ok, %Snapshot{} = started_run} =
                 WorkflowRuns.start_dependency_recovery(attrs)

        assert started_run.queue == queue
        assert started_run.workflow == "Elixir.MinimalHostApp.Workflows.DependencyRecovery"
        assert started_run.status == :running

        assert {:ok, %Snapshot{} = completed_run} =
                 drain_default_journal_run(started_run.run_id, queue, 10)

        assert completed_run.status == :completed
        assert completed_run.applied_runnable_keys == completed_run.planned_runnable_keys

        assert {:ok, listed_runs} = WorkflowRuns.list_runs(now: DateTime.utc_now())

        listed_run = Enum.find(listed_runs, &(&1.run_id == started_run.run_id))
        assert %Summary{} = listed_run

        assert listed_run.run_id == started_run.run_id
        assert listed_run.queue == queue
        assert listed_run.status == :completed
        refute Map.has_key?(Map.from_struct(listed_run), :attempts)
        refute Map.has_key?(Map.from_struct(listed_run), :input)
        refute Map.has_key?(Map.from_struct(listed_run), :result)

        row = %{
          id: listed_run.run_id,
          workflow: listed_run.workflow,
          queue: listed_run.queue,
          status: listed_run.status,
          inserted_at: listed_run.indexed_at
        }

        assert row.id == started_run.run_id
        assert row.queue == queue

        assert {:ok, %Snapshot{} = inspected_run} =
                 WorkflowRuns.inspect_run(row.id,
                   queue: row.queue,
                   now: DateTime.utc_now(),
                   include_history: true
                 )

        assert inspected_run.run_id == row.id
        assert inspected_run.queue == row.queue

        assert Enum.map(completed_run.attempts, &{&1.step, &1.status, &1.applied?}) == [
                 {"load_account", :completed, true},
                 {"load_invoice", :completed, true},
                 {"prepare_notification", :completed, true}
               ]
      end
    )
  end

  test "cancels a dependency workflow through inferred Ecto journal defaults" do
    queue = "minimal-host-app-default-journal-cancel-#{System.unique_integer([:positive])}"

    with_squid_mesh_env(
      [
        repo: Repo,
        queue: queue
      ],
      fn ->
        attrs = %{
          account_id: "acct_default_journal_cancel",
          invoice_id: "inv_default_journal_cancel",
          attempt_id: "attempt_default_journal_cancel"
        }

        assert {:ok, %Snapshot{} = started_run} =
                 WorkflowRuns.start_dependency_recovery(attrs)

        assert started_run.queue == queue
        assert started_run.status == :running
        assert [_attempt | _] = started_run.visible_attempts

        assert {:ok, %Snapshot{} = cancelled_run} = WorkflowRuns.cancel_run(started_run.run_id)

        assert cancelled_run.run_id == started_run.run_id
        assert cancelled_run.queue == queue
        assert cancelled_run.status == :cancelled
        assert cancelled_run.terminal?
        assert cancelled_run.terminal_status == :cancelled
        assert cancelled_run.visible_attempts == []

        assert {:ok, %Snapshot{} = inspected_run} = WorkflowRuns.inspect_run(started_run.run_id)
        assert inspected_run.status == :cancelled
        assert inspected_run.queue == queue

        assert {:ok, :none} =
                 SquidMesh.execute_next(owner_id: "minimal-host-app-default-journal-cancel-test")
      end
    )
  end

  test "replays a dependency workflow through inferred Ecto journal defaults" do
    queue = "minimal-host-app-default-journal-replay-#{System.unique_integer([:positive])}"

    with_squid_mesh_env(
      [
        repo: Repo,
        queue: queue
      ],
      fn ->
        attrs = %{
          account_id: "acct_default_journal_replay",
          invoice_id: "inv_default_journal_replay",
          attempt_id: "attempt_default_journal_replay"
        }

        assert {:ok, %Snapshot{} = started_run} =
                 WorkflowRuns.start_dependency_recovery(attrs)

        assert {:ok, %Snapshot{status: :completed} = completed_run} =
                 drain_default_journal_run(started_run.run_id, queue, 10)

        assert {:ok, %Snapshot{} = replayed_run} = WorkflowRuns.replay_run(completed_run.run_id)

        assert replayed_run.run_id != completed_run.run_id
        assert replayed_run.replayed_from_run_id == completed_run.run_id
        assert replayed_run.queue == queue
        assert replayed_run.input == attrs
        assert replayed_run.status == :running
        assert replayed_run.visible_attempts != []
      end
    )
  end

  test "commits local repo transaction groups through the host boundary" do
    assert {:ok, run} =
             WorkflowRuns.start_local_ledger_checkout(%{
               account_id: "acct_local_123",
               fail_after_reserve: false
             })

    assert :ok = MinimalHostApp.RuntimeHarness.wait_for_execution()
    assert {:ok, completed_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.run_id)

    assert completed_run.status == :completed
    assert completed_run.context.local_ledger == %{status: "committed", entries: 2}
    assert local_ledger_entries(run.run_id) == ["reserve", "capture"]
  end

  test "rolls back local repo transaction groups when the step fails" do
    assert {:ok, run} =
             WorkflowRuns.start_local_ledger_checkout(%{
               account_id: "acct_local_456",
               fail_after_reserve: true
             })

    assert :ok = MinimalHostApp.RuntimeHarness.wait_for_execution()
    assert {:ok, failed_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.run_id)

    assert failed_run.status == :failed

    assert [%{step: "post_local_ledger_entries", status: :failed, error: error}] =
             failed_run.attempts

    assert error.message == "step execution failed"
    assert error.retryable? == false

    assert local_ledger_entries(run.run_id) == []
  end

  test "approves a manual approval workflow through the host boundary" do
    assert {:ok, run} = WorkflowRuns.start_manual_approval(%{account_id: "acct_approval_123"})

    assert {:ok, %Snapshot{status: :paused}} =
             SquidMesh.execute_next(owner_id: "minimal-host-app-approval-test")

    assert {:ok, paused_run} = WorkflowRuns.inspect_run(run.run_id, include_history: true)

    assert paused_run.status == :paused
    assert paused_run.manual_state.step == "wait_for_approval"

    assert {:ok, resumed_run} =
             WorkflowRuns.approve_run(
               run.run_id,
               %{actor: "ops_123", comment: "approved", metadata: %{ticket: "SUP-123"}}
             )

    assert resumed_run.status == :running
    assert [%{step: "record_approval", status: :available}] = resumed_run.visible_attempts

    assert {:ok, completed_run} =
             MinimalHostApp.RuntimeHarness.await_terminal_run(run.run_id)

    assert {:ok, completed_history} = WorkflowRuns.inspect_run(run.run_id, include_history: true)

    assert completed_run.status == :completed

    assert completed_run.context.approval.status == "approved"
    assert completed_run.context.approval.decision == "approved"
    assert completed_run.context.approval.actor == "ops_123"
    assert completed_run.context.approval.comment == "approved"

    assert completed_history.manual_state == nil
  end

  test "runs the daily digest workflow through its manual trigger" do
    attrs = %{channel: "ops-manual", digest_date: "2026-05-10"}

    assert {:ok, run} = WorkflowRuns.start_manual_digest(attrs)

    assert run.workflow == "Elixir.MinimalHostApp.Workflows.DailyDigest"
    assert run.trigger == "manual_digest"
    assert run.input == attrs

    assert :ok = MinimalHostApp.RuntimeHarness.wait_for_execution()
    assert {:ok, completed_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.run_id)

    assert completed_run.status == :completed
    assert completed_run.context.digest_delivery.channel == "ops-manual"
    assert completed_run.context.digest_delivery.digest_date == "2026-05-10"

    assert {:ok, inspected_run} = WorkflowRuns.inspect_run(run.run_id)
    assert inspected_run.input == attrs
  end

  test "runs the daily digest workflow through its cron trigger" do
    signal_id = unique_reboot_signal_id()

    existing_run_ids =
      case WorkflowRuns.list_daily_digest_runs() do
        {:ok, runs} -> MapSet.new(runs, & &1.run_id)
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

    assert {:ok, runs} = WorkflowRuns.list_daily_digest_runs()
    run = Enum.find(runs, fn run -> not MapSet.member?(existing_run_ids, run.run_id) end)

    assert %Summary{} = run

    assert {:ok, inspected_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.run_id)
    assert inspected_run.status == :completed
    assert inspected_run.trigger == "daily_digest"
    assert is_binary(inspected_run.input.digest_date)
    assert inspected_run.input.channel == "ops"
    assert inspected_run.context.schedule.idempotency == :return_existing_run
    assert inspected_run.context.schedule.idempotency_key == signal_id
  end

  test "the host worker forwards non-cron payload errors from the runtime" do
    assert {:error, {:invalid_squid_mesh_payload, %{"kind" => "step", "run_id" => _run_id}}} =
             SquidMeshWorker.perform(%Job{
               args: %{
                 "kind" => "step",
                 "run_id" => Ecto.UUID.generate(),
                 "step" => "charge_card"
               }
             })
  end

  test "the cron delivery adapter reports adapter metadata" do
    assert {:ok, metadata} =
             MinimalHostApp.SquidMeshExecutor.enqueue_cron(
               %{},
               DailyDigest,
               :daily_digest,
               signal_id: "minimal-host-app:metadata-test"
             )

    assert metadata.adapter == MinimalHostApp.SquidMeshExecutor
    assert metadata.queue == :squid_mesh
    assert metadata.worker == "MinimalHostApp.Workers.SquidMeshWorker"
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
        with {:ok, inspected_run} <- WorkflowRuns.inspect_run(run.run_id) do
          inspected_run.context.schedule.idempotency_key == signal_id
        else
          {:error, _reason} -> false
        end
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

    assert {:ok, %Snapshot{status: :paused}} =
             SquidMesh.execute_next(owner_id: "minimal-host-app-rejection-test")

    assert {:ok, paused_run} = WorkflowRuns.inspect_run(run.run_id, include_history: true)

    assert paused_run.status == :paused
    assert paused_run.manual_state.step == "wait_for_approval"

    assert {:ok, resumed_run} =
             WorkflowRuns.reject_run(run.run_id, %{actor: "ops_456", comment: "rejected"})

    assert resumed_run.status == :running
    assert [%{step: "record_rejection", status: :available}] = resumed_run.visible_attempts

    assert {:ok, completed_run} = MinimalHostApp.RuntimeHarness.await_terminal_run(run.run_id)
    assert {:ok, completed_history} = WorkflowRuns.inspect_run(run.run_id, include_history: true)

    assert completed_run.status == :completed
    assert completed_run.context.approval.status == "rejected"
    assert completed_run.context.approval.decision == "rejected"
    assert completed_run.context.approval.actor == "ops_456"
    assert completed_run.context.approval.comment == "rejected"

    assert completed_history.manual_state == nil
  end

  test "runs the documented smoke path" do
    assert %{
             payment_recovery: payment_recovery,
             dependency_recovery: dependency_recovery,
             manual_approval: manual_approval,
             manual_digest: manual_digest,
             local_ledger_checkout: local_ledger_checkout,
             local_ledger_rollback: local_ledger_rollback,
             journal_executor: journal_executor,
             journal_recovery: journal_recovery,
             journal_cancellation: journal_cancellation,
             journal_replay: journal_replay,
             journal_cron_digest: journal_cron_digest,
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
    assert manual_digest.trigger == "manual_digest"

    assert local_ledger_checkout.status == :completed
    assert local_ledger_checkout.context.local_ledger.entries == 2

    assert local_ledger_rollback.status == :failed

    assert [%{step: "post_local_ledger_entries", status: :failed}] =
             local_ledger_rollback.attempts

    assert journal_executor.status == :completed
    assert journal_executor.applied_runnable_keys == journal_executor.planned_runnable_keys

    assert journal_recovery.status == :completed
    assert journal_recovery.applied_runnable_keys == journal_recovery.planned_runnable_keys

    assert journal_cancellation.status == :cancelled
    assert journal_cancellation.visible_attempts == []

    assert journal_replay.status == :completed
    assert journal_replay.replayed_from_run_id

    assert journal_cron_digest.status == :completed
    assert journal_cron_digest.trigger == "daily_digest"
    assert journal_cron_digest.context.schedule.signal_id

    assert daily_digest.status == :completed
    assert daily_digest.trigger == "daily_digest"
  end

  test "smoke run clears stale journal rows before starting" do
    now = DateTime.utc_now(:microsecond)
    thread_id = "squid_mesh:dispatch:stale-smoke-#{System.unique_integer([:positive])}"
    unknown_atom_name = "squid_mesh_unknown_atom_#{System.unique_integer([:positive])}"

    Repo.insert_all("squid_mesh_journal_threads", [
      %{
        id: thread_id,
        rev: 1,
        metadata: %{},
        created_at_ms: 0,
        updated_at_ms: 0,
        inserted_at: now,
        updated_at: now
      }
    ])

    Repo.insert_all("squid_mesh_journal_entries", [
      %{
        id: Ecto.UUID.dump!(Ecto.UUID.generate()),
        thread_id: thread_id,
        seq: 0,
        entry: :erlang.term_to_binary({:squid_mesh_ecto_term_v1, {:atom, unknown_atom_name}}),
        inserted_at: now,
        updated_at: now
      }
    ])

    assert %Snapshot{} = run = Smoke.run!()
    assert run.status == :completed
  end

  test "runs the journal executor smoke path" do
    assert %SquidMesh.ReadModel.Inspection.Snapshot{} = run = Smoke.run_journal_executor!()

    assert run.status == :completed
    assert run.workflow == "Elixir.MinimalHostApp.Workflows.DependencyRecovery"
    assert run.applied_runnable_keys == run.planned_runnable_keys

    assert Enum.map(run.attempts, &{&1.step, &1.status, &1.applied?}) == [
             {"load_account", :completed, true},
             {"load_invoice", :completed, true},
             {"prepare_notification", :completed, true}
           ]

    assert Enum.find_value(run.attempts, fn
             %{step: "prepare_notification", result: %{notification: notification}} ->
               notification

             _attempt ->
               nil
           end) == %{
             account_id: "acct_journal_demo",
             account_tier: "standard",
             channel: "email",
             invoice_id: "inv_journal_demo"
           }
  end

  test "recovers the journal executor smoke path from persisted entries" do
    assert %SquidMesh.ReadModel.Inspection.Snapshot{} = run = Smoke.run_journal_recovery!()

    assert run.status == :completed
    assert run.workflow == "Elixir.MinimalHostApp.Workflows.DependencyRecovery"
    assert run.applied_runnable_keys == run.planned_runnable_keys
  end

  test "runs the journal cancellation smoke path" do
    assert %SquidMesh.ReadModel.Inspection.Snapshot{} = run = Smoke.run_journal_cancellation!()

    assert run.status == :cancelled
    assert run.terminal?
    assert run.visible_attempts == []
  end

  test "runs the journal replay smoke path" do
    assert %SquidMesh.ReadModel.Inspection.Snapshot{} = run = Smoke.run_journal_replay!()

    assert run.status == :completed
    assert run.replayed_from_run_id
    assert run.applied_runnable_keys == run.planned_runnable_keys
  end

  test "runs the journal cron smoke path" do
    assert %SquidMesh.ReadModel.Inspection.Snapshot{} = run = Smoke.run_journal_cron_digest!()

    assert run.status == :completed
    assert run.trigger == "daily_digest"
    assert run.context.schedule.signal_id
  end

  test "runs the journal cron duplicate smoke path" do
    assert %SquidMesh.ReadModel.Inspection.Snapshot{} =
             run = Smoke.run_journal_cron_duplicate_digest!()

    assert run.status == :completed
    assert run.trigger == "daily_digest"
    assert run.context.schedule.idempotency == :return_existing_run
  end

  test "runs the cancellation smoke path" do
    assert %SquidMesh.ReadModel.Inspection.Snapshot{} = run = Smoke.run_cancellation!()
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

  defp with_squid_mesh_env(config, fun) when is_list(config) and is_function(fun, 0) do
    original_config = Application.get_all_env(:squid_mesh)

    try do
      :squid_mesh
      |> Application.get_all_env()
      |> Keyword.keys()
      |> Enum.each(&Application.delete_env(:squid_mesh, &1))

      Enum.each(config, fn {key, value} -> Application.put_env(:squid_mesh, key, value) end)

      fun.()
    after
      :squid_mesh
      |> Application.get_all_env()
      |> Keyword.keys()
      |> Enum.each(&Application.delete_env(:squid_mesh, &1))

      Enum.each(original_config, fn {key, value} ->
        Application.put_env(:squid_mesh, key, value)
      end)
    end
  end

  defp await_terminal_without_harness(run_id, attempts \\ 50)
  defp await_terminal_without_harness(_run_id, 0), do: {:error, :timeout}

  defp await_terminal_without_harness(run_id, attempts_remaining)
       when attempts_remaining > 0 do
    case WorkflowRuns.inspect_run(run_id) do
      {:ok, %{status: status} = run} when status in [:completed, :failed, :cancelled] ->
        {:ok, run}

      {:ok, _run} ->
        Process.sleep(20)
        await_terminal_without_harness(run_id, attempts_remaining - 1)

      {:error, _reason} = error ->
        error
    end
  end

  defp drain_default_journal_run(_run_id, _queue, 0), do: {:error, :timeout}

  defp drain_default_journal_run(run_id, queue, attempts_remaining) when attempts_remaining > 0 do
    case SquidMesh.inspect_run(run_id) do
      {:ok, %Snapshot{terminal?: true} = run} ->
        {:ok, run}

      {:ok, %Snapshot{}} ->
        case SquidMesh.execute_next(
               owner_id: "minimal-host-app-default-journal-test",
               queue: queue
             ) do
          {:ok, %Snapshot{terminal?: true} = run} ->
            {:ok, run}

          {:ok, %Snapshot{}} ->
            drain_default_journal_run(run_id, queue, attempts_remaining - 1)

          {:ok, :none} ->
            Process.sleep(50)
            drain_default_journal_run(run_id, queue, attempts_remaining - 1)

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end
end
