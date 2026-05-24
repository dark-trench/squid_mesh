defmodule MinimalHostApp.Smoke do
  @moduledoc """
  Repeatable smoke-test entrypoint for the example host app.
  """

  import Ecto.Query, only: [from: 2]

  alias MinimalHostApp.Cron
  alias MinimalHostApp.Repo
  alias MinimalHostApp.RuntimeHarness
  alias MinimalHostApp.WorkflowRuns
  alias MinimalHostApp.Workers.SquidMeshWorker
  alias SquidMesh.Executor.Payload
  alias SquidMesh.Runtime.Journal
  alias SquidMesh.Runtime.Journal.Storage.Ecto, as: JournalStorage
  alias SquidMesh.Runtime.Runner

  @poll_attempts 20
  @journal_executor_attempts 10
  @journal_executor_queue_prefix "minimal-host-app-journal-smoke"
  @journal_executor_storage {SquidMesh.Runtime.Journal.Storage.Ecto, repo: Repo}

  @spec run!() :: SquidMesh.ReadModel.Inspection.Snapshot.t()
  def run! do
    RuntimeHarness.ensure_runtime_started()
    reset_runtime_state!()

    {server_pid, port} =
      RuntimeHarness.start_gateway_server(
        fn _attempt -> RuntimeHarness.success_gateway_response("retry_required") end,
        1
      )

    attrs = %{
      account_id: "acct_demo",
      invoice_id: "inv_demo",
      attempt_id: "attempt_demo",
      gateway_url: RuntimeHarness.endpoint_url(port, "/gateway")
    }

    with {:ok, run} <- WorkflowRuns.start_payment_recovery(attrs),
         :ok <- RuntimeHarness.wait_for_execution(),
         {:ok, inspected_run} <-
           RuntimeHarness.await_terminal_run(run.run_id, attempts: @poll_attempts) do
      IO.puts("started run #{run.run_id} for #{inspect(run.workflow)}")
      RuntimeHarness.stop_gateway_server(server_pid)

      unless inspected_run.run_id == run.run_id and inspected_run.status == :completed do
        raise "unexpected smoke result"
      end

      inspected_run
    else
      {:error, reason} ->
        raise "smoke test failed: #{inspect(reason)}"
    end
  end

  @spec run_all!() :: %{
          payment_recovery: SquidMesh.ReadModel.Inspection.Snapshot.t(),
          dependency_recovery: SquidMesh.ReadModel.Inspection.Snapshot.t(),
          manual_approval: SquidMesh.ReadModel.Inspection.Snapshot.t(),
          manual_digest: SquidMesh.ReadModel.Inspection.Snapshot.t(),
          local_ledger_checkout: SquidMesh.ReadModel.Inspection.Snapshot.t(),
          local_ledger_rollback: SquidMesh.ReadModel.Inspection.Snapshot.t(),
          saga_checkout: SquidMesh.ReadModel.Inspection.Snapshot.t(),
          journal_executor: SquidMesh.ReadModel.Inspection.Snapshot.t(),
          journal_recovery: SquidMesh.ReadModel.Inspection.Snapshot.t(),
          journal_cancellation: SquidMesh.ReadModel.Inspection.Snapshot.t(),
          journal_replay: SquidMesh.ReadModel.Inspection.Snapshot.t(),
          journal_cron_digest: SquidMesh.ReadModel.Inspection.Snapshot.t(),
          daily_digest: SquidMesh.ReadModel.Inspection.Snapshot.t()
        }
  def run_all! do
    payment_recovery = run!()
    dependency_recovery = run_dependency_recovery!()
    manual_approval = run_manual_approval!()
    manual_digest = run_manual_digest!()
    {local_ledger_checkout, local_ledger_rollback} = run_local_ledger_checkout!()
    saga_checkout = run_saga_checkout!()
    journal_executor = run_journal_executor!()
    journal_recovery = run_journal_recovery!()
    journal_cancellation = run_journal_cancellation!()
    journal_replay = run_journal_replay!()
    journal_cron_digest = run_journal_cron_digest!()
    existing_daily_digest_run_ids = daily_digest_run_ids()

    with :ok <- run_cron_digest(),
         {:ok, cron_run} <-
           await_daily_digest_run(existing_daily_digest_run_ids, @poll_attempts) do
      unless cron_run.status == :completed and cron_run.trigger == "daily_digest" do
        raise "unexpected cron smoke result"
      end

      %{
        payment_recovery: payment_recovery,
        dependency_recovery: dependency_recovery,
        manual_approval: manual_approval,
        manual_digest: manual_digest,
        local_ledger_checkout: local_ledger_checkout,
        local_ledger_rollback: local_ledger_rollback,
        saga_checkout: saga_checkout,
        journal_executor: journal_executor,
        journal_recovery: journal_recovery,
        journal_cancellation: journal_cancellation,
        journal_replay: journal_replay,
        journal_cron_digest: journal_cron_digest,
        daily_digest: cron_run
      }
    else
      {:error, reason} ->
        raise "cron smoke test failed: #{inspect(reason)}"
    end
  end

  @spec run_dependency_recovery!() :: SquidMesh.ReadModel.Inspection.Snapshot.t()
  def run_dependency_recovery! do
    attrs = %{
      account_id: "acct_dependency_demo",
      invoice_id: "inv_dependency_demo",
      attempt_id: "attempt_dependency_demo"
    }

    with {:ok, run} <- WorkflowRuns.start_dependency_recovery(attrs),
         :ok <- RuntimeHarness.wait_for_execution(),
         {:ok, inspected_run} <-
           RuntimeHarness.await_terminal_run(run.run_id, attempts: @poll_attempts),
         {:ok, history_run} <- WorkflowRuns.inspect_run(run.run_id, include_history: true) do
      unless inspected_run.run_id == run.run_id and inspected_run.status == :completed do
        raise "unexpected dependency recovery smoke result"
      end

      unless Enum.map(history_run.attempts, &{&1.step, &1.status, &1.applied?}) == [
               {"load_account", :completed, true},
               {"load_invoice", :completed, true},
               {"prepare_notification", :completed, true}
             ] do
        raise "unexpected dependency inspection history"
      end

      unless mapped_dependency_input?(history_run) do
        raise "unexpected dependency mapped input"
      end

      inspected_run
    else
      {:error, reason} ->
        raise "dependency recovery smoke test failed: #{inspect(reason)}"
    end
  end

  @doc """
  Runs the dependency-based example workflow through the journal executor.
  """
  @spec run_journal_executor!() :: SquidMesh.ReadModel.Inspection.Snapshot.t()
  def run_journal_executor! do
    RuntimeHarness.ensure_runtime_started()
    queue = journal_executor_queue()

    attrs = %{
      account_id: "acct_journal_demo",
      invoice_id: "inv_journal_demo",
      attempt_id: "attempt_journal_demo"
    }

    with_journal_runtime_config(queue, fn ->
      with {:ok, started_run} <-
             SquidMesh.start_run(
               MinimalHostApp.Workflows.DependencyRecovery,
               :dependency_recovery,
               attrs
             ),
           {:ok, inspected_run} <-
             drain_journal_executor(started_run.run_id, @journal_executor_attempts),
           {:ok, explanation} <- SquidMesh.explain_run(started_run.run_id) do
        unless started_run.queue == queue and
                 inspected_run.queue == queue and
                 explanation.queue == queue do
          raise "unexpected journal executor queue"
        end

        unless inspected_run.status == :completed do
          raise "unexpected journal executor smoke result"
        end

        inspected_run
      else
        {:error, reason} ->
          raise "journal executor smoke test failed: #{inspect(reason)}"
      end
    end)
  end

  @doc """
  Runs a journal executor smoke path after dropping checkpoints.

  The append-only Jido thread log remains the source of truth, so inspection and
  execution must recover from persisted entries when checkpoint accelerators are
  missing.
  """
  @spec run_journal_recovery!() :: SquidMesh.ReadModel.Inspection.Snapshot.t()
  def run_journal_recovery! do
    RuntimeHarness.ensure_runtime_started()
    queue = journal_executor_queue()

    attrs = %{
      account_id: "acct_journal_recovery_demo",
      invoice_id: "inv_journal_recovery_demo",
      attempt_id: "attempt_journal_recovery_demo"
    }

    with_journal_runtime_config(queue, fn ->
      with {:ok, started_run} <-
             SquidMesh.start_run(
               MinimalHostApp.Workflows.DependencyRecovery,
               :dependency_recovery,
               attrs
             ),
           :ok <- delete_journal_checkpoints(started_run.run_id, queue),
           {:ok, recovered_run} <- SquidMesh.inspect_run(started_run.run_id),
           {:ok, completed_run} <-
             drain_journal_executor(started_run.run_id, @journal_executor_attempts) do
        unless recovered_run.run_id == started_run.run_id and recovered_run.queue == queue do
          raise "unexpected recovered journal run"
        end

        unless completed_run.status == :completed do
          raise "unexpected journal recovery smoke result"
        end

        completed_run
      else
        {:error, reason} ->
          raise "journal recovery smoke test failed: #{inspect(reason)}"
      end
    end)
  end

  @doc """
  Runs journal cancellation through the example app's configured Ecto storage.
  """
  @spec run_journal_cancellation!() :: SquidMesh.ReadModel.Inspection.Snapshot.t()
  def run_journal_cancellation! do
    RuntimeHarness.ensure_runtime_started()
    queue = journal_executor_queue()

    attrs = %{
      account_id: "acct_journal_cancel_demo",
      invoice_id: "inv_journal_cancel_demo",
      attempt_id: "attempt_journal_cancel_demo"
    }

    with_journal_runtime_config(queue, fn ->
      with {:ok, started_run} <-
             SquidMesh.start_run(
               MinimalHostApp.Workflows.DependencyRecovery,
               :dependency_recovery,
               attrs
             ),
           {:ok, cancelled_run} <- SquidMesh.cancel_run(started_run.run_id),
           {:ok, inspected_run} <- SquidMesh.inspect_run(started_run.run_id),
           {:ok, :none} <- SquidMesh.execute_next(journal_executor_execute_options()) do
        unless started_run.queue == queue and
                 cancelled_run.queue == queue and
                 inspected_run.queue == queue do
          raise "unexpected journal cancellation queue"
        end

        unless cancelled_run.status == :cancelled and inspected_run.status == :cancelled and
                 cancelled_run.visible_attempts == [] do
          raise "unexpected journal cancellation smoke result"
        end

        cancelled_run
      else
        {:error, reason} ->
          raise "journal cancellation smoke test failed: #{inspect(reason)}"
      end
    end)
  end

  @doc """
  Runs journal replay through the example app's configured Ecto storage.
  """
  @spec run_journal_replay!() :: SquidMesh.ReadModel.Inspection.Snapshot.t()
  def run_journal_replay! do
    RuntimeHarness.ensure_runtime_started()
    queue = journal_executor_queue()

    attrs = %{
      account_id: "acct_journal_replay_demo",
      invoice_id: "inv_journal_replay_demo",
      attempt_id: "attempt_journal_replay_demo"
    }

    with_journal_runtime_config(queue, fn ->
      with {:ok, started_run} <-
             SquidMesh.start_run(
               MinimalHostApp.Workflows.DependencyRecovery,
               :dependency_recovery,
               attrs
             ),
           {:ok, completed_run} <-
             drain_journal_executor(started_run.run_id, @journal_executor_attempts),
           {:ok, replayed_run} <- SquidMesh.replay_run(completed_run.run_id),
           {:ok, completed_replay} <-
             drain_journal_executor(replayed_run.run_id, @journal_executor_attempts) do
        unless completed_run.status == :completed and completed_replay.status == :completed do
          raise "unexpected journal replay smoke result"
        end

        unless replayed_run.replayed_from_run_id == completed_run.run_id and
                 replayed_run.input == attrs do
          raise "unexpected journal replay lineage"
        end

        completed_replay
      else
        {:error, reason} ->
          raise "journal replay smoke test failed: #{inspect(reason)}"
      end
    end)
  end

  @doc """
  Starts the daily digest cron trigger through the journal runtime.
  """
  @spec run_journal_cron_digest!() :: SquidMesh.ReadModel.Inspection.Snapshot.t()
  def run_journal_cron_digest! do
    RuntimeHarness.ensure_runtime_started()
    queue = journal_executor_queue()
    signal_id = "minimal-host-app:journal:daily_digest:#{System.unique_integer([:positive])}"

    payload =
      Payload.cron(
        MinimalHostApp.Workflows.DailyDigest,
        :daily_digest,
        signal_id: signal_id
      )

    with_journal_runtime_config(queue, fn ->
      with :ok <- Runner.perform(payload),
           {:ok, run_id} <- journal_daily_digest_run_id(queue),
           {:ok, completed_run} <- drain_journal_executor(run_id, @journal_executor_attempts) do
        unless completed_run.status == :completed and completed_run.trigger == "daily_digest" do
          raise "unexpected journal cron smoke result"
        end

        unless completed_run.context.schedule.signal_id == signal_id do
          raise "unexpected journal cron schedule context"
        end

        completed_run
      else
        {:error, reason} ->
          raise "journal cron smoke test failed: #{inspect(reason)}"
      end
    end)
  end

  @doc """
  Proves duplicate daily digest cron delivery is fenced by the journal runtime.
  """
  @spec run_journal_cron_duplicate_digest!() :: SquidMesh.ReadModel.Inspection.Snapshot.t()
  def run_journal_cron_duplicate_digest! do
    RuntimeHarness.ensure_runtime_started()
    queue = journal_executor_queue()
    signal_id = "minimal-host-app:journal:daily_digest:duplicate"

    payload =
      Payload.cron(
        MinimalHostApp.Workflows.DailyDigest,
        :daily_digest,
        signal_id: signal_id
      )

    with_journal_runtime_config(queue, fn ->
      with :ok <- Runner.perform(payload),
           {:ok, run_id} <- journal_daily_digest_run_id(queue),
           {:ok, {:duplicate_schedule_start, ^run_id}} <-
             Runner.start_cron_trigger(payload["workflow"], payload["trigger"], payload, []),
           {:ok, completed_run} <- drain_journal_executor(run_id, @journal_executor_attempts) do
        unless completed_run.context.schedule.signal_id == signal_id do
          raise "unexpected journal cron duplicate schedule context"
        end

        completed_run
      else
        {:error, reason} ->
          raise "journal cron duplicate smoke test failed: #{inspect(reason)}"

        other ->
          raise "journal cron duplicate smoke test failed: #{inspect(other)}"
      end
    end)
  end

  @spec run_cancellation!() :: SquidMesh.ReadModel.Inspection.Snapshot.t()
  def run_cancellation! do
    RuntimeHarness.ensure_runtime_started()

    case run_cancellation_smoke() do
      {:ok, cancelled_run} ->
        cancelled_run

      {:error, reason} ->
        raise "cancellation smoke test failed: #{inspect(reason)}"
    end
  end

  @spec run_manual_approval!() :: SquidMesh.ReadModel.Inspection.Snapshot.t()
  def run_manual_approval! do
    with {:ok, run} <- WorkflowRuns.start_manual_approval(%{account_id: "acct_manual_demo"}),
         {:ok, _paused_run} <- await_paused_run(run.run_id, @poll_attempts),
         {:ok, explanation} <- WorkflowRuns.explain_run(run.run_id),
         :ok <- ensure_paused_approval_explanation(explanation),
         {:ok, resumed_run} <-
           WorkflowRuns.approve_run(
             run.run_id,
             %{actor: "ops_smoke", comment: "approved", metadata: %{ticket: "SMOKE-1"}}
           ),
         :ok <- ensure_resumed(resumed_run),
         :ok <- RuntimeHarness.wait_for_execution(),
         {:ok, inspected_run} <-
           RuntimeHarness.await_terminal_run(run.run_id, attempts: @poll_attempts),
         {:ok, history_run} <- WorkflowRuns.inspect_run(run.run_id, include_history: true),
         :ok <- ensure_manual_approval_audit(history_run) do
      unless inspected_run.run_id == run.run_id and inspected_run.status == :completed do
        raise "unexpected manual approval smoke result"
      end

      history_run
    else
      {:error, reason} ->
        raise "manual approval smoke test failed: #{inspect(reason)}"
    end
  end

  @spec run_manual_digest!() :: SquidMesh.ReadModel.Inspection.Snapshot.t()
  def run_manual_digest! do
    attrs = %{channel: "ops-manual", digest_date: Date.utc_today() |> Date.to_iso8601()}

    with {:ok, run} <- WorkflowRuns.start_manual_digest(attrs),
         :ok <- RuntimeHarness.wait_for_execution(),
         {:ok, inspected_run} <-
           RuntimeHarness.await_terminal_run(run.run_id, attempts: @poll_attempts) do
      unless inspected_run.status == :completed and inspected_run.trigger == "manual_digest" do
        raise "unexpected manual digest smoke result"
      end

      unless inspected_run.context.digest_delivery.channel == attrs.channel and
               inspected_run.context.digest_delivery.digest_date == attrs.digest_date do
        raise "unexpected manual digest payload"
      end

      inspected_run
    else
      {:error, reason} ->
        raise "manual digest smoke test failed: #{inspect(reason)}"
    end
  end

  @spec run_local_ledger_checkout!() ::
          {SquidMesh.ReadModel.Inspection.Snapshot.t(),
           SquidMesh.ReadModel.Inspection.Snapshot.t()}
  def run_local_ledger_checkout! do
    committed_attrs = %{account_id: "acct_local_commit", fail_after_reserve: false}
    rolled_back_attrs = %{account_id: "acct_local_rollback", fail_after_reserve: true}

    with {:ok, committed_run} <- WorkflowRuns.start_local_ledger_checkout(committed_attrs),
         :ok <- RuntimeHarness.wait_for_execution(),
         {:ok, committed_terminal_run} <-
           RuntimeHarness.await_terminal_run(committed_run.run_id, attempts: @poll_attempts),
         :ok <- ensure_local_ledger_entries(committed_terminal_run, ["reserve", "capture"]),
         {:ok, rolled_back_run} <- WorkflowRuns.start_local_ledger_checkout(rolled_back_attrs),
         :ok <- RuntimeHarness.wait_for_execution(),
         {:ok, rolled_back_terminal_run} <-
           RuntimeHarness.await_terminal_run(rolled_back_run.run_id, attempts: @poll_attempts),
         :ok <- ensure_local_ledger_entries(rolled_back_terminal_run, []) do
      unless committed_terminal_run.status == :completed and
               rolled_back_terminal_run.status == :failed do
        raise "unexpected local ledger smoke result"
      end

      {committed_terminal_run, rolled_back_terminal_run}
    else
      {:error, reason} ->
        raise "local ledger smoke test failed: #{inspect(reason)}"
    end
  end

  @doc """
  Runs the saga checkout example and verifies persisted retry failure history.
  """
  @spec run_saga_checkout!() :: SquidMesh.ReadModel.Inspection.Snapshot.t()
  def run_saga_checkout! do
    attrs = %{account_id: "acct_saga_demo", order_id: "ord_saga_demo"}

    with {:ok, run} <- WorkflowRuns.start_saga_checkout(attrs),
         :ok <- RuntimeHarness.wait_for_execution(),
         {:ok, inspected_run} <-
           RuntimeHarness.await_terminal_run(run.run_id, attempts: @poll_attempts),
         {:ok, history_run} <- WorkflowRuns.inspect_run(run.run_id, include_history: true),
         :ok <- ensure_saga_failure_history(history_run) do
      unless inspected_run.status == :failed and
               Enum.any?(inspected_run.attempts, &(&1.step == "capture_payment")) do
        raise "unexpected saga checkout smoke result"
      end

      history_run
    else
      {:error, reason} ->
        raise "saga checkout smoke test failed: #{inspect(reason)}"
    end
  end

  @spec wait_for_execution() :: :ok
  defp wait_for_execution do
    RuntimeHarness.wait_for_execution()
  end

  defp smoke_cron_signal_id do
    "minimal-host-app:smoke:daily_digest:#{System.unique_integer([:positive])}"
  end

  defp journal_daily_digest_run_id(queue) do
    case SquidMesh.list_runs(workflow: MinimalHostApp.Workflows.DailyDigest) do
      {:ok, runs} ->
        runs
        |> Enum.find(&(&1.queue == queue))
        |> case do
          %{run_id: run_id} when is_binary(run_id) -> {:ok, run_id}
          _missing -> {:error, :missing_journal_daily_digest_run}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp mapped_dependency_input?(%SquidMesh.ReadModel.Inspection.Snapshot{attempts: attempts})
       when is_list(attempts) do
    Enum.any?(attempts, fn
      %{step: "prepare_notification", input: input} ->
        input == %{
          account_id: "acct_dependency_demo",
          invoice_id: "inv_dependency_demo",
          account_tier: "standard"
        }

      _step_run ->
        false
    end)
  end

  defp mapped_dependency_input?(_run), do: false

  @spec run_cron_digest() :: :ok
  defp run_cron_digest do
    if manual_oban_testing?() do
      # Manual Oban testing disables plugins, so start the real plugin to
      # validate its configuration and then invoke the cron worker explicitly.
      Cron.ensure_started!()

      %Oban.Job{
        args: %{
          "kind" => "cron",
          "workflow" => "Elixir.MinimalHostApp.Workflows.DailyDigest",
          "trigger" => "daily_digest",
          "signal_id" => smoke_cron_signal_id()
        }
      }
      |> SquidMeshWorker.perform()
      |> case do
        :ok -> wait_for_execution()
        {:error, reason} -> raise "manual cron smoke trigger failed: #{inspect(reason)}"
      end
    else
      Cron.evaluate!()
      wait_for_execution()
    end
  end

  @spec run_cancellation_smoke() ::
          {:ok, SquidMesh.ReadModel.Inspection.Snapshot.t()} | {:error, term()}
  defp run_cancellation_smoke do
    with {:ok, run} <- WorkflowRuns.start_cancellable_wait(%{account_id: "acct_demo"}),
         :ok <- wait_for_execution(),
         {:ok, cancelling_run} <- WorkflowRuns.cancel_run(run.run_id),
         :ok <- ensure_cancelling(cancelling_run),
         {:ok, cancelled_run} <-
           RuntimeHarness.await_terminal_run(run.run_id, attempts: @poll_attempts) do
      {:ok, cancelled_run}
    else
      {:error, _reason} = error -> error
      other -> {:error, other}
    end
  end

  @spec ensure_cancelling(SquidMesh.ReadModel.Inspection.Snapshot.t()) ::
          :ok | {:error, :unexpected_cancellation_status}
  defp ensure_cancelling(%SquidMesh.ReadModel.Inspection.Snapshot{status: :cancelled}), do: :ok

  defp ensure_cancelling(%SquidMesh.ReadModel.Inspection.Snapshot{}),
    do: {:error, :unexpected_cancellation_status}

  @spec await_paused_run(Ecto.UUID.t(), non_neg_integer()) ::
          {:ok, SquidMesh.ReadModel.Inspection.Snapshot.t()} | {:error, term()}
  defp await_paused_run(_run_id, 0), do: {:error, :timeout}

  defp await_paused_run(run_id, attempts_remaining) when attempts_remaining > 0 do
    :ok = RuntimeHarness.wait_for_execution()
    _result = SquidMesh.execute_next(owner_id: "minimal-host-app-manual-smoke")

    case WorkflowRuns.inspect_run(run_id, include_history: true) do
      {:ok, %SquidMesh.ReadModel.Inspection.Snapshot{} = run} ->
        case ensure_paused(run) do
          :ok ->
            {:ok, run}

          {:error, _reason} ->
            Process.sleep(50)
            await_paused_run(run_id, attempts_remaining - 1)
        end

      {:error, _reason} = error ->
        error
    end
  end

  @spec ensure_paused(SquidMesh.ReadModel.Inspection.Snapshot.t()) ::
          :ok | {:error, :unexpected_paused_status}
  defp ensure_paused(%SquidMesh.ReadModel.Inspection.Snapshot{
         status: :paused,
         manual_state: %{step: "wait_for_approval"}
       }),
       do: :ok

  defp ensure_paused(%SquidMesh.ReadModel.Inspection.Snapshot{}),
    do: {:error, :unexpected_paused_status}

  @spec ensure_paused_approval_explanation(SquidMesh.ReadModel.Explanation.Diagnostic.t()) ::
          :ok | {:error, :unexpected_explanation}
  defp ensure_paused_approval_explanation(%SquidMesh.ReadModel.Explanation.Diagnostic{
         status: :paused,
         next_actions: next_actions
       }) do
    if :resolve_manual_step in next_actions do
      :ok
    else
      {:error, :unexpected_explanation}
    end
  end

  defp ensure_paused_approval_explanation(%SquidMesh.ReadModel.Explanation.Diagnostic{}),
    do: {:error, :unexpected_explanation}

  @spec ensure_resumed(SquidMesh.ReadModel.Inspection.Snapshot.t()) ::
          :ok | {:error, :unexpected_resumed_status}
  defp ensure_resumed(%SquidMesh.ReadModel.Inspection.Snapshot{
         status: :running,
         visible_attempts: [%{step: "record_approval"} | _]
       }),
       do: :ok

  defp ensure_resumed(%SquidMesh.ReadModel.Inspection.Snapshot{}),
    do: {:error, :unexpected_resumed_status}

  @spec drain_journal_executor(String.t(), non_neg_integer()) ::
          {:ok, SquidMesh.ReadModel.Inspection.Snapshot.t()} | {:error, :timeout | term()}
  defp drain_journal_executor(_run_id, 0), do: {:error, :timeout}

  defp drain_journal_executor(run_id, attempts_remaining) when attempts_remaining > 0 do
    case SquidMesh.inspect_run(run_id) do
      {:ok, %SquidMesh.ReadModel.Inspection.Snapshot{terminal?: true} = run} ->
        {:ok, run}

      {:ok, %SquidMesh.ReadModel.Inspection.Snapshot{}} ->
        case SquidMesh.execute_next(journal_executor_execute_options()) do
          {:ok, %SquidMesh.ReadModel.Inspection.Snapshot{terminal?: true} = run} ->
            {:ok, run}

          {:ok, %SquidMesh.ReadModel.Inspection.Snapshot{}} ->
            drain_journal_executor(run_id, attempts_remaining - 1)

          {:ok, :none} ->
            Process.sleep(50)
            drain_journal_executor(run_id, attempts_remaining - 1)

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp journal_executor_execute_options do
    [
      owner_id: "minimal-host-app-smoke"
    ]
  end

  defp journal_executor_queue do
    "#{@journal_executor_queue_prefix}-#{System.unique_integer([:positive])}"
  end

  defp delete_journal_checkpoints(run_id, queue) when is_binary(run_id) and is_binary(queue) do
    [
      Journal.thread_id({:run, run_id}),
      Journal.thread_id({:dispatch, queue})
    ]
    |> Enum.each(fn thread_id ->
      {:ok, _checkpoint} =
        JournalStorage.get_checkpoint({"squid_mesh", :checkpoint, thread_id}, repo: Repo)

      :ok = JournalStorage.delete_checkpoint({"squid_mesh", :checkpoint, thread_id}, repo: Repo)
    end)

    :ok
  end

  defp with_journal_runtime_config(queue, fun) when is_binary(queue) and is_function(fun, 0) do
    original_config = Application.get_all_env(:squid_mesh)

    try do
      Application.put_env(:squid_mesh, :runtime, :journal)
      Application.put_env(:squid_mesh, :read_model, :read_model)
      Application.put_env(:squid_mesh, :journal_storage, @journal_executor_storage)
      Application.put_env(:squid_mesh, :queue, queue)

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

  @spec ensure_manual_approval_audit(SquidMesh.ReadModel.Inspection.Snapshot.t()) ::
          :ok | {:error, :unexpected_manual_approval_audit}
  defp ensure_manual_approval_audit(%SquidMesh.ReadModel.Inspection.Snapshot{
         context: %{approval: %{status: "approved", actor: "ops_smoke"}}
       }) do
    :ok
  end

  defp ensure_manual_approval_audit(%SquidMesh.ReadModel.Inspection.Snapshot{}),
    do: {:error, :unexpected_manual_approval_audit}

  @spec ensure_saga_failure_history(SquidMesh.ReadModel.Inspection.Snapshot.t()) ::
          :ok | {:error, :unexpected_saga_compensation}
  defp ensure_saga_failure_history(%SquidMesh.ReadModel.Inspection.Snapshot{attempts: attempts})
       when is_list(attempts) do
    expected_steps = [
      {"reserve_inventory", :completed, true, 1},
      {"authorize_payment", :completed, true, 1},
      {"capture_payment", :failed, false, 1},
      {"capture_payment", :failed, false, 2}
    ]

    if Enum.map(attempts, &{&1.step, &1.status, &1.applied?, &1.attempt_number}) ==
         expected_steps do
      :ok
    else
      {:error, :unexpected_saga_compensation}
    end
  end

  defp ensure_saga_failure_history(%SquidMesh.ReadModel.Inspection.Snapshot{}),
    do: {:error, :unexpected_saga_compensation}

  @spec ensure_local_ledger_entries(SquidMesh.ReadModel.Inspection.Snapshot.t(), [String.t()]) ::
          :ok | {:error, :unexpected_local_ledger_entries}
  defp ensure_local_ledger_entries(
         %SquidMesh.ReadModel.Inspection.Snapshot{run_id: run_id},
         expected_entries
       ) do
    entries =
      Repo.all(
        from(entry in "local_ledger_entries",
          where: entry.run_id == ^run_id,
          order_by: [asc: entry.id],
          select: entry.entry
        )
      )

    if entries == expected_entries do
      :ok
    else
      {:error, :unexpected_local_ledger_entries}
    end
  end

  @spec latest_daily_digest_run([SquidMesh.ReadModel.Listing.Summary.t()]) ::
          {:ok, SquidMesh.ReadModel.Listing.Summary.t()} | {:error, :missing_daily_digest_run}
  defp latest_daily_digest_run(runs) when is_list(runs) do
    case Enum.max_by(runs, & &1.indexed_at) do
      %SquidMesh.ReadModel.Listing.Summary{} = run -> {:ok, run}
      _other -> {:error, :missing_daily_digest_run}
    end
  rescue
    Enum.EmptyError -> {:error, :missing_daily_digest_run}
  end

  @spec await_daily_digest_run(MapSet.t(Ecto.UUID.t()), non_neg_integer()) ::
          {:ok, SquidMesh.ReadModel.Inspection.Snapshot.t()} | {:error, term()}
  defp await_daily_digest_run(_existing_run_ids, 0), do: {:error, :missing_daily_digest_run}

  defp await_daily_digest_run(existing_run_ids, attempts_remaining) when attempts_remaining > 0 do
    :ok = wait_for_execution()

    case WorkflowRuns.list_daily_digest_runs() do
      {:ok, []} ->
        Process.sleep(50)
        await_daily_digest_run(existing_run_ids, attempts_remaining - 1)

      {:ok, runs} ->
        new_runs =
          Enum.reject(runs, fn run -> MapSet.member?(existing_run_ids, run.run_id) end)

        with {:ok, run} <- latest_daily_digest_run(new_runs) do
          RuntimeHarness.await_terminal_run(run.run_id, attempts: @poll_attempts)
        else
          {:error, :missing_daily_digest_run} ->
            Process.sleep(50)
            await_daily_digest_run(existing_run_ids, attempts_remaining - 1)

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp daily_digest_run_ids do
    case WorkflowRuns.list_daily_digest_runs() do
      {:ok, runs} -> MapSet.new(runs, & &1.run_id)
      {:error, _reason} -> MapSet.new()
    end
  end

  defp manual_oban_testing? do
    case Application.fetch_env(:minimal_host_app, Oban) do
      {:ok, config} -> Keyword.get(config, :testing) == :manual
      :error -> false
    end
  end

  defp reset_runtime_state! do
    Repo.delete_all("squid_mesh_journal_entries")
    Repo.delete_all("squid_mesh_journal_checkpoints")
    Repo.delete_all("squid_mesh_journal_threads")
    Repo.delete_all("local_ledger_entries")
    Repo.delete_all("oban_jobs")
    Repo.delete_all("oban_peers")
    :ok
  end
end
