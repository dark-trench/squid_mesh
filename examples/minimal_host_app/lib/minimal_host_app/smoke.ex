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

  @poll_attempts 20

  @spec run!() :: SquidMesh.Run.t()
  def run! do
    RuntimeHarness.ensure_runtime_started()

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
           RuntimeHarness.await_terminal_run(run.id, attempts: @poll_attempts) do
      IO.puts("started run #{run.id} for #{inspect(run.workflow)}")
      RuntimeHarness.stop_gateway_server(server_pid)

      unless inspected_run.id == run.id and inspected_run.status == :completed do
        raise "unexpected smoke result"
      end

      inspected_run
    else
      {:error, reason} ->
        raise "smoke test failed: #{inspect(reason)}"
    end
  end

  @spec run_all!() :: %{
          payment_recovery: SquidMesh.Run.t(),
          dependency_recovery: SquidMesh.Run.t(),
          manual_approval: SquidMesh.Run.t(),
          manual_digest: SquidMesh.Run.t(),
          local_ledger_checkout: SquidMesh.Run.t(),
          local_ledger_rollback: SquidMesh.Run.t(),
          saga_checkout: SquidMesh.Run.t(),
          daily_digest: SquidMesh.Run.t()
        }
  def run_all! do
    payment_recovery = run!()
    dependency_recovery = run_dependency_recovery!()
    manual_approval = run_manual_approval!()
    manual_digest = run_manual_digest!()
    {local_ledger_checkout, local_ledger_rollback} = run_local_ledger_checkout!()
    saga_checkout = run_saga_checkout!()
    existing_daily_digest_run_ids = daily_digest_run_ids()

    with :ok <- run_cron_digest(),
         {:ok, cron_run} <-
           await_daily_digest_run(existing_daily_digest_run_ids, @poll_attempts) do
      unless cron_run.status == :completed and cron_run.trigger == :daily_digest do
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
        daily_digest: cron_run
      }
    else
      {:error, reason} ->
        raise "cron smoke test failed: #{inspect(reason)}"
    end
  end

  @spec run_dependency_recovery!() :: SquidMesh.Run.t()
  def run_dependency_recovery! do
    attrs = %{
      account_id: "acct_dependency_demo",
      invoice_id: "inv_dependency_demo",
      attempt_id: "attempt_dependency_demo"
    }

    with {:ok, run} <- WorkflowRuns.start_dependency_recovery(attrs),
         :ok <- RuntimeHarness.wait_for_execution(),
         {:ok, inspected_run} <-
           RuntimeHarness.await_terminal_run(run.id, attempts: @poll_attempts),
         {:ok, history_run} <- WorkflowRuns.inspect_run(run.id, include_history: true) do
      unless inspected_run.id == run.id and inspected_run.status == :completed do
        raise "unexpected dependency recovery smoke result"
      end

      unless Enum.map(history_run.steps, &{&1.step, &1.status, &1.depends_on}) == [
               {:load_account, :completed, []},
               {:load_invoice, :completed, []},
               {:prepare_notification, :completed, [:load_account, :load_invoice]}
             ] do
        raise "unexpected dependency inspection history"
      end

      inspected_run
    else
      {:error, reason} ->
        raise "dependency recovery smoke test failed: #{inspect(reason)}"
    end
  end

  @spec run_cancellation!() :: SquidMesh.Run.t()
  def run_cancellation! do
    RuntimeHarness.ensure_runtime_started()

    case run_cancellation_smoke() do
      {:ok, cancelled_run} ->
        cancelled_run

      {:error, reason} ->
        raise "cancellation smoke test failed: #{inspect(reason)}"
    end
  end

  @spec run_manual_approval!() :: SquidMesh.Run.t()
  def run_manual_approval! do
    with {:ok, run} <- WorkflowRuns.start_manual_approval(%{account_id: "acct_manual_demo"}),
         {:ok, _paused_run} <- await_paused_run(run.id, @poll_attempts),
         {:ok, explanation} <- WorkflowRuns.explain_run(run.id),
         :ok <- ensure_paused_approval_explanation(explanation),
         {:ok, resumed_run} <-
           WorkflowRuns.approve_run(
             run.id,
             %{actor: "ops_smoke", comment: "approved", metadata: %{ticket: "SMOKE-1"}}
           ),
         :ok <- ensure_resumed(resumed_run),
         :ok <- RuntimeHarness.wait_for_execution(),
         {:ok, inspected_run} <-
           RuntimeHarness.await_terminal_run(run.id, attempts: @poll_attempts),
         {:ok, history_run} <- WorkflowRuns.inspect_run(run.id, include_history: true),
         :ok <- ensure_manual_approval_audit(history_run) do
      unless inspected_run.id == run.id and inspected_run.status == :completed do
        raise "unexpected manual approval smoke result"
      end

      history_run
    else
      {:error, reason} ->
        raise "manual approval smoke test failed: #{inspect(reason)}"
    end
  end

  @spec run_manual_digest!() :: SquidMesh.Run.t()
  def run_manual_digest! do
    attrs = %{channel: "ops-manual", digest_date: Date.utc_today() |> Date.to_iso8601()}

    with {:ok, run} <- WorkflowRuns.start_manual_digest(attrs),
         :ok <- RuntimeHarness.wait_for_execution(),
         {:ok, inspected_run} <-
           RuntimeHarness.await_terminal_run(run.id, attempts: @poll_attempts) do
      unless inspected_run.status == :completed and inspected_run.trigger == :manual_digest do
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

  @spec run_local_ledger_checkout!() :: {SquidMesh.Run.t(), SquidMesh.Run.t()}
  def run_local_ledger_checkout! do
    committed_attrs = %{account_id: "acct_local_commit", fail_after_reserve: false}
    rolled_back_attrs = %{account_id: "acct_local_rollback", fail_after_reserve: true}

    with {:ok, committed_run} <- WorkflowRuns.start_local_ledger_checkout(committed_attrs),
         :ok <- RuntimeHarness.wait_for_execution(),
         {:ok, committed_terminal_run} <-
           RuntimeHarness.await_terminal_run(committed_run.id, attempts: @poll_attempts),
         :ok <- ensure_local_ledger_entries(committed_terminal_run, ["reserve", "capture"]),
         {:ok, rolled_back_run} <- WorkflowRuns.start_local_ledger_checkout(rolled_back_attrs),
         :ok <- RuntimeHarness.wait_for_execution(),
         {:ok, rolled_back_terminal_run} <-
           RuntimeHarness.await_terminal_run(rolled_back_run.id, attempts: @poll_attempts),
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
  Runs the saga checkout example and verifies persisted compensation history.
  """
  @spec run_saga_checkout!() :: SquidMesh.Run.t()
  def run_saga_checkout! do
    attrs = %{account_id: "acct_saga_demo", order_id: "ord_saga_demo"}

    with {:ok, run} <- WorkflowRuns.start_saga_checkout(attrs),
         :ok <- RuntimeHarness.wait_for_execution(),
         {:ok, inspected_run} <-
           RuntimeHarness.await_terminal_run(run.id, attempts: @poll_attempts),
         {:ok, history_run} <- WorkflowRuns.inspect_run(run.id, include_history: true),
         :ok <- ensure_saga_compensation(history_run) do
      unless inspected_run.status == :failed and inspected_run.current_step == :capture_payment do
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

  @spec run_cancellation_smoke() :: {:ok, SquidMesh.Run.t()} | {:error, term()}
  defp run_cancellation_smoke do
    with {:ok, run} <- WorkflowRuns.start_cancellable_wait(%{account_id: "acct_demo"}),
         :ok <- wait_for_execution(),
         {:ok, cancelling_run} <- WorkflowRuns.cancel_run(run.id),
         :ok <- ensure_cancelling(cancelling_run),
         :ok <- RuntimeHarness.perform_scheduled_step!(run.id, "record_delivery"),
         {:ok, cancelled_run} <-
           RuntimeHarness.await_terminal_run(run.id, attempts: @poll_attempts) do
      {:ok, cancelled_run}
    else
      {:error, _reason} = error -> error
      other -> {:error, other}
    end
  end

  @spec ensure_cancelling(SquidMesh.Run.t()) :: :ok | {:error, :unexpected_cancellation_status}
  defp ensure_cancelling(%SquidMesh.Run{status: :cancelling}), do: :ok
  defp ensure_cancelling(%SquidMesh.Run{}), do: {:error, :unexpected_cancellation_status}

  @spec await_paused_run(Ecto.UUID.t(), non_neg_integer()) ::
          {:ok, SquidMesh.Run.t()} | {:error, term()}
  defp await_paused_run(_run_id, 0), do: {:error, :timeout}

  defp await_paused_run(run_id, attempts_remaining) when attempts_remaining > 0 do
    :ok = RuntimeHarness.wait_for_execution()

    case WorkflowRuns.inspect_run(run_id, include_history: true) do
      {:ok, %SquidMesh.Run{} = run} ->
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

  @spec ensure_paused(SquidMesh.Run.t()) :: :ok | {:error, :unexpected_paused_status}
  defp ensure_paused(%SquidMesh.Run{status: :paused, current_step: :wait_for_approval}), do: :ok
  defp ensure_paused(%SquidMesh.Run{}), do: {:error, :unexpected_paused_status}

  @spec ensure_paused_approval_explanation(SquidMesh.RunExplanation.t()) ::
          :ok | {:error, :unexpected_explanation}
  defp ensure_paused_approval_explanation(%SquidMesh.RunExplanation{
         status: :paused,
         reason: :paused_for_approval,
         step: :wait_for_approval,
         next_actions: [:approve_run, :reject_run, :cancel_run]
       }),
       do: :ok

  defp ensure_paused_approval_explanation(%SquidMesh.RunExplanation{}),
    do: {:error, :unexpected_explanation}

  @spec ensure_resumed(SquidMesh.Run.t()) :: :ok | {:error, :unexpected_resumed_status}
  defp ensure_resumed(%SquidMesh.Run{status: :running, current_step: :record_approval}), do: :ok
  defp ensure_resumed(%SquidMesh.Run{}), do: {:error, :unexpected_resumed_status}

  @spec ensure_manual_approval_audit(SquidMesh.Run.t()) ::
          :ok | {:error, :unexpected_manual_approval_audit}
  defp ensure_manual_approval_audit(%SquidMesh.Run{audit_events: audit_events})
       when is_list(audit_events) do
    case Enum.map(audit_events, &{&1.type, &1.step, &1.actor, &1.comment, &1.metadata}) do
      [
        {:paused, :wait_for_approval, nil, nil, nil},
        {:approved, :wait_for_approval, "ops_smoke", "approved", %{ticket: "SMOKE-1"}}
      ] ->
        :ok

      _other ->
        {:error, :unexpected_manual_approval_audit}
    end
  end

  defp ensure_manual_approval_audit(%SquidMesh.Run{}),
    do: {:error, :unexpected_manual_approval_audit}

  @spec ensure_saga_compensation(SquidMesh.Run.t()) ::
          :ok | {:error, :unexpected_saga_compensation}
  defp ensure_saga_compensation(%SquidMesh.Run{step_runs: step_runs}) when is_list(step_runs) do
    expected_steps = [
      {:reserve_inventory, :completed},
      {:authorize_payment, :completed},
      {:capture_payment, :failed}
    ]

    compensation_statuses =
      step_runs
      |> Enum.filter(&(&1.step in [:reserve_inventory, :authorize_payment]))
      |> Enum.map(fn step_run ->
        {step_run.step, get_in(step_run.recovery, [:compensation, :status])}
      end)

    if Enum.map(step_runs, &{&1.step, &1.status}) == expected_steps and
         Enum.sort(compensation_statuses) == [
           {:authorize_payment, :completed},
           {:reserve_inventory, :completed}
         ] do
      :ok
    else
      {:error, :unexpected_saga_compensation}
    end
  end

  defp ensure_saga_compensation(%SquidMesh.Run{}), do: {:error, :unexpected_saga_compensation}

  @spec ensure_local_ledger_entries(SquidMesh.Run.t(), [String.t()]) ::
          :ok | {:error, :unexpected_local_ledger_entries}
  defp ensure_local_ledger_entries(%SquidMesh.Run{id: run_id}, expected_entries) do
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

  @spec latest_daily_digest_run([SquidMesh.Run.t()]) ::
          {:ok, SquidMesh.Run.t()} | {:error, :missing_daily_digest_run}
  defp latest_daily_digest_run(runs) when is_list(runs) do
    case Enum.max_by(runs, & &1.inserted_at) do
      %SquidMesh.Run{} = run -> {:ok, run}
      _other -> {:error, :missing_daily_digest_run}
    end
  rescue
    Enum.EmptyError -> {:error, :missing_daily_digest_run}
  end

  @spec await_daily_digest_run(MapSet.t(Ecto.UUID.t()), non_neg_integer()) ::
          {:ok, SquidMesh.Run.t()} | {:error, term()}
  defp await_daily_digest_run(_existing_run_ids, 0), do: {:error, :missing_daily_digest_run}

  defp await_daily_digest_run(existing_run_ids, attempts_remaining) when attempts_remaining > 0 do
    :ok = wait_for_execution()

    case WorkflowRuns.list_daily_digest_runs() do
      {:ok, []} ->
        Process.sleep(50)
        await_daily_digest_run(existing_run_ids, attempts_remaining - 1)

      {:ok, runs} ->
        new_runs =
          Enum.reject(runs, fn run -> MapSet.member?(existing_run_ids, run.id) end)

        with {:ok, run} <- latest_daily_digest_run(new_runs) do
          RuntimeHarness.await_terminal_run(run.id, attempts: @poll_attempts)
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
      {:ok, runs} -> MapSet.new(runs, & &1.id)
      {:error, _reason} -> MapSet.new()
    end
  end

  defp manual_oban_testing? do
    case Application.fetch_env(:minimal_host_app, Oban) do
      {:ok, config} -> Keyword.get(config, :testing) == :manual
      :error -> false
    end
  end
end
