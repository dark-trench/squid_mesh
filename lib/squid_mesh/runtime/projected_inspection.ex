defmodule SquidMesh.Runtime.ProjectedInspection do
  @moduledoc """
  Projection-backed inspection for the Jido-native runtime path.

  The current public `SquidMesh.inspect_run/2` API reads the stable Postgres
  runtime tables. This module is the first read-model boundary for the
  Jido-native runtime: it rebuilds workflow and dispatch agents from
  `Jido.Storage`, combines their projections, and returns a factual snapshot of
  one run.

  The snapshot is intentionally read-only. It does not recover missing dispatch
  entries, apply completed results, or mutate checkpoints. Recovery remains
  owned by `SquidMesh.Runtime.AgentRecovery`; inspection reports what the
  durable journals currently prove.
  """

  alias Jido.Agent
  alias SquidMesh.Runtime.DispatchAgent
  alias SquidMesh.Runtime.DispatchProtocol.ActionAttempt
  alias SquidMesh.Runtime.DispatchProtocol.Projection, as: DispatchProjection
  alias SquidMesh.Runtime.Journal
  alias SquidMesh.Runtime.ProjectedInspection.Snapshot
  alias SquidMesh.Runtime.WorkflowAgent
  alias SquidMesh.Runtime.WorkflowAgent.Projection, as: WorkflowProjection

  @type storage_config :: Journal.storage_config()
  @type snapshot_option :: {:queue, atom() | String.t()} | {:now, DateTime.t()}
  @type snapshot_error ::
          :not_found
          | {:invalid_option,
             {:now, term()} | {:queue, term()} | {:opts, term()} | {:option, atom()}}
          | term()

  @doc """
  Builds a projection-backed inspection snapshot for one workflow run.

  Options:

  - `:queue` selects the dispatch queue projection to join with the run
    projection. It defaults to `"default"`.
  - `:now` controls visibility and lease-expiry calculations. It defaults to
    `DateTime.utc_now/0`.

  Missing run threads return `{:error, :not_found}`. A missing dispatch thread is
  treated as an empty queue projection because a run can be planned before its
  dispatch intents have been recovered.
  """
  @spec snapshot(storage_config(), WorkflowAgent.run_id(), [snapshot_option()]) ::
          {:ok, Snapshot.t()} | {:error, snapshot_error()}
  def snapshot(storage, run_id, opts \\ [])

  def snapshot(storage, run_id, opts) when is_binary(run_id) and is_list(opts) do
    with {:ok, opts} <- snapshot_options(opts),
         {:ok, queue} <- snapshot_queue(opts),
         {:ok, now} <- snapshot_time(opts),
         {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, run_id),
         {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue) do
      {:ok, build_snapshot(workflow_agent, dispatch_agent, queue, now)}
    end
  end

  def snapshot(_storage, run_id, opts) when is_binary(run_id) do
    {:error, {:invalid_option, {:opts, opts}}}
  end

  defp build_snapshot(
         %Agent{
           agent_module: WorkflowAgent,
           state: %{
             run_id: run_id,
             workflow: workflow,
             projection: %WorkflowProjection{} = workflow_projection,
             thread_rev: run_thread_rev
           }
         } = workflow_agent,
         %Agent{
           agent_module: DispatchAgent,
           state: %{
             projection: %DispatchProjection{} = dispatch_projection,
             thread_rev: dispatch_thread_rev
           }
         } = dispatch_agent,
         queue,
         %DateTime{} = now
       ) do
    pending_dispatches = workflow_agent |> WorkflowAgent.pending_dispatches(dispatch_agent)
    pending_results = workflow_agent |> WorkflowAgent.pending_results(dispatch_agent)

    terminal? = WorkflowProjection.terminal?(workflow_projection)

    visible_attempts =
      dispatch_agent |> DispatchAgent.visible_attempts(now) |> attempts_for(run_id)

    expired_claims = dispatch_agent |> DispatchAgent.expired_claims(now) |> attempts_for(run_id)

    {visible_attempts, expired_claims} =
      if terminal? do
        {[], []}
      else
        {visible_attempts, expired_claims}
      end

    attempts = dispatch_projection |> run_attempts(run_id)

    %Snapshot{
      run_id: run_id,
      workflow: workflow,
      queue: queue,
      status: WorkflowAgent.status(workflow_agent),
      reason:
        snapshot_reason(
          workflow_projection,
          pending_dispatches,
          pending_results,
          visible_attempts,
          expired_claims,
          attempts
        ),
      terminal?: terminal?,
      thread_revisions: %{run: run_thread_rev, dispatch: dispatch_thread_rev},
      planned_runnables: normalize_runnables(WorkflowAgent.planned_runnables(workflow_agent)),
      planned_runnable_keys: WorkflowAgent.planned_runnable_keys(workflow_agent),
      applied_runnable_keys:
        workflow_agent
        |> WorkflowAgent.applied_runnable_keys()
        |> MapSet.to_list()
        |> Enum.sort(),
      pending_dispatches: normalize_runnables(pending_dispatches),
      pending_results: Enum.map(pending_results, &attempt_snapshot/1),
      visible_attempts: Enum.map(visible_attempts, &attempt_snapshot/1),
      expired_claims: Enum.map(expired_claims, &attempt_snapshot/1),
      attempts: Enum.map(attempts, &attempt_snapshot/1),
      anomalies: projection_anomalies(workflow_projection, dispatch_projection)
    }
  end

  defp snapshot_reason(
         %WorkflowProjection{} = workflow_projection,
         pending_dispatches,
         pending_results,
         visible_attempts,
         expired_claims,
         attempts
       ) do
    cond do
      WorkflowProjection.terminal?(workflow_projection) ->
        :terminal

      pending_results != [] ->
        :completed_result_pending_apply

      pending_dispatches != [] ->
        :planned_dispatch_pending_schedule

      expired_claims != [] ->
        :expired_claim

      visible_attempts != [] ->
        :attempt_visible

      true ->
        idle_snapshot_reason(workflow_projection, attempts)
    end
  end

  defp idle_snapshot_reason(workflow_projection, attempts) do
    cond do
      Enum.any?(attempts, &(&1.status == :claimed)) ->
        :attempt_claimed

      WorkflowProjection.status(workflow_projection) == :idle ->
        :idle

      attempts == [] ->
        :run_started

      true ->
        :waiting_for_dispatch
    end
  end

  defp attempts_for(attempts, run_id) do
    attempts
    |> Enum.filter(&(&1.run_id == run_id))
    |> sort_attempts()
  end

  defp run_attempts(%DispatchProjection{attempts: attempts}, run_id) do
    attempts
    |> Map.values()
    |> attempts_for(run_id)
  end

  defp sort_attempts(attempts) do
    Enum.sort_by(attempts, fn attempt ->
      {DateTime.to_unix(attempt.visible_at, :microsecond), attempt.runnable_key,
       attempt.attempt_number}
    end)
  end

  defp normalize_runnables(runnables) do
    runnables
    |> Enum.map(&Map.new/1)
    |> Enum.sort_by(&runnable_key/1)
  end

  defp runnable_key(runnable) when is_map(runnable) do
    Map.get(runnable, :runnable_key) || Map.get(runnable, "runnable_key") ||
      Map.get(runnable, :key) || Map.get(runnable, "key") || ""
  end

  defp attempt_snapshot(%ActionAttempt{} = attempt) do
    %{
      runnable_key: attempt.runnable_key,
      status: attempt.status,
      attempt_number: attempt.attempt_number,
      step: attempt.step,
      input: attempt.input,
      visible_at: attempt.visible_at,
      idempotency_key: attempt.idempotency_key,
      claim_id: attempt.claim_id,
      owner_id: attempt.owner_id,
      lease_until: attempt.lease_until,
      result: attempt.result,
      error: attempt.error,
      wakeup_emitted?: attempt.wakeup_emitted?,
      applied?: attempt.applied?
    }
    |> compact()
  end

  defp projection_anomalies(
         %WorkflowProjection{} = workflow_projection,
         %DispatchProjection{} = dispatch_projection
       ) do
    workflow_projection
    |> WorkflowProjection.anomalies()
    |> Enum.map(&Map.put(&1, :source, :workflow))
    |> Kernel.++(
      dispatch_projection
      |> DispatchProjection.anomalies()
      |> Enum.map(&Map.put(&1, :source, :dispatch))
    )
  end

  defp snapshot_time(opts) do
    case Keyword.get(opts, :now, DateTime.utc_now()) do
      %DateTime{} = now -> {:ok, now}
      invalid -> {:error, {:invalid_option, {:now, invalid}}}
    end
  end

  defp snapshot_options(opts) when is_list(opts) do
    cond do
      not Keyword.keyword?(opts) ->
        {:error, {:invalid_option, {:opts, opts}}}

      unsupported = Enum.find(Keyword.keys(opts), &(&1 not in [:queue, :now])) ->
        {:error, {:invalid_option, {:option, unsupported}}}

      true ->
        {:ok, opts}
    end
  end

  defp snapshot_queue(opts) do
    case Keyword.get(opts, :queue, "default") do
      queue when is_atom(queue) -> queue |> Atom.to_string() |> validate_queue(queue)
      queue when is_binary(queue) -> validate_queue(queue, queue)
      invalid -> {:error, {:invalid_option, {:queue, invalid}}}
    end
  end

  defp validate_queue("", original), do: {:error, {:invalid_option, {:queue, original}}}
  defp validate_queue(queue, _original), do: {:ok, queue}

  defp compact(map) do
    Map.reject(map, fn {_key, value} -> is_nil(value) end)
  end
end
