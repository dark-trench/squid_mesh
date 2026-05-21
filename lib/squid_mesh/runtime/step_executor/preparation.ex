defmodule SquidMesh.Runtime.StepExecutor.Preparation do
  @moduledoc """
  Prepares a runnable workflow step for execution.

  This phase resolves which step a worker is allowed to execute, ensures the
  run is in the correct lifecycle state, claims durable step-run state, and
  builds the normalized input that execution will consume.
  """

  alias SquidMesh.Config
  alias SquidMesh.Run
  alias SquidMesh.Runs
  alias SquidMesh.Runtime.StepExecutor.PreparedStep
  alias SquidMesh.Runtime.StepInput
  alias SquidMesh.Runtime.StepRecovery
  alias SquidMesh.Steps

  @type prepare_result ::
          {:execute, PreparedStep.t()}
          | {:reconcile, PreparedStep.t()}
          | {:skip, PreparedStep.t()}
          | {:cancel, Run.t()}
          | :skip
          | {:error, term()}
  @doc false
  @spec prepare(Config.t(), SquidMesh.Workflow.Definition.t(), Run.t(), atom() | nil) ::
          prepare_result()
  def prepare(%Config{} = config, definition, %Run{} = run, expected_step) do
    # Lock the run before claiming a step so stale workers cannot start side
    # effects after cancellation or another terminal transition wins the race.
    case config.repo.transaction(fn ->
           config
           |> lock_prepare_run(run.id)
           |> prepare_locked_run(config, definition, expected_step)
         end) do
      {:ok, {:prepared, {:recover_stale, prepared}, events}} ->
        emit_post_commit_events(events)
        recover_existing_step(config, prepared)

      {:ok, {:prepared, result, events}} ->
        emit_post_commit_events(events)
        result

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp lock_prepare_run(config, run_id), do: Runs.Store.get_run_for_update(config.repo, run_id)

  defp prepare_locked_run({:ok, locked_run}, config, definition, expected_step) do
    {result, events} = do_prepare(config, definition, locked_run, expected_step)
    {:prepared, result, events}
  end

  defp prepare_locked_run({:error, reason}, config, _definition, _expected_step) do
    config.repo.rollback(reason)
  end

  defp do_prepare(%Config{} = config, definition, %Run{status: status} = run, expected_step)
       when status in [:pending, :running, :retrying] do
    case resolve_execution_step(config.repo, definition, run, expected_step) do
      {:ok, execution_step} ->
        prepare_execution_step(config, definition, run, execution_step)

      :skip ->
        {:skip, []}

      {:error, _reason} = error ->
        {error, []}
    end
  end

  defp do_prepare(_config, _definition, %Run{status: :cancelling} = run, _expected_step) do
    {{:cancel, run}, []}
  end

  defp do_prepare(_config, _definition, %Run{}, _expected_step), do: {:skip, []}

  defp prepare_execution_step(config, definition, run, execution_step) do
    with {:ok, step} <- SquidMesh.Workflow.Definition.step(definition, execution_step),
         {:ok, input_mapping} <-
           SquidMesh.Workflow.Definition.step_input_mapping(definition, execution_step),
         {:ok, running_run, events} <-
           ensure_running(config.repo, run, definition, execution_step),
         candidate_input = StepInput.build_step_input(running_run, input_mapping),
         {:ok, recovery_policy} <-
           SquidMesh.Workflow.Definition.step_recovery_policy(definition, execution_step),
         {:ok, step_run, execution_mode} <-
           Steps.Store.begin_step(
             config.repo,
             running_run.id,
             execution_step,
             candidate_input,
             recovery_policy
           ) do
      prepared =
        prepared_step(
          config,
          definition,
          running_run,
          execution_step,
          step,
          step_run,
          candidate_input
        )

      prepare_execution_mode(config, prepared, execution_mode, events)
    end
  end

  defp prepared_step(config, definition, running_run, execution_step, step, step_run, input) do
    %PreparedStep{
      config: config,
      definition: definition,
      run: running_run,
      step_name: execution_step,
      step: step,
      step_run: step_run,
      input: execution_input(step_run, input)
    }
  end

  defp prepare_execution_mode(_config, prepared, :execute, events),
    do: {{:execute, prepared}, events}

  defp prepare_execution_mode(config, prepared, :skip, events) do
    {prepare_existing_step(config, prepared), events}
  end

  @spec ensure_running(module(), Run.t(), SquidMesh.Workflow.Definition.t(), atom()) ::
          {:ok, Run.t(), [tuple()]} | {:error, term()}
  defp ensure_running(
         repo,
         %Run{status: :pending, id: run_id},
         definition,
         execution_step
       ) do
    case Runs.Store.transition_run_silent(repo, run_id, :running, %{
           current_step: running_step(definition, execution_step)
         }) do
      {:ok, {running_run, from_status, to_status}} ->
        {:ok, running_run, [run_transition_event(running_run, from_status, to_status)]}

      {:error, {:invalid_transition, :running, :running}} ->
        get_already_running_run(repo, run_id)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ensure_running(
         repo,
         %Run{status: :retrying, id: run_id},
         definition,
         execution_step
       ) do
    case Runs.Store.transition_run_silent(repo, run_id, :running, %{
           current_step: running_step(definition, execution_step)
         }) do
      {:ok, {running_run, from_status, to_status}} ->
        {:ok, running_run, [run_transition_event(running_run, from_status, to_status)]}

      {:error, {:invalid_transition, :running, :running}} ->
        get_already_running_run(repo, run_id)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp ensure_running(_repo, %Run{} = run, _definition, _execution_step), do: {:ok, run, []}

  @spec resolve_execution_step(module(), SquidMesh.Workflow.Definition.t(), Run.t(), atom() | nil) ::
          {:ok, atom()} | :skip | {:error, term()}
  defp resolve_execution_step(
         repo,
         definition,
         %Run{current_step: current_step, id: run_id},
         expected_step
       ) do
    resolve_execution_step_mode(
      SquidMesh.Workflow.Definition.dependency_mode?(definition),
      repo,
      run_id,
      current_step,
      expected_step
    )
  end

  defp resolve_execution_step_mode(true, repo, run_id, _current_step, expected_step)
       when is_atom(expected_step) do
    resolve_dependency_execution_step(repo, run_id, expected_step)
  end

  defp resolve_execution_step_mode(_dependency_mode?, _repo, _run_id, current_step, expected_step) do
    cond do
      is_atom(current_step) and expected_step in [nil, current_step] -> {:ok, current_step}
      is_atom(expected_step) and current_step != expected_step -> :skip
      true -> {:error, {:invalid_step, current_step}}
    end
  end

  defp resolve_dependency_execution_step(repo, run_id, expected_step) do
    case Steps.Store.get_step_run(repo, run_id, expected_step) do
      %SquidMesh.Persistence.StepRun{status: status} when status in ["pending", "failed"] ->
        {:ok, expected_step}

      %SquidMesh.Persistence.StepRun{} ->
        :skip

      nil ->
        :skip
    end
  end

  defp running_step(definition, execution_step) do
    if SquidMesh.Workflow.Definition.dependency_mode?(definition), do: nil, else: execution_step
  end

  defp execution_input(step_run, fallback_input) do
    step_run.input
    |> Kernel.||(fallback_input)
    |> StepInput.normalize_map_keys()
  end

  defp get_already_running_run(repo, run_id) do
    with {:ok, run} <- Runs.Store.get_run(repo, run_id) do
      {:ok, run, []}
    end
  end

  defp emit_post_commit_events(events) do
    Enum.each(events, fn {:run_transition, run, from_status, to_status} ->
      SquidMesh.Observability.emit_run_transition(run, from_status, to_status)
    end)
  end

  defp run_transition_event(run, from_status, to_status) do
    {:run_transition, run, from_status, to_status}
  end

  defp prepare_existing_step(_config, %PreparedStep{step_run: %{status: "completed"}} = prepared) do
    {:reconcile, prepared}
  end

  defp prepare_existing_step(
         _config,
         %PreparedStep{step_run: %{status: "running"}} = prepared
       ) do
    {:recover_stale, prepared}
  end

  defp prepare_existing_step(_config, prepared), do: {:skip, prepared}

  defp recover_existing_step(
         %Config{stale_step_timeout: :disabled},
         %PreparedStep{} = prepared
       ) do
    {:skip, prepared}
  end

  defp recover_existing_step(
         %Config{stale_step_timeout: stale_step_timeout} = config,
         %PreparedStep{} = prepared
       ) do
    # A duplicate delivery normally skips a running step. If the previous worker
    # died, reclaim outside the run lock, then re-enter preparation. This keeps
    # the lock order compatible with normal completion: attempt/step before run.
    case StepRecovery.reclaim_stale_running_step(
           config.repo,
           prepared.step_run,
           stale_step_timeout
         ) do
      {:ok, :reclaimed} ->
        prepare(config, prepared.definition, prepared.run, prepared.step_name)

      {:ok, _not_reclaimed} ->
        {:skip, prepared}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
