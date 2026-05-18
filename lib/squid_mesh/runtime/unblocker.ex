defmodule SquidMesh.Runtime.Unblocker do
  @moduledoc """
  Resumes runs that are intentionally paused for manual intervention.

  This module validates the paused step, completes its durable running step
  state, and hands control back to the normal success progression path.
  """

  import Ecto.Query

  alias SquidMesh.AttemptStore
  alias SquidMesh.Config
  alias SquidMesh.Observability
  alias SquidMesh.Persistence.StepAttempt
  alias SquidMesh.Persistence.StepRun
  alias SquidMesh.Run
  alias SquidMesh.RunStore.Persistence
  alias SquidMesh.RunStore.Serialization
  alias SquidMesh.Runtime.Dispatcher
  alias SquidMesh.Runtime.ManualAction
  alias SquidMesh.StepRunStore

  @doc false
  @spec unblock(Config.t(), Run.t(), map()) :: :ok | {:error, term()}
  def unblock(config, run, attrs \\ %{})

  def unblock(%Config{} = config, %Run{workflow: workflow} = run, attrs)
      when is_atom(workflow) and is_map(attrs) do
    case ManualAction.validate(attrs) do
      :ok ->
        config
        |> run_unblock_transaction(run, attrs, workflow)
        |> emit_unblock_events()

      {:error, {:invalid_manual_action, details}} ->
        {:error, {:invalid_resume, details}}
    end
  end

  def unblock(%Config{}, %Run{workflow: workflow}, _attrs) do
    {:error, {:invalid_workflow, workflow}}
  end

  defp run_unblock_transaction(config, run, attrs, workflow) do
    config.repo.transaction(fn -> do_unblock(config, run, attrs, workflow) end)
  end

  defp do_unblock(config, run, attrs, workflow) do
    with {:ok, {paused_run, run_record}} <- locked_paused_run(config.repo, run.id),
         {:ok, step_name} <- paused_step_name(paused_run),
         {:ok, definition} <- SquidMesh.Workflow.Definition.load(workflow),
         {:ok, _pause_step} <- paused_step_definition(definition, step_name),
         {:ok, step_run} <- running_step_run(config.repo, paused_run.id, step_name),
         {:ok, mapped_output, target} <- resume_metadata(step_run, definition, step_name),
         manual_event = ManualAction.build(:resumed, attrs),
         {:ok, attempt} <- running_attempt(config.repo, step_run.id),
         {:ok, _attempt} <- AttemptStore.complete_attempt(config.repo, attempt.id),
         {:ok, _step_run} <-
           StepRunStore.complete_manual_step(
             config.repo,
             step_run.id,
             mapped_output,
             manual_event
           ),
         {:ok, resumed_run, from_status, to_status} <-
           resume_paused_run(config, config.repo, run_record, paused_run, target, mapped_output) do
      {paused_run, step_name, attempt, resumed_run, from_status, to_status}
    else
      {:error, reason} -> config.repo.rollback(reason)
    end
  end

  defp emit_unblock_events(
         {:ok, {paused_run, step_name, attempt, resumed_run, from_status, to_status}}
       ) do
    Observability.emit_step_completed(
      paused_run,
      step_name,
      attempt.attempt_number,
      Observability.duration_since(attempt.inserted_at)
    )

    Observability.emit_run_transition(resumed_run, from_status, to_status)
    :ok
  end

  defp emit_unblock_events({:error, reason}), do: {:error, reason}

  defp locked_paused_run(repo, run_id) do
    case locked_run_record(repo, run_id) do
      %SquidMesh.Persistence.Run{status: "paused"} = run_record ->
        {:ok, {Serialization.to_public_run(run_record), run_record}}

      %SquidMesh.Persistence.Run{status: status} ->
        {:error, {:invalid_transition, Serialization.deserialize_status(status), :running}}

      nil ->
        {:error, :not_found}
    end
  end

  defp paused_step_name(%Run{current_step: step_name}) when is_atom(step_name),
    do: {:ok, step_name}

  defp paused_step_name(%Run{current_step: step_name}), do: {:error, {:invalid_step, step_name}}

  defp paused_step_definition(definition, step_name) do
    with {:ok, step} <- SquidMesh.Workflow.Definition.step(definition, step_name) do
      case step do
        %{module: :pause} -> {:ok, step}
        _other -> {:error, {:invalid_step, step_name}}
      end
    end
  end

  defp running_step_run(repo, run_id, step_name) do
    serialized_step = SquidMesh.Workflow.Definition.serialize_step(step_name)

    case locked_step_run(repo, run_id, serialized_step) do
      %StepRun{status: "running"} = step_run -> {:ok, step_run}
      _other -> {:error, {:invalid_step, step_name}}
    end
  end

  defp running_attempt(repo, step_run_id) do
    case locked_latest_attempt(repo, step_run_id) do
      %StepAttempt{status: "running"} = attempt -> {:ok, attempt}
      _other -> {:error, :not_found}
    end
  end

  defp locked_run_record(repo, run_id) do
    SquidMesh.Persistence.Run
    |> where([run], run.id == ^run_id)
    |> lock("FOR UPDATE")
    |> repo.one()
  end

  defp resume_metadata(
         %StepRun{resume: %{"output" => output, "target" => target}},
         definition,
         _step_name
       )
       when is_map(output) and is_binary(target) do
    {:ok, output, deserialize_resume_target(definition, target)}
  end

  defp resume_metadata(%StepRun{}, definition, step_name) do
    with {:ok, mapped_output} <-
           SquidMesh.Workflow.Definition.apply_output_mapping(definition, step_name, %{}),
         {:ok, target} <-
           SquidMesh.Workflow.Definition.transition_target(definition, step_name, :ok) do
      {:ok, mapped_output, target}
    end
  end

  defp resume_paused_run(%Config{} = config, repo, run_record, paused_run, target, mapped_output) do
    attrs = %{
      context: merged_context(paused_run, mapped_output),
      last_error: nil
    }

    case target do
      :complete ->
        with {:ok, updated_run} <-
               Persistence.update_run_record(
                 repo,
                 run_record,
                 Persistence.transition_changeset_attrs(
                   :completed,
                   Map.put(attrs, :current_step, nil)
                 )
               ) do
          {:ok, updated_run, :paused, :completed}
        end

      next_step when is_atom(next_step) ->
        dispatch_resumed_run(config, repo, run_record, attrs, next_step)

      _other ->
        {:error, {:invalid_step, paused_run.current_step}}
    end
  end

  defp dispatch_resumed_run(config, repo, run_record, attrs, next_step) do
    with {:ok, updated_run} <-
           Persistence.update_run_record(
             repo,
             run_record,
             Persistence.transition_changeset_attrs(
               :running,
               Map.put(attrs, :current_step, next_step)
             )
           ) do
      case Dispatcher.dispatch_run(config, updated_run, []) do
        {:ok, _job} -> {:ok, updated_run, :paused, :running}
        {:error, reason} -> {:error, {:dispatch_failed, reason}}
      end
    end
  end

  defp merged_context(%Run{} = run, mapped_output) do
    Map.merge(run.context || %{}, mapped_output)
  end

  defp deserialize_resume_target(_definition, "__complete__"), do: :complete

  defp deserialize_resume_target(definition, target),
    do: SquidMesh.Workflow.Definition.deserialize_step(definition, target)

  defp locked_step_run(repo, run_id, step_name) do
    StepRun
    |> where([step_run], step_run.run_id == ^run_id and step_run.step == ^step_name)
    |> lock("FOR UPDATE")
    |> repo.one()
  end

  defp locked_latest_attempt(repo, step_run_id) do
    StepAttempt
    |> where([attempt], attempt.step_run_id == ^step_run_id)
    |> order_by([attempt],
      desc: attempt.attempt_number,
      desc: attempt.inserted_at,
      desc: attempt.id
    )
    |> limit(1)
    |> lock("FOR UPDATE")
    |> repo.one()
  end
end
