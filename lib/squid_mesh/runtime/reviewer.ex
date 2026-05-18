defmodule SquidMesh.Runtime.Reviewer do
  @moduledoc """
  Applies explicit approval decisions to paused approval steps.

  Approval and rejection reuse the durable paused-step lifecycle from the
  runtime, but this module owns the decision-specific contract: validating
  reviewer input, completing the paused step with decision output, and
  resuming the run through the persisted approval targets.
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

  @type decision :: :approved | :rejected
  @type review_attrs :: %{
          required(:actor) => String.t() | map(),
          optional(:comment) => String.t(),
          optional(:metadata) => map()
        }

  @doc false
  @spec review(Config.t(), Run.t(), decision(), map()) :: :ok | {:error, term()}
  def review(%Config{} = config, %Run{workflow: workflow} = run, decision, attrs)
      when is_atom(workflow) and decision in [:approved, :rejected] and is_map(attrs) do
    with :ok <- validate_review_attrs(attrs) do
      config
      |> run_review_transaction(run, workflow, decision, attrs)
      |> emit_review_events()
    end
  end

  def review(%Config{}, %Run{workflow: workflow}, _decision, _attrs) do
    {:error, {:invalid_workflow, workflow}}
  end

  defp run_review_transaction(config, run, workflow, decision, attrs) do
    config.repo.transaction(fn -> do_review(config, run, workflow, decision, attrs) end)
  end

  defp do_review(config, run, workflow, decision, attrs) do
    with {:ok, {paused_run, run_record}} <- locked_paused_run(config.repo, run.id),
         {:ok, step_name} <- paused_step_name(paused_run),
         {:ok, definition} <- SquidMesh.Workflow.Definition.load(workflow),
         {:ok, _approval_step} <- approval_step_definition(definition, step_name),
         {:ok, step_run} <- running_step_run(config.repo, paused_run.id, step_name),
         {:ok, mapped_output, target} <-
           review_metadata(step_run, definition, step_name, decision, attrs),
         manual_event = ManualAction.build(decision, attrs),
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
           resume_reviewed_run(config, config.repo, run_record, paused_run, target, mapped_output) do
      {paused_run, step_name, attempt, resumed_run, from_status, to_status}
    else
      {:error, reason} -> config.repo.rollback(reason)
    end
  end

  defp emit_review_events(
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

  defp emit_review_events({:error, reason}), do: {:error, reason}

  defp validate_review_attrs(%{actor: actor} = attrs)
       when (is_binary(actor) and actor != "") or is_map(actor) do
    case ManualAction.validate(attrs, require_actor: true) do
      :ok ->
        :ok

      {:error, {:invalid_manual_action, details}} ->
        {:error, {:invalid_review, details}}
    end
  end

  defp validate_review_attrs(_attrs), do: {:error, {:invalid_review, %{actor: :required}}}

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

  defp approval_step_definition(definition, step_name) do
    with {:ok, step} <- SquidMesh.Workflow.Definition.step(definition, step_name) do
      case step do
        %{module: :approval} -> {:ok, step}
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

  defp review_metadata(
         %StepRun{
           resume: %{
             "kind" => "approval",
             "ok_target" => ok_target,
             "error_target" => error_target,
             "output_key" => output_key
           }
         },
         definition,
         step_name,
         decision,
         attrs
       )
       when is_binary(ok_target) and is_binary(error_target) do
    with {:ok, resolved_output_key} <- persisted_output_key(step_name, output_key) do
      {:ok, map_review_output(attrs, decision, resolved_output_key),
       deserialize_decision_target(definition, decision, ok_target, error_target)}
    end
  end

  defp review_metadata(%StepRun{}, definition, step_name, decision, attrs) do
    with {:ok, targets} <-
           SquidMesh.Workflow.Definition.approval_transition_targets(definition, step_name),
         {:ok, output_key} <-
           SquidMesh.Workflow.Definition.step_output_mapping(definition, step_name) do
      {:ok, map_review_output(attrs, decision, output_key), decision_target(decision, targets)}
    end
  end

  defp resume_reviewed_run(
         %Config{} = config,
         repo,
         run_record,
         paused_run,
         target,
         mapped_output
       ) do
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

  defp map_review_output(attrs, decision, output_key) do
    review_output =
      %{
        decision: serialize_decision(decision),
        actor: Map.fetch!(attrs, :actor),
        decided_at: DateTime.to_iso8601(DateTime.utc_now(:microsecond))
      }
      |> maybe_put(:comment, Map.get(attrs, :comment))
      |> maybe_put(:metadata, Map.get(attrs, :metadata))

    case output_key do
      nil -> review_output
      mapped_key -> %{mapped_key => review_output}
    end
  end

  defp deserialize_decision_target(definition, :approved, ok_target, _error_target),
    do: deserialize_resume_target(definition, ok_target)

  defp deserialize_decision_target(definition, :rejected, _ok_target, error_target),
    do: deserialize_resume_target(definition, error_target)

  defp deserialize_resume_target(_definition, "__complete__"), do: :complete

  defp deserialize_resume_target(definition, target) when is_binary(target) do
    SquidMesh.Workflow.Definition.deserialize_step(definition, target)
  end

  defp decision_target(:approved, targets), do: Map.fetch!(targets, :ok)
  defp decision_target(:rejected, targets), do: Map.fetch!(targets, :error)

  defp persisted_output_key(_step_name, nil), do: {:ok, nil}

  defp persisted_output_key(_step_name, output_key) when is_binary(output_key),
    do: {:ok, output_key}

  defp persisted_output_key(step_name, _output_key),
    do: {:error, {:invalid_resume_metadata, step_name}}

  defp serialize_decision(:approved), do: "approved"
  defp serialize_decision(:rejected), do: "rejected"

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp merged_context(%Run{} = run, mapped_output) do
    Map.merge(run.context || %{}, mapped_output)
  end

  defp locked_run_record(repo, run_id) do
    SquidMesh.Persistence.Run
    |> where([run], run.id == ^run_id)
    |> lock("FOR UPDATE")
    |> repo.one()
  end

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
