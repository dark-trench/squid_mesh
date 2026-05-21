defmodule SquidMesh.Runs.Store.Serialization do
  @moduledoc """
  Read-side serialization and hydration helpers for workflow runs.

  `SquidMesh.Runs.Store` remains the public boundary. This module keeps the
  translation between persistence records and public structs in one place so
  command code can stay focused on lifecycle transitions.
  """

  import Ecto.Query

  alias SquidMesh.Run
  alias SquidMesh.RunAuditEvent
  alias SquidMesh.RunStepState
  alias SquidMesh.StepAttempt
  alias SquidMesh.StepRun

  @type list_filter ::
          {:workflow, module()} | {:status, Run.status()} | {:limit, pos_integer()}
  @type list_filters :: [list_filter()]

  @doc false
  @spec to_public_run(SquidMesh.Persistence.Run.t()) :: Run.t()
  def to_public_run(run) do
    {workflow, definition} = deserialize_workflow(run.workflow)
    step_runs = to_public_step_runs(run, definition)

    %Run{
      id: run.id,
      workflow: workflow,
      trigger: SquidMesh.Workflow.Definition.deserialize_trigger(definition, run.trigger),
      status: deserialize_status(run.status),
      payload: SquidMesh.Workflow.Definition.deserialize_payload(definition, run.input || %{}),
      context: deserialize_map(run.context || %{}),
      current_step: deserialize_step(definition, run.current_step),
      last_error: deserialize_run_error(definition, run.last_error),
      audit_events: to_public_audit_events(definition, run.step_runs),
      steps: to_public_steps(definition, step_runs),
      step_runs: step_runs,
      replayed_from_run_id: run.replayed_from_run_id,
      inserted_at: run.inserted_at,
      updated_at: run.updated_at
    }
  end

  @doc false
  @spec serialize_filters(list_filters()) :: keyword()
  def serialize_filters(filters) do
    Enum.map(filters, fn
      {:workflow, workflow} ->
        {:workflow, SquidMesh.Workflow.Definition.serialize_workflow(workflow)}

      {:status, status} ->
        {:status, serialize_status(status)}

      {:limit, limit} ->
        {:limit, limit}
    end)
  end

  @doc false
  @spec serialize_status(Run.status()) :: String.t()
  def serialize_status(status) when is_atom(status), do: Atom.to_string(status)

  @doc false
  @spec maybe_preload_history(Ecto.Queryable.t(), boolean()) :: Ecto.Query.t()
  def maybe_preload_history(query, true) do
    preload(query, [run], step_runs: ^step_runs_preload_query())
  end

  def maybe_preload_history(query, false), do: query

  @doc false
  @spec deserialize_status(String.t()) :: Run.status()
  def deserialize_status("pending"), do: :pending
  def deserialize_status("running"), do: :running
  def deserialize_status("retrying"), do: :retrying
  def deserialize_status("paused"), do: :paused
  def deserialize_status("failed"), do: :failed
  def deserialize_status("completed"), do: :completed
  def deserialize_status("cancelling"), do: :cancelling
  def deserialize_status("cancelled"), do: :cancelled

  @doc false
  @spec deserialize_map(map() | nil) :: map() | nil
  def deserialize_map(nil), do: nil

  def deserialize_map(map) when is_map(map) do
    Map.new(map, fn
      {key, value} when is_binary(key) ->
        {deserialize_key(key), deserialize_value(value)}

      {key, value} ->
        {key, deserialize_value(value)}
    end)
  end

  @doc false
  @spec deserialize_step(SquidMesh.Workflow.Definition.t() | nil, String.t() | nil) ::
          atom() | String.t() | nil
  def deserialize_step(nil, step_name), do: step_name

  def deserialize_step(definition, step_name) do
    SquidMesh.Workflow.Definition.deserialize_step(definition, step_name)
  end

  @doc false
  @spec deserialize_workflow(String.t()) ::
          {module() | String.t(), SquidMesh.Workflow.Definition.t() | nil}
  def deserialize_workflow(workflow_name) do
    case SquidMesh.Workflow.Definition.load_serialized(workflow_name) do
      {:ok, workflow, definition} -> {workflow, definition}
      {:error, _reason} -> {workflow_name, nil}
    end
  end

  @doc false
  @spec deserialize_run_error(SquidMesh.Workflow.Definition.t() | nil, map() | nil) :: map() | nil
  def deserialize_run_error(_definition, nil), do: nil

  def deserialize_run_error(definition, error) when is_map(error) do
    error
    |> deserialize_map()
    |> maybe_update_error_step(:next_step, definition)
    |> maybe_update_error_step(:failed_step, definition)
    |> maybe_update_error_steps(:pending_steps, definition)
  end

  defp to_public_step_runs(
         %SquidMesh.Persistence.Run{step_runs: %Ecto.Association.NotLoaded{}},
         _definition
       ),
       do: nil

  defp to_public_step_runs(%SquidMesh.Persistence.Run{step_runs: step_runs}, definition)
       when is_list(step_runs) do
    Enum.map(step_runs, &to_public_step_run(&1, definition))
  end

  defp to_public_step_run(step_run, definition) do
    %StepRun{
      id: step_run.id,
      step: deserialize_step(definition, step_run.step),
      status: deserialize_step_status(step_run.status),
      input: deserialize_map(step_run.input || %{}),
      output: deserialize_map(step_run.output),
      last_error: deserialize_map(step_run.last_error),
      recovery: step_recovery_policy(definition, step_run),
      attempts: to_public_attempts(step_run),
      inserted_at: step_run.inserted_at,
      updated_at: step_run.updated_at
    }
  end

  defp to_public_steps(_definition, nil), do: nil

  defp to_public_steps(nil, step_runs) when is_list(step_runs) do
    Enum.map(step_runs, &to_public_step_state(&1, []))
  end

  defp to_public_steps(definition, step_runs) when is_list(step_runs) do
    step_runs_by_step = Map.new(step_runs, &{&1.step, &1})

    declared_steps =
      SquidMesh.Workflow.Definition.inspect_steps(definition, step_statuses(step_runs))

    declared_step_names = MapSet.new(Enum.map(declared_steps, & &1.step))

    declared_states =
      Enum.map(declared_steps, fn %{
                                    step: step,
                                    depends_on: depends_on,
                                    status: status,
                                    recovery: recovery
                                  } ->
        case Map.fetch(step_runs_by_step, step) do
          {:ok, step_run} -> to_public_step_state(step_run, depends_on)
          :error -> to_waiting_step_state(step, depends_on, status, recovery)
        end
      end)

    extra_states =
      step_runs
      |> Enum.reject(&MapSet.member?(declared_step_names, &1.step))
      |> Enum.map(&to_public_step_state(&1, []))

    declared_states ++ extra_states
  end

  defp to_public_step_state(step_run, depends_on) do
    %RunStepState{
      step: step_run.step,
      status: step_run.status,
      depends_on: depends_on,
      input: step_run.input,
      output: step_run.output,
      last_error: step_run.last_error,
      recovery: step_run.recovery,
      attempts: step_run.attempts,
      inserted_at: step_run.inserted_at,
      updated_at: step_run.updated_at
    }
  end

  defp to_waiting_step_state(step, depends_on, status, recovery) do
    %RunStepState{
      step: step,
      status: status,
      depends_on: depends_on,
      recovery: recovery,
      attempts: []
    }
  end

  defp step_recovery_policy(definition, %SquidMesh.Persistence.StepRun{
         recovery: recovery,
         step: step
       }) do
    deserialize_recovery_policy(definition, recovery) || step_recovery_policy(definition, step)
  end

  defp step_recovery_policy(nil, _step), do: nil

  defp step_recovery_policy(definition, step) when is_binary(step) do
    case deserialize_step(definition, step) do
      step_name when is_atom(step_name) -> step_recovery_policy(definition, step_name)
      _unknown -> nil
    end
  end

  defp step_recovery_policy(definition, step) when is_atom(step) do
    case SquidMesh.Workflow.Definition.step_recovery_policy(definition, step) do
      {:ok, recovery} -> recovery
      {:error, _reason} -> nil
    end
  end

  defp deserialize_recovery_policy(_definition, nil), do: nil

  defp deserialize_recovery_policy(definition, recovery) when is_map(recovery) do
    recovery
    |> deserialize_map()
    |> SquidMesh.Workflow.Definition.normalize_recovery_policy()
    |> deserialize_recovery_failure_target(definition)
  end

  defp to_public_audit_events(_definition, %Ecto.Association.NotLoaded{}), do: nil

  defp to_public_audit_events(definition, step_runs) when is_list(step_runs) do
    step_runs
    |> Enum.flat_map(&step_run_audit_events(definition, &1))
    |> Enum.sort_by(& &1.at, DateTime)
  end

  defp step_statuses(step_runs) do
    Map.new(step_runs, &{&1.step, &1.status})
  end

  defp to_public_attempts(%SquidMesh.Persistence.StepRun{
         attempts: %Ecto.Association.NotLoaded{}
       }),
       do: []

  defp to_public_attempts(%SquidMesh.Persistence.StepRun{attempts: attempts})
       when is_list(attempts) do
    Enum.map(attempts, fn attempt ->
      %StepAttempt{
        id: attempt.id,
        attempt_number: attempt.attempt_number,
        status: deserialize_attempt_status(attempt.status),
        error: deserialize_map(attempt.error),
        inserted_at: attempt.inserted_at,
        updated_at: attempt.updated_at
      }
    end)
  end

  defp deserialize_step_status("pending"), do: :pending
  defp deserialize_step_status("running"), do: :running
  defp deserialize_step_status("completed"), do: :completed
  defp deserialize_step_status("failed"), do: :failed

  defp deserialize_attempt_status("running"), do: :running
  defp deserialize_attempt_status("completed"), do: :completed
  defp deserialize_attempt_status("failed"), do: :failed

  defp deserialize_manual_event(nil, _definition, _step_run), do: nil

  defp deserialize_manual_event(manual, definition, step_run) when is_map(manual) do
    step = deserialize_step(definition, step_run.step)
    type = deserialize_audit_type(Map.get(manual, "event"))

    if is_nil(type) do
      nil
    else
      %RunAuditEvent{
        type: type,
        step: step,
        actor: deserialize_value(Map.get(manual, "actor")),
        comment: Map.get(manual, "comment"),
        metadata: deserialize_map(Map.get(manual, "metadata")),
        at: deserialize_event_at(Map.get(manual, "at"), step_run.updated_at)
      }
    end
  end

  defp step_run_audit_events(definition, %SquidMesh.Persistence.StepRun{} = step_run) do
    failure_events = failure_recovery_audit_events(definition, step_run)
    step = deserialize_step(definition, step_run.step)

    manual_events =
      case manual_step_kind(definition, step, step_run) do
        nil ->
          []

        _kind ->
          paused_event = %RunAuditEvent{type: :paused, step: step, at: step_run.inserted_at}

          Enum.reject([paused_event | completed_manual_events(definition, step_run)], &is_nil(&1))
      end

    failure_events ++ manual_events
  end

  defp deserialize_recovery_failure_target(
         %{failure: %{target: target} = failure} = policy,
         definition
       )
       when is_binary(target) do
    Map.put(
      policy,
      :failure,
      Map.put(failure, :target, deserialize_failure_target(definition, target))
    )
  end

  defp deserialize_recovery_failure_target(policy, _definition), do: policy

  defp deserialize_failure_target(_definition, target)
       when target in ["__complete__", "complete"],
       do: :complete

  defp deserialize_failure_target(definition, target) do
    deserialize_step(definition, target) || target
  end

  defp failure_recovery_audit_events(_definition, %SquidMesh.Persistence.StepRun{status: status})
       when status != "failed",
       do: []

  defp failure_recovery_audit_events(definition, %SquidMesh.Persistence.StepRun{} = step_run) do
    case step_recovery_policy(definition, step_run) do
      %{failure: %{strategy: strategy, target: target}} when strategy in [:compensation, :undo] ->
        [
          %RunAuditEvent{
            type: failure_recovery_audit_type(strategy),
            step: deserialize_step(definition, step_run.step),
            metadata: %{target: target},
            at: step_run.updated_at
          }
        ]

      _ignored ->
        []
    end
  end

  defp failure_recovery_audit_type(:compensation), do: :compensation_routed
  defp failure_recovery_audit_type(:undo), do: :undo_routed

  defp completed_manual_events(
         definition,
         %SquidMesh.Persistence.StepRun{status: "completed"} = step_run
       ) do
    case deserialize_manual_event(step_run.manual, definition, step_run) do
      %RunAuditEvent{} = event ->
        [event]

      nil ->
        fallback_manual_event(definition, step_run)
    end
  end

  defp completed_manual_events(_definition, _step_run), do: []

  defp fallback_manual_event(definition, %SquidMesh.Persistence.StepRun{} = step_run) do
    case manual_step_kind(definition, deserialize_step(definition, step_run.step), step_run) do
      :pause ->
        [
          %RunAuditEvent{
            type: :resumed,
            step: deserialize_step(definition, step_run.step),
            at: step_run.updated_at
          }
        ]

      :approval ->
        case decision_payload(step_run.output) do
          %{decision: decision} = payload when decision in ["approved", "rejected"] ->
            [
              %RunAuditEvent{
                type: deserialize_audit_type(decision),
                step: deserialize_step(definition, step_run.step),
                actor: payload_value(payload, :actor),
                comment: payload_value(payload, :comment),
                metadata: payload_value(payload, :metadata),
                at: deserialize_event_at(payload_value(payload, :decided_at), step_run.updated_at)
              }
            ]

          _ignored ->
            []
        end

      _ignored ->
        []
    end
  end

  defp decision_payload(nil), do: nil

  defp decision_payload(%{decision: _decision, decided_at: _decided_at} = payload), do: payload

  defp decision_payload(%{"decision" => _decision, "decided_at" => _decided_at} = payload),
    do: deserialize_map(payload)

  defp decision_payload(output) when is_map(output) do
    Enum.find_value(output, fn
      {_ignored_key, %{decision: _decision, decided_at: _decided_at} = payload} ->
        payload

      {_ignored_key, %{"decision" => _decision, "decided_at" => _decided_at} = payload} ->
        deserialize_map(payload)

      _ignored ->
        nil
    end)
  end

  defp payload_value(payload, key) when is_map(payload) do
    Map.get(payload, key, Map.get(payload, Atom.to_string(key)))
  end

  defp manual_step_kind(_definition, _step, %SquidMesh.Persistence.StepRun{
         manual: %{"event" => event}
       })
       when event in ["resumed", "approved", "rejected"] do
    case event do
      "resumed" -> :pause
      _approval_event -> :approval
    end
  end

  defp manual_step_kind(_definition, _step, %SquidMesh.Persistence.StepRun{
         resume: %{"kind" => "approval"}
       }),
       do: :approval

  defp manual_step_kind(_definition, _step, %SquidMesh.Persistence.StepRun{resume: resume})
       when is_map(resume),
       do: :pause

  defp manual_step_kind(nil, _step, %SquidMesh.Persistence.StepRun{output: output})
       when is_map(output) do
    if decision_payload(output), do: :approval, else: nil
  end

  defp manual_step_kind(nil, _step, %SquidMesh.Persistence.StepRun{}), do: nil

  defp manual_step_kind(definition, step, %SquidMesh.Persistence.StepRun{}) do
    case SquidMesh.Workflow.Definition.step(definition, step) do
      {:ok, %{module: module}} when module in [:pause, :approval] -> module
      _ignored -> nil
    end
  end

  defp deserialize_audit_type("paused"), do: :paused
  defp deserialize_audit_type("resumed"), do: :resumed
  defp deserialize_audit_type("approved"), do: :approved
  defp deserialize_audit_type("rejected"), do: :rejected
  defp deserialize_audit_type(_value), do: nil

  defp deserialize_event_at(nil, fallback), do: fallback

  defp deserialize_event_at(timestamp, fallback) when is_binary(timestamp) do
    case DateTime.from_iso8601(timestamp) do
      {:ok, datetime, _offset} -> datetime
      _ignored -> fallback
    end
  end

  defp step_runs_preload_query do
    from(step_run in SquidMesh.Persistence.StepRun,
      order_by: [asc: step_run.inserted_at, asc: step_run.id],
      preload: [attempts: ^attempts_preload_query()]
    )
  end

  defp attempts_preload_query do
    from(attempt in SquidMesh.Persistence.StepAttempt,
      order_by: [asc: attempt.attempt_number, asc: attempt.inserted_at, asc: attempt.id]
    )
  end

  defp deserialize_value(value) when is_map(value), do: deserialize_map(value)
  defp deserialize_value(value) when is_list(value), do: Enum.map(value, &deserialize_value/1)
  defp deserialize_value(value), do: value

  defp deserialize_error_step(nil, step), do: step

  defp deserialize_error_step(definition, step) when is_binary(step),
    do: deserialize_step(definition, step)

  defp deserialize_error_step(_definition, step), do: step

  defp maybe_update_error_step(error, key, definition) do
    case Map.fetch(error, key) do
      {:ok, step} -> Map.put(error, key, deserialize_error_step(definition, step))
      :error -> error
    end
  end

  defp maybe_update_error_steps(error, key, definition) do
    case Map.fetch(error, key) do
      {:ok, steps} when is_list(steps) ->
        Map.put(error, key, Enum.map(steps, &deserialize_error_step(definition, &1)))

      _ignored ->
        error
    end
  end

  defp deserialize_key(key) do
    String.to_existing_atom(key)
  rescue
    ArgumentError -> key
  end
end
