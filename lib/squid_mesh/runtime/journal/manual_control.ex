defmodule SquidMesh.Runtime.Journal.ManualControl do
  @moduledoc """
  Journal-backed manual intervention controls.

  This module resolves manual pause boundaries by appending run-thread facts.
  The dispatch thread is updated only after the run thread contains the durable
  resolution and any successor runnable intent.
  """

  alias Jido.Agent
  alias SquidMesh.ReadModel.Inspection
  alias SquidMesh.Runtime.DispatchAgent
  alias SquidMesh.Runtime.DispatchProtocol
  alias SquidMesh.Runtime.DispatchProtocol.Entry
  alias SquidMesh.Runtime.Journal
  alias SquidMesh.Runtime.Journal.Options
  alias SquidMesh.Runtime.ManualAction
  alias SquidMesh.Runtime.StepInput
  alias SquidMesh.Runtime.WorkflowAgent
  alias SquidMesh.Runtime.WorkflowAgent.Projection
  alias SquidMesh.Workflow.Definition

  @run_append_retries 25
  @dispatch_append_retries 25

  @type control_error ::
          :not_found
          | :invalid_run_id
          | {:invalid_option, term()}
          | {:invalid_resume, map()}
          | {:invalid_transition, atom(), atom()}
          | {:invalid_step, atom() | String.t()}
          | {:invalid_workflow, String.t()}
          | term()

  @doc """
  Resumes a journal-paused `:pause` step and schedules its successor.
  """
  @spec resume(String.t(), map(), keyword()) ::
          {:ok, Inspection.Snapshot.t()} | {:error, control_error()}
  def resume(run_id, attrs, opts \\ [])

  def resume(run_id, attrs, opts)
      when is_binary(run_id) and is_map(attrs) and is_list(opts) do
    with {:ok, run_id} <- run_id(run_id),
         :ok <- validate_resume(attrs),
         {:ok, storage} <- journal_storage(opts),
         {:ok, queue} <- queue(opts),
         {:ok, now} <- now(opts),
         {:ok, workflow_agent} <-
           resolve_or_repair(storage, run_id, queue, attrs, now, @run_append_retries),
         {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue),
         {:ok, _schedule_update} <-
           schedule_pending_dispatches(
             storage,
             workflow_agent,
             dispatch_agent,
             now,
             @dispatch_append_retries
           ) do
      Inspection.snapshot(storage, run_id, queue: queue, now: now)
    end
  end

  def resume(_run_id, _attrs, _opts), do: {:error, :invalid_run_id}

  @doc """
  Approves a journal-paused `:approval` step and schedules its success path.
  """
  @spec approve(String.t(), map(), keyword()) ::
          {:ok, Inspection.Snapshot.t()} | {:error, control_error()}
  def approve(run_id, attrs, opts \\ []), do: review(run_id, :approved, attrs, opts)

  @doc """
  Rejects a journal-paused `:approval` step and schedules its rejection path.
  """
  @spec reject(String.t(), map(), keyword()) ::
          {:ok, Inspection.Snapshot.t()} | {:error, control_error()}
  def reject(run_id, attrs, opts \\ []), do: review(run_id, :rejected, attrs, opts)

  defp review(run_id, decision, attrs, opts)
       when is_binary(run_id) and decision in [:approved, :rejected] and is_map(attrs) and
              is_list(opts) do
    with {:ok, run_id} <- run_id(run_id),
         :ok <- validate_review(attrs),
         {:ok, storage} <- journal_storage(opts),
         {:ok, queue} <- queue(opts),
         {:ok, now} <- now(opts),
         {:ok, workflow_agent} <-
           resolve_or_repair_review(
             storage,
             run_id,
             queue,
             decision,
             attrs,
             now,
             @run_append_retries
           ),
         {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue),
         {:ok, _schedule_update} <-
           schedule_pending_dispatches(
             storage,
             workflow_agent,
             dispatch_agent,
             now,
             @dispatch_append_retries
           ) do
      Inspection.snapshot(storage, run_id, queue: queue, now: now)
    end
  end

  defp review(_run_id, _decision, _attrs, _opts), do: {:error, :invalid_run_id}

  defp resolve_or_repair(_storage, _run_id, _queue, _attrs, _now, 0), do: {:error, :conflict}

  defp resolve_or_repair(storage, run_id, queue, attrs, %DateTime{} = now, retries_left) do
    with {:ok, workflow_agent} <- rebuild_workflow_agent(storage, run_id),
         {:ok, resolution} <- resolution_target(storage, workflow_agent) do
      append_resolution(storage, run_id, queue, attrs, now, retries_left, resolution)
    end
  end

  defp resolve_or_repair_review(_storage, _run_id, _queue, _decision, _attrs, _now, 0),
    do: {:error, :conflict}

  defp resolve_or_repair_review(
         storage,
         run_id,
         queue,
         decision,
         attrs,
         %DateTime{} = now,
         retries_left
       ) do
    with {:ok, workflow_agent} <- rebuild_workflow_agent(storage, run_id),
         {:ok, resolution} <- review_resolution_target(storage, workflow_agent, decision) do
      append_review_resolution(
        storage,
        run_id,
        queue,
        decision,
        attrs,
        now,
        retries_left,
        resolution
      )
    end
  end

  defp resolution_target(
         storage,
         %Agent{agent_module: WorkflowAgent, state: %{projection: %Projection{} = projection}} =
           workflow_agent
       ) do
    case active_pause_state(projection) do
      {:ok, manual_state} ->
        {:ok, {:append, workflow_agent, manual_state}}

      {:error, {:invalid_transition, status, :running}} when status != :paused ->
        if resumed_pause_recorded?(storage, workflow_agent.state.run_id) do
          {:ok, {:repair, workflow_agent}}
        else
          {:error, {:invalid_transition, status, :running}}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp review_resolution_target(
         storage,
         %Agent{agent_module: WorkflowAgent, state: %{projection: %Projection{} = projection}} =
           workflow_agent,
         decision
       ) do
    case active_approval_state(projection) do
      {:ok, manual_state} ->
        {:ok, {:append, workflow_agent, manual_state}}

      {:error, {:invalid_transition, status, :running}} when status != :paused ->
        if manual_resolution_recorded?(storage, workflow_agent.state.run_id, decision) do
          {:ok, {:repair, workflow_agent}}
        else
          {:error, {:invalid_transition, status, :running}}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp append_resolution(_storage, _run_id, _queue, _attrs, _now, _retries_left, {
         :repair,
         workflow_agent
       }) do
    {:ok, workflow_agent}
  end

  defp append_resolution(storage, run_id, queue, attrs, %DateTime{} = now, retries_left, {
         :append,
         workflow_agent,
         manual_state
       }) do
    with {:ok, entries} <-
           resolution_entries(workflow_agent, storage, queue, attrs, now, manual_state) do
      case Journal.append_entries(storage, entries, expected_rev: workflow_agent.state.thread_rev) do
        {:ok, _thread} ->
          WorkflowAgent.rebuild(storage, run_id)

        {:error, :conflict} ->
          resolve_or_repair(storage, run_id, queue, attrs, now, retries_left - 1)

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp append_review_resolution(
         _storage,
         _run_id,
         _queue,
         _decision,
         _attrs,
         _now,
         _retries_left,
         {
           :repair,
           workflow_agent
         }
       ) do
    {:ok, workflow_agent}
  end

  defp append_review_resolution(
         storage,
         run_id,
         queue,
         decision,
         attrs,
         %DateTime{} = now,
         retries_left,
         {:append, workflow_agent, manual_state}
       ) do
    with {:ok, entries} <-
           review_resolution_entries(
             workflow_agent,
             storage,
             queue,
             decision,
             attrs,
             now,
             manual_state
           ) do
      case Journal.append_entries(storage, entries, expected_rev: workflow_agent.state.thread_rev) do
        {:ok, _thread} ->
          WorkflowAgent.rebuild(storage, run_id)

        {:error, :conflict} ->
          resolve_or_repair_review(storage, run_id, queue, decision, attrs, now, retries_left - 1)

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp resolution_entries(
         %Agent{agent_module: WorkflowAgent, state: %{projection: %Projection{} = projection}} =
           workflow_agent,
         storage,
         queue,
         attrs,
         %DateTime{} = now,
         manual_state
       ) do
    with {:ok, _workflow, definition, step_name} <-
           resolved_pause_definition(storage, workflow_agent, manual_state),
         {:ok, target} <- manual_target(definition, manual_state),
         {:ok, result} <- manual_result(manual_state, projection),
         {:ok, progression_entries} <-
           resolution_progression_entries(
             workflow_agent,
             definition,
             target,
             result,
             manual_input(manual_state, projection),
             queue,
             now
           ) do
      {:ok,
       [
         manual_step_resolved_entry!(
           workflow_agent.state.run_id,
           step_name,
           :resumed,
           result,
           ManualAction.build(:resumed, attrs, now),
           now
         )
         | progression_entries
       ]}
    end
  end

  defp review_resolution_entries(
         %Agent{agent_module: WorkflowAgent, state: %{projection: %Projection{} = projection}} =
           workflow_agent,
         storage,
         queue,
         decision,
         attrs,
         %DateTime{} = now,
         manual_state
       ) do
    with {:ok, _workflow, definition, step_name} <-
           resolved_approval_definition(storage, workflow_agent, manual_state),
         {:ok, result, target} <-
           review_result_and_target(definition, step_name, manual_state, decision, attrs, now),
         {:ok, progression_entries} <-
           resolution_progression_entries(
             workflow_agent,
             definition,
             target,
             result,
             manual_input(manual_state, projection),
             queue,
             now
           ) do
      {:ok,
       [
         manual_step_resolved_entry!(
           workflow_agent.state.run_id,
           step_name,
           decision,
           result,
           ManualAction.build(decision, attrs, now),
           now
         )
         | progression_entries
       ]}
    end
  end

  defp active_pause_state(%Projection{} = projection) do
    case Projection.manual_state(projection) do
      %{kind: "pause"} = manual_state ->
        {:ok, manual_state}

      %{step: step, kind: kind} ->
        {:error, {:unsupported_journal_manual_step, step, kind}}

      nil ->
        {:error, {:invalid_transition, Projection.status(projection), :running}}
    end
  end

  defp active_approval_state(%Projection{} = projection) do
    case Projection.manual_state(projection) do
      %{kind: "approval"} = manual_state ->
        {:ok, manual_state}

      %{step: step, kind: kind} ->
        {:error, {:unsupported_journal_manual_step, step, kind}}

      nil ->
        {:error, {:invalid_transition, Projection.status(projection), :running}}
    end
  end

  defp resolved_pause_definition(storage, workflow_agent, %{step: step}) do
    with {:ok, workflow, definition} <- Definition.load_serialized(workflow_agent.state.workflow),
         :ok <- validate_definition_fingerprint(storage, workflow_agent.state.run_id, definition),
         step_name when is_atom(step_name) <- Definition.deserialize_step(definition, step),
         {:ok, %{module: :pause}} <- Definition.step(definition, step_name) do
      {:ok, workflow, definition, step_name}
    else
      step_name when is_binary(step_name) -> {:error, {:unknown_step, step_name}}
      {:ok, _other_step} -> {:error, {:invalid_step, step}}
      {:error, _reason} = error -> error
    end
  end

  defp resolved_approval_definition(storage, workflow_agent, %{step: step}) do
    with {:ok, workflow, definition} <- Definition.load_serialized(workflow_agent.state.workflow),
         :ok <- validate_definition_fingerprint(storage, workflow_agent.state.run_id, definition),
         step_name when is_atom(step_name) <- Definition.deserialize_step(definition, step),
         {:ok, %{module: :approval}} <- Definition.step(definition, step_name) do
      {:ok, workflow, definition, step_name}
    else
      step_name when is_binary(step_name) -> {:error, {:unknown_step, step_name}}
      {:ok, _other_step} -> {:error, {:invalid_step, step}}
      {:error, _reason} = error -> error
    end
  end

  defp manual_target(definition, %{step: step, metadata: metadata}) when is_map(metadata) do
    case Map.get(metadata, :target) || Map.get(metadata, "target") do
      "__complete__" ->
        {:ok, :complete}

      target when is_binary(target) ->
        case Definition.deserialize_step(definition, target) do
          target_step when is_atom(target_step) -> {:ok, target_step}
          _unknown -> {:error, {:unknown_step, target}}
        end

      _missing ->
        case Definition.deserialize_step(definition, step) do
          step_name when is_atom(step_name) ->
            Definition.transition_target(definition, step_name, :ok)

          _unknown ->
            {:error, {:unknown_step, step}}
        end
    end
  end

  defp manual_result(%{step: step, metadata: metadata}, %Projection{} = projection)
       when is_map(metadata) do
    case Map.get(metadata, :output) || Map.get(metadata, "output") do
      output when is_map(output) ->
        {:ok, output}

      _missing_or_invalid ->
        with {:ok, runnable_key} <- Projection.applied_runnable_key_for_step(projection, step),
             {:ok, output} when is_map(output) <-
               Projection.applied_result(projection, runnable_key) do
          {:ok, output}
        else
          _missing -> {:ok, %{}}
        end
    end
  end

  defp review_result_and_target(
         definition,
         step_name,
         %{metadata: metadata},
         decision,
         attrs,
         now
       )
       when is_map(metadata) and decision in [:approved, :rejected] do
    with {:ok, output_key} <- review_output_key(definition, step_name, metadata),
         {:ok, target} <- review_target(definition, step_name, metadata, decision) do
      {:ok, map_review_output(attrs, decision, output_key, now), target}
    end
  end

  defp review_output_key(definition, step_name, metadata) do
    case Map.get(metadata, :output_key) || Map.get(metadata, "output_key") do
      nil ->
        Definition.step_output_mapping(definition, step_name)

      output_key when is_binary(output_key) ->
        {:ok, deserialize_output_key(output_key)}

      output_key when is_atom(output_key) ->
        {:ok, output_key}

      _invalid ->
        {:error, {:invalid_resume_metadata, step_name}}
    end
  end

  defp review_target(definition, step_name, metadata, decision) do
    target_key = decision_metadata_target_key(decision)
    target = Map.get(metadata, target_key) || Map.get(metadata, Atom.to_string(target_key))

    resolve_review_target(definition, step_name, decision, target)
  end

  defp resolve_review_target(definition, step_name, decision, nil) do
    with {:ok, targets} <- Definition.approval_transition_targets(definition, step_name) do
      {:ok, Map.fetch!(targets, decision_target_key(decision))}
    end
  end

  defp resolve_review_target(_definition, _step_name, _decision, "__complete__"),
    do: {:ok, :complete}

  defp resolve_review_target(definition, _step_name, _decision, target) when is_binary(target) do
    case Definition.deserialize_step(definition, target) do
      target_step when is_atom(target_step) -> {:ok, target_step}
      _unknown -> {:error, {:unknown_step, target}}
    end
  end

  defp resolve_review_target(_definition, _step_name, _decision, target) when is_atom(target) do
    {:ok, target}
  end

  defp resolve_review_target(_definition, step_name, _decision, _target) do
    {:error, {:invalid_resume_metadata, step_name}}
  end

  defp decision_metadata_target_key(:approved), do: :ok_target
  defp decision_metadata_target_key(:rejected), do: :error_target

  defp decision_target_key(:approved), do: :ok
  defp decision_target_key(:rejected), do: :error

  defp map_review_output(attrs, decision, output_key, %DateTime{} = now) do
    review_output =
      %{
        decision: serialize_decision(decision),
        actor: Map.fetch!(attrs, :actor),
        decided_at: DateTime.to_iso8601(now)
      }
      |> maybe_put(:comment, Map.get(attrs, :comment))
      |> maybe_put(:metadata, Map.get(attrs, :metadata))

    case output_key do
      nil ->
        review_output

      mapped_key ->
        %{mapped_key => review_output}
    end
  end

  defp serialize_decision(:approved), do: "approved"
  defp serialize_decision(:rejected), do: "rejected"

  defp deserialize_output_key(output_key) do
    String.to_existing_atom(output_key)
  rescue
    ArgumentError -> output_key
  end

  defp resolution_progression_entries(
         %Agent{agent_module: WorkflowAgent} = workflow_agent,
         _definition,
         :complete,
         _result,
         _input,
         _queue,
         %DateTime{} = now
       ) do
    {:ok, [run_terminal_entry!(workflow_agent.state.run_id, :completed, now)]}
  end

  defp resolution_progression_entries(
         %Agent{agent_module: WorkflowAgent} = workflow_agent,
         definition,
         next_step,
         result,
         input,
         queue,
         %DateTime{} = now
       )
       when is_atom(next_step) do
    context =
      workflow_agent
      |> applied_result_context()
      |> Map.merge(input || %{})
      |> Map.merge(result)

    with {:ok, input} <- successor_input(context, definition, next_step) do
      {:ok,
       [
         runnables_planned_entry!(
           workflow_agent.state.run_id,
           [journal_runnable(workflow_agent.state.run_id, queue, next_step, input, 1, now)],
           now
         )
       ]}
    end
  end

  defp schedule_pending_dispatches(_storage, workflow_agent, dispatch_agent, _now, _retries_left)
       when workflow_agent.state.projection.terminal_status in [:completed, :failed, :cancelled] do
    {:ok, %{agent: dispatch_agent, runnables: []}}
  end

  defp schedule_pending_dispatches(_storage, _workflow_agent, _dispatch_agent, _now, 0),
    do: {:error, :conflict}

  defp schedule_pending_dispatches(storage, workflow_agent, dispatch_agent, now, retries_left) do
    case WorkflowAgent.schedule_pending_dispatches(storage, workflow_agent, dispatch_agent,
           now: now
         ) do
      {:ok, _schedule_update} = ok ->
        ok

      {:error, :conflict} ->
        with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, workflow_agent.state.run_id),
             {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, dispatch_agent.state.queue) do
          schedule_pending_dispatches(
            storage,
            workflow_agent,
            dispatch_agent,
            now,
            retries_left - 1
          )
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp validate_resume(attrs) do
    case ManualAction.validate(attrs) do
      :ok -> :ok
      {:error, {:invalid_manual_action, details}} -> {:error, {:invalid_resume, details}}
    end
  end

  defp validate_review(attrs) do
    case ManualAction.validate(attrs, require_actor: true) do
      :ok -> :ok
      {:error, {:invalid_manual_action, details}} -> {:error, {:invalid_review, details}}
    end
  end

  defp rebuild_workflow_agent(storage, run_id) do
    case WorkflowAgent.rebuild(storage, run_id) do
      {:ok, _workflow_agent} = ok -> ok
      {:error, :not_found} -> {:error, :not_found}
      {:error, _reason} = error -> error
    end
  end

  defp resumed_pause_recorded?(storage, run_id) do
    manual_resolution_recorded?(storage, run_id, :resumed)
  end

  defp manual_resolution_recorded?(storage, run_id, action) when is_atom(action) do
    serialized_action = Atom.to_string(action)

    case Journal.load_entries(storage, {:run, run_id}) do
      {:ok, entries} ->
        Enum.any?(entries, fn
          %{type: :manual_step_resolved, data: %{action: ^serialized_action}} -> true
          _entry -> false
        end)

      {:error, _reason} ->
        false
    end
  end

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp validate_definition_fingerprint(storage, run_id, definition) do
    case persisted_definition_fingerprint(storage, run_id) do
      {:ok, nil} ->
        {:error, %{code: "incompatible_workflow_definition", retryable?: false}}

      {:ok, fingerprint} ->
        if fingerprint == Definition.fingerprint(definition) do
          :ok
        else
          {:error, %{code: "incompatible_workflow_definition", retryable?: false}}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp persisted_definition_fingerprint(storage, run_id) do
    with {:ok, %{entries: entries}} <- Journal.load_thread(storage, {:run, run_id}) do
      fingerprint =
        Enum.find_value(entries, fn
          %{type: :run_started, data: data} -> Map.get(data, :definition_fingerprint)
          _entry -> nil
        end)

      {:ok, fingerprint}
    end
  end

  defp applied_result_context(%Agent{
         agent_module: WorkflowAgent,
         state: %{projection: projection}
       }) do
    projection
    |> Projection.applied_results()
    |> Map.values()
    |> Enum.filter(&is_map/1)
    |> Enum.reduce(%{}, &Map.merge(&2, &1))
  end

  defp manual_input(%{step: step}, %Projection{} = projection) do
    projection
    |> Projection.planned_runnables()
    |> Enum.find_value(%{}, fn runnable ->
      runnable_step = Map.get(runnable, :step) || Map.get(runnable, "step")

      if runnable_step == step do
        Map.get(runnable, :input) || Map.get(runnable, "input") || %{}
      end
    end)
  end

  defp successor_input(context, definition, next_step) do
    case Definition.step_input_mapping(definition, next_step) do
      {:ok, input_mapping} -> StepInput.apply_input_mapping(context, input_mapping)
      {:error, _reason} = error -> error
    end
  end

  defp manual_step_resolved_entry!(run_id, step_name, action, result, metadata, %DateTime{} = now)
       when is_binary(run_id) and is_atom(step_name) and is_atom(action) and is_map(result) and
              is_map(metadata) do
    entry!(:manual_step_resolved, %{
      run_id: run_id,
      step: Definition.serialize_step(step_name),
      action: Atom.to_string(action),
      result: result,
      metadata: metadata,
      occurred_at: now
    })
  end

  defp runnables_planned_entry!(run_id, runnables, %DateTime{} = now) do
    entry!(:runnables_planned, %{
      run_id: run_id,
      runnables: runnables,
      occurred_at: now
    })
  end

  defp run_terminal_entry!(run_id, status, %DateTime{} = now) do
    entry!(:run_terminal, %{
      run_id: run_id,
      status: status,
      occurred_at: now
    })
  end

  defp entry!(type, attrs) do
    {:ok, %Entry{} = entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end

  defp journal_runnable(run_id, queue, step_name, input, attempt_number, %DateTime{} = now) do
    step = Definition.serialize_step(step_name)
    runnable_key = "#{run_id}:#{step}:#{attempt_number}"

    %{
      run_id: run_id,
      runnable_key: runnable_key,
      idempotency_key: runnable_key,
      attempt_number: attempt_number,
      queue: queue,
      step: step,
      input: input,
      visible_at: now
    }
  end

  defp run_id(run_id) do
    case Ecto.UUID.cast(run_id) do
      {:ok, uuid} -> {:ok, uuid}
      :error -> {:error, :invalid_run_id}
    end
  end

  defp journal_storage(opts) do
    opts
    |> Keyword.get(:journal_storage)
    |> Options.storage()
  end

  defp queue(opts) do
    opts
    |> Keyword.get(:queue, "default")
    |> Options.queue()
  end

  defp now(opts) do
    now = Keyword.get(opts, :now, DateTime.utc_now(:microsecond))

    if match?(%DateTime{}, now) do
      {:ok, now}
    else
      {:error, {:invalid_option, {:now, :invalid}}}
    end
  end
end
