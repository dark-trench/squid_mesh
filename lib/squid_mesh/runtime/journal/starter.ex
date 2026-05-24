defmodule SquidMesh.Runtime.Journal.Starter do
  @moduledoc """
  Opt-in journal-backed workflow start boundary for the Jido-native runtime.

  This module proves the first live cutover path away from legacy runtime
  tables. It resolves the public Squid Mesh workflow contract, plans initial
  runnables through Runic, appends durable run and run-index facts to
  `Jido.Storage`, then uses `WorkflowAgent` and `DispatchAgent` as rebuildable
  coordinators to schedule dispatch attempts from the journal.

  This is an incremental cutover gate, not a long-term compatibility layer. The
  table-backed runtime remains in place only while the rest of execution,
  controls, and recovery move onto the journal-backed path. The journal runtime
  can execute normal action steps, immediate built-in `:log` steps, and
  built-in `:wait` steps in transition and dependency workflows, where waits
  delay downstream runnable visibility. Built-in `:pause` steps persist
  inspectable manual intervention state and can be resumed through journal
  manual controls. Approval steps remain rejected until their decision semantics
  are represented in journal facts. Callers enter this path explicitly with
  `runtime: :journal`,
  `journal_storage:`, and optional queue or clock overrides. No Jido primitive
  is required in workflow authoring.
  """

  alias SquidMesh.ReadModel.Inspection
  alias SquidMesh.Runtime.AgentRecovery
  alias SquidMesh.Runtime.DispatchAgent
  alias SquidMesh.Runtime.DispatchProtocol
  alias SquidMesh.Runtime.Journal
  alias SquidMesh.Runtime.Journal.Options
  alias SquidMesh.Runtime.RunIndexProjection
  alias SquidMesh.Runtime.WorkflowAgent
  alias SquidMesh.Workflow.Definition
  alias SquidMesh.Workflow.RunicPlanner

  @dispatch_schedule_retries 25

  @type start_error ::
          {:invalid_payload, Definition.payload_error_details()}
          | Definition.load_error()
          | Definition.trigger_error()
          | {:unsupported_journal_step, atom(), Definition.built_in_step_kind()}
          | {:invalid_option,
             {:journal_storage, nil} | {:now, term()} | {:queue, term()} | {:run_id, term()}}
          | term()

  @doc """
  Starts a workflow run by appending Jido journal facts and scheduling dispatch.

  The returned snapshot is rebuilt from the same journal-backed read model used
  by `SquidMesh.inspect_run/2` with `read_model: :read_model`.
  """
  @spec start_run(module(), atom() | nil, map(), keyword()) ::
          {:ok, Inspection.Snapshot.t()} | {:error, start_error()}
  def start_run(workflow, trigger_name, payload, opts)
      when is_atom(workflow) and is_map(payload) and is_list(opts) do
    with {:ok, storage} <- journal_storage(opts),
         {:ok, queue} <- queue(opts),
         {:ok, now} <- now(opts),
         {:ok, definition} <- Definition.load(workflow),
         :ok <- reject_unsupported_built_ins(definition),
         {:ok, trigger} <- trigger(definition, trigger_name),
         {:ok, resolved_payload} <- Definition.resolve_payload(trigger, payload),
         {:ok, planner} <- RunicPlanner.new(workflow),
         {:ok, _planned, runnables} <- RunicPlanner.plan(planner, resolved_payload),
         {:ok, run_id} <- run_id(opts),
         {:ok, journal_runnables} <- journal_runnables(run_id, queue, runnables, now),
         :ok <- ensure_run_started(storage, workflow, definition, run_id, journal_runnables, now) do
      complete_started_run(storage, workflow, run_id, queue, now)
    end
  end

  defp trigger(definition, nil),
    do: Definition.trigger(definition, Definition.default_trigger(definition))

  defp trigger(definition, trigger_name), do: Definition.trigger(definition, trigger_name)

  defp reject_unsupported_built_ins(definition) do
    case Enum.find(definition.steps, &unsupported_built_in_step?/1) do
      %{name: step_name, module: kind} -> {:error, {:unsupported_journal_step, step_name, kind}}
      nil -> :ok
    end
  end

  defp unsupported_built_in_step?(%{module: :approval}), do: true
  defp unsupported_built_in_step?(_step), do: false

  defp ensure_run_started(storage, workflow, definition, run_id, runnables, %DateTime{} = now) do
    expected_fingerprint = Definition.fingerprint(definition)

    with {:ok, run_started} <-
           DispatchProtocol.new_entry(:run_started, %{
             run_id: run_id,
             workflow: Definition.serialize_workflow(workflow),
             definition_fingerprint: expected_fingerprint,
             occurred_at: now
           }),
         {:ok, runnables_planned} <-
           DispatchProtocol.new_entry(:runnables_planned, %{
             run_id: run_id,
             runnables: runnables,
             occurred_at: now
           }),
         {:ok, _run_thread} <-
           Journal.append_entries(storage, [run_started, runnables_planned], expected_rev: 0) do
      :ok
    else
      {:error, :conflict} ->
        rebuild_existing_start(storage, workflow, run_id, runnables, expected_fingerprint)

      {:error, _reason} = error ->
        error
    end
  end

  defp ensure_run_queued(storage, run_id, queue, %DateTime{} = now) do
    ensure_run_queued(storage, run_id, queue, now, @dispatch_schedule_retries)
  end

  defp ensure_run_queued(_storage, _run_id, _queue, %DateTime{}, 0), do: {:error, :conflict}

  defp ensure_run_queued(storage, run_id, queue, %DateTime{} = now, retries_left) do
    with {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue) do
      case DispatchAgent.ensure_run_queued(storage, dispatch_agent, run_id, now: now) do
        {:ok, _queued} ->
          :ok

        {:error, :conflict} ->
          ensure_run_queued(storage, run_id, queue, now, retries_left - 1)

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp journal_runnables(run_id, queue, runnables, %DateTime{} = now) do
    result =
      Enum.reduce_while(runnables, {:ok, []}, fn runnable, {:ok, acc} ->
        case journal_runnable(run_id, queue, runnable, now) do
          {:ok, journal_runnable} -> {:cont, {:ok, [journal_runnable | acc]}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)

    case result do
      {:ok, journal_runnables} -> {:ok, Enum.reverse(journal_runnables)}
      {:error, _reason} = error -> error
    end
  end

  defp journal_runnable(
         run_id,
         queue,
         %{step: step, input: input},
         %DateTime{} = now
       )
       when is_atom(step) do
    attempt_number = 1
    step_name = Definition.serialize_step(step)
    runnable_key = "#{run_id}:#{step_name}:#{attempt_number}"

    {:ok,
     %{
       run_id: run_id,
       runnable_key: runnable_key,
       idempotency_key: runnable_key,
       attempt_number: attempt_number,
       queue: queue,
       step: step_name,
       input: input || %{},
       visible_at: now
     }}
  end

  defp journal_runnable(_run_id, _queue, runnable, %DateTime{}) do
    {:error, {:invalid_runnable, runnable}}
  end

  defp put_checkpoints(storage, workflow_agent, dispatch_agent, %DateTime{} = now) do
    _checkpoint_result = WorkflowAgent.put_checkpoint(storage, workflow_agent, updated_at: now)
    _checkpoint_result = DispatchAgent.put_checkpoint(storage, dispatch_agent, updated_at: now)

    :ok
  end

  defp ensure_run_indexed(storage, workflow, run_id, %DateTime{} = now) do
    workflow = Definition.serialize_workflow(workflow)

    with {:ok, entry} <-
           DispatchProtocol.new_entry(:run_indexed, %{
             run_id: run_id,
             workflow: workflow,
             occurred_at: now
           }) do
      ensure_run_indexed(storage, workflow, run_id, entry, @dispatch_schedule_retries)
    end
  end

  defp ensure_run_indexed(storage, workflow, run_id, entry, retries_left) do
    case load_run_index(storage, workflow) do
      {:ok, %{rev: rev, projection: projection}} ->
        append_missing_run_index(storage, workflow, run_id, entry, rev, projection, retries_left)

      {:error, _reason} = error ->
        error
    end
  end

  defp append_missing_run_index(storage, workflow, run_id, entry, rev, projection, retries_left) do
    if run_indexed?(projection, run_id) do
      :ok
    else
      append_run_index_entry(storage, workflow, run_id, entry, rev, retries_left)
    end
  end

  defp append_run_index_entry(storage, workflow, run_id, entry, rev, retries_left) do
    case Journal.append_entries(storage, [entry], expected_rev: rev) do
      {:ok, _thread} ->
        :ok

      {:error, :conflict} when retries_left > 0 ->
        ensure_run_indexed(storage, workflow, run_id, entry, retries_left - 1)

      {:error, _reason} = error ->
        error
    end
  end

  defp load_run_index(storage, workflow) do
    case Journal.load_thread(storage, {:run_index, workflow}) do
      {:ok, %{rev: rev, entries: entries}} ->
        {:ok, %{rev: rev, projection: RunIndexProjection.rebuild(entries)}}

      {:error, :not_found} ->
        {:ok, %{rev: 0, projection: RunIndexProjection.new(workflow)}}

      {:error, _reason} = error ->
        error
    end
  end

  defp run_indexed?(%RunIndexProjection{} = projection, run_id) do
    run_id in RunIndexProjection.run_ids(projection)
  end

  defp complete_started_run(storage, workflow, run_id, queue, %DateTime{} = now) do
    case do_complete_started_run(storage, workflow, run_id, queue, now) do
      {:ok, %Inspection.Snapshot{}} = ok -> ok
      {:error, reason} -> {:error, {:journal_start_committed, run_id, reason}}
    end
  end

  defp do_complete_started_run(storage, workflow, run_id, queue, %DateTime{} = now) do
    with :ok <- ensure_run_queued(storage, run_id, queue, now),
         :ok <- ensure_run_indexed(storage, workflow, run_id, now),
         {:ok, %{workflow_agent: workflow_agent, dispatch_agent: dispatch_agent}} <-
           recover_or_continue(storage, run_id, queue, now),
         :ok <- put_checkpoints(storage, workflow_agent, dispatch_agent, now) do
      Inspection.snapshot(storage, run_id, queue: queue, now: now)
    end
  end

  defp rebuild_existing_start(storage, workflow, run_id, expected_runnables, expected_fingerprint) do
    with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, run_id),
         {:ok, existing_fingerprint} <- persisted_definition_fingerprint(storage, run_id) do
      validate_existing_start(
        workflow_agent,
        workflow,
        expected_runnables,
        expected_fingerprint,
        existing_fingerprint
      )
    end
  end

  defp validate_existing_start(
         workflow_agent,
         workflow,
         expected_runnables,
         expected_fingerprint,
         existing_fingerprint
       ) do
    existing_workflow = workflow_agent.state.workflow
    expected_workflow = Definition.serialize_workflow(workflow)
    existing_runnables = WorkflowAgent.planned_runnables(workflow_agent)

    cond do
      existing_workflow != expected_workflow ->
        {:error, :conflict}

      not equivalent_definition_fingerprint?(existing_fingerprint, expected_fingerprint) ->
        {:error, :conflict}

      equivalent_runnables?(existing_runnables, expected_runnables) ->
        :ok

      true ->
        {:error, :conflict}
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

  defp equivalent_definition_fingerprint?(existing_fingerprint, expected_fingerprint),
    do: existing_fingerprint == expected_fingerprint

  defp equivalent_runnables?(left, right) when length(left) == length(right) do
    left =
      left
      |> Enum.map(&stable_runnable/1)
      |> Enum.sort()

    right =
      right
      |> Enum.map(&stable_runnable/1)
      |> Enum.sort()

    left == right
  end

  defp equivalent_runnables?(_left, _right), do: false

  defp stable_runnable(runnable) when is_map(runnable) do
    %{
      run_id: runnable_value(runnable, :run_id),
      runnable_key: runnable_value(runnable, :runnable_key),
      idempotency_key: runnable_value(runnable, :idempotency_key),
      attempt_number: runnable_value(runnable, :attempt_number),
      queue: runnable_value(runnable, :queue),
      step: runnable_value(runnable, :step),
      input: runnable_value(runnable, :input)
    }
  end

  defp recover_or_continue(storage, run_id, queue, %DateTime{} = now) do
    case recover(storage, run_id, queue, now, @dispatch_schedule_retries) do
      {:ok, recovery} ->
        {:ok, recovery}

      {:error, :conflict} ->
        with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, run_id),
             {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue) do
          {:ok,
           %{
             workflow_agent: workflow_agent,
             dispatch_agent: dispatch_agent,
             scheduled_runnables: [],
             applied_attempts: []
           }}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp recover(storage, run_id, queue, now, retries_left) do
    case AgentRecovery.recover(storage, run_id, queue, now: now) do
      {:ok, recovery} ->
        {:ok, recovery}

      {:error, :conflict} when retries_left > 0 ->
        recover(storage, run_id, queue, now, retries_left - 1)

      {:error, _reason} = error ->
        error
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
    case Keyword.get(opts, :now, DateTime.utc_now()) do
      %DateTime{} = now -> {:ok, now}
      _invalid -> {:error, {:invalid_option, {:now, :invalid}}}
    end
  end

  defp run_id(opts) do
    case Keyword.get(opts, :run_id, Ecto.UUID.generate()) do
      run_id -> Options.uuid(run_id)
    end
  end

  defp runnable_value(runnable, key) when is_map(runnable) and is_atom(key) do
    Map.get(runnable, key) || Map.get(runnable, Atom.to_string(key))
  end
end
