defmodule SquidMesh.Runtime.Journal.Executor do
  @moduledoc """
  Executes one visible attempt from the journal-backed runtime queue.

  The executor is the side-effect boundary for the Jido-native runtime path. It
  claims a visible attempt with the dispatch agent, runs the declared workflow
  step once, records either a completed or failed attempt fact, and then applies
  any completed dispatch results back to the workflow journal.
  """

  alias SquidMesh.ReadModel.Inspection
  alias SquidMesh.Runtime.BuiltInStep
  alias SquidMesh.Runtime.DispatchAgent
  alias SquidMesh.Runtime.DispatchProtocol
  alias SquidMesh.Runtime.DispatchProtocol.ActionAttempt
  alias SquidMesh.Runtime.Journal
  alias SquidMesh.Runtime.Journal.Options
  alias SquidMesh.Runtime.RetryPolicy
  alias SquidMesh.Runtime.StepInput
  alias SquidMesh.Runtime.WorkflowAgent
  alias SquidMesh.Runtime.WorkflowAgent.Projection
  alias SquidMesh.Workflow.Definition

  @dispatch_append_retries 25
  @run_append_retries 25

  @type execute_error ::
          {:invalid_option,
           {:opts, term()}
           | {:runtime, term()}
           | {:journal_storage, nil}
           | {:queue, term()}
           | {:now, term()}
           | {:finished_at, term()}
           | {:owner_id, term()}
           | {:claim_id, term()}
           | {:claim_token, term()}
           | {:option, atom()}}
          | Definition.load_error()
          | {:unknown_step, atom()}
          | term()

  @type execute_result ::
          {:ok, Inspection.Snapshot.t()} | {:ok, :none} | {:error, execute_error()}

  @doc """
  Executes the next visible journal attempt, if one exists.

  Options:

  - `:runtime` must be `:journal`.
  - `:journal_storage` is the Jido storage adapter config.
  - `:queue` selects the dispatch queue and defaults to `"default"`.
  - `:owner_id` identifies the worker claiming the attempt.
  - `:claim_id` and `:claim_token` may be supplied by tests or host executors
    that need deterministic fencing values.
  - `:now` controls visibility, lease, and event timestamps.
  - `:finished_at` controls completion/failure timestamps for deterministic
    tests. Runtime callers normally omit it so the timestamp is captured after
    action execution.
  """
  @spec execute_next(keyword()) :: execute_result()
  def execute_next(opts) when is_list(opts) do
    with {:ok, opts} <- execute_options(opts),
         {:ok, storage} <- journal_storage(opts),
         {:ok, queue} <- queue(opts),
         {:ok, now} <- now(opts),
         {:ok, owner_id} <- owner_id(opts),
         {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue),
         {:ok, recovery_result} <-
           recover_pending_progressions(storage, dispatch_agent, queue, now) do
      execute_after_recovery(storage, queue, now, owner_id, opts, recovery_result)
    end
  end

  def execute_next(_opts), do: {:error, {:invalid_option, {:opts, :invalid}}}

  defp execute_after_recovery(_storage, _queue, _now, _owner_id, _opts, {:recovered, snapshot}) do
    {:ok, snapshot}
  end

  defp execute_after_recovery(storage, queue, %DateTime{} = now, owner_id, opts, :none) do
    with {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, queue),
         {:ok, claim_result} <- claim_next(storage, dispatch_agent, owner_id, opts, now) do
      execute_claim_result(storage, queue, now, opts, claim_result)
    end
  end

  defp claim_next(storage, dispatch_agent, owner_id, opts, %DateTime{} = now) do
    claim_opts =
      opts
      |> Keyword.take([:claim_id, :claim_token, :lease_for])
      |> Keyword.put(:now, now)

    DispatchAgent.claim_next(storage, dispatch_agent, owner_id, claim_opts)
  end

  defp execute_claim_result(_storage, _queue, _claim_now, _opts, :none), do: {:ok, :none}

  defp execute_claim_result(storage, queue, %DateTime{} = claim_now, opts, %{
         agent: dispatch_agent,
         attempt: %ActionAttempt{} = attempt,
         claim_id: claim_id,
         claim_token: claim_token
       })
       when is_list(opts) do
    with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, attempt.run_id),
         {:ok, workflow, definition, step_name, step} <-
           executable_step(storage, workflow_agent, attempt) do
      context = step_context(attempt, workflow, step_name)
      finished_at = lifecycle_time(opts, claim_now)
      runtime = %{storage: storage, queue: queue, now: finished_at}
      claim = claim_context(dispatch_agent, workflow_agent, attempt, claim_id, claim_token)

      case run_step(step, attempt.input, context) do
        {:ok, output, execution_opts} ->
          complete_attempt(runtime, claim, definition, step_name, output, execution_opts)

        {:error, reason} ->
          fail_attempt(runtime, claim, workflow, definition, step_name, reason)
      end
    else
      {:error, reason} ->
        finished_at = lifecycle_time(opts, claim_now)

        fail_incompatible_attempt(
          storage,
          queue,
          finished_at,
          dispatch_agent,
          attempt,
          claim_id,
          claim_token,
          reason
        )
    end
  end

  defp claim_context(dispatch_agent, workflow_agent, attempt, claim_id, claim_token) do
    %{
      dispatch_agent: dispatch_agent,
      workflow_agent: workflow_agent,
      attempt: attempt,
      claim_id: claim_id,
      claim_token: claim_token
    }
  end

  defp complete_attempt(
         %{storage: storage, queue: queue, now: now},
         %{
           dispatch_agent: dispatch_agent,
           workflow_agent: workflow_agent,
           attempt: %ActionAttempt{} = attempt,
           claim_id: claim_id,
           claim_token: claim_token
         },
         definition,
         step_name,
         output,
         execution_opts
       ) do
    runtime = %{storage: storage, queue: queue, now: now}

    claim = %{
      dispatch_agent: dispatch_agent,
      workflow_agent: workflow_agent,
      attempt: attempt,
      claim_id: claim_id,
      claim_token: claim_token
    }

    with {:ok, result} <- Definition.apply_output_mapping(definition, step_name, output),
         {:ok, %{agent: dispatch_agent}} <-
           complete_current_claim(
             storage,
             dispatch_agent,
             attempt.runnable_key,
             claim_id,
             claim_token,
             result,
             now: now
           ) do
      append_completed_attempt_progression(
        runtime,
        %{claim | dispatch_agent: dispatch_agent},
        definition,
        step_name,
        result,
        execution_opts
      )
    else
      {:error, _reason} = error ->
        error
    end
  end

  defp append_completed_attempt_progression(
         %{storage: storage, queue: queue, now: now} = runtime,
         %{
           dispatch_agent: dispatch_agent,
           workflow_agent: workflow_agent,
           attempt: %ActionAttempt{} = attempt
         } = claim,
         definition,
         step_name,
         result,
         execution_opts
       ) do
    case append_success_progression(
           workflow_agent,
           runtime,
           %{
             attempt: attempt,
             definition: definition,
             step_name: step_name,
             result: result,
             execution_opts: execution_opts
           }
         ) do
      {:ok, workflow_agent} ->
        with {:ok, _schedule_update} <-
               schedule_pending_dispatches(storage, workflow_agent, dispatch_agent, now) do
          Inspection.snapshot(storage, attempt.run_id, queue: queue, now: now)
        end

      {:error, reason} when is_tuple(reason) ->
        if StepInput.input_mapping_error?(reason) do
          fail_success_progression(runtime, claim, result, reason)
        else
          {:error, reason}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp fail_success_progression(
         %{storage: storage, queue: queue, now: now},
         %{
           dispatch_agent: dispatch_agent,
           workflow_agent: workflow_agent,
           attempt: %ActionAttempt{} = attempt
         },
         result,
         reason
       ) do
    error = normalize_error(reason)

    with {:ok, workflow_agent} <-
           append_failed_success_progression(
             %{storage: storage, now: now},
             workflow_agent,
             attempt,
             result,
             error,
             @run_append_retries
           ),
         {:ok, _schedule_update} <-
           schedule_pending_dispatches(storage, workflow_agent, dispatch_agent, now) do
      Inspection.snapshot(storage, attempt.run_id, queue: queue, now: now)
    end
  end

  defp fail_attempt(
         %{storage: storage, queue: queue, now: now} = runtime,
         %{
           dispatch_agent: dispatch_agent,
           workflow_agent: workflow_agent,
           attempt: %ActionAttempt{} = attempt,
           claim_id: claim_id,
           claim_token: claim_token
         },
         workflow,
         definition,
         step_name,
         reason
       ) do
    error = normalize_error(reason)

    retry_opts =
      retry_options(workflow, step_name, attempt, error, now)

    with {:ok, _failed} <-
           fail_current_claim(
             storage,
             dispatch_agent,
             attempt.runnable_key,
             claim_id,
             claim_token,
             error,
             Keyword.put(retry_opts, :now, now)
           ),
         {:ok, workflow_agent} <-
           append_failure_progression(
             runtime,
             workflow_agent,
             attempt,
             definition,
             step_name,
             error,
             retry_opts
           ),
         {:ok, _schedule_update} <-
           schedule_pending_dispatches(storage, workflow_agent, dispatch_agent, now) do
      Inspection.snapshot(storage, attempt.run_id, queue: queue, now: now)
    end
  end

  defp fail_incompatible_attempt(
         storage,
         queue,
         %DateTime{} = now,
         dispatch_agent,
         %ActionAttempt{} = attempt,
         claim_id,
         claim_token,
         reason
       ) do
    error =
      normalize_error(%{
        code: incompatible_error_code(reason),
        message: "journal attempt is incompatible with the current workflow definition",
        reason: reason,
        retryable?: false
      })

    with {:ok, _failed} <-
           fail_current_claim(
             storage,
             dispatch_agent,
             attempt.runnable_key,
             claim_id,
             claim_token,
             error,
             now: now
           ),
         {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, attempt.run_id),
         {:ok, _workflow_agent} <-
           append_run_entries(
             storage,
             workflow_agent,
             [run_terminal_entry!(attempt.run_id, :failed, now)],
             @run_append_retries
           ) do
      Inspection.snapshot(storage, attempt.run_id, queue: queue, now: now)
    end
  end

  defp append_success_progression(
         workflow_agent,
         %{storage: _storage, queue: _queue, now: %DateTime{}} = runtime,
         %{attempt: %ActionAttempt{}} = success
       ) do
    success = Map.put_new(success, :schedule_base_at, runtime.now)
    append_success_progression(runtime, workflow_agent, success, @run_append_retries)
  end

  defp append_success_progression(
         runtime,
         workflow_agent,
         %{attempt: %ActionAttempt{} = attempt} = success,
         retries_left
       )
       when retries_left > 0 do
    if success_progression_recorded?(workflow_agent, attempt) do
      {:ok, workflow_agent}
    else
      append_recomputed_success_progression(runtime, workflow_agent, success, retries_left)
    end
  end

  defp append_success_progression(_runtime, _workflow_agent, _success, 0),
    do: {:error, :conflict}

  defp append_recomputed_success_progression(
         %{queue: queue, now: now} = runtime,
         workflow_agent,
         %{
           attempt: %ActionAttempt{} = attempt,
           definition: definition,
           step_name: step_name,
           result: result,
           execution_opts: execution_opts
         } = success,
         retries_left
       ) do
    schedule_base_at = Map.get(success, :schedule_base_at, now)

    progression = %{
      execution_opts: execution_opts,
      queue: queue,
      schedule_base_at: schedule_base_at,
      now: now
    }

    with {:ok, transition, progression_entries} <-
           success_progression_entries(
             workflow_agent,
             attempt,
             definition,
             step_name,
             result,
             progression
           ) do
      entries = [
        runnable_applied_entry!(
          attempt,
          result,
          transition,
          now,
          execution_opts,
          schedule_base_at
        )
        | progression_entries
      ]

      append_success_entries(runtime, workflow_agent, success, entries, retries_left)
    end
  end

  defp append_success_entries(
         %{storage: storage} = runtime,
         workflow_agent,
         success,
         entries,
         retries_left
       ) do
    case Journal.append_entries(storage, entries, expected_rev: workflow_agent.state.thread_rev) do
      {:ok, _thread} ->
        WorkflowAgent.rebuild(storage, workflow_agent.state.run_id)

      {:error, :conflict} ->
        with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, workflow_agent.state.run_id) do
          append_success_progression(runtime, workflow_agent, success, retries_left - 1)
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp success_progression_recorded?(workflow_agent, %ActionAttempt{} = attempt) do
    MapSet.member?(WorkflowAgent.applied_runnable_keys(workflow_agent), attempt.runnable_key) or
      workflow_agent.state.projection.terminal_status in [:completed, :failed, :cancelled]
  end

  defp append_failed_success_progression(
         _runtime,
         workflow_agent,
         %ActionAttempt{},
         _result,
         _error,
         _retries_left
       )
       when workflow_agent.state.projection.terminal_status in [:completed, :failed, :cancelled] do
    {:ok, workflow_agent}
  end

  defp append_failed_success_progression(
         %{storage: storage, now: now} = runtime,
         workflow_agent,
         %ActionAttempt{} = attempt,
         result,
         error,
         retries_left
       )
       when retries_left > 0 do
    entries =
      if MapSet.member?(WorkflowAgent.applied_runnable_keys(workflow_agent), attempt.runnable_key) do
        [run_terminal_entry!(attempt.run_id, :failed, now, error)]
      else
        [
          runnable_applied_entry!(attempt, result, now),
          run_terminal_entry!(attempt.run_id, :failed, now, error)
        ]
      end

    case Journal.append_entries(storage, entries, expected_rev: workflow_agent.state.thread_rev) do
      {:ok, _thread} ->
        WorkflowAgent.rebuild(storage, workflow_agent.state.run_id)

      {:error, :conflict} ->
        with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, workflow_agent.state.run_id) do
          append_failed_success_progression(
            runtime,
            workflow_agent,
            attempt,
            result,
            error,
            retries_left - 1
          )
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp append_failed_success_progression(
         _runtime,
         _workflow_agent,
         %ActionAttempt{},
         _result,
         _error,
         0
       ),
       do: {:error, :conflict}

  defp success_progression_entries(
         workflow_agent,
         attempt,
         definition,
         step_name,
         result,
         progression
       ) do
    if Definition.dependency_mode?(definition) do
      with {:ok, progression_entries} <-
             dependency_success_progression_entries(
               workflow_agent,
               attempt,
               definition,
               step_name,
               result,
               progression
             ) do
        {:ok, nil, progression_entries}
      end
    else
      context = journal_context(workflow_agent, attempt, result)

      with {:ok, %{to: target} = transition} <-
             Definition.transition(definition, step_name, :ok, context),
           {:ok, progression_entries} <-
             success_progression_entries(
               attempt,
               definition,
               target,
               context,
               progression
             ) do
        {:ok, Definition.serialize_transition_decision(transition), progression_entries}
      end
    end
  end

  defp append_failure_progression(
         %{storage: storage, queue: queue, now: now},
         workflow_agent,
         %ActionAttempt{} = attempt,
         _definition,
         step_name,
         _error,
         retry_opts
       )
       when retry_opts != [] do
    if failed_progression_recorded?(workflow_agent, attempt) do
      {:ok, workflow_agent}
    else
      retry_runnable_key = Keyword.fetch!(retry_opts, :retry_runnable_key)
      retry_visible_at = Keyword.fetch!(retry_opts, :retry_visible_at)
      attempt_number = attempt.attempt_number + 1

      retry_runnable = %{
        run_id: attempt.run_id,
        runnable_key: retry_runnable_key,
        idempotency_key: retry_runnable_key,
        attempt_number: attempt_number,
        queue: queue,
        step: Definition.serialize_step(step_name),
        input: attempt.input || %{},
        visible_at: retry_visible_at
      }

      append_failure_run_entries(
        storage,
        workflow_agent,
        attempt,
        [runnables_planned_entry!(attempt.run_id, [retry_runnable], now)],
        @run_append_retries
      )
    end
  end

  defp append_failure_progression(
         %{storage: storage, queue: queue, now: now},
         workflow_agent,
         %ActionAttempt{} = attempt,
         definition,
         step_name,
         _error,
         []
       ) do
    case Definition.transition(
           definition,
           step_name,
           :error,
           journal_context(workflow_agent, attempt, %{})
         ) do
      {:ok, %{to: :complete} = transition} ->
        append_failure_run_entries(
          storage,
          workflow_agent,
          attempt,
          [
            runnable_applied_entry!(
              attempt,
              %{},
              Definition.serialize_transition_decision(transition),
              now
            ),
            run_terminal_entry!(attempt.run_id, :completed, now)
          ],
          @run_append_retries
        )

      {:ok, %{to: next_step} = transition} when is_atom(next_step) ->
        with {:ok, runnable} <-
               successor_runnable(
                 attempt,
                 definition,
                 next_step,
                 journal_context(workflow_agent, attempt, %{}),
                 queue,
                 now
               ) do
          append_failure_run_entries(
            storage,
            workflow_agent,
            attempt,
            [
              runnable_applied_entry!(
                attempt,
                %{},
                Definition.serialize_transition_decision(transition),
                now
              ),
              runnables_planned_entry!(attempt.run_id, [runnable], now)
            ],
            @run_append_retries
          )
        end

      {:error, {:unknown_transition, _from_step, :error}} ->
        append_run_entries(
          storage,
          workflow_agent,
          [run_terminal_entry!(attempt.run_id, :failed, now)],
          @run_append_retries
        )

      {:error, {:no_matching_transition, _from_step, :error}} ->
        append_run_entries(
          storage,
          workflow_agent,
          [run_terminal_entry!(attempt.run_id, :failed, now)],
          @run_append_retries
        )

      {:error, _reason} = error ->
        error
    end
  end

  defp append_failure_run_entries(
         storage,
         workflow_agent,
         %ActionAttempt{} = attempt,
         entries,
         retries_left
       ) do
    if failed_progression_recorded?(workflow_agent, attempt) do
      {:ok, workflow_agent}
    else
      append_failure_run_entries_with_pending_progression(
        storage,
        workflow_agent,
        attempt,
        entries,
        retries_left
      )
    end
  end

  defp append_failure_run_entries_with_pending_progression(
         storage,
         workflow_agent,
         %ActionAttempt{} = attempt,
         entries,
         retries_left
       )
       when retries_left > 0 do
    case Journal.append_entries(storage, entries, expected_rev: workflow_agent.state.thread_rev) do
      {:ok, _thread} ->
        WorkflowAgent.rebuild(storage, workflow_agent.state.run_id)

      {:error, :conflict} ->
        with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, workflow_agent.state.run_id) do
          append_failure_run_entries(storage, workflow_agent, attempt, entries, retries_left - 1)
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp append_failure_run_entries_with_pending_progression(
         _storage,
         _workflow_agent,
         %ActionAttempt{},
         _entries,
         0
       ),
       do: {:error, :conflict}

  defp success_progression_entries(
         %ActionAttempt{} = attempt,
         _definition,
         :complete,
         _result,
         %{now: now}
       ) do
    {:ok, [run_terminal_entry!(attempt.run_id, :completed, now)]}
  end

  defp success_progression_entries(
         %ActionAttempt{} = attempt,
         definition,
         next_step,
         result,
         %{
           execution_opts: execution_opts,
           queue: queue,
           schedule_base_at: %DateTime{} = schedule_base_at,
           now: %DateTime{} = now
         }
       )
       when is_atom(next_step) do
    visible_at = successor_visible_at(schedule_base_at, execution_opts)

    with {:ok, runnable} <-
           successor_runnable(attempt, definition, next_step, result, queue, visible_at) do
      {:ok, [runnables_planned_entry!(attempt.run_id, [runnable], now)]}
    end
  end

  defp dependency_success_progression_entries(
         workflow_agent,
         %ActionAttempt{} = attempt,
         definition,
         step_name,
         result,
         %{now: now} = progression
       ) do
    step_statuses = dependency_step_statuses(workflow_agent, definition, step_name)

    case Definition.dependency_progress(definition, step_statuses) do
      :complete ->
        {:ok, [run_terminal_entry!(attempt.run_id, :completed, now)]}

      {:dispatch, next_steps} ->
        context = dependency_context(workflow_agent, result)

        case dependency_success_runnables(
               workflow_agent,
               attempt,
               context,
               definition,
               next_steps,
               progression
             ) do
          {:ok, runnables} ->
            {:ok, [runnables_planned_entry!(attempt.run_id, runnables, now)]}

          {:error, _reason} = error ->
            error
        end

      {:wait, _phase_steps} ->
        {:ok, []}

      {:error, _reason} = error ->
        error
    end
  end

  defp dependency_success_runnables(
         workflow_agent,
         attempt,
         context,
         definition,
         next_steps,
         %{
           execution_opts: execution_opts,
           queue: queue,
           schedule_base_at: %DateTime{} = schedule_base_at
         }
       ) do
    base_visible_at = successor_visible_at(schedule_base_at, execution_opts)

    result =
      Enum.reduce_while(next_steps, {:ok, []}, fn next_step, {:ok, acc} ->
        case successor_input(context, definition, next_step) do
          {:ok, input} ->
            visible_at =
              dependency_successor_visible_at(
                workflow_agent,
                definition,
                next_step,
                base_visible_at
              )

            runnable = journal_runnable(attempt.run_id, queue, next_step, input, 1, visible_at)
            {:cont, {:ok, [runnable | acc]}}

          {:error, _reason} = error ->
            {:halt, error}
        end
      end)

    case result do
      {:ok, runnables} -> {:ok, Enum.reverse(runnables)}
      {:error, _reason} = error -> error
    end
  end

  defp dependency_successor_visible_at(workflow_agent, definition, next_step, %DateTime{} = base) do
    workflow_agent
    |> completed_wait_dependency_visible_ats(definition, next_step)
    |> Enum.reduce(base, &max_datetime/2)
  end

  defp completed_wait_dependency_visible_ats(
         %{state: %{projection: %Projection{} = projection}},
         definition,
         next_step
       ) do
    definition
    |> dependency_steps(next_step)
    |> Enum.flat_map(&completed_wait_visible_at(projection, definition, &1))
  end

  defp dependency_steps(definition, next_step) do
    case Definition.step(definition, next_step) do
      {:ok, %{opts: opts}} ->
        opts
        |> Keyword.get(:after, [])
        |> List.wrap()

      {:error, _reason} ->
        []
    end
  end

  defp completed_wait_visible_at(%Projection{} = projection, definition, dependency_step) do
    with {:ok, %{module: :wait, opts: opts}} <- Definition.step(definition, dependency_step),
         {:ok, runnable_key} <-
           Projection.applied_runnable_key_for_step(
             projection,
             Definition.serialize_step(dependency_step)
           ),
         %DateTime{} = applied_at <- Projection.applied_at(projection, runnable_key) do
      execution_opts = wait_dependency_execution_opts(projection, runnable_key, opts)

      [successor_visible_at(applied_at, execution_opts)]
    else
      _not_a_completed_wait_dependency -> []
    end
  end

  defp wait_dependency_execution_opts(%Projection{} = projection, runnable_key, step_opts) do
    case Projection.applied_execution_opts(projection, runnable_key) do
      [] -> recovered_execution_opts(%{module: :wait, opts: step_opts})
      execution_opts -> execution_opts
    end
  end

  defp max_datetime(%DateTime{} = left, %DateTime{} = right) do
    case DateTime.compare(left, right) do
      :gt -> left
      _lte_or_eq -> right
    end
  end

  defp dependency_step_statuses(workflow_agent, definition, completed_step) do
    applied_keys = WorkflowAgent.applied_runnable_keys(workflow_agent)

    workflow_agent
    |> WorkflowAgent.planned_runnables()
    |> Enum.reduce(%{}, fn runnable, acc ->
      runnable_key = Map.get(runnable, :runnable_key) || Map.get(runnable, "runnable_key")
      step = Map.get(runnable, :step) || Map.get(runnable, "step")

      cond do
        MapSet.member?(applied_keys, runnable_key) ->
          Map.put(acc, Definition.deserialize_step(definition, step), :completed)

        Definition.deserialize_step(definition, step) == completed_step ->
          Map.put(acc, completed_step, :completed)

        true ->
          acc
      end
    end)
  end

  defp dependency_context(workflow_agent, current_result) do
    applied_results =
      applied_result_context(workflow_agent)

    Map.merge(applied_results, current_result || %{})
  end

  defp journal_context(workflow_agent, %ActionAttempt{input: input}, current_result) do
    workflow_agent
    |> applied_result_context()
    |> Map.merge(input || %{})
    |> Map.merge(current_result || %{})
  end

  defp applied_result_context(workflow_agent) do
    workflow_agent.state.projection
    |> Map.get(:applied_results, %{})
    |> Map.values()
    |> Enum.filter(&is_map/1)
    |> Enum.reduce(%{}, &Map.merge(&2, &1))
  end

  defp successor_runnable(
         %ActionAttempt{} = attempt,
         definition,
         next_step,
         context,
         queue,
         %DateTime{} = now
       ) do
    input =
      successor_input(context, definition, next_step)

    case input do
      {:ok, input} ->
        {:ok, journal_runnable(attempt.run_id, queue, next_step, input, 1, now)}

      {:error, _reason} = error ->
        error
    end
  end

  defp successor_input(context, definition, next_step) do
    case Definition.step_input_mapping(definition, next_step) do
      {:ok, input_mapping} -> StepInput.apply_input_mapping(context, input_mapping)
      {:error, _reason} = error -> error
    end
  end

  defp successor_visible_at(%DateTime{} = now, execution_opts) when is_list(execution_opts) do
    case Keyword.get(execution_opts, :schedule_in) do
      seconds when is_integer(seconds) and seconds > 0 -> DateTime.add(now, seconds, :second)
      _immediate -> now
    end
  end

  defp append_run_entries(_storage, workflow_agent, _entries, _retries_left)
       when workflow_agent.state.projection.terminal_status in [:completed, :failed, :cancelled] do
    {:ok, workflow_agent}
  end

  defp append_run_entries(storage, workflow_agent, entries, retries_left) when retries_left > 0 do
    case Journal.append_entries(storage, entries, expected_rev: workflow_agent.state.thread_rev) do
      {:ok, _thread} ->
        WorkflowAgent.rebuild(storage, workflow_agent.state.run_id)

      {:error, :conflict} ->
        with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, workflow_agent.state.run_id) do
          append_run_entries(storage, workflow_agent, entries, retries_left - 1)
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp append_run_entries(_storage, _workflow_agent, _entries, 0), do: {:error, :conflict}

  defp schedule_pending_dispatches(storage, workflow_agent, dispatch_agent, %DateTime{} = now) do
    schedule_pending_dispatches(
      storage,
      workflow_agent,
      dispatch_agent,
      now,
      @dispatch_append_retries
    )
  end

  defp schedule_pending_dispatches(_storage, workflow_agent, dispatch_agent, _now, _retries_left)
       when workflow_agent.state.projection.terminal_status in [:completed, :failed, :cancelled] do
    {:ok, %{agent: dispatch_agent, runnables: []}}
  end

  defp schedule_pending_dispatches(storage, workflow_agent, dispatch_agent, now, retries_left)
       when retries_left > 0 do
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

  defp schedule_pending_dispatches(_storage, _workflow_agent, _dispatch_agent, _now, 0),
    do: {:error, :conflict}

  defp complete_current_claim(
         storage,
         dispatch_agent,
         runnable_key,
         claim_id,
         claim_token,
         result,
         opts
       ) do
    complete_current_claim(
      storage,
      dispatch_agent,
      runnable_key,
      claim_id,
      claim_token,
      result,
      opts,
      @dispatch_append_retries
    )
  end

  defp complete_current_claim(
         storage,
         dispatch_agent,
         runnable_key,
         claim_id,
         claim_token,
         result,
         opts,
         retries_left
       )
       when retries_left > 0 do
    case DispatchAgent.complete(
           storage,
           dispatch_agent,
           runnable_key,
           claim_id,
           claim_token,
           result,
           opts
         ) do
      {:ok, _update} = ok ->
        ok

      {:error, :conflict} ->
        with {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, dispatch_agent.state.queue) do
          complete_current_claim(
            storage,
            dispatch_agent,
            runnable_key,
            claim_id,
            claim_token,
            result,
            opts,
            retries_left - 1
          )
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp complete_current_claim(_storage, _agent, _key, _claim_id, _claim_token, _result, _opts, 0),
    do: {:error, :conflict}

  defp fail_current_claim(
         storage,
         dispatch_agent,
         runnable_key,
         claim_id,
         claim_token,
         error,
         opts
       ) do
    fail_current_claim(
      storage,
      dispatch_agent,
      runnable_key,
      claim_id,
      claim_token,
      error,
      opts,
      @dispatch_append_retries
    )
  end

  defp fail_current_claim(
         storage,
         dispatch_agent,
         runnable_key,
         claim_id,
         claim_token,
         error,
         opts,
         retries_left
       )
       when retries_left > 0 do
    case DispatchAgent.fail(
           storage,
           dispatch_agent,
           runnable_key,
           claim_id,
           claim_token,
           error,
           opts
         ) do
      {:ok, _update} = ok ->
        ok

      {:error, :conflict} ->
        with {:ok, dispatch_agent} <- DispatchAgent.rebuild(storage, dispatch_agent.state.queue) do
          fail_current_claim(
            storage,
            dispatch_agent,
            runnable_key,
            claim_id,
            claim_token,
            error,
            opts,
            retries_left - 1
          )
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp fail_current_claim(_storage, _agent, _key, _claim_id, _claim_token, _error, _opts, 0),
    do: {:error, :conflict}

  defp retry_options(workflow, step_name, %ActionAttempt{} = attempt, error, %DateTime{} = now) do
    if Map.get(error, :retryable?) == false do
      []
    else
      case RetryPolicy.resolve(workflow, step_name, attempt.attempt_number) do
        {:retry, next_attempt, delay_ms} ->
          retry_visible_at = DateTime.add(now, retry_delay_ms(error, delay_ms), :millisecond)

          [
            retry_runnable_key: runnable_key(attempt.run_id, step_name, next_attempt),
            retry_visible_at: retry_visible_at
          ]

        _no_retry ->
          []
      end
    end
  end

  defp retry_delay_ms(%{retry_after: retry_after}, _policy_delay_ms)
       when is_integer(retry_after) and retry_after >= 0 do
    retry_after
  end

  defp retry_delay_ms(_error, policy_delay_ms), do: policy_delay_ms

  defp runnable_applied_entry!(%ActionAttempt{} = attempt, result, %DateTime{} = now) do
    runnable_applied_entry!(attempt, result, nil, now, [], now)
  end

  defp runnable_applied_entry!(%ActionAttempt{} = attempt, result, transition, %DateTime{} = now) do
    runnable_applied_entry!(attempt, result, transition, now, [], now)
  end

  defp runnable_applied_entry!(
         %ActionAttempt{} = attempt,
         result,
         transition,
         %DateTime{} = now,
         execution_opts,
         %DateTime{} = applied_at
       )
       when is_list(execution_opts) do
    entry!(:runnable_applied, %{
      run_id: attempt.run_id,
      runnable_key: attempt.runnable_key,
      result: result,
      execution_opts: execution_opts,
      applied_at: applied_at,
      transition: transition,
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

  defp run_terminal_entry!(run_id, status, %DateTime{} = now, error) when is_map(error) do
    entry!(:run_terminal, %{
      run_id: run_id,
      status: status,
      error: error,
      occurred_at: now
    })
  end

  defp entry!(type, attrs) do
    {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end

  defp journal_runnable(run_id, queue, step_name, input, attempt_number, %DateTime{} = now) do
    step = Definition.serialize_step(step_name)
    runnable_key = runnable_key(run_id, step_name, attempt_number)

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

  defp runnable_key(run_id, step_name, attempt_number) do
    "#{run_id}:#{Definition.serialize_step(step_name)}:#{attempt_number}"
  end

  defp recover_pending_progressions(storage, dispatch_agent, queue, %DateTime{} = now) do
    attempts = Map.values(dispatch_agent.state.projection.attempts)

    case recover_pending_dispatches(storage, dispatch_agent, queue, attempts, now) do
      {:ok, :none} ->
        cond do
          attempt = Enum.find(attempts, &recoverable_completed_attempt?(storage, &1)) ->
            recover_completed_progression(storage, dispatch_agent, queue, attempt, now)

          attempt = Enum.find(attempts, &recoverable_failed_attempt?(storage, &1)) ->
            recover_failed_progression(storage, dispatch_agent, queue, attempt, now)

          true ->
            {:ok, :none}
        end

      {:ok, {:recovered, %Inspection.Snapshot{}}} = recovered ->
        recovered

      {:error, _reason} = error ->
        error
    end
  end

  defp recover_pending_dispatches(storage, dispatch_agent, queue, attempts, %DateTime{} = now) do
    dispatch_agent
    |> DispatchAgent.run_ids()
    |> MapSet.union(attempt_run_ids(attempts))
    |> Enum.sort()
    |> Enum.reduce_while({:ok, :none}, fn run_id, {:ok, :none} ->
      case WorkflowAgent.rebuild(storage, run_id) do
        {:ok, workflow_agent} ->
          maybe_schedule_pending_dispatches(storage, workflow_agent, dispatch_agent, queue, now)

        {:error, _reason} ->
          {:cont, {:ok, :none}}
      end
    end)
  end

  defp attempt_run_ids(attempts) do
    attempts
    |> Enum.map(& &1.run_id)
    |> MapSet.new()
  end

  defp maybe_schedule_pending_dispatches(storage, workflow_agent, dispatch_agent, queue, now) do
    case pending_dispatches_for_queue(workflow_agent, dispatch_agent, queue) do
      [] ->
        {:cont, {:ok, :none}}

      [_runnable | _runnables] ->
        storage
        |> schedule_pending_dispatches(workflow_agent, dispatch_agent, now)
        |> pending_dispatch_recovery_result(storage, workflow_agent, queue, now)
    end
  end

  defp pending_dispatches_for_queue(workflow_agent, dispatch_agent, queue) do
    workflow_agent
    |> WorkflowAgent.pending_dispatches(dispatch_agent)
    |> Enum.filter(&(runnable_queue(&1) == queue))
  end

  defp runnable_queue(runnable) when is_map(runnable) do
    Map.get(runnable, :queue) || Map.get(runnable, "queue")
  end

  defp pending_dispatch_recovery_result(
         {:ok, %{runnables: []}},
         _storage,
         _workflow_agent,
         _queue,
         _now
       ) do
    {:cont, {:ok, :none}}
  end

  defp pending_dispatch_recovery_result({:ok, %{}}, storage, workflow_agent, queue, now) do
    case Inspection.snapshot(storage, workflow_agent.state.run_id, queue: queue, now: now) do
      {:ok, %Inspection.Snapshot{} = snapshot} -> {:halt, {:ok, {:recovered, snapshot}}}
      {:error, _reason} = error -> {:halt, error}
    end
  end

  defp pending_dispatch_recovery_result(
         {:error, _reason} = error,
         _storage,
         _workflow_agent,
         _queue,
         _now
       ),
       do: {:halt, error}

  defp recover_completed_progression(
         storage,
         dispatch_agent,
         queue,
         %ActionAttempt{} = attempt,
         now
       ) do
    with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, attempt.run_id) do
      recover_completed_step(storage, dispatch_agent, queue, workflow_agent, attempt, now)
    end
  end

  defp recover_completed_step(storage, dispatch_agent, queue, workflow_agent, attempt, now) do
    case executable_step(storage, workflow_agent, attempt) do
      {:ok, _workflow, definition, step_name, step} ->
        append_recovered_success(
          %{storage: storage, queue: queue, now: now},
          dispatch_agent,
          workflow_agent,
          attempt,
          definition,
          step_name,
          step
        )

      {:error, reason} ->
        recover_incompatible_progression(storage, queue, workflow_agent, attempt, now, reason)
    end
  end

  defp append_recovered_success(
         %{storage: storage, queue: queue, now: now} = runtime,
         dispatch_agent,
         workflow_agent,
         attempt,
         definition,
         step_name,
         step
       ) do
    with {:ok, workflow_agent} <-
           append_success_progression(
             workflow_agent,
             runtime,
             %{
               attempt: attempt,
               definition: definition,
               step_name: step_name,
               result: attempt.result || %{},
               execution_opts: recovered_execution_opts(step),
               schedule_base_at: attempt_completion_at(attempt, now)
             }
           ),
         {:ok, _schedule_update} <-
           schedule_pending_dispatches(storage, workflow_agent, dispatch_agent, now),
         {:ok, %Inspection.Snapshot{} = snapshot} <-
           Inspection.snapshot(storage, attempt.run_id, queue: queue, now: now) do
      {:ok, {:recovered, snapshot}}
    else
      {:error, reason} when is_tuple(reason) ->
        if StepInput.input_mapping_error?(reason) do
          recover_success_progression_failure(
            storage,
            queue,
            workflow_agent,
            attempt,
            now,
            reason
          )
        else
          {:error, reason}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp recover_success_progression_failure(storage, queue, workflow_agent, attempt, now, reason) do
    error = normalize_error(reason)

    with {:ok, _workflow_agent} <-
           append_failed_success_progression(
             %{storage: storage, now: now},
             workflow_agent,
             attempt,
             attempt.result || %{},
             error,
             @run_append_retries
           ),
         {:ok, %Inspection.Snapshot{} = snapshot} <-
           Inspection.snapshot(storage, attempt.run_id, queue: queue, now: now) do
      {:ok, {:recovered, snapshot}}
    end
  end

  defp recover_failed_progression(storage, dispatch_agent, queue, %ActionAttempt{} = attempt, now) do
    with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, attempt.run_id) do
      recover_failed_step(storage, dispatch_agent, queue, workflow_agent, attempt, now)
    end
  end

  defp recover_failed_step(storage, dispatch_agent, queue, workflow_agent, attempt, now) do
    case executable_step(storage, workflow_agent, attempt) do
      {:ok, _workflow, definition, step_name, _step} ->
        append_recovered_failure(
          storage,
          dispatch_agent,
          queue,
          workflow_agent,
          attempt,
          definition,
          step_name,
          now
        )

      {:error, reason} ->
        recover_incompatible_progression(storage, queue, workflow_agent, attempt, now, reason)
    end
  end

  defp append_recovered_failure(
         storage,
         dispatch_agent,
         queue,
         workflow_agent,
         attempt,
         definition,
         step_name,
         now
       ) do
    retry_opts = durable_retry_options(dispatch_agent, attempt)
    runtime = %{storage: storage, queue: queue, now: now}

    with {:ok, workflow_agent} <-
           append_failure_progression(
             runtime,
             workflow_agent,
             attempt,
             definition,
             step_name,
             attempt.error || %{},
             retry_opts
           ),
         {:ok, _schedule_update} <-
           schedule_pending_dispatches(storage, workflow_agent, dispatch_agent, now),
         {:ok, %Inspection.Snapshot{} = snapshot} <-
           Inspection.snapshot(storage, attempt.run_id, queue: queue, now: now) do
      {:ok, {:recovered, snapshot}}
    end
  end

  defp recover_incompatible_progression(
         storage,
         queue,
         workflow_agent,
         %ActionAttempt{} = attempt,
         %DateTime{} = now,
         _reason
       ) do
    with {:ok, _workflow_agent} <-
           append_run_entries(
             storage,
             workflow_agent,
             [run_terminal_entry!(attempt.run_id, :failed, now)],
             @run_append_retries
           ),
         {:ok, %Inspection.Snapshot{} = snapshot} <-
           Inspection.snapshot(storage, attempt.run_id, queue: queue, now: now) do
      {:ok, {:recovered, snapshot}}
    end
  end

  defp recoverable_completed_attempt?(storage, %ActionAttempt{status: :completed} = attempt) do
    case WorkflowAgent.rebuild(storage, attempt.run_id) do
      {:ok, workflow_agent} ->
        not MapSet.member?(
          WorkflowAgent.applied_runnable_keys(workflow_agent),
          attempt.runnable_key
        ) and
          workflow_agent.state.projection.terminal_status not in [:completed, :failed, :cancelled]

      {:error, _reason} ->
        false
    end
  end

  defp recoverable_completed_attempt?(_storage, %ActionAttempt{}), do: false

  defp recoverable_failed_attempt?(storage, %ActionAttempt{status: :failed} = attempt) do
    case WorkflowAgent.rebuild(storage, attempt.run_id) do
      {:ok, workflow_agent} ->
        workflow_agent.state.projection.terminal_status not in [:completed, :failed, :cancelled] and
          not failed_progression_recorded?(workflow_agent, attempt)

      {:error, _reason} ->
        false
    end
  end

  defp recoverable_failed_attempt?(_storage, %ActionAttempt{}), do: false

  defp failed_progression_recorded?(workflow_agent, %ActionAttempt{} = attempt) do
    retry_key = runnable_key(attempt.run_id, attempt.step, attempt.attempt_number + 1)

    MapSet.member?(WorkflowAgent.applied_runnable_keys(workflow_agent), attempt.runnable_key) or
      Enum.member?(WorkflowAgent.planned_runnable_keys(workflow_agent), retry_key) or
      workflow_agent.state.projection.terminal_status in [:completed, :failed, :cancelled]
  end

  defp durable_retry_options(dispatch_agent, %ActionAttempt{} = failed_attempt) do
    dispatch_agent.state.projection.attempts
    |> Map.values()
    |> Enum.find(fn %ActionAttempt{} = attempt ->
      attempt.run_id == failed_attempt.run_id and
        attempt.step == failed_attempt.step and
        attempt.attempt_number == failed_attempt.attempt_number + 1 and
        attempt.status in [:available, :retry_scheduled, :claimed, :completed, :failed]
    end)
    |> case do
      %ActionAttempt{} = retry_attempt ->
        [
          retry_runnable_key: retry_attempt.runnable_key,
          retry_visible_at: retry_attempt.visible_at
        ]

      nil ->
        []
    end
  end

  defp executable_step(storage, workflow_agent, %ActionAttempt{} = attempt) do
    with {:ok, workflow, definition} <- Definition.load_serialized(workflow_agent.state.workflow),
         :ok <- validate_definition_fingerprint(storage, attempt.run_id, definition),
         step_name when is_atom(step_name) <-
           Definition.deserialize_step(definition, attempt.step),
         {:ok, step} <- Definition.step(definition, step_name) do
      {:ok, workflow, definition, step_name, step}
    else
      step_name when is_binary(step_name) -> {:error, {:unknown_step, step_name}}
      {:error, _reason} = error -> error
    end
  end

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

  defp step_context(%ActionAttempt{} = attempt, workflow, step_name) do
    %{
      run_id: attempt.run_id,
      workflow: workflow,
      step: step_name,
      attempt: attempt.attempt_number,
      state: attempt.input
    }
  end

  defp recovered_execution_opts(%{module: :wait, opts: opts}) when is_list(opts) do
    {:ok, _output, execution_opts} = BuiltInStep.execute_wait(opts)
    execution_opts
  end

  defp recovered_execution_opts(_step), do: []

  defp attempt_completion_at(%ActionAttempt{} = attempt, %DateTime{} = fallback) do
    case Map.get(attempt, :completed_at) do
      %DateTime{} = completed_at -> completed_at
      _missing -> fallback
    end
  end

  @spec run_step(map(), map(), map()) :: {:ok, map(), keyword()} | {:error, term()}
  defp run_step(%{module: :wait, opts: opts}, _input, _context) when is_list(opts) do
    BuiltInStep.execute_wait(opts)
  end

  defp run_step(%{module: :log, opts: opts}, _input, _context) when is_list(opts) do
    {:ok, output, _execution_opts} = BuiltInStep.execute_log(opts)
    {:ok, output, []}
  end

  defp run_step(%{module: module}, input, context) do
    if module in [:pause, :approval] do
      {:error,
       %{
         message: "journal executor cannot execute manual built-in steps yet",
         retryable?: false,
         step_kind: module
       }}
    else
      run_action_step(module, input, context)
    end
  end

  defp run_action_step(action, input, context) do
    {action, input} = action_input(action, input)

    result =
      :erlang.apply(Jido.Exec, :run, [
        action,
        input,
        context,
        [max_retries: 0, log_level: :none, telemetry: :silent]
      ])

    case result do
      {:ok, output} when is_map(output) -> {:ok, output, []}
      {:ok, output, extras} when is_map(output) and is_list(extras) -> {:ok, output, []}
      {:ok, output, _extras} when is_map(output) -> {:ok, output, []}
      {:error, reason} -> {:error, reason}
      other -> unexpected_exec_result(other)
    end
  end

  defp unexpected_exec_result(result) do
    {:error,
     %{
       message: "unexpected Jido.Exec.run result",
       retryable?: false,
       result: inspect(result)
     }}
  end

  defp action_input(action, input) do
    if SquidMesh.Step.native_step?(action) do
      {SquidMesh.Step.Action, %{step: action, input: input}}
    else
      {action, input}
    end
  end

  defp normalize_error(%{__struct__: _struct, message: message, details: details})
       when is_binary(message) and is_map(details) do
    details
    |> Map.put(:message, message)
    |> redact_error()
  end

  defp normalize_error({:missing_input_path, _details} = reason) do
    StepInput.input_mapping_error_to_map(reason)
  end

  defp normalize_error(%{__struct__: _struct, message: message}) when is_binary(message) do
    redact_error(%{message: message})
  end

  defp normalize_error(reason) when is_map(reason), do: redact_error(reason)
  defp normalize_error(reason) when is_binary(reason), do: redact_error(%{message: reason})
  defp normalize_error(_reason), do: %{message: "step execution failed"}

  defp redact_error(error) when is_map(error) do
    %{}
    |> maybe_put_safe(:code, safe_error_code(Map.get(error, :code)))
    |> maybe_put_safe(:retryable?, Map.get(error, :retryable?))
    |> maybe_put_safe(:retry_after, Map.get(error, :retry_after))
    |> Map.put(:message, safe_error_message(Map.get(error, :message)))
  end

  defp maybe_put_safe(acc, key, value)
       when is_binary(value) or is_boolean(value) or is_integer(value) do
    Map.put(acc, key, value)
  end

  defp maybe_put_safe(acc, _key, _value), do: acc

  defp incompatible_error_code(%{code: code}) when is_binary(code), do: code
  defp incompatible_error_code(_reason), do: "incompatible_journal_attempt"

  defp safe_error_code(code) when is_binary(code) do
    if Regex.match?(~r/^[a-z][a-z0-9_]{0,63}$/, code) do
      code
    else
      "step_error"
    end
  end

  defp safe_error_code(_code), do: nil

  defp safe_error_message(message)
       when message in [
              "gateway timeout",
              "journal attempt is incompatible with the current workflow definition",
              "step execution failed"
            ] do
    message
  end

  defp safe_error_message(_message), do: "step execution failed"

  defp execute_options(opts) when is_list(opts) do
    cond do
      not Keyword.keyword?(opts) ->
        {:error, {:invalid_option, {:opts, :invalid}}}

      unsupported = Enum.find(Keyword.keys(opts), &(&1 not in supported_options())) ->
        {:error, {:invalid_option, {:option, unsupported}}}

      Keyword.has_key?(opts, :finished_at) and not match?(%DateTime{}, opts[:finished_at]) ->
        invalid_option(:finished_at)

      Keyword.get(opts, :runtime) != :journal ->
        invalid_option(:runtime)

      true ->
        {:ok, opts}
    end
  end

  defp supported_options do
    [
      :runtime,
      :journal_storage,
      :queue,
      :owner_id,
      :claim_id,
      :claim_token,
      :lease_for,
      :now,
      :finished_at
    ]
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
      _invalid -> invalid_option(:now)
    end
  end

  defp lifecycle_time(opts, %DateTime{} = claim_now) do
    cond do
      Keyword.has_key?(opts, :finished_at) ->
        Keyword.fetch!(opts, :finished_at)

      Keyword.has_key?(opts, :now) ->
        claim_now

      true ->
        DateTime.utc_now()
    end
  end

  defp owner_id(opts) do
    case Keyword.get(opts, :owner_id, "squid_mesh") do
      owner_id when is_binary(owner_id) and owner_id != "" -> {:ok, owner_id}
      _owner_id -> invalid_option(:owner_id)
    end
  end

  defp invalid_option(field), do: {:error, {:invalid_option, {field, :invalid}}}
end
