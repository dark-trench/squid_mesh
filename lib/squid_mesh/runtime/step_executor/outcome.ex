defmodule SquidMesh.Runtime.StepExecutor.Outcome do
  @moduledoc """
  Persistence and dispatch handling for completed step executions.

  `SquidMesh.Runtime.StepExecutor` delegates here after a step finishes so the
  orchestration flow stays readable while success, failure, retry, and dispatch
  error handling remain together.
  """

  require Logger

  alias SquidMesh.AttemptStore
  alias SquidMesh.Config
  alias SquidMesh.Observability
  alias SquidMesh.Run
  alias SquidMesh.RunStore
  alias SquidMesh.Runtime.Compensation
  alias SquidMesh.Runtime.Dispatcher
  alias SquidMesh.Runtime.RetryPolicy
  alias SquidMesh.Runtime.StepExecutor.PreparedStep
  alias SquidMesh.Runtime.StepExecutor.Progression
  alias SquidMesh.Runtime.StepExecutor.Progression.Complete
  alias SquidMesh.Runtime.StepExecutor.Progression.DispatchRun
  alias SquidMesh.Runtime.StepExecutor.Progression.DispatchSteps
  alias SquidMesh.Runtime.StepExecutor.Progression.Update
  alias SquidMesh.Runtime.StepInput
  alias SquidMesh.StepRunStore
  alias SquidMesh.Workflow.Definition, as: WorkflowDefinition

  @reserved_context_keys [:schedule]

  @type execution_error ::
          :not_found
          | {:invalid_workflow, module() | String.t()}
          | {:invalid_step, atom() | String.t() | nil}
          | {:dispatch_failed, term()}
          | {:invalid_run, Ecto.Changeset.t()}
          | {:invalid_transition, Run.status(), Run.status()}
          | {:no_runnable_step, [atom()]}
          | {:unknown_transition, atom(), atom()}
          | {:unknown_step, atom()}
          | {:missing_config, [atom()]}

  @type execution_context :: %{
          required(:config) => Config.t(),
          required(:definition) => WorkflowDefinition.t(),
          required(:run) => Run.t(),
          required(:step_name) => atom(),
          required(:step_run_id) => Ecto.UUID.t(),
          required(:attempt_id) => Ecto.UUID.t(),
          required(:attempt_number) => pos_integer(),
          required(:started_at) => integer()
        }

  @doc false
  @spec apply_execution_result({:ok, map(), keyword()} | {:error, term()}, execution_context()) ::
          :ok | {:error, execution_error() | term()}
  def apply_execution_result(result, context) when is_map(context) do
    context = Map.put(context, :result, result)

    # Persist the attempt, step state, run progression, and successor dispatch
    # as one unit so a crash cannot commit terminal step history ahead of work.
    case context.config.repo.transaction(fn ->
           context
           |> do_apply_execution_result()
           |> rollback_execution_error(context.config)
         end) do
      {:ok, events} ->
        emit_post_commit_events(events)
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp rollback_execution_error({:ok, events}, _config), do: events
  defp rollback_execution_error({:error, reason}, config), do: config.repo.rollback(reason)

  defp do_apply_execution_result(
         %{
           result: {:ok, output, execution_opts},
           definition: definition,
           step_name: step_name,
           started_at: started_at
         } = context
       ) do
    duration = System.monotonic_time() - started_at

    with {:ok, step} <- WorkflowDefinition.step(definition, step_name),
         {:ok, mapped_output} <-
           WorkflowDefinition.apply_output_mapping(definition, step_name, output) do
      step
      |> pause_kind(execution_opts)
      |> handle_success_result(
        Map.merge(context, %{duration: duration, mapped_output: mapped_output})
      )
    end
  end

  defp do_apply_execution_result(%{
         result: {:error, reason},
         config: %Config{} = config,
         definition: definition,
         run: %Run{} = run,
         step_name: step_name,
         step_run_id: step_run_id,
         attempt_id: attempt_id,
         attempt_number: attempt_number,
         started_at: started_at
       }) do
    error = normalize_error(reason)
    duration = System.monotonic_time() - started_at

    with {:ok, _attempt} <- AttemptStore.fail_attempt(config.repo, attempt_id, error),
         {:ok, _step_run} <- StepRunStore.fail_step(config.repo, step_run_id, error),
         {:ok, events} <-
           apply_failure_progression(
             config,
             definition,
             run,
             step_name,
             step_run_id,
             attempt_number,
             error
           ) do
      {:ok, [step_failed_event(run, step_name, attempt_number, duration, error) | events]}
    end
  end

  defp apply_failure_progression(
         config,
         definition,
         run,
         step_name,
         step_run_id,
         attempt_number,
         error
       ) do
    if Map.get(error, :retryable?) == false do
      handle_terminal_or_routed_failure(config, definition, run, step_name, step_run_id, error)
    else
      case RetryPolicy.resolve(run.workflow, step_name, attempt_number) do
        {:retry, _next_attempt, delay_ms} ->
          delay_ms = retry_delay_ms(error, delay_ms)
          schedule_retry(config, run, step_name, attempt_number, error, delay_ms)

        _no_retry ->
          handle_terminal_or_routed_failure(
            config,
            definition,
            run,
            step_name,
            step_run_id,
            error
          )
      end
    end
  end

  defp schedule_retry(config, run, step_name, attempt_number, error, delay_ms) do
    dispatch_opts = retry_dispatch_opts(delay_ms)

    case RunStore.progress_run_with_events(
           config.repo,
           run.id,
           fn _current_run ->
             %{
               current_step: step_name,
               last_error: error
             }
           end,
           {:transition_or_dispatch_or_fail, :retrying,
            fn retried_run ->
              dispatch_run_events(config, retried_run, dispatch_opts)
            end,
            fn _current_run, reason ->
              retry_dispatch_failure_attrs(step_name, error, reason)
            end}
         ) do
      {:ok, %Run{status: :retrying}, events} ->
        {:ok, [step_retry_scheduled_event(run, step_name, attempt_number, delay_ms) | events]}

      {:ok, _failed_or_noop, events} ->
        {:ok, events}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp handle_success_result(:pause, %{
         config: %Config{} = config,
         definition: definition,
         run: run,
         step_name: step_name,
         step_run_id: step_run_id,
         attempt_id: attempt_id,
         attempt_number: attempt_number,
         duration: duration,
         mapped_output: mapped_output
       }) do
    with {:ok, pause_target} <- WorkflowDefinition.transition_target(definition, step_name, :ok),
         {:ok, _step_run} <-
           StepRunStore.persist_pause_resume(
             config.repo,
             step_run_id,
             mapped_output,
             pause_target
           ) do
      apply_pause_progression(
        config,
        run,
        step_name,
        step_run_id,
        attempt_id,
        attempt_number,
        duration
      )
    end
  end

  defp handle_success_result(:approval, %{
         config: %Config{} = config,
         definition: definition,
         run: run,
         step_name: step_name,
         step_run_id: step_run_id,
         attempt_id: attempt_id,
         attempt_number: attempt_number,
         duration: duration
       }) do
    with {:ok, targets} <- WorkflowDefinition.approval_transition_targets(definition, step_name),
         {:ok, output_key} <- WorkflowDefinition.step_output_mapping(definition, step_name),
         {:ok, _step_run} <-
           StepRunStore.persist_approval_resume(config.repo, step_run_id, targets, output_key) do
      apply_pause_progression(
        config,
        run,
        step_name,
        step_run_id,
        attempt_id,
        attempt_number,
        duration
      )
    end
  end

  defp handle_success_result(nil, %{
         config: %Config{} = config,
         definition: definition,
         run: run,
         step_name: step_name,
         step_run_id: step_run_id,
         attempt_id: attempt_id,
         attempt_number: attempt_number,
         duration: duration,
         mapped_output: mapped_output,
         result: {:ok, _output, execution_opts}
       }) do
    with {:ok, _attempt} <- AttemptStore.complete_attempt(config.repo, attempt_id),
         {:ok, _step_run} <- StepRunStore.complete_step(config.repo, step_run_id, mapped_output) do
      with {:ok, events} <-
             advance_after_completed_step(
               config,
               definition,
               run,
               step_name,
               mapped_output,
               execution_opts
             ) do
        {:ok, [step_completed_event(run, step_name, attempt_number, duration) | events]}
      end
    end
  end

  defp apply_pause_progression(
         %Config{} = config,
         %Run{} = run,
         step_name,
         step_run_id,
         attempt_id,
         attempt_number,
         duration
       ) do
    case RunStore.pause_run(
           config.repo,
           run.id,
           step_run_id,
           attempt_id,
           %{
             current_step: step_name,
             last_error: nil
           }
         ) do
      {:ok, %{terminal_noop?: true, finalized_step?: true, error: error}} ->
        {:ok, [step_failed_event(run, step_name, attempt_number, duration, error)]}

      {:ok, %{terminal_noop?: true}} ->
        {:ok, []}

      {:ok,
       %{
         run: %Run{status: :cancelled} = cancelled_run,
         from_status: from_status,
         to_status: to_status
       }} ->
        {:ok,
         [
           step_failed_event(
             run,
             step_name,
             attempt_number,
             duration,
             RunStore.pause_cancellation_error()
           ),
           run_transition_event(cancelled_run, from_status, to_status)
         ]}

      {:ok, %{run: paused_run, from_status: from_status, to_status: to_status}} ->
        {:ok, [run_transition_event(paused_run, from_status, to_status)]}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc false
  @spec reconcile_completed_step(Config.t(), WorkflowDefinition.t(), Run.t(), PreparedStep.t()) ::
          :ok | {:error, execution_error() | term()}
  def reconcile_completed_step(
        %Config{} = config,
        definition,
        %Run{} = run,
        %PreparedStep{step_name: step_name, step_run: %{output: output}}
      ) do
    mapped_output = StepInput.normalize_map_keys(output || %{})

    with {:ok, events} <-
           advance_after_completed_step(config, definition, run, step_name, mapped_output, []) do
      emit_post_commit_events(events)
      :ok
    end
  end

  defp advance_after_completed_step(
         config,
         definition,
         run,
         step_name,
         mapped_output,
         execution_opts
       ) do
    case success_resolution(config.repo, definition, run, step_name) do
      {:ok, latest_run, target} ->
        progression =
          success_progression(
            config,
            definition,
            latest_run,
            step_name,
            target,
            mapped_output,
            execution_opts
          )

        apply_progression(config, latest_run.id, progression)

      :already_terminal ->
        {:ok, []}

      {:retrying, _latest_run} ->
        RunStore.progress_run_with_events(
          config.repo,
          run.id,
          fn current_run ->
            %{context: merged_context(current_run, mapped_output)}
          end,
          :update
        )
        |> progress_events_result()

      {:error, latest_run, reason} ->
        mark_failed_after_success_resolution_error(
          config.repo,
          run,
          step_name,
          merged_context(latest_run, mapped_output),
          reason
        )
    end
  end

  defp success_progression(
         _config,
         _definition,
         _run,
         _step_name,
         :complete,
         output,
         _execution_opts
       ) do
    Progression.complete(fn current_run ->
      %{
        context: merged_context(current_run, output),
        current_step: nil,
        last_error: nil
      }
    end)
  end

  defp success_progression(
         _config,
         definition,
         _run,
         _step_name,
         {:dispatch, next_steps},
         output,
         execution_opts
       )
       when is_list(next_steps) do
    dispatch_opts = Keyword.take(execution_opts, [:schedule_in])

    Progression.dispatch_steps(
      fn current_run -> success_attrs(definition, current_run, output, nil) end,
      next_steps,
      dispatch_opts,
      fn current_run, reason ->
        dispatch_error = %{
          message: "failed to dispatch workflow step",
          next_steps: next_steps,
          dispatch_reason: normalize_dispatch_cause(reason)
        }

        failed_dispatch_attrs(current_run, output, nil, dispatch_error)
      end
    )
  end

  defp success_progression(
         _config,
         definition,
         _run,
         _step_name,
         {:wait, _phase_steps},
         output,
         _execution_opts
       ) do
    Progression.update(fn current_run -> success_attrs(definition, current_run, output, nil) end)
  end

  defp success_progression(
         _config,
         definition,
         _run,
         _step_name,
         next_step,
         output,
         execution_opts
       )
       when is_atom(next_step) do
    dispatch_opts = Keyword.take(execution_opts, [:schedule_in])

    Progression.dispatch_run(
      fn current_run -> success_attrs(definition, current_run, output, next_step) end,
      dispatch_opts,
      fn current_run, reason ->
        dispatch_error = %{
          message: "failed to dispatch workflow step",
          next_step: next_step,
          cause: normalize_dispatch_cause(reason)
        }

        failed_dispatch_attrs(current_run, output, next_step, dispatch_error)
      end
    )
  end

  defp success_resolution(repo, definition, run, step_name) do
    with {:ok, latest_run} <- RunStore.get_run(repo, run.id) do
      cond do
        latest_run.status in [:failed, :completed, :cancelled] ->
          :already_terminal

        latest_run.status == :retrying and WorkflowDefinition.dependency_mode?(definition) ->
          {:retrying, latest_run}

        WorkflowDefinition.dependency_mode?(definition) ->
          resolve_dependency_success(repo, definition, latest_run)

        true ->
          success_transition_target(definition, latest_run, step_name)
      end
    end
  end

  defp success_transition_target(definition, latest_run, step_name) do
    case WorkflowDefinition.transition_target(definition, step_name, :ok) do
      {:ok, target} -> {:ok, latest_run, target}
      {:error, reason} -> {:error, latest_run, reason}
    end
  end

  defp apply_progression(%Config{} = config, run_id, %Complete{attrs_fun: attrs_fun}) do
    RunStore.progress_run_with_events(config.repo, run_id, attrs_fun, {:transition, :completed})
    |> progress_events_result()
  end

  defp apply_progression(%Config{} = config, run_id, %Update{attrs_fun: attrs_fun}) do
    RunStore.progress_run_with_events(config.repo, run_id, attrs_fun, :update)
    |> progress_events_result()
  end

  defp apply_progression(
         %Config{} = config,
         run_id,
         %DispatchSteps{
           attrs_fun: attrs_fun,
           steps: steps,
           dispatch_opts: dispatch_opts,
           dispatch_error_handler: failure_attrs_fun
         }
       ) do
    RunStore.progress_run_with_events(
      config.repo,
      run_id,
      attrs_fun,
      {:dispatch_or_fail,
       fn updated_run ->
         dispatch_steps_events(
           config,
           updated_run,
           steps,
           Keyword.put(dispatch_opts, :schedule_pending, true)
         )
       end, failure_attrs_fun}
    )
    |> progress_events_result()
  end

  defp apply_progression(
         %Config{} = config,
         run_id,
         %DispatchRun{
           attrs_fun: attrs_fun,
           dispatch_opts: dispatch_opts,
           dispatch_error_handler: failure_attrs_fun
         }
       ) do
    RunStore.progress_run_with_events(
      config.repo,
      run_id,
      attrs_fun,
      {:dispatch_or_fail,
       fn updated_run -> dispatch_run_events(config, updated_run, dispatch_opts) end,
       failure_attrs_fun}
    )
    |> progress_events_result()
  end

  defp handle_terminal_or_routed_failure(config, definition, run, step_name, step_run_id, error) do
    if WorkflowDefinition.dependency_mode?(definition) do
      Logger.error("workflow step failed")
      fail_run_and_request_compensation(config, definition, run, step_name, error)
    else
      case WorkflowDefinition.transition_target(definition, step_name, :error) do
        {:ok, target} ->
          Logger.warning("workflow step failed; routing to error transition")
          record_failure_and_advance(config, definition, run, step_name, step_run_id, target)

        {:error, {:unknown_transition, _from_step, :error}} ->
          Logger.error("workflow step failed")
          fail_run_and_request_compensation(config, definition, run, step_name, error)

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp record_failure_and_advance(config, definition, run, step_name, step_run_id, target) do
    case record_failure_recovery(config.repo, definition, step_name, step_run_id) do
      :ok -> advance_after_failure(config, run, target)
      {:error, reason} -> {:error, reason}
    end
  end

  defp record_failure_recovery(repo, definition, step_name, step_run_id) do
    case WorkflowDefinition.failure_recovery(definition, step_name) do
      {:ok, nil} ->
        :ok

      {:ok, failure_recovery} ->
        case StepRunStore.record_failure_recovery(repo, step_run_id, failure_recovery) do
          {:ok, _step_run} -> :ok
          {:error, reason} -> {:error, reason}
        end

      {:error, {:unknown_transition, _from_step, :error}} ->
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp advance_after_failure(config, run, :complete) do
    config
    |> apply_progression(
      run.id,
      Progression.complete(fn _run -> %{current_step: nil, last_error: nil} end)
    )
  end

  defp advance_after_failure(config, run, next_step) when is_atom(next_step) do
    attrs = %{current_step: next_step, last_error: nil}

    config
    |> apply_progression(
      run.id,
      Progression.dispatch_run(
        fn _current_run -> attrs end,
        [],
        fn _current_run, reason ->
          dispatch_error = %{
            message: "failed to dispatch workflow step",
            next_step: next_step,
            dispatch_reason: normalize_dispatch_cause(reason)
          }

          attrs
          |> Map.take([:context, :current_step])
          |> Map.put(:last_error, dispatch_error)
        end
      )
    )
  end

  defp fail_run_and_request_compensation(config, definition, run, step_name, error) do
    operation =
      if Compensation.compensation_available?(config.repo, definition, run.id) do
        {:transition_or_dispatch, :failed,
         fn failed_run -> dispatch_compensation_events(config, failed_run, definition) end}
      else
        {:transition, :failed}
      end

    RunStore.progress_run_with_events(
      config.repo,
      run.id,
      failure_attrs(step_name, error),
      operation
    )
    |> progress_events_result()
  end

  defp failure_attrs(step_name, error) do
    fn _current_run ->
      %{
        current_step: step_name,
        last_error: error
      }
    end
  end

  defp progress_events_result({:ok, _result, events}), do: {:ok, events}
  defp progress_events_result({:error, reason}), do: {:error, reason}

  defp emit_post_commit_events(events) do
    Enum.each(events, &emit_post_commit_event/1)
  end

  defp emit_post_commit_event({:step_completed, run, step_name, attempt_number, duration}) do
    Observability.emit_step_completed(run, step_name, attempt_number, duration)
  end

  defp emit_post_commit_event({:step_failed, run, step_name, attempt_number, duration, error}) do
    Observability.emit_step_failed(run, step_name, attempt_number, duration, error)
  end

  defp emit_post_commit_event({:step_retry_scheduled, run, step_name, attempt_number, delay_ms}) do
    Logger.warning("workflow step failed; scheduling retry")
    Observability.emit_step_retry_scheduled(run, step_name, attempt_number, delay_ms)
  end

  defp emit_post_commit_event({:run_transition, run, from_status, to_status}) do
    Observability.emit_run_transition(run, from_status, to_status)
  end

  defp emit_post_commit_event({:run_dispatched, run, metadata}) do
    Observability.emit_run_dispatched(run, metadata)
  end

  defp step_completed_event(run, step_name, attempt_number, duration) do
    {:step_completed, run, step_name, attempt_number, duration}
  end

  defp step_failed_event(run, step_name, attempt_number, duration, error) do
    {:step_failed, run, step_name, attempt_number, duration, error}
  end

  defp step_retry_scheduled_event(run, step_name, attempt_number, delay_ms) do
    {:step_retry_scheduled, run, step_name, attempt_number, delay_ms}
  end

  defp run_transition_event(run, from_status, to_status) do
    {:run_transition, run, from_status, to_status}
  end

  defp dispatch_run_events(config, run, opts) do
    with {:ok, _jobs, events} <- Dispatcher.dispatch_run_with_events(config, run, opts) do
      {:ok, {:dispatch_events, events}}
    end
  end

  defp dispatch_steps_events(config, run, steps, opts) do
    with {:ok, _jobs, events} <- Dispatcher.dispatch_steps_with_events(config, run, steps, opts) do
      {:ok, {:dispatch_events, events}}
    end
  end

  defp dispatch_compensation_events(config, run, _definition) do
    with {:ok, _job, events} <- Dispatcher.dispatch_compensation_with_events(config, run) do
      {:ok, {:dispatch_events, events}}
    end
  end

  defp normalize_error(%{__struct__: module} = error) do
    details =
      error
      |> Map.from_struct()
      |> Map.get(:details, %{})
      |> StepInput.normalize_map_keys()

    base_error = %{message: Exception.message(error)}

    case details do
      %{} = empty when map_size(empty) == 0 ->
        Map.put(base_error, :type, inspect(module))

      %{} = detail_map ->
        Map.merge(base_error, detail_map)
    end
  end

  defp normalize_error(%{} = error), do: error
  defp normalize_error(error), do: %{message: inspect(error)}

  defp mark_failed_after_success_resolution_error(
         repo,
         run,
         step_name,
         context,
         {:no_runnable_step, pending_steps}
       ) do
    case RunStore.transition_run_silent(repo, run.id, :failed, %{
           context: context,
           current_step: step_name,
           last_error: %{
             message: "workflow step completed but no runnable next step was found",
             failed_step: step_name,
             pending_steps: pending_steps
           }
         }) do
      {:ok, {failed_run, from_status, to_status}} ->
        {:ok, [run_transition_event(failed_run, from_status, to_status)]}

      {:error, transition_reason} ->
        {:error, transition_reason}
    end
  end

  defp mark_failed_after_success_resolution_error(repo, run, step_name, context, reason) do
    case RunStore.transition_run_silent(repo, run.id, :failed, %{
           context: context,
           current_step: step_name,
           last_error: %{
             message: "workflow step completed but next step resolution failed",
             failed_step: step_name,
             cause: normalize_success_resolution_error(reason)
           }
         }) do
      {:ok, {failed_run, from_status, to_status}} ->
        {:ok, [run_transition_event(failed_run, from_status, to_status)]}

      {:error, transition_reason} ->
        {:error, transition_reason}
    end
  end

  defp retry_dispatch_failure_attrs(step_name, step_error, reason) do
    %{
      current_step: step_name,
      last_error: %{
        message: "failed to dispatch workflow step",
        failed_step: step_name,
        cause: step_error,
        dispatch_reason: normalize_dispatch_cause(reason)
      }
    }
  end

  defp failed_dispatch_attrs(current_run, output, next_step, dispatch_error) do
    %{
      context: merged_context(current_run, output),
      current_step: next_step,
      last_error: dispatch_error
    }
  end

  defp normalize_success_resolution_error({:unknown_transition, from_step, outcome}) do
    %{from_step: from_step, outcome: outcome}
  end

  defp normalize_success_resolution_error({:invalid_dependency_graph, message}) do
    %{reason: :invalid_dependency_graph, message: message}
  end

  defp normalize_dispatch_cause({:dispatch_failed, reason}), do: normalize_dispatch_cause(reason)

  defp normalize_dispatch_cause(%{__struct__: _module} = error),
    do: %{message: Exception.message(error)}

  defp normalize_dispatch_cause(reason), do: reason

  defp retry_dispatch_opts(delay_ms) when is_integer(delay_ms) and delay_ms > 0 do
    [schedule_in: ceil(delay_ms / 1_000)]
  end

  defp retry_dispatch_opts(_delay_ms), do: []

  defp retry_delay_ms(%{retry_after: retry_after}, _policy_delay_ms)
       when is_integer(retry_after) and retry_after >= 0 do
    retry_after
  end

  defp retry_delay_ms(_error, policy_delay_ms), do: policy_delay_ms

  defp pause_kind(%{module: :pause}, execution_opts) when is_list(execution_opts) do
    if Keyword.get(execution_opts, :pause, false), do: :pause, else: nil
  end

  defp pause_kind(%{module: :approval}, execution_opts) when is_list(execution_opts) do
    if Keyword.get(execution_opts, :pause, false), do: :approval, else: nil
  end

  defp pause_kind(_step, _execution_opts), do: nil

  defp resolve_dependency_success(repo, definition, latest_run) do
    step_statuses = StepRunStore.step_statuses(repo, latest_run.id)

    try do
      case WorkflowDefinition.dependency_progress(definition, step_statuses) do
        :complete -> {:ok, latest_run, :complete}
        {:dispatch, steps} -> {:ok, latest_run, {:dispatch, steps}}
        {:wait, phase_steps} -> {:ok, latest_run, {:wait, phase_steps}}
        {:error, reason} -> {:error, latest_run, reason}
      end
    rescue
      exception in ArgumentError ->
        if Exception.message(exception) == "workflow dependency graph must be acyclic" do
          {:error, latest_run, {:invalid_dependency_graph, Exception.message(exception)}}
        else
          reraise exception, __STACKTRACE__
        end
    end
  end

  defp success_attrs(definition, run, output, next_step) do
    %{}
    |> Map.put(:context, merged_context(run, output))
    |> Map.put(:current_step, success_current_step(definition, next_step))
    |> Map.put(:last_error, nil)
  end

  defp success_current_step(definition, next_step) do
    if WorkflowDefinition.dependency_mode?(definition), do: nil, else: next_step
  end

  defp merged_context(run, output) do
    base_context = run.context || %{}

    base_context
    |> Map.merge(output)
    |> preserve_reserved_context(base_context)
  end

  defp preserve_reserved_context(context, base_context) do
    Enum.reduce(@reserved_context_keys, context, fn key, acc ->
      case context_value(base_context, key) do
        nil -> acc
        value -> put_reserved_context(acc, key, value)
      end
    end)
  end

  defp put_reserved_context(context, key, value) do
    context
    |> Map.delete(key)
    |> Map.delete(Atom.to_string(key))
    |> Map.put(key, value)
  end

  defp context_value(context, key) do
    case Map.fetch(context, key) do
      {:ok, value} -> value
      :error -> Map.get(context, Atom.to_string(key))
    end
  end
end
