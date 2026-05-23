defmodule SquidMesh.Runtime.Dispatcher do
  @moduledoc """
  Enqueues durable workflow step execution.

  The workflow contract stays declarative while this module bridges runtime
  intent into the host application's configured executor.
  """

  alias SquidMesh.Config
  alias SquidMesh.Observability
  alias SquidMesh.Run
  alias SquidMesh.Runtime.StepInput
  alias SquidMesh.Steps

  @type dispatch_error :: Ecto.Changeset.t() | term()
  @type dispatch_opts :: [schedule_in: pos_integer()]
  @type dispatch_target :: atom()
  @type dispatch_metadata :: SquidMesh.Executor.metadata()
  @type dispatch_event :: {:run_dispatched, Run.t(), dispatch_metadata()}

  @doc false
  @spec dispatch_run(Config.t(), Run.t(), dispatch_opts()) ::
          {:ok, dispatch_metadata() | [dispatch_metadata()]} | {:error, dispatch_error()}
  def dispatch_run(config, run, opts \\ [])

  def dispatch_run(%Config{} = config, %Run{} = run, opts) do
    case dispatch_run_with_events(config, run, opts) do
      {:ok, jobs, events} ->
        emit_dispatch_events(events)
        {:ok, jobs}

      {:error, _reason} = error ->
        error
    end
  end

  @doc false
  @spec dispatch_run_with_events(Config.t(), Run.t(), dispatch_opts()) ::
          {:ok, dispatch_metadata() | [dispatch_metadata()], [dispatch_event()]}
          | {:error, dispatch_error()}
  def dispatch_run_with_events(config, run, opts \\ [])

  def dispatch_run_with_events(
        %Config{} = config,
        %Run{workflow: workflow, current_step: nil} = run,
        opts
      )
      when is_atom(workflow) do
    with {:ok, definition} <- SquidMesh.Workflow.Definition.load(workflow),
         true <-
           SquidMesh.Workflow.Definition.dependency_mode?(definition) ||
             {:error, {:invalid_step, nil}} do
      dispatch_steps_with_events(
        config,
        run,
        SquidMesh.Workflow.Definition.entry_steps(definition),
        Keyword.put(opts, :schedule_pending, true)
      )
    end
  end

  def dispatch_run_with_events(%Config{} = config, %Run{current_step: current_step} = run, opts)
      when is_atom(current_step) do
    dispatch_steps_with_events(config, run, [current_step], opts)
  end

  def dispatch_run_with_events(%Config{}, %Run{current_step: current_step}, _opts) do
    {:error, {:invalid_step, current_step}}
  end

  @doc false
  @spec dispatch_steps(Config.t(), Run.t(), [dispatch_target()], keyword()) ::
          {:ok, dispatch_metadata() | [dispatch_metadata()]} | {:error, dispatch_error()}
  def dispatch_steps(%Config{} = config, %Run{} = run, steps, opts \\ []) when is_list(steps) do
    case dispatch_steps_with_events(config, run, steps, opts) do
      {:ok, jobs, events} ->
        emit_dispatch_events(events)
        {:ok, jobs}

      {:error, _reason} = error ->
        error
    end
  end

  @doc false
  @spec dispatch_steps_with_events(Config.t(), Run.t(), [dispatch_target()], keyword()) ::
          {:ok, dispatch_metadata() | [dispatch_metadata()], [dispatch_event()]}
          | {:error, dispatch_error()}
  def dispatch_steps_with_events(%Config{} = config, %Run{} = run, steps, opts \\ [])
      when is_list(steps) do
    schedule_in = Keyword.get(opts, :schedule_in)
    schedule_pending? = Keyword.get(opts, :schedule_pending, false)

    with {:ok, jobs} <-
           dispatch_steps_transaction(
             config,
             run,
             steps,
             dispatch_opts(schedule_in),
             schedule_pending?
           ) do
      events = Enum.map(jobs, &run_dispatched_event(run, &1, schedule_in))

      case jobs do
        [job] -> {:ok, job, events}
        multiple_jobs -> {:ok, multiple_jobs, events}
      end
    end
  end

  @doc """
  Enqueues the durable compensation worker for a failed run.

  This is called from the same transaction that marks the run failed, giving the
  terminal run transition and compensation dispatch the same atomicity as normal
  successor-step dispatch.
  """
  @spec dispatch_compensation_with_events(Config.t(), Run.t()) ::
          {:ok, dispatch_metadata(), [dispatch_event()]} | {:error, dispatch_error()}
  def dispatch_compensation_with_events(%Config{} = config, %Run{} = run) do
    with {:ok, metadata} <- config.executor.enqueue_compensation(config, run, []) do
      {:ok, metadata, [run_dispatched_event(run, metadata, nil)]}
    end
  end

  defp emit_dispatch_events(events) do
    Enum.each(events, fn {:run_dispatched, run, metadata} ->
      Observability.emit_run_dispatched(run, metadata)
    end)
  end

  defp run_dispatched_event(run, metadata, schedule_in) do
    {:run_dispatched, run, dispatch_metadata(metadata, schedule_in)}
  end

  defp dispatch_steps_transaction(config, run, steps, job_opts, schedule_pending?) do
    dispatch_steps_transaction_mode(
      config.repo.in_transaction?(),
      config,
      run,
      steps,
      job_opts,
      schedule_pending?
    )
  end

  defp dispatch_steps_transaction_mode(true, config, run, steps, job_opts, schedule_pending?) do
    dispatch_steps_without_transaction(config, run, steps, job_opts, schedule_pending?)
  end

  defp dispatch_steps_transaction_mode(false, config, run, steps, job_opts, schedule_pending?) do
    # Pending step rows and executor enqueue are one dispatch unit when the
    # host executor uses the same repo transaction.
    case config.repo.transaction(fn ->
           do_dispatch_steps_transaction(config, run, steps, job_opts, schedule_pending?)
         end) do
      {:ok, jobs} -> {:ok, jobs}
      {:error, reason} -> {:error, reason}
    end
  end

  defp do_dispatch_steps_transaction(config, run, steps, job_opts, schedule_pending?) do
    config
    |> dispatch_steps_without_transaction(run, steps, job_opts, schedule_pending?)
    |> rollback_dispatch_error(config)
  end

  defp rollback_dispatch_error({:ok, jobs}, _config), do: jobs
  defp rollback_dispatch_error({:error, reason}, config), do: config.repo.rollback(reason)

  defp dispatch_steps_without_transaction(config, run, steps, job_opts, schedule_pending?) do
    with {:ok, steps_to_dispatch} <- steps_to_dispatch(config, run, steps, schedule_pending?) do
      case insert_step_jobs(config, run, steps_to_dispatch, job_opts) do
        {:ok, jobs} ->
          {:ok, jobs}

        {:error, _reason} = error ->
          cleanup_scheduled_steps(config, run, steps_to_dispatch, schedule_pending?)
          error
      end
    end
  end

  defp cleanup_scheduled_steps(config, run, steps, true) do
    Steps.Store.delete_pending_steps(config.repo, run.id, steps)
  end

  defp cleanup_scheduled_steps(_config, _run, _steps, false), do: :ok

  defp steps_to_dispatch(config, run, steps, true) do
    step_inputs_result =
      steps
      |> Enum.uniq()
      |> build_scheduled_step_inputs(config, run)

    case step_inputs_result do
      {:ok, step_inputs} -> Steps.Store.schedule_steps(config.repo, run.id, step_inputs)
      {:error, _reason} = error -> error
    end
  end

  defp steps_to_dispatch(_config, _run, steps, false), do: {:ok, Enum.uniq(steps)}

  defp build_scheduled_step_inputs(steps, config, run) do
    result =
      Enum.reduce_while(steps, {:ok, []}, fn step, {:ok, step_inputs} ->
        case scheduled_step_input(config, run, step) do
          {:ok, input, recovery} -> {:cont, {:ok, [{step, input, recovery} | step_inputs]}}
          {:error, _reason} = error -> {:halt, error}
        end
      end)

    case result do
      {:ok, step_inputs} -> {:ok, Enum.reverse(step_inputs)}
      {:error, _reason} = error -> error
    end
  end

  defp insert_step_jobs(_config, _run, [], _job_opts), do: {:ok, []}

  defp insert_step_jobs(config, %Run{} = run, steps, job_opts) do
    case steps do
      [step] -> enqueue_single_step(config, run, step, job_opts)
      multiple_steps -> config.executor.enqueue_steps(config, run, multiple_steps, job_opts)
    end
  rescue
    exception -> {:error, exception}
  end

  defp enqueue_single_step(config, run, step, opts) do
    with {:ok, metadata} <- config.executor.enqueue_step(config, run, step, opts) do
      {:ok, [metadata]}
    end
  end

  defp dispatch_opts(schedule_in)
       when is_integer(schedule_in) and schedule_in > 0 do
    [schedule_in: schedule_in]
  end

  defp dispatch_opts(_schedule_in), do: []

  defp dispatch_metadata(metadata, schedule_in) when is_map(metadata) do
    Map.put_new(metadata, :schedule_in, schedule_in)
  end

  defp dispatch_metadata(metadata, schedule_in) do
    %{result: metadata, schedule_in: schedule_in}
  end

  defp scheduled_step_input(%Config{repo: repo}, %Run{workflow: workflow} = run, step_name)
       when is_atom(workflow) do
    with {:ok, definition} <- SquidMesh.Workflow.Definition.load(workflow),
         {:ok, input_mapping} <-
           SquidMesh.Workflow.Definition.step_input_mapping(definition, step_name),
         {:ok, recovery} <-
           SquidMesh.Workflow.Definition.step_recovery_policy(definition, step_name) do
      with {:ok, input} <- build_scheduled_step_input(definition, repo, run, input_mapping) do
        {:ok, input, recovery}
      end
    end
  end

  defp scheduled_step_input(_config, %Run{} = run, _step_name),
    do: with({:ok, input} <- StepInput.resolve_step_input(run), do: {:ok, input, nil})

  defp build_scheduled_step_input(definition, repo, run, input_mapping) do
    if SquidMesh.Workflow.Definition.dependency_mode?(definition) do
      StepInput.resolve_dependency_step_input(repo, run, input_mapping)
    else
      StepInput.resolve_step_input(run, input_mapping)
    end
  end
end
