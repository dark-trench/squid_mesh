defmodule SquidMesh.Runtime.Runner do
  @moduledoc """
  Backend-neutral runtime entrypoints for host executors.

  Executor jobs should call these functions when queued work is delivered.
  """

  require Logger

  alias SquidMesh.Observability
  alias SquidMesh.Runtime.ScheduleMetadata
  alias SquidMesh.Runtime.StepExecutor
  alias SquidMesh.Workflow.Definition, as: WorkflowDefinition

  @doc """
  Executes one queued executor payload.

  Host job backends should store payloads produced by
  `SquidMesh.Executor.Payload` and pass the payload back here when the job is
  delivered. The runner accepts step, compensation, and cron activation payloads.

  Cron payloads create a new run. Any scheduler metadata carried by
  `SquidMesh.Executor.Payload.cron/3` is persisted into the run context before
  the first workflow step is dispatched.
  """
  @spec perform(map(), keyword()) :: :ok | {:error, term()}
  def perform(args, overrides \\ [])

  def perform(%{"kind" => "step", "run_id" => run_id, "step" => step}, overrides)
      when is_binary(run_id) and is_binary(step) do
    execute_step(run_id, step, overrides)
  end

  def perform(%{"kind" => "compensation", "run_id" => run_id}, overrides)
      when is_binary(run_id) do
    execute_compensation(run_id, overrides)
  end

  def perform(%{"kind" => "cron", "workflow" => workflow, "trigger" => trigger} = args, overrides)
      when is_binary(workflow) and is_binary(trigger) do
    case start_cron_trigger(workflow, trigger, args, overrides) do
      {:ok, {:duplicate_schedule_start, _run_id}} -> :ok
      {:ok, {:skipped_schedule_start, _run_id}} -> :ok
      result -> result
    end
  end

  def perform(args, _overrides) do
    {:error, {:invalid_executor_payload, args}}
  end

  @doc """
  Executes a queued step payload by run id and step name.

  The step name may be the serialized string stored in a job payload. The step
  executor reloads the durable run, validates that the step is still runnable,
  and applies the normal retry, transition, and dispatch rules.
  """
  @spec execute_step(Ecto.UUID.t(), atom() | String.t(), keyword()) :: :ok | {:error, term()}
  def execute_step(run_id, step, overrides \\ []) when is_binary(run_id) do
    Observability.with_run_metadata(run_stub(run_id, step), fn ->
      try do
        case StepExecutor.execute(run_id, step, overrides) do
          :ok ->
            :ok

          {:error, reason} = error ->
            Logger.error("step execution failed: #{inspect(reason)}")
            error
        end
      rescue
        exception ->
          Logger.error("""
          unexpected step execution exception: #{Exception.format(:error, exception, __STACKTRACE__)}
          """)

          {:error, {:exception, Exception.message(exception)}}
      end
    end)
  end

  @doc """
  Executes queued compensation for a failed run.

  Compensation uses persisted run and step history to determine which reversible
  steps need compensation work. Delivery errors are returned as structured
  `{:error, reason}` tuples so job backends can apply their own retry policy.
  """
  @spec execute_compensation(Ecto.UUID.t(), keyword()) :: :ok | {:error, term()}
  def execute_compensation(run_id, overrides \\ []) when is_binary(run_id) do
    Observability.with_run_metadata(run_stub(run_id, nil), fn ->
      try do
        case StepExecutor.compensate(run_id, overrides) do
          :ok ->
            :ok

          {:error, reason} = error ->
            Logger.error("compensation execution failed: #{inspect(reason)}")
            error
        end
      rescue
        exception ->
          Logger.error("""
          unexpected compensation exception: #{Exception.format(:error, exception, __STACKTRACE__)}
          """)

          {:error, {:exception, Exception.message(exception)}}
      end
    end)
  end

  @doc """
  Starts a workflow run from a serialized cron trigger.

  This arity is useful for host schedulers that only know the workflow and
  trigger names. It records schedule metadata, including the actual receive
  timestamp. A signal id is recorded only when the scheduler supplies one or an
  intended window is available for deterministic derivation.
  """
  @spec start_cron_trigger(String.t(), String.t(), keyword()) ::
          :ok
          | {:ok, {:duplicate_schedule_start, Ecto.UUID.t()}}
          | {:ok, {:skipped_schedule_start, Ecto.UUID.t()}}
          | {:error, term()}
  def start_cron_trigger(workflow_name, trigger_name, overrides \\ [])
      when is_binary(workflow_name) and is_binary(trigger_name) do
    start_cron_trigger(workflow_name, trigger_name, %{}, overrides)
  end

  @doc """
  Starts a workflow run from a serialized cron trigger and scheduler payload.

  `signal_payload` is the scheduler metadata subset from a cron executor
  payload. When it contains `"signal_id"` and `"intended_window"`, the runtime
  stores those values under `run.context.schedule` before dispatching the first
  step, making delayed delivery and restart recovery observable to workflow
  steps and operators.
  """
  @spec start_cron_trigger(String.t(), String.t(), map(), keyword()) ::
          :ok
          | {:ok, {:duplicate_schedule_start, Ecto.UUID.t()}}
          | {:ok, {:skipped_schedule_start, Ecto.UUID.t()}}
          | {:error, term()}
  def start_cron_trigger(workflow_name, trigger_name, signal_payload, overrides)
      when is_binary(workflow_name) and is_binary(trigger_name) and is_map(signal_payload) and
             is_list(overrides) do
    with {:ok, workflow, definition} <- WorkflowDefinition.load_serialized(workflow_name),
         trigger when is_atom(trigger) <-
           WorkflowDefinition.deserialize_trigger(definition, trigger_name),
         {:ok, trigger_definition} <- WorkflowDefinition.trigger(definition, trigger),
         {:ok, schedule_context} <-
           ScheduleMetadata.cron_context(workflow, trigger_definition, signal_payload),
         {:ok, run_result} <-
           start_cron_run(
             workflow,
             trigger,
             schedule_context,
             overrides
           ) do
      cron_start_result(run_result)
    else
      {:error, reason} ->
        {:error, reason}

      invalid_trigger ->
        {:error, {:invalid_trigger, invalid_trigger}}
    end
  end

  defp run_stub(run_id, step) do
    %SquidMesh.Run{id: run_id, workflow: nil, trigger: nil, status: nil, current_step: step}
  end

  defp start_cron_run(workflow, trigger, schedule_context, overrides) do
    SquidMesh.start_run_with_initial_context(
      workflow,
      trigger,
      %{},
      schedule_context,
      overrides
    )
  end

  defp cron_start_result(
         {:duplicate_schedule_start, %SquidMesh.Run{id: run_id, context: context}}
       ) do
    case schedule_idempotency(context) do
      :skip_duplicate -> {:ok, {:skipped_schedule_start, run_id}}
      _other -> {:ok, {:duplicate_schedule_start, run_id}}
    end
  end

  defp cron_start_result(%SquidMesh.Run{}), do: :ok

  defp schedule_idempotency(context) when is_map(context) do
    context
    |> schedule_context()
    |> schedule_value(:idempotency)
    |> case do
      "skip_duplicate" -> :skip_duplicate
      :skip_duplicate -> :skip_duplicate
      strategy -> strategy
    end
  end

  defp schedule_idempotency(_context), do: nil

  defp schedule_context(context) do
    case Map.fetch(context, :schedule) do
      {:ok, schedule} -> schedule
      :error -> Map.get(context, "schedule", %{})
    end
  end

  defp schedule_value(schedule, key) when is_map(schedule) do
    case Map.fetch(schedule, key) do
      {:ok, value} -> value
      :error -> Map.get(schedule, Atom.to_string(key))
    end
  end

  defp schedule_value(_schedule, _key), do: nil
end
