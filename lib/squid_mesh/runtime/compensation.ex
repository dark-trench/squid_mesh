defmodule SquidMesh.Runtime.Compensation do
  @moduledoc """
  Executes durable saga compensation for completed workflow steps.

  Compensation is intentionally separate from normal failure routing. Forward
  retries and `:error` transitions decide whether the workflow can continue.
  Only after a run reaches terminal failure does the runtime dispatch a
  compensation job, which walks completed reversible steps in reverse completion
  order and records each callback result in the original step's recovery
  metadata.
  """

  alias Jido.Instruction
  alias SquidMesh.Config
  alias SquidMesh.Run
  alias SquidMesh.Runtime.StepInput
  alias SquidMesh.StepRunStore
  alias SquidMesh.Workflow.Definition, as: WorkflowDefinition

  @type compensation_error :: {:compensation_failed, [map()]}

  @doc """
  Returns whether a failed run has any completed step with compensation pending.

  The runtime uses this before enqueuing a compensation job so workflows without
  reversible completed work keep their historical worker-count and dispatch
  behavior.
  """
  @spec compensation_available?(module(), WorkflowDefinition.t(), Ecto.UUID.t()) :: boolean()
  def compensation_available?(repo, definition, run_id) do
    repo
    |> StepRunStore.completed_step_runs_for_compensation(run_id)
    |> Enum.any?(fn step_run ->
      step_name = WorkflowDefinition.deserialize_step(definition, step_run.step)

      is_atom(step_name) and ensure_not_completed(step_run.recovery) == :ok and
        match?(
          {:ok, callback} when is_atom(callback) and not is_nil(callback),
          WorkflowDefinition.step_compensation_callback(definition, step_name)
        )
    end)
  end

  @doc """
  Runs all pending compensation callbacks for a failed run.

  Completed steps are evaluated in reverse completion order. A successful
  callback stores `:completed` plus its output under `recovery.compensation`;
  a failed callback stores `:failed` plus the normalized error and returns a
  structured `{:compensation_failed, failures}` error after all eligible
  callbacks have been attempted.
  """
  @spec compensate_completed_steps(Config.t(), WorkflowDefinition.t(), Run.t(), map()) ::
          :ok | {:error, compensation_error() | term()}
  def compensate_completed_steps(%Config{} = config, definition, %Run{} = run, failure)
      when is_map(failure) do
    config.repo
    |> StepRunStore.completed_step_runs_for_compensation(run.id)
    |> Enum.reduce_while({:ok, []}, fn step_run, {:ok, failures} ->
      case compensate_step(config, definition, run, step_run, failure) do
        :ok -> {:cont, {:ok, failures}}
        {:error, reason} -> {:cont, {:ok, [reason | failures]}}
      end
    end)
    |> case do
      {:ok, []} -> :ok
      {:ok, failures} -> {:error, {:compensation_failed, Enum.reverse(failures)}}
    end
  end

  defp compensate_step(config, definition, run, step_run, failure) do
    step_name = WorkflowDefinition.deserialize_step(definition, step_run.step)

    with step when is_atom(step) <- step_name,
         {:ok, callback} when is_atom(callback) and not is_nil(callback) <-
           WorkflowDefinition.step_compensation_callback(definition, step),
         :ok <- ensure_not_completed(step_run.recovery),
         {:ok, recovery} <- mark_compensation_running(config, step_run, callback),
         result <- execute_callback(callback, run, step, step_run, failure),
         :ok <- persist_compensation_result(config, step_run, recovery, result) do
      result_status(result)
    else
      {:ok, nil} -> :ok
      :already_completed -> :ok
      {:error, _reason} = error -> error
      other -> {:error, %{step: step_name, reason: inspect(other)}}
    end
  end

  defp ensure_not_completed(%{"compensation" => %{"status" => "completed"}}),
    do: :already_completed

  defp ensure_not_completed(%{compensation: %{status: :completed}}), do: :already_completed
  defp ensure_not_completed(_recovery), do: :ok

  defp mark_compensation_running(%Config{} = config, step_run, callback) do
    recovery =
      step_run.recovery
      |> Kernel.||(%{})
      |> StepInput.normalize_map_keys()
      |> Map.put(:compensation, %{
        callback: callback,
        status: :running,
        started_at: DateTime.utc_now() |> DateTime.to_iso8601()
      })

    with {:ok, _step_run} <- StepRunStore.update_recovery(config.repo, step_run.id, recovery) do
      {:ok, recovery}
    end
  end

  defp execute_callback(callback, %Run{} = run, step_name, step_run, failure) do
    input = %{
      payload: run.payload || %{},
      context: run.context || %{},
      step: %{
        name: step_name,
        input: StepInput.normalize_map_keys(step_run.input || %{}),
        output: StepInput.normalize_map_keys(step_run.output || %{})
      },
      failure: failure
    }

    context = %{
      run_id: run.id,
      workflow: run.workflow,
      step: step_name,
      compensation: true,
      state: run.context || %{}
    }

    {callback, input} = callback_input(callback, input)
    run_callback_instruction(callback, input, context)
  end

  defp run_callback_instruction(callback, input, context) do
    case Instruction.new(
           action: callback,
           params: input,
           context: context,
           opts: [max_retries: 0]
         ) do
      {:ok, instruction} ->
        instruction
        |> Jido.Exec.run()
        |> normalize_callback_result()

      {:error, reason} ->
        {:error, normalize_error(reason)}
    end
  end

  defp normalize_callback_result({:ok, output}) when is_map(output), do: {:ok, output}
  defp normalize_callback_result({:ok, output, _extras}) when is_map(output), do: {:ok, output}
  defp normalize_callback_result({:error, reason}), do: {:error, normalize_error(reason)}

  defp callback_input(callback, input) do
    if SquidMesh.Step.native_step?(callback) do
      {SquidMesh.Step.Action, %{step: callback, input: input}}
    else
      {callback, input}
    end
  end

  defp persist_compensation_result(config, step_run, recovery, {:ok, output}) do
    compensation =
      recovery.compensation
      |> Map.put(:status, :completed)
      |> Map.put(:output, output)
      |> Map.put(:completed_at, DateTime.utc_now() |> DateTime.to_iso8601())

    recovery = Map.put(recovery, :compensation, compensation)

    with {:ok, _step_run} <- StepRunStore.update_recovery(config.repo, step_run.id, recovery) do
      :ok
    end
  end

  defp persist_compensation_result(config, step_run, recovery, {:error, error}) do
    compensation =
      recovery.compensation
      |> Map.put(:status, :failed)
      |> Map.put(:error, error)
      |> Map.put(:failed_at, DateTime.utc_now() |> DateTime.to_iso8601())

    recovery = Map.put(recovery, :compensation, compensation)

    with {:ok, _step_run} <- StepRunStore.update_recovery(config.repo, step_run.id, recovery) do
      :ok
    end
  end

  defp result_status({:ok, _output}), do: :ok
  defp result_status({:error, error}), do: {:error, error}

  defp normalize_error(%{__struct__: module} = error) do
    details =
      error
      |> Map.from_struct()
      |> Map.get(:details, %{})
      |> StepInput.normalize_map_keys()

    base_error = %{message: Exception.message(error)}

    case details do
      %{} = empty when map_size(empty) == 0 -> Map.put(base_error, :type, inspect(module))
      %{} = detail_map -> Map.merge(base_error, detail_map)
    end
  end

  defp normalize_error(error), do: %{message: inspect(error)}
end
