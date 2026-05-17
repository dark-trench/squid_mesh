defmodule SquidMesh.Runtime.StepExecutor do
  @moduledoc """
  Executes one workflow step through Jido and persists the outcome.

  This module is the runtime boundary where declarative workflow definitions are
  turned into durable step execution and persisted run progress.
  """

  alias SquidMesh.AttemptStore
  alias SquidMesh.Config
  alias SquidMesh.Observability
  alias SquidMesh.Run
  alias SquidMesh.RunStore
  alias SquidMesh.Runtime.Compensation
  alias SquidMesh.Runtime.StepExecutor.Execution
  alias SquidMesh.Runtime.StepExecutor.Outcome
  alias SquidMesh.Runtime.StepExecutor.Preparation
  alias SquidMesh.Runtime.StepInput
  alias SquidMesh.Workflow.Definition, as: WorkflowDefinition

  @type execution_error ::
          :not_found
          | {:invalid_workflow, module() | String.t()}
          | {:invalid_step, atom() | String.t() | nil}
          | {:dispatch_failed, term()}
          | {:invalid_run, Ecto.Changeset.t()}
          | {:invalid_transition, Run.status(), Run.status()}
          | {:invalid_compensation_run_status, Run.status()}
          | {:unknown_transition, atom(), atom()}
          | {:unknown_step, atom()}
          | {:missing_config, [atom()]}

  @type expected_step :: atom() | String.t() | nil

  @doc false
  @spec execute(Ecto.UUID.t(), expected_step(), keyword()) ::
          :ok | {:error, execution_error() | term()}
  def execute(run_id, expected_step \\ nil, overrides \\ []) when is_binary(run_id) do
    with {:ok, config} <- Config.load(overrides),
         {:ok, run} <- RunStore.get_run(config.repo, run_id) do
      execute_run(config, run, expected_step)
    end
  end

  @doc """
  Executes pending compensation for a failed run.

  Compensation jobs are separate from step execution jobs so the failed run and
  failed step attempt are durable before rollback side effects start. If a
  compensation callback fails, the run remains failed and its `last_error` is
  updated with the compensation failure details for inspection.
  """
  @spec compensate(Ecto.UUID.t(), keyword()) :: :ok | {:error, execution_error() | term()}
  def compensate(run_id, overrides \\ []) when is_binary(run_id) do
    with {:ok, config} <- Config.load(overrides),
         {:ok, %Run{} = run} <- RunStore.get_run(config.repo, run_id),
         :ok <- ensure_failed_compensation_run(run),
         {:ok, definition} <- WorkflowDefinition.load(run.workflow) do
      config
      |> Compensation.compensate_completed_steps(definition, run, run.last_error || %{})
      |> persist_compensation_failure(config, run)
    end
  end

  defp persist_compensation_failure(:ok, _config, _run), do: :ok

  defp persist_compensation_failure({:error, {:compensation_failed, failures}}, config, run) do
    compensation_error = %{
      message: "workflow step failed and compensation failed",
      failed_step: run.current_step,
      cause: run.last_error,
      compensation_failures: failures
    }

    config.repo
    |> RunStore.update_run(run.id, %{
      current_step: run.current_step,
      last_error: compensation_error
    })
    |> case do
      {:ok, _run} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp ensure_failed_compensation_run(%Run{status: :failed}), do: :ok

  defp ensure_failed_compensation_run(%Run{status: status}),
    do: {:error, {:invalid_compensation_run_status, status}}

  @spec execute_run(Config.t(), Run.t(), atom() | nil) ::
          :ok | {:error, execution_error() | term()}
  defp execute_run(_config, %Run{status: status}, _expected_step)
       when status in [:completed, :failed, :cancelled, :paused] do
    :ok
  end

  defp execute_run(config, %Run{status: :cancelling} = run, _expected_step) do
    case RunStore.transition_run(config.repo, run.id, :cancelled, %{
           current_step: nil
         }) do
      {:ok, _cancelled_run} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp execute_run(config, %Run{workflow: workflow} = run, expected_step)
       when is_atom(workflow) do
    with {:ok, definition} <- WorkflowDefinition.load(workflow) do
      case StepInput.deserialize_expected_step(expected_step, definition) do
        {:ok, normalized_expected_step} ->
          prepare_and_execute(config, definition, run, normalized_expected_step)

        {:error, _reason} = error ->
          error
      end
    end
  end

  defp execute_run(_config, %Run{current_step: current_step}, _expected_step) do
    {:error, {:invalid_step, current_step}}
  end

  defp prepare_and_execute(config, definition, run, expected_step) do
    case Preparation.prepare(config, definition, run, expected_step) do
      {:execute, prepared} ->
        execute_prepared_step(prepared)

      {:reconcile, prepared} ->
        Outcome.reconcile_completed_step(
          prepared.config,
          prepared.definition,
          prepared.run,
          prepared
        )

      {:cancel, locked_run} ->
        converge_cancellation(config, locked_run)

      {:skip, prepared} ->
        Observability.emit_step_skipped(
          prepared.run,
          prepared.step_name,
          "already_#{prepared.step_run.status}"
        )

        :ok

      :skip ->
        :ok

      {:error, _reason} = error ->
        error
    end
  end

  defp converge_cancellation(config, %Run{status: :cancelling} = run) do
    case RunStore.transition_run(config.repo, run.id, :cancelled, %{current_step: nil}) do
      {:ok, _cancelled_run} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp converge_cancellation(_config, %Run{}), do: :ok

  defp execute_prepared_step(prepared) do
    with {:ok, attempt} <- AttemptStore.begin_attempt(prepared.config.repo, prepared.step_run.id) do
      attempt_number = attempt.attempt_number

      Observability.with_step_metadata(prepared.run, prepared.step_name, attempt_number, fn ->
        Observability.emit_step_started(prepared.run, prepared.step_name, attempt_number)

        started_at = System.monotonic_time()

        prepared
        |> Execution.execute(attempt_number)
        |> Outcome.apply_execution_result(%{
          config: prepared.config,
          definition: prepared.definition,
          run: prepared.run,
          step_name: prepared.step_name,
          step_run_id: prepared.step_run.id,
          attempt_id: attempt.id,
          attempt_number: attempt_number,
          started_at: started_at
        })
      end)
    end
  end
end
