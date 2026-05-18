defmodule SquidMesh.Runtime.StepRecovery do
  @moduledoc """
  Recovery helpers for step claims left running by interrupted workers.
  """

  import Ecto.Query

  alias SquidMesh.AttemptStore
  alias SquidMesh.Persistence.StepAttempt
  alias SquidMesh.Persistence.StepRun
  alias SquidMesh.StepRunStore

  @type reclaim_status :: :reclaimed | :fresh | :not_running
  @type reclaim_result :: {:ok, reclaim_status()} | {:error, term()}

  @doc false
  @spec reclaim_stale_running_step(module(), StepRun.t(), non_neg_integer()) :: reclaim_result()
  def reclaim_stale_running_step(repo, %StepRun{id: step_run_id}, timeout_ms)
      when is_integer(timeout_ms) and timeout_ms >= 0 do
    repo.transaction(fn ->
      # Match normal terminal persistence order: attempt first, then step.
      # Otherwise redelivery recovery can deadlock with a finishing worker.
      latest_attempt = locked_latest_attempt(repo, step_run_id)

      case locked_step_run(repo, step_run_id) do
        %StepRun{status: "running"} = step_run ->
          reclaim_locked_running_step(repo, step_run, latest_attempt, timeout_ms)

        %StepRun{} ->
          :not_running

        nil ->
          repo.rollback(:not_found)
      end
    end)
  end

  defp reclaim_locked_running_step(repo, step_run, latest_attempt, timeout_ms) do
    if stale?(step_run, timeout_ms) do
      reclaim_stale_step(repo, step_run, latest_attempt, stale_step_error(timeout_ms))
    else
      :fresh
    end
  end

  defp reclaim_stale_step(repo, step_run, latest_attempt, error) do
    with :ok <- fail_latest_running_attempt(repo, latest_attempt, error),
         {:ok, _step_run} <- StepRunStore.fail_step(repo, step_run.id, error) do
      :reclaimed
    else
      {:error, reason} -> repo.rollback(reason)
    end
  end

  defp fail_latest_running_attempt(repo, latest_attempt, error) do
    case latest_attempt do
      %StepAttempt{id: attempt_id, status: "running"} ->
        case AttemptStore.fail_attempt(repo, attempt_id, error) do
          {:ok, _attempt} -> :ok
          {:error, reason} -> {:error, reason}
        end

      %StepAttempt{} ->
        :ok

      nil ->
        :ok
    end
  end

  defp stale?(%StepRun{updated_at: updated_at}, timeout_ms) do
    now_ms = DateTime.to_unix(DateTime.utc_now(), :millisecond)
    updated_ms = DateTime.to_unix(updated_at, :millisecond)

    now_ms - updated_ms >= timeout_ms
  end

  defp stale_step_error(timeout_ms) do
    %{
      message: "step attempt reclaimed after exceeding stale running timeout",
      reason: "stale_running_step",
      stale_step_timeout_ms: timeout_ms
    }
  end

  defp locked_step_run(repo, step_run_id) do
    StepRun
    |> where([step_run], step_run.id == ^step_run_id)
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
