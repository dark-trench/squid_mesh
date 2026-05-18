defmodule SquidMesh.AttemptStore do
  @moduledoc """
  Durable store for step-attempt history.

  Attempts are recorded separately from step runs so retry policy and
  observability can reason about attempt numbers, failure history, and the
  latest known attempt state.
  """

  import Ecto.Query

  alias SquidMesh.Persistence.StepAttempt

  @type attempt_attrs :: %{optional(:error) => map() | nil}
  @type stale_error :: {:stale_attempt, String.t()}
  @max_allocation_retries 5

  @doc """
  Allocates the next attempt number for a step run and persists it as running.
  """
  @spec begin_attempt(module(), Ecto.UUID.t()) ::
          {:ok, StepAttempt.t()} | {:error, Ecto.Changeset.t()}
  def begin_attempt(repo, step_run_id) do
    do_begin_attempt(repo, step_run_id, @max_allocation_retries)
  end

  @doc """
  Marks one attempt as completed.
  """
  @spec complete_attempt(module(), Ecto.UUID.t()) ::
          {:ok, StepAttempt.t()} | {:error, :not_found | stale_error()}
  def complete_attempt(repo, attempt_id) do
    update_running_attempt(repo, attempt_id, %{status: "completed", error: nil})
  end

  @doc """
  Marks one attempt as failed and stores the normalized error payload.
  """
  @spec fail_attempt(module(), Ecto.UUID.t(), map()) ::
          {:ok, StepAttempt.t()} | {:error, :not_found | stale_error()}
  def fail_attempt(repo, attempt_id, error) when is_map(error) do
    update_running_attempt(repo, attempt_id, %{status: "failed", error: error})
  end

  @doc """
  Records one step attempt with optional failure details.
  """
  @spec record_attempt(module(), Ecto.UUID.t(), pos_integer(), String.t(), attempt_attrs()) ::
          {:ok, StepAttempt.t()} | {:error, Ecto.Changeset.t()}
  def record_attempt(repo, step_run_id, attempt_number, status, attrs \\ %{})
      when is_integer(attempt_number) and attempt_number > 0 and is_map(attrs) do
    attrs =
      attrs
      |> Map.take([:error])
      |> Map.merge(%{
        step_run_id: step_run_id,
        attempt_number: attempt_number,
        status: status
      })

    %StepAttempt{}
    |> StepAttempt.changeset(attrs)
    |> repo.insert()
  end

  @doc """
  Returns how many attempts have been recorded for a step run.
  """
  @spec attempt_count(module(), Ecto.UUID.t()) :: non_neg_integer()
  def attempt_count(repo, step_run_id) do
    StepAttempt
    |> where([attempt], attempt.step_run_id == ^step_run_id)
    |> select([attempt], count(attempt.id))
    |> repo.one()
  end

  @doc """
  Returns the next available attempt number for a step run.
  """
  @spec next_attempt_number(module(), Ecto.UUID.t()) :: pos_integer()
  def next_attempt_number(repo, step_run_id) do
    case latest_attempt(repo, step_run_id) do
      %StepAttempt{attempt_number: attempt_number} -> attempt_number + 1
      nil -> 1
    end
  end

  @doc """
  Returns the latest recorded attempt for a step run.
  """
  @spec latest_attempt(module(), Ecto.UUID.t()) :: StepAttempt.t() | nil
  def latest_attempt(repo, step_run_id) do
    StepAttempt
    |> where([attempt], attempt.step_run_id == ^step_run_id)
    |> order_by([attempt],
      desc: attempt.attempt_number,
      desc: attempt.inserted_at,
      desc: attempt.id
    )
    |> limit(1)
    |> repo.one()
  end

  @spec do_begin_attempt(module(), Ecto.UUID.t(), non_neg_integer()) ::
          {:ok, StepAttempt.t()} | {:error, Ecto.Changeset.t()}
  defp do_begin_attempt(repo, step_run_id, retries_remaining) do
    step_run_id
    |> next_running_attempt_attrs(repo)
    |> then(fn attrs ->
      %StepAttempt{}
      |> StepAttempt.changeset(attrs)
      |> repo.insert()
    end)
    |> case do
      {:ok, attempt} ->
        {:ok, attempt}

      {:error, %Ecto.Changeset{} = changeset} ->
        if retries_remaining > 0 and duplicate_attempt_number?(changeset) do
          do_begin_attempt(repo, step_run_id, retries_remaining - 1)
        else
          {:error, changeset}
        end
    end
  end

  @spec next_running_attempt_attrs(Ecto.UUID.t(), module()) :: map()
  defp next_running_attempt_attrs(step_run_id, repo) do
    %{
      step_run_id: step_run_id,
      attempt_number: next_attempt_number(repo, step_run_id),
      status: "running"
    }
  end

  @spec update_running_attempt(module(), Ecto.UUID.t(), map()) ::
          {:ok, StepAttempt.t()} | {:error, :not_found | stale_error()}
  defp update_running_attempt(repo, attempt_id, attrs) do
    updates =
      attrs
      |> Map.put(:updated_at, now_utc())
      |> Map.to_list()

    {count, _rows} =
      StepAttempt
      |> where([attempt], attempt.id == ^attempt_id and attempt.status == "running")
      |> repo.update_all(set: updates)

    case count do
      1 ->
        {:ok, repo.get!(StepAttempt, attempt_id)}

      0 ->
        stale_attempt_error(repo, attempt_id)
    end
  end

  defp stale_attempt_error(repo, attempt_id) do
    case repo.get(StepAttempt, attempt_id) do
      %StepAttempt{status: status} -> {:error, {:stale_attempt, status}}
      nil -> {:error, :not_found}
    end
  end

  @spec duplicate_attempt_number?(Ecto.Changeset.t()) :: boolean()
  defp duplicate_attempt_number?(%Ecto.Changeset{errors: errors}) do
    Enum.any?(errors, fn
      {field, {"has already been taken", opts}} when field in [:attempt_number, :step_run_id] ->
        Keyword.get(opts, :constraint) == :unique

      _other ->
        false
    end)
  end

  defp now_utc do
    DateTime.utc_now(:microsecond)
  end
end
