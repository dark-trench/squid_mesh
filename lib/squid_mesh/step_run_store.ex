defmodule SquidMesh.StepRunStore do
  @moduledoc """
  Durable store for per-step workflow execution state.

  Step runs are used to detect stale or duplicate deliveries and to persist
  step input, output, and failure details separately from the parent run.
  """

  import Ecto.Query

  alias SquidMesh.Persistence.StepAttempt
  alias SquidMesh.Persistence.StepRun

  @type step_identifier :: atom() | String.t()
  @type step_input :: map()
  @type step_output :: map()
  @type step_error :: map()
  @type recovery_policy :: map() | nil
  @type failure_recovery :: %{strategy: :compensation | :undo, target: step_identifier()}
  @type pause_target :: :complete | atom()
  @type approval_targets :: %{ok: pause_target(), error: pause_target()}
  @type manual_event :: map()
  @type step_status :: :pending | :running | :completed | :failed
  @type stale_error :: {:stale_step_run, String.t()}
  @type recovery_attrs :: map()
  @type begin_result :: {:ok, StepRun.t(), :execute | :skip}
  @type schedule_result :: {:ok, StepRun.t(), :schedule | :skip} | {:error, Ecto.Changeset.t()}
  @type step_schedule_input ::
          {step_identifier(), step_input()} | {step_identifier(), step_input(), recovery_policy()}

  @doc """
  Marks a step as ready for execution if it has not already completed or been
  claimed by another delivery of the same workflow step.
  """
  @spec begin_step(module(), Ecto.UUID.t(), step_identifier(), step_input()) :: begin_result()
  def begin_step(repo, run_id, step, input) when is_map(input) do
    begin_step(repo, run_id, step, input, nil)
  end

  @doc false
  @spec begin_step(module(), Ecto.UUID.t(), step_identifier(), step_input(), recovery_policy()) ::
          begin_result()
  def begin_step(repo, run_id, step, input, recovery) when is_map(input) do
    serialized_step = serialize_step(step)

    attrs = %{
      run_id: run_id,
      step: serialized_step,
      status: "running",
      input: input,
      output: nil,
      recovery: serialize_recovery(recovery),
      resume: nil,
      last_error: nil
    }

    case insert_step_run(repo, attrs) do
      {:ok, step_run} ->
        {:ok, step_run, :execute}

      :duplicate ->
        claim_existing_step(repo, run_id, serialized_step, attrs)
    end
  end

  @doc """
  Persists that a step has been scheduled but not yet claimed by a worker.
  """
  @spec schedule_step(module(), Ecto.UUID.t(), step_identifier(), step_input()) ::
          schedule_result()
  def schedule_step(repo, run_id, step, input) when is_map(input) do
    schedule_step(repo, run_id, step, input, nil)
  end

  @doc false
  @spec schedule_step(module(), Ecto.UUID.t(), step_identifier(), step_input(), recovery_policy()) ::
          schedule_result()
  def schedule_step(repo, run_id, step, input, recovery) when is_map(input) do
    serialized_step = serialize_step(step)

    attrs = %{
      id: Ecto.UUID.generate(),
      run_id: run_id,
      step: serialized_step,
      status: "pending",
      input: input,
      output: nil,
      recovery: serialize_recovery(recovery),
      resume: nil,
      last_error: nil,
      inserted_at: DateTime.utc_now(),
      updated_at: DateTime.utc_now()
    }

    case repo.insert_all(
           StepRun,
           [attrs],
           on_conflict: :nothing,
           conflict_target: [:run_id, :step]
         ) do
      {1, _rows} ->
        {:ok, get_step_run(repo, run_id, serialized_step), :schedule}

      {0, _rows} ->
        case get_step_run(repo, run_id, serialized_step) do
          %StepRun{} = step_run -> {:ok, step_run, :skip}
          nil -> {:error, Ecto.Changeset.change(%StepRun{}, attrs)}
        end
    end
  end

  @doc false
  @spec schedule_steps(module(), Ecto.UUID.t(), [step_schedule_input()]) ::
          {:ok, [step_identifier()]} | {:error, term()}
  def schedule_steps(_repo, _run_id, []), do: {:ok, []}

  def schedule_steps(repo, run_id, step_inputs) when is_list(step_inputs) do
    attrs = Enum.map(step_inputs, &scheduled_step_attrs(run_id, &1))

    # Bulk scheduling is used before fan-out job dispatch. It avoids committing
    # a prefix of pending step rows if a later step in the same fan-out is bad.
    try do
      case repo.insert_all(
             StepRun,
             attrs,
             on_conflict: :nothing,
             conflict_target: [:run_id, :step],
             returning: [:step]
           ) do
        {_count, inserted_rows} ->
          inserted_steps = MapSet.new(inserted_rows, & &1.step)

          scheduled_steps =
            step_inputs
            |> Enum.map(fn
              {step, _input} -> step
              {step, _input, _recovery} -> step
            end)
            |> Enum.filter(&(serialize_step(&1) in inserted_steps))

          {:ok, scheduled_steps}
      end
    rescue
      exception -> {:error, exception}
    end
  end

  @doc false
  @spec delete_pending_steps(module(), Ecto.UUID.t(), [step_identifier()]) :: :ok
  def delete_pending_steps(_repo, _run_id, []), do: :ok

  def delete_pending_steps(repo, run_id, steps) when is_list(steps) do
    serialized_steps = Enum.map(steps, &serialize_step/1)

    StepRun
    |> where(
      [step_run],
      step_run.run_id == ^run_id and step_run.step in ^serialized_steps and
        step_run.status == "pending"
    )
    |> repo.delete_all()

    :ok
  end

  @doc """
  Marks a step run as completed and persists its output.
  """
  @spec complete_step(module(), Ecto.UUID.t(), step_output()) ::
          {:ok, StepRun.t()} | {:error, :not_found | stale_error()}
  def complete_step(repo, step_run_id, output) when is_map(output) do
    update_running_step(repo, step_run_id, %{
      status: "completed",
      output: output,
      manual: nil,
      resume: nil,
      last_error: nil
    })
  end

  @doc """
  Marks a paused manual step as completed, persists its output, and records the
  durable manual action metadata.
  """
  @spec complete_manual_step(module(), Ecto.UUID.t(), step_output(), manual_event()) ::
          {:ok, StepRun.t()} | {:error, :not_found | stale_error()}
  def complete_manual_step(repo, step_run_id, output, manual)
      when is_map(output) and is_map(manual) do
    update_running_step(repo, step_run_id, %{
      status: "completed",
      output: output,
      manual: manual,
      last_error: nil
    })
  end

  @doc """
  Marks a step run as failed and persists the last error.
  """
  @spec fail_step(module(), Ecto.UUID.t(), step_error()) ::
          {:ok, StepRun.t()} | {:error, :not_found | stale_error()}
  def fail_step(repo, step_run_id, error) when is_map(error) do
    update_running_step(repo, step_run_id, %{
      status: "failed",
      manual: nil,
      resume: nil,
      last_error: error
    })
  end

  @doc false
  @spec record_failure_recovery(module(), Ecto.UUID.t(), failure_recovery()) ::
          {:ok, StepRun.t()} | {:error, :not_found | stale_error()}
  def record_failure_recovery(repo, step_run_id, %{strategy: strategy, target: target} = failure)
      when strategy in [:compensation, :undo] and (is_atom(target) or is_binary(target)) do
    case repo.get(StepRun, step_run_id) do
      %StepRun{status: "failed", recovery: recovery} ->
        update_failure_recovery(repo, step_run_id, recovery, failure)

      %StepRun{status: status} ->
        {:error, {:stale_step_run, status}}

      nil ->
        {:error, :not_found}
    end
  end

  defp update_failure_recovery(repo, step_run_id, recovery, failure) do
    updated_recovery = Map.put(recovery || %{}, "failure", serialize_recovery_value(failure))
    updates = [recovery: updated_recovery, updated_at: now_utc()]

    StepRun
    |> where([step_run], step_run.id == ^step_run_id and step_run.status == "failed")
    |> repo.update_all(set: updates)
    |> case do
      {1, _rows} -> {:ok, repo.get!(StepRun, step_run_id)}
      {0, _rows} -> stale_step_run_error(repo, step_run_id)
    end
  end

  @doc """
  Persists pause-resume metadata for a running pause step without completing it.
  """
  @spec persist_pause_resume(module(), Ecto.UUID.t(), step_output(), pause_target()) ::
          {:ok, StepRun.t()} | {:error, :not_found | stale_error()}
  def persist_pause_resume(repo, step_run_id, output, target)
      when is_map(output) and (target == :complete or is_atom(target)) do
    update_running_step(repo, step_run_id, %{
      resume: %{
        "output" => output,
        "target" => serialize_pause_target(target)
      }
    })
  end

  @doc """
  Persists approval resume metadata for a running approval step without
  completing it.
  """
  @spec persist_approval_resume(module(), Ecto.UUID.t(), approval_targets(), atom() | nil) ::
          {:ok, StepRun.t()} | {:error, :not_found | stale_error()}
  def persist_approval_resume(
        repo,
        step_run_id,
        %{ok: ok_target, error: error_target},
        output_key
      )
      when (ok_target == :complete or is_atom(ok_target)) and
             (error_target == :complete or is_atom(error_target)) and
             (is_atom(output_key) or is_nil(output_key)) do
    update_running_step(repo, step_run_id, %{
      resume: %{
        "kind" => "approval",
        "ok_target" => serialize_pause_target(ok_target),
        "error_target" => serialize_pause_target(error_target),
        "output_key" => serialize_output_key(output_key)
      }
    })
  end

  @doc """
  Fetches the persisted step run for one workflow run and step identifier.
  """
  @spec get_step_run(module(), Ecto.UUID.t(), step_identifier()) :: StepRun.t() | nil
  def get_step_run(repo, run_id, step) do
    serialized_step = serialize_step(step)

    StepRun
    |> where([step_run], step_run.run_id == ^run_id and step_run.step == ^serialized_step)
    |> repo.one()
  end

  @doc """
  Lists the completed step identifiers for one workflow run.
  """
  @spec completed_steps(module(), Ecto.UUID.t()) :: [String.t()]
  def completed_steps(repo, run_id) do
    StepRun
    |> where([step_run], step_run.run_id == ^run_id and step_run.status == "completed")
    |> order_by([step_run], asc: step_run.inserted_at)
    |> select([step_run], step_run.step)
    |> repo.all()
  end

  @doc """
  Lists the completed step outputs for one workflow run in completion order.
  """
  @spec completed_outputs(module(), Ecto.UUID.t()) :: [step_output()]
  def completed_outputs(repo, run_id) do
    StepRun
    |> where([step_run], step_run.run_id == ^run_id and step_run.status == "completed")
    |> order_by([step_run], asc: step_run.inserted_at)
    |> select([step_run], step_run.output)
    |> repo.all()
    |> Enum.map(&Kernel.||(&1, %{}))
  end

  @doc """
  Lists completed step runs for one workflow run in compensation order.

  Saga rollback must undo the most recently completed reversible effect first.
  The ordering uses the completed forward attempt timestamp so compensation
  metadata updates do not change rollback order on redelivery.
  """
  @spec completed_step_runs_for_compensation(module(), Ecto.UUID.t()) :: [StepRun.t()]
  def completed_step_runs_for_compensation(repo, run_id) do
    latest_completion_query =
      from(attempt in StepAttempt,
        where: attempt.status == "completed",
        group_by: attempt.step_run_id,
        select: %{step_run_id: attempt.step_run_id, completed_at: max(attempt.updated_at)}
      )

    StepRun
    |> join(:left, [step_run], completion in subquery(latest_completion_query),
      on: completion.step_run_id == step_run.id
    )
    |> where([step_run], step_run.run_id == ^run_id and step_run.status == "completed")
    |> order_by([step_run, completion],
      desc: completion.completed_at,
      desc: step_run.inserted_at,
      desc: step_run.id
    )
    |> repo.all()
  end

  @doc """
  Updates the persisted recovery metadata for one step run.

  The compensation runtime uses this to mark callbacks as running, completed, or
  failed without changing the original forward step status.
  """
  @spec update_recovery(module(), Ecto.UUID.t(), recovery_attrs()) ::
          {:ok, StepRun.t()} | {:error, :not_found}
  def update_recovery(repo, step_run_id, recovery) when is_map(recovery) do
    updates = [
      recovery: serialize_recovery(recovery),
      updated_at: now_utc()
    ]

    {count, _rows} =
      StepRun
      |> where([step_run], step_run.id == ^step_run_id)
      |> repo.update_all(set: updates)

    case count do
      1 -> {:ok, repo.get!(StepRun, step_run_id)}
      0 -> {:error, :not_found}
    end
  end

  @doc """
  Lists the persisted step status for each declared step in a workflow run.
  """
  @spec step_statuses(module(), Ecto.UUID.t()) :: %{optional(String.t()) => step_status()}
  def step_statuses(repo, run_id) do
    StepRun
    |> where([step_run], step_run.run_id == ^run_id)
    |> select([step_run], {step_run.step, step_run.status})
    |> repo.all()
    |> Map.new(fn {step, status} -> {step, deserialize_status(status)} end)
  end

  @spec insert_step_run(module(), map()) :: {:ok, StepRun.t()} | :duplicate
  defp insert_step_run(repo, attrs) do
    timestamps = %{id: Ecto.UUID.generate(), inserted_at: now_utc(), updated_at: now_utc()}
    attrs = Map.merge(timestamps, attrs)

    # This function is called inside locked preparation transactions. Use
    # `ON CONFLICT` instead of relying on a unique-constraint error because a
    # PostgreSQL error would abort the whole transaction before recovery logic.
    case repo.insert_all(
           StepRun,
           [attrs],
           on_conflict: :nothing,
           conflict_target: [:run_id, :step]
         ) do
      {1, _rows} -> {:ok, get_step_run(repo, attrs.run_id, attrs.step)}
      {0, _rows} -> :duplicate
    end
  end

  defp scheduled_step_attrs(run_id, {step, input}) do
    scheduled_step_attrs(run_id, {step, input, nil})
  end

  defp scheduled_step_attrs(run_id, {step, input, recovery}) do
    now = now_utc()

    %{
      id: Ecto.UUID.generate(),
      run_id: run_id,
      step: serialize_step(step),
      status: "pending",
      input: input,
      output: nil,
      recovery: serialize_recovery(recovery),
      resume: nil,
      last_error: nil,
      inserted_at: now,
      updated_at: now
    }
  end

  @spec claim_existing_step(module(), Ecto.UUID.t(), String.t(), map()) :: begin_result()
  defp claim_existing_step(repo, run_id, step, attrs) do
    case transition_pending_step_to_running(repo, run_id, step, attrs) do
      {:ok, %StepRun{} = step_run} ->
        {:ok, step_run, :execute}

      :not_updated ->
        claim_failed_or_existing_step(repo, run_id, step, attrs)
    end
  end

  defp claim_failed_or_existing_step(repo, run_id, step, attrs) do
    case transition_failed_step_to_running(repo, run_id, step, attrs) do
      {:ok, %StepRun{} = step_run} -> {:ok, step_run, :execute}
      :not_updated -> existing_or_inserted_step(repo, run_id, step, attrs)
    end
  end

  defp existing_or_inserted_step(repo, run_id, step, attrs) do
    case get_step_run(repo, run_id, step) do
      %StepRun{} = step_run -> {:ok, step_run, :skip}
      nil -> insert_or_reclaim_step(repo, run_id, step, attrs)
    end
  end

  defp insert_or_reclaim_step(repo, run_id, step, attrs) do
    case insert_step_run(repo, attrs) do
      {:ok, step_run} -> {:ok, step_run, :execute}
      :duplicate -> claim_existing_step(repo, run_id, step, attrs)
    end
  end

  @spec transition_pending_step_to_running(module(), Ecto.UUID.t(), String.t(), map()) ::
          {:ok, StepRun.t()} | :not_updated
  defp transition_pending_step_to_running(repo, run_id, step, attrs) do
    updates =
      attrs
      |> Map.take([:status, :output, :resume, :last_error])
      |> Map.put(:status, "running")
      |> Map.put(:updated_at, now_utc())

    {count, _rows} =
      StepRun
      |> where(
        [step_run],
        step_run.run_id == ^run_id and step_run.step == ^step and step_run.status == "pending"
      )
      |> repo.update_all(set: Map.to_list(updates))

    case count do
      1 -> {:ok, get_step_run(repo, run_id, step)}
      _count -> :not_updated
    end
  end

  @spec transition_failed_step_to_running(module(), Ecto.UUID.t(), String.t(), map()) ::
          {:ok, StepRun.t()} | :not_updated
  defp transition_failed_step_to_running(repo, run_id, step, attrs) do
    updates =
      attrs
      |> Map.take([:status, :input, :output, :recovery, :resume, :last_error])
      |> Map.put(:updated_at, now_utc())

    {count, _rows} =
      StepRun
      |> where(
        [step_run],
        step_run.run_id == ^run_id and step_run.step == ^step and step_run.status == "failed"
      )
      |> repo.update_all(set: Map.to_list(updates))

    case count do
      1 -> {:ok, get_step_run(repo, run_id, step)}
      _count -> :not_updated
    end
  end

  @spec update_running_step(module(), Ecto.UUID.t(), map()) ::
          {:ok, StepRun.t()} | {:error, :not_found | stale_error()}
  defp update_running_step(repo, step_run_id, attrs) do
    updates =
      attrs
      |> Map.put(:updated_at, now_utc())
      |> Map.to_list()

    {count, _rows} =
      StepRun
      |> where([step_run], step_run.id == ^step_run_id and step_run.status == "running")
      |> repo.update_all(set: updates)

    case count do
      1 ->
        {:ok, repo.get!(StepRun, step_run_id)}

      0 ->
        stale_step_run_error(repo, step_run_id)
    end
  end

  defp stale_step_run_error(repo, step_run_id) do
    case repo.get(StepRun, step_run_id) do
      %StepRun{status: status} -> {:error, {:stale_step_run, status}}
      nil -> {:error, :not_found}
    end
  end

  @spec deserialize_status(String.t()) :: step_status()
  defp deserialize_status("pending"), do: :pending
  defp deserialize_status("running"), do: :running
  defp deserialize_status("completed"), do: :completed
  defp deserialize_status("failed"), do: :failed

  @spec serialize_pause_target(pause_target()) :: String.t()
  defp serialize_pause_target(:complete), do: "__complete__"
  defp serialize_pause_target(target) when is_atom(target), do: serialize_step(target)

  @spec serialize_output_key(atom() | nil) :: String.t() | nil
  defp serialize_output_key(nil), do: nil
  defp serialize_output_key(output_key), do: Atom.to_string(output_key)

  @spec serialize_step(step_identifier()) :: String.t()
  defp serialize_step(step) when is_atom(step), do: Atom.to_string(step)
  defp serialize_step(step) when is_binary(step), do: step

  defp serialize_recovery(nil), do: nil

  defp serialize_recovery(recovery) when is_map(recovery) do
    serialize_recovery_value(recovery)
  end

  defp serialize_recovery_value(recovery) when is_map(recovery) do
    Map.new(recovery, fn {key, value} ->
      {serialize_recovery_key(key), serialize_recovery_value(value)}
    end)
  end

  defp serialize_recovery_value(value) when is_boolean(value), do: value
  defp serialize_recovery_value(nil), do: nil
  defp serialize_recovery_value(:complete), do: "__complete__"
  defp serialize_recovery_value(value) when is_atom(value), do: Atom.to_string(value)

  defp serialize_recovery_value(value) when is_list(value),
    do: Enum.map(value, &serialize_recovery_value/1)

  defp serialize_recovery_value(value), do: value

  defp serialize_recovery_key(key) when is_atom(key), do: Atom.to_string(key)
  defp serialize_recovery_key(key), do: key

  defp now_utc do
    DateTime.utc_now(:microsecond)
  end
end
