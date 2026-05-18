defmodule SquidMesh.Runtime.ProjectedExplanation do
  @moduledoc """
  Projection-backed explanation for the Jido-native runtime path.

  `SquidMesh.Runtime.ProjectedInspection` answers what durable journal
  projections currently show. This module answers why that state matters to an
  operator by deriving a deterministic reason, high-signal details, and the
  runtime boundary that would make progress.

  Explanations are read-only. They do not schedule missing dispatches, apply
  completed results, recover expired claims, or mutate checkpoints.
  """

  alias SquidMesh.Runtime.ProjectedExplanation.Explanation
  alias SquidMesh.Runtime.ProjectedInspection
  alias SquidMesh.Runtime.ProjectedInspection.Snapshot

  @type storage_config :: ProjectedInspection.storage_config()
  @type explanation_option :: ProjectedInspection.snapshot_option()
  @type explanation_error ::
          ProjectedInspection.snapshot_error() | {:invalid_option, {:run_id, term()}}

  @doc """
  Builds a projection-backed explanation for one workflow run.

  Options are the same as `SquidMesh.Runtime.ProjectedInspection.snapshot/3`.
  Missing runs and invalid options return the same structured errors as the
  underlying snapshot call.
  """
  @spec explain(storage_config(), String.t(), [explanation_option()]) ::
          {:ok, Explanation.t()} | {:error, explanation_error()}
  def explain(storage, run_id, opts \\ [])

  def explain(storage, run_id, opts) when is_binary(run_id) do
    with {:ok, snapshot} <- ProjectedInspection.snapshot(storage, run_id, opts) do
      {:ok, from_snapshot(snapshot)}
    end
  end

  def explain(_storage, run_id, _opts) do
    {:error, {:invalid_option, {:run_id, run_id}}}
  end

  @doc """
  Derives an explanation from an existing projection-backed snapshot.

  This is useful when a caller already has a snapshot and wants a stable
  diagnostic view without re-reading storage.
  """
  @spec from_snapshot(Snapshot.t()) :: Explanation.t()
  def from_snapshot(%Snapshot{} = snapshot) do
    {summary, details, next_actions, step} = explanation_parts(snapshot)

    %Explanation{
      run_id: snapshot.run_id,
      workflow: snapshot.workflow,
      queue: snapshot.queue,
      status: snapshot.status,
      reason: snapshot.reason,
      step: step,
      summary: summary,
      details: details,
      next_actions: next_actions,
      evidence: evidence(snapshot)
    }
  end

  defp explanation_parts(%Snapshot{reason: :planned_dispatch_pending_schedule} = snapshot) do
    runnables = snapshot.pending_dispatches

    {
      "Planned runnable has not been recorded in the dispatch journal.",
      %{
        pending_dispatch_count: length(runnables),
        runnable_keys: runnable_keys(runnables)
      },
      [:schedule_pending_dispatch],
      first_step(runnables)
    }
  end

  defp explanation_parts(%Snapshot{reason: :completed_result_pending_apply} = snapshot) do
    attempts = snapshot.pending_results

    {
      "Dispatch result is complete but has not been applied to the run journal.",
      %{
        pending_result_count: length(attempts),
        runnable_keys: runnable_keys(attempts)
      },
      [:apply_pending_result],
      first_step(attempts)
    }
  end

  defp explanation_parts(%Snapshot{reason: :expired_claim} = snapshot) do
    attempts = snapshot.expired_claims

    {
      "A claimed dispatch attempt has expired and is recoverable.",
      %{
        expired_claim_count: length(attempts),
        runnable_keys: runnable_keys(attempts),
        oldest_lease_until: oldest_lease_until(attempts)
      },
      [:recover_expired_claim],
      first_step(attempts)
    }
  end

  defp explanation_parts(%Snapshot{reason: :attempt_visible} = snapshot) do
    attempts = snapshot.visible_attempts

    {
      "A dispatch attempt is visible and waiting for a worker claim.",
      %{
        visible_attempt_count: length(attempts),
        runnable_keys: runnable_keys(attempts)
      },
      [:wait_for_worker_claim],
      first_step(attempts)
    }
  end

  defp explanation_parts(%Snapshot{reason: :attempt_scheduled_for_later} = snapshot) do
    attempts = snapshot.scheduled_attempts

    {
      "A dispatch attempt is scheduled for future visibility.",
      %{
        scheduled_attempt_count: length(attempts),
        runnable_keys: runnable_keys(attempts),
        next_visible_at: snapshot.next_visible_at
      },
      [:wait_until_attempt_visible],
      first_step(attempts)
    }
  end

  defp explanation_parts(%Snapshot{reason: :attempt_claimed} = snapshot) do
    attempts = claimed_attempts(snapshot.attempts)

    {
      "A dispatch attempt is claimed and waiting for completion.",
      %{
        claimed_attempt_count: length(attempts),
        runnable_keys: runnable_keys(attempts),
        earliest_lease_until: oldest_lease_until(attempts)
      },
      [:wait_for_attempt_completion],
      first_step(attempts)
    }
  end

  defp explanation_parts(%Snapshot{reason: :manual_intervention_required} = snapshot) do
    manual_state = snapshot.manual_state || %{}

    {
      "The run is paused for manual intervention.",
      manual_state,
      [:resolve_manual_step],
      item_value(manual_state, :step)
    }
  end

  defp explanation_parts(%Snapshot{reason: :terminal} = snapshot) do
    {
      "The run is terminal according to the run journal.",
      %{terminal?: snapshot.terminal?, terminal_status: snapshot.terminal_status},
      [:inspect_terminal_run],
      nil
    }
  end

  defp explanation_parts(%Snapshot{reason: :idle} = snapshot) do
    {
      "All currently planned runnables have been applied.",
      %{
        planned_count: length(snapshot.planned_runnable_keys),
        applied_count: length(snapshot.applied_runnable_keys)
      },
      [:wait_for_new_runnables],
      nil
    }
  end

  defp explanation_parts(%Snapshot{reason: :run_started} = snapshot) do
    {
      "The run has started but no dispatch attempts have been recorded yet.",
      %{planned_count: length(snapshot.planned_runnable_keys)},
      [:inspect_dispatch_state],
      nil
    }
  end

  defp explanation_parts(%Snapshot{reason: :waiting_for_dispatch} = snapshot) do
    {
      "The run has dispatch state but no visible recovery action is currently implied.",
      %{attempt_count: length(snapshot.attempts)},
      [:inspect_dispatch_state],
      first_step(snapshot.attempts)
    }
  end

  defp evidence(%Snapshot{} = snapshot) do
    %{
      snapshot_reason: snapshot.reason,
      thread_revisions: snapshot.thread_revisions,
      terminal_status: snapshot.terminal_status,
      manual_state: snapshot.manual_state,
      planned_runnable_keys: snapshot.planned_runnable_keys,
      applied_runnable_keys: snapshot.applied_runnable_keys,
      next_visible_at: snapshot.next_visible_at,
      attempt_counts: attempt_counts(snapshot.attempts),
      anomaly_count: length(snapshot.anomalies),
      anomalies: snapshot.anomalies
    }
  end

  defp attempt_counts(attempts) do
    Enum.reduce(attempts, %{}, fn attempt, counts ->
      case item_value(attempt, :status) do
        nil -> counts
        status -> Map.update(counts, status, 1, &(&1 + 1))
      end
    end)
  end

  defp claimed_attempts(attempts) do
    Enum.filter(attempts, &(item_value(&1, :status) == :claimed))
  end

  defp runnable_keys(items) do
    items
    |> Enum.map(&item_value(&1, :runnable_key))
    |> Enum.reject(&is_nil/1)
    |> Enum.sort()
  end

  defp first_step([first | _items]), do: item_value(first, :step)
  defp first_step([]), do: nil

  defp oldest_lease_until(attempts) do
    attempts
    |> Enum.map(&item_value(&1, :lease_until))
    |> Enum.reject(&is_nil/1)
    |> Enum.sort_by(&DateTime.to_unix(&1, :microsecond))
    |> List.first()
  end

  defp item_value(item, key) when is_map(item) and is_atom(key) do
    Map.get(item, key) || Map.get(item, Atom.to_string(key))
  end
end
