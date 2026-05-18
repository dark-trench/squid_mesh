defmodule SquidMesh.Runtime.DispatchProtocol do
  @moduledoc """
  Defines the durable dispatch journal contract.

  The protocol separates durable facts from live effects:

  - run-thread entries record workflow lifecycle facts
  - dispatch-thread entries record runnable intent, claims, leases, heartbeats,
    completions, failures, retries, and live wakeups
  - run-index entries support rebuildable lookup projections

  A live wakeup or action execution is valid only after the runnable intent is
  appended. Claims are fenced by `claim_id` and `claim_token_hash`;
  completions, failures, and heartbeats from stale claim owners are ignored by
  the projection and surfaced as anomalies.
  """

  alias SquidMesh.Runtime.DispatchProtocol.Entry

  @type entry_type ::
          :run_started
          | :runnables_planned
          | :runnable_applied
          | :manual_step_paused
          | :manual_step_resolved
          | :run_terminal
          | :run_indexed
          | :attempt_scheduled
          | :attempt_claimed
          | :attempt_heartbeat
          | :attempt_completed
          | :attempt_failed
          | :live_wakeup_emitted

  @manual_entry_types [:manual_step_paused, :manual_step_resolved]

  @run_entry_types [
    :run_started,
    :runnables_planned,
    :runnable_applied,
    :manual_step_paused,
    :manual_step_resolved,
    :run_terminal
  ]

  @dispatch_entry_types [
    :attempt_scheduled,
    :attempt_claimed,
    :attempt_heartbeat,
    :attempt_completed,
    :attempt_failed,
    :live_wakeup_emitted
  ]

  @run_index_entry_types [:run_indexed]

  @required_fields %{
    run_started: [:run_id, :workflow, :occurred_at],
    runnables_planned: [:run_id, :runnables, :occurred_at],
    runnable_applied: [:run_id, :runnable_key, :occurred_at],
    manual_step_paused: [:run_id, :step, :kind, :occurred_at],
    manual_step_resolved: [:run_id, :step, :action, :occurred_at],
    run_terminal: [:run_id, :status, :occurred_at],
    run_indexed: [:run_id, :workflow, :occurred_at],
    attempt_scheduled: [
      :run_id,
      :runnable_key,
      :idempotency_key,
      :attempt_number,
      :queue,
      :step,
      :input,
      :visible_at,
      :occurred_at
    ],
    attempt_claimed: [
      :run_id,
      :runnable_key,
      :claim_id,
      :claim_token_hash,
      :owner_id,
      :queue,
      :lease_until,
      :occurred_at
    ],
    attempt_heartbeat: [
      :run_id,
      :runnable_key,
      :claim_id,
      :claim_token_hash,
      :queue,
      :lease_until,
      :occurred_at
    ],
    attempt_completed: [
      :run_id,
      :runnable_key,
      :claim_id,
      :claim_token_hash,
      :queue,
      :result,
      :occurred_at
    ],
    attempt_failed: [
      :run_id,
      :runnable_key,
      :claim_id,
      :claim_token_hash,
      :queue,
      :error,
      :occurred_at
    ],
    live_wakeup_emitted: [:run_id, :runnable_key, :queue, :occurred_at]
  }

  @entry_types @run_entry_types ++ @dispatch_entry_types ++ @run_index_entry_types

  @doc false
  @spec new_entry(entry_type(), map() | keyword()) ::
          {:ok, Entry.t()} | {:error, {:unknown_entry_type, atom()} | {:missing_fields, [atom()]}}
  def new_entry(type, attrs) when is_atom(type) and type in @entry_types do
    attrs =
      attrs
      |> Map.new()
      |> normalize_attrs(type)

    with :ok <- require_fields(type, attrs) do
      {:ok,
       %Entry{
         type: type,
         thread: thread_for(type, attrs),
         data: attrs,
         occurred_at: attrs.occurred_at
       }}
    end
  end

  def new_entry(type, _attrs) when is_atom(type), do: {:error, {:unknown_entry_type, type}}

  defp normalize_attrs(attrs, type) when type in @dispatch_entry_types do
    Map.update(attrs, :queue, "default", &normalize_queue/1)
  end

  defp normalize_attrs(attrs, type) when type in @manual_entry_types do
    attrs
    |> Map.update(:step, nil, &normalize_manual_value/1)
    |> Map.update(:kind, nil, &normalize_manual_value/1)
    |> Map.update(:action, nil, &normalize_manual_value/1)
    |> Map.put_new(:metadata, %{})
    |> Map.put_new(:result, %{})
  end

  defp normalize_attrs(attrs, type) when type in @run_index_entry_types do
    Map.update(attrs, :workflow, nil, &normalize_workflow/1)
  end

  defp normalize_attrs(attrs, _type), do: attrs

  defp require_fields(type, attrs) do
    missing_fields =
      type
      |> then(&Map.fetch!(@required_fields, &1))
      |> Enum.reject(&(Map.has_key?(attrs, &1) and not is_nil(Map.fetch!(attrs, &1))))

    case missing_fields do
      [] -> :ok
      missing -> {:error, {:missing_fields, missing}}
    end
  end

  defp thread_for(type, attrs) when type in @run_entry_types, do: {:run, attrs.run_id}

  defp thread_for(type, attrs) when type in @run_index_entry_types,
    do: {:run_index, attrs.workflow}

  defp thread_for(type, attrs) when type in @dispatch_entry_types do
    {:dispatch, attrs.queue}
  end

  defp normalize_workflow(nil), do: nil

  defp normalize_workflow(workflow) when is_atom(workflow),
    do: SquidMesh.Workflow.Definition.serialize_workflow(workflow)

  defp normalize_workflow(workflow), do: normalize_thread_id(workflow)

  defp normalize_queue(nil), do: nil
  defp normalize_queue(queue), do: normalize_thread_id(queue)

  defp normalize_manual_value(nil), do: nil
  defp normalize_manual_value(value), do: normalize_thread_id(value)

  defp normalize_thread_id(id) when is_binary(id), do: id
  defp normalize_thread_id(id), do: to_string(id)
end
