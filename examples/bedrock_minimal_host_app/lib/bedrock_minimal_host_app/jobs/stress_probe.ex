defmodule BedrockMinimalHostApp.Jobs.StressProbe do
  @moduledoc """
  Test-only Bedrock job used by the example app stress harness.
  """

  use Bedrock.JobQueue.Job,
    topic: "stress:probe",
    max_retries: 3,
    priority: 100

  alias Bedrock.JobQueue.Payload

  @table __MODULE__

  @impl true
  def perform(%{"event" => event}, meta) do
    record_event(event, meta)
  end

  def perform(%{event: event}, meta) do
    record_event(event, meta)
  end

  def perform_item(item) do
    payload = Payload.decode(item.payload)

    perform(payload, %{
      topic: item.topic,
      queue_id: item.queue_id,
      item_id: item.id,
      attempt: item.error_count + 1,
      priority: item.priority
    })
  end

  def reset! do
    ensure_table!()
    :ets.delete_all_objects(@table)
    :ok
  end

  def events do
    ensure_table!()

    events = :ets.tab2list(@table)
    ordered_events = Enum.sort_by(events, fn {sequence, _event} -> sequence end)
    Enum.map(ordered_events, fn {_sequence, event} -> event end)
  end

  defp record_event(event, meta) do
    record!(%{
      event: event,
      topic: meta.topic,
      queue_id: meta.queue_id,
      item_id: meta.item_id,
      attempt: meta.attempt,
      priority: Map.get(meta, :priority)
    })

    :ok
  end

  defp record!(event) do
    ensure_table!()
    sequence = System.unique_integer([:positive, :monotonic])
    true = :ets.insert(@table, {sequence, event})
    :ok
  end

  defp ensure_table! do
    case :ets.whereis(@table) do
      :undefined ->
        :ets.new(@table, [:named_table, :public, :ordered_set])

      _tid ->
        :ok
    end
  end
end
