defmodule SquidMesh.ProjectedReadModelTest do
  use ExUnit.Case, async: false

  alias SquidMesh.Runtime.DispatchProtocol
  alias SquidMesh.Runtime.Journal
  alias SquidMesh.Runtime.ProjectedExplanation.Explanation
  alias SquidMesh.Runtime.ProjectedInspection.Snapshot

  @storage {Jido.Storage.ETS, table: :squid_mesh_projected_read_model_test}
  @run_id "run_123"
  @workflow "BillingWorkflow"
  @queue "default"
  @runnable_key "run_123:charge_card:1"
  @idempotency_key "run_123:charge_card:payment_456"
  @started_at ~U[2026-05-15 00:00:00Z]
  @visible_at ~U[2026-05-15 00:00:10Z]

  setup do
    cleanup_storage()
    on_exit(&cleanup_storage/0)
  end

  test "inspect_run/2 can read from the journal projection read model" do
    append_run_entries([run_started(), runnables_planned()])
    append_dispatch_entries([attempt_scheduled()])

    assert {:ok, %Snapshot{} = snapshot} =
             SquidMesh.inspect_run(@run_id,
               read_model: :journal_projection,
               journal_storage: @storage,
               queue: @queue,
               now: @visible_at
             )

    assert snapshot.run_id == @run_id
    assert snapshot.workflow == @workflow
    assert snapshot.queue == @queue
    assert snapshot.reason == :attempt_visible
    assert [%{runnable_key: @runnable_key, status: :available}] = snapshot.visible_attempts
  end

  test "explain_run/2 can read from the journal projection read model" do
    append_run_entries([run_started(), runnables_planned()])

    assert {:ok, %Explanation{} = explanation} =
             SquidMesh.explain_run(@run_id,
               read_model: :journal_projection,
               journal_storage: @storage,
               queue: @queue,
               now: @visible_at
             )

    assert explanation.run_id == @run_id
    assert explanation.workflow == @workflow
    assert explanation.queue == @queue
    assert explanation.reason == :planned_dispatch_pending_schedule
    assert explanation.next_actions == [:schedule_pending_dispatch]
  end

  test "journal projection read model requires explicit journal storage" do
    assert {:error, {:invalid_option, {:journal_storage, nil}}} =
             SquidMesh.inspect_run(@run_id, read_model: :journal_projection)

    assert {:error, {:invalid_option, {:journal_storage, nil}}} =
             SquidMesh.explain_run(@run_id, read_model: :journal_projection)
  end

  test "returns a structured error for unsupported read models" do
    assert {:error, {:invalid_option, {:read_model, :unknown}}} =
             SquidMesh.inspect_run(@run_id, read_model: :unknown)

    assert {:error, {:invalid_option, {:read_model, :unknown}}} =
             SquidMesh.explain_run(@run_id, read_model: :unknown)
  end

  test "returns a structured error for malformed option lists" do
    assert {:error, {:invalid_option, {:opts, [:bad]}}} =
             SquidMesh.inspect_run(@run_id, [:bad])

    assert {:error, {:invalid_option, {:opts, [:bad]}}} =
             SquidMesh.explain_run(@run_id, [:bad])
  end

  defp append_run_entries(entries) do
    assert {:ok, _thread} = Journal.append_entries(@storage, entries)
  end

  defp append_dispatch_entries(entries) do
    assert {:ok, _thread} = Journal.append_entries(@storage, entries)
  end

  defp run_started do
    entry!(:run_started, %{
      run_id: @run_id,
      workflow: @workflow,
      occurred_at: @started_at
    })
  end

  defp runnables_planned do
    entry!(:runnables_planned, %{
      run_id: @run_id,
      runnables: [planned_runnable()],
      occurred_at: @visible_at
    })
  end

  defp attempt_scheduled do
    entry!(:attempt_scheduled, scheduled_attrs())
  end

  defp planned_runnable do
    scheduled_attrs()
    |> Map.delete(:occurred_at)
  end

  defp scheduled_attrs do
    %{
      run_id: @run_id,
      runnable_key: @runnable_key,
      idempotency_key: @idempotency_key,
      attempt_number: 1,
      queue: @queue,
      step: "charge_card",
      input: %{"payment_id" => "pay_123"},
      visible_at: @visible_at,
      occurred_at: @started_at
    }
  end

  defp entry!(type, attrs) do
    assert {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end

  defp cleanup_storage do
    for suffix <- [:checkpoints, :threads, :thread_meta] do
      delete_table_if_present(:"squid_mesh_projected_read_model_test_#{suffix}")
    end
  end

  defp delete_table_if_present(table) do
    if :ets.whereis(table) != :undefined do
      :ets.delete(table)
    end
  rescue
    ArgumentError -> :ok
  end
end
