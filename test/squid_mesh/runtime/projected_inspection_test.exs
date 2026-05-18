defmodule SquidMesh.Runtime.ProjectedInspectionTest do
  use ExUnit.Case, async: false

  alias SquidMesh.Runtime.DispatchProtocol
  alias SquidMesh.Runtime.Journal
  alias SquidMesh.Runtime.ProjectedInspection
  alias SquidMesh.Runtime.ProjectedInspection.Snapshot

  @storage {Jido.Storage.ETS, table: :squid_mesh_projected_inspection_test}
  @run_id "run_123"
  @workflow "BillingWorkflow"
  @queue "default"
  @runnable_key "run_123:charge_card:1"
  @second_runnable_key "run_123:send_receipt:1"
  @idempotency_key "run_123:charge_card:payment_456"
  @second_idempotency_key "run_123:send_receipt:payment_456"
  @started_at ~U[2026-05-15 00:00:00Z]
  @visible_at ~U[2026-05-15 00:00:10Z]
  @later_visible_at ~U[2026-05-15 00:00:30Z]
  @claimed_at ~U[2026-05-15 00:00:20Z]
  @completed_at ~U[2026-05-15 00:00:40Z]
  @lease_until ~U[2026-05-15 00:01:00Z]
  @expired_at ~U[2026-05-15 00:01:01Z]

  setup do
    cleanup_storage()
    on_exit(&cleanup_storage/0)
  end

  test "builds a snapshot from run and dispatch projections" do
    append_run_entries([run_started(), runnables_planned()])
    append_dispatch_entries([attempt_scheduled()])

    assert {:ok, %Snapshot{} = snapshot} =
             ProjectedInspection.snapshot(@storage, @run_id, queue: @queue, now: @visible_at)

    assert snapshot.run_id == @run_id
    assert snapshot.workflow == @workflow
    assert snapshot.queue == @queue
    assert snapshot.status == :running
    assert snapshot.reason == :attempt_visible
    assert snapshot.thread_revisions == %{run: 2, dispatch: 1}
    assert snapshot.planned_runnable_keys == [@runnable_key]
    assert snapshot.applied_runnable_keys == []
    assert [%{runnable_key: @runnable_key, status: :available}] = snapshot.visible_attempts
    assert snapshot.pending_dispatches == []
    assert snapshot.pending_results == []
    assert snapshot.expired_claims == []
    assert snapshot.terminal? == false
  end

  test "shows scheduled attempts before they become visible" do
    append_run_entries([run_started(), runnables_planned()])
    append_dispatch_entries([attempt_scheduled()])

    assert {:ok, %Snapshot{} = snapshot} =
             ProjectedInspection.snapshot(@storage, @run_id, queue: @queue, now: @started_at)

    assert snapshot.status == :running
    assert snapshot.reason == :attempt_scheduled_for_later
    assert snapshot.visible_attempts == []
    assert snapshot.expired_claims == []

    assert [
             %{
               runnable_key: @runnable_key,
               status: :available,
               visible_at: @visible_at
             }
           ] = snapshot.scheduled_attempts
  end

  test "reports the earliest visible time across scheduled attempts" do
    append_run_entries([
      run_started(),
      runnables_planned([
        planned_runnable(),
        planned_runnable(
          idempotency_key: @second_idempotency_key,
          runnable_key: @second_runnable_key,
          step: "send_receipt",
          visible_at: @later_visible_at
        )
      ])
    ])

    append_dispatch_entries([
      attempt_scheduled(),
      attempt_scheduled(
        idempotency_key: @second_idempotency_key,
        runnable_key: @second_runnable_key,
        step: "send_receipt",
        visible_at: @later_visible_at
      )
    ])

    assert {:ok, %Snapshot{} = snapshot} =
             ProjectedInspection.snapshot(@storage, @run_id, queue: @queue, now: @started_at)

    assert snapshot.reason == :attempt_scheduled_for_later
    assert snapshot.next_visible_at == @visible_at

    assert Enum.map(snapshot.scheduled_attempts, & &1.runnable_key) == [
             @runnable_key,
             @second_runnable_key
           ]
  end

  test "shows planned runnables that have not been durably scheduled yet" do
    append_run_entries([run_started(), runnables_planned()])

    assert {:ok, %Snapshot{} = snapshot} =
             ProjectedInspection.snapshot(@storage, @run_id, queue: @queue, now: @visible_at)

    assert snapshot.status == :running
    assert snapshot.reason == :planned_dispatch_pending_schedule
    assert snapshot.thread_revisions == %{run: 2, dispatch: 0}

    assert [
             %{
               runnable_key: @runnable_key,
               step: "charge_card",
               input: %{"payment_id" => "pay_123"}
             }
           ] = snapshot.pending_dispatches

    assert snapshot.visible_attempts == []
    assert snapshot.attempts == []
  end

  test "shows completed dispatch results that are not applied to the run thread yet" do
    append_run_entries([run_started(), runnables_planned()])
    append_dispatch_entries([attempt_scheduled(), attempt_claimed(), attempt_completed()])

    assert {:ok, %Snapshot{} = snapshot} =
             ProjectedInspection.snapshot(@storage, @run_id, queue: @queue, now: @completed_at)

    assert snapshot.status == :running
    assert snapshot.reason == :completed_result_pending_apply

    assert [
             %{
               runnable_key: @runnable_key,
               status: :completed,
               result: %{"status" => "captured"},
               applied?: false
             }
           ] = snapshot.pending_results

    assert snapshot.visible_attempts == []
    assert snapshot.expired_claims == []
  end

  test "derives idle reason from applied run-thread facts" do
    append_run_entries([run_started(), runnables_planned(), runnable_applied()])
    append_dispatch_entries([attempt_scheduled(), attempt_claimed(), attempt_completed()])

    assert {:ok, %Snapshot{} = snapshot} =
             ProjectedInspection.snapshot(@storage, @run_id, queue: @queue, now: @completed_at)

    assert snapshot.status == :idle
    assert snapshot.reason == :idle
    assert snapshot.applied_runnable_keys == [@runnable_key]
    assert snapshot.pending_results == []

    assert [%{runnable_key: @runnable_key, status: :completed, applied?: false}] =
             snapshot.attempts
  end

  test "shows manual pause state without suggesting dispatch recovery" do
    append_run_entries([run_started(), runnables_planned(), manual_step_paused()])
    append_dispatch_entries([attempt_scheduled()])

    assert {:ok, %Snapshot{} = snapshot} =
             ProjectedInspection.snapshot(@storage, @run_id, queue: @queue, now: @visible_at)

    assert snapshot.status == :paused
    assert snapshot.reason == :manual_intervention_required

    assert snapshot.manual_state == %{
             step: "wait_for_review",
             kind: "approval",
             paused_at: @completed_at,
             metadata: %{output_key: "approval"}
           }

    assert [%{runnable_key: @runnable_key, status: :available}] = snapshot.visible_attempts
  end

  test "uses terminal run facts to suppress dispatch redelivery views" do
    append_run_entries([run_started(), runnables_planned(), run_terminal(:completed)])
    append_dispatch_entries([attempt_scheduled(), attempt_claimed()])

    assert {:ok, %Snapshot{} = snapshot} =
             ProjectedInspection.snapshot(@storage, @run_id, queue: @queue, now: @expired_at)

    assert snapshot.status == :completed
    assert snapshot.reason == :terminal
    assert snapshot.terminal? == true
    assert snapshot.terminal_status == :completed
    assert snapshot.scheduled_attempts == []
    assert snapshot.visible_attempts == []
    assert snapshot.expired_claims == []
    assert [%{runnable_key: @runnable_key, status: :claimed}] = snapshot.attempts
  end

  test "uses terminal run facts to suppress scheduled attempt views" do
    append_run_entries([run_started(), runnables_planned(), run_terminal(:cancelled)])
    append_dispatch_entries([attempt_scheduled()])

    assert {:ok, %Snapshot{} = snapshot} =
             ProjectedInspection.snapshot(@storage, @run_id, queue: @queue, now: @started_at)

    assert snapshot.status == :cancelled
    assert snapshot.reason == :terminal
    assert snapshot.manual_state == nil
    assert snapshot.scheduled_attempts == []
    assert snapshot.visible_attempts == []
    assert [%{runnable_key: @runnable_key, status: :available}] = snapshot.attempts
  end

  test "keeps failed and cancelled terminal statuses visible in snapshots" do
    for status <- [:failed, :cancelled] do
      cleanup_storage()
      append_run_entries([run_started(), runnables_planned(), run_terminal(status)])
      append_dispatch_entries([attempt_scheduled(), attempt_claimed()])

      assert {:ok, %Snapshot{} = snapshot} =
               ProjectedInspection.snapshot(@storage, @run_id, queue: @queue, now: @expired_at)

      assert snapshot.status == status
      assert snapshot.reason == :terminal
      assert snapshot.terminal? == true
      assert snapshot.terminal_status == status
      assert snapshot.visible_attempts == []
      assert snapshot.expired_claims == []
    end
  end

  test "returns not found when the run thread is missing" do
    assert {:error, :not_found} =
             ProjectedInspection.snapshot(@storage, "missing_run",
               queue: @queue,
               now: @visible_at
             )
  end

  test "returns invalid option errors for invalid option values" do
    assert {:error, {:invalid_option, {:now, :soon}}} =
             ProjectedInspection.snapshot(@storage, @run_id, queue: @queue, now: :soon)

    assert {:error, {:invalid_option, {:queue, %{name: @queue}}}} =
             ProjectedInspection.snapshot(@storage, @run_id,
               queue: %{name: @queue},
               now: @visible_at
             )
  end

  test "returns invalid option errors for malformed or unsupported options" do
    assert {:error, {:invalid_option, {:opts, %{queue: @queue}}}} =
             ProjectedInspection.snapshot(@storage, @run_id, %{queue: @queue})

    assert {:error, {:invalid_option, {:opts, [:bad]}}} =
             ProjectedInspection.snapshot(@storage, @run_id, [:bad])

    assert {:error, {:invalid_option, {:option, :unknown}}} =
             ProjectedInspection.snapshot(@storage, @run_id, unknown: true)
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

  defp runnables_planned(runnables \\ [planned_runnable()]) do
    entry!(:runnables_planned, %{
      run_id: @run_id,
      runnables: runnables,
      occurred_at: @visible_at
    })
  end

  defp runnable_applied do
    entry!(:runnable_applied, %{
      run_id: @run_id,
      runnable_key: @runnable_key,
      result: %{"status" => "captured"},
      occurred_at: @completed_at
    })
  end

  defp run_terminal(status) do
    entry!(:run_terminal, %{
      run_id: @run_id,
      status: status,
      occurred_at: @completed_at
    })
  end

  defp manual_step_paused do
    entry!(:manual_step_paused, %{
      run_id: @run_id,
      step: :wait_for_review,
      kind: :approval,
      metadata: %{output_key: "approval"},
      occurred_at: @completed_at
    })
  end

  defp attempt_scheduled(overrides \\ []) do
    entry!(:attempt_scheduled, scheduled_attrs(overrides))
  end

  defp attempt_claimed do
    entry!(:attempt_claimed, %{
      run_id: @run_id,
      runnable_key: @runnable_key,
      claim_id: "claim_1",
      claim_token_hash: "token_hash_1",
      owner_id: "worker_1",
      queue: @queue,
      lease_until: @lease_until,
      occurred_at: @claimed_at
    })
  end

  defp attempt_completed do
    entry!(:attempt_completed, %{
      run_id: @run_id,
      runnable_key: @runnable_key,
      claim_id: "claim_1",
      claim_token_hash: "token_hash_1",
      queue: @queue,
      result: %{"status" => "captured"},
      occurred_at: @completed_at
    })
  end

  defp planned_runnable(overrides \\ []) do
    overrides
    |> scheduled_attrs()
    |> Map.delete(:occurred_at)
  end

  defp scheduled_attrs(overrides) do
    base = %{
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

    Map.merge(base, Map.new(overrides))
  end

  defp entry!(type, attrs) do
    assert {:ok, entry} = DispatchProtocol.new_entry(type, attrs)
    entry
  end

  defp table_name(:checkpoints), do: :squid_mesh_projected_inspection_test_checkpoints
  defp table_name(:threads), do: :squid_mesh_projected_inspection_test_threads
  defp table_name(:thread_meta), do: :squid_mesh_projected_inspection_test_thread_meta

  defp cleanup_storage do
    for suffix <- [:checkpoints, :threads, :thread_meta] do
      delete_table_if_present(table_name(suffix))
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
