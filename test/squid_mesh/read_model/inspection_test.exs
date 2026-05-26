defmodule SquidMesh.ReadModel.InspectionTest do
  use ExUnit.Case, async: false

  alias SquidMesh.ReadModel.Inspection
  alias SquidMesh.ReadModel.Inspection.Snapshot
  alias SquidMesh.Runtime.DispatchProtocol
  alias SquidMesh.Runtime.Journal

  @storage {Jido.Storage.ETS, table: :squid_mesh_read_model_inspection_test}
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
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @visible_at)

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
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @started_at)

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
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @started_at)

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
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @visible_at)

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
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @completed_at)

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
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @completed_at)

    assert snapshot.status == :idle
    assert snapshot.reason == :idle
    assert snapshot.applied_runnable_keys == [@runnable_key]
    assert snapshot.pending_results == []

    assert [%{runnable_key: @runnable_key, status: :completed, applied?: true}] =
             snapshot.attempts
  end

  test "exposes child runs reconstructed from the parent run thread" do
    append_run_entries([run_started(), child_run_started()])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @visible_at)

    assert [
             %{
               child_run_id: "child_run_123",
               child_workflow: @workflow,
               child_trigger: "manual",
               child_key: "digest_subscription_1",
               origin: %{
                 runnable_key: @runnable_key,
                 step: "charge_card",
                 attempt: 1
               },
               metadata: %{subscription_id: "sub_123"}
             }
           ] = snapshot.child_runs
  end

  test "exposes parent run context on child snapshots" do
    append_run_entries([
      run_started(%{
        parent: %{
          run_id: "parent_run_123",
          runnable_key: "parent_run_123:fanout:1",
          step: "fanout",
          attempt: 1,
          child_key: "digest_subscription_1",
          metadata: %{subscription_id: "sub_123"}
        }
      })
    ])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @visible_at)

    assert snapshot.parent_run == %{
             run_id: "parent_run_123",
             runnable_key: "parent_run_123:fanout:1",
             step: "fanout",
             attempt: 1,
             child_key: "digest_subscription_1",
             metadata: %{subscription_id: "sub_123"}
           }
  end

  test "merges applied result context in durable application order" do
    approval_gate_key = "#{@run_id}:wait_for_approval:1"
    record_approval_key = "#{@run_id}:record_approval:1"
    approval_recorded_at = DateTime.add(@completed_at, 1, :second)

    append_run_entries([
      run_started(),
      runnables_planned([
        planned_runnable(
          runnable_key: approval_gate_key,
          idempotency_key: approval_gate_key,
          step: "wait_for_approval"
        ),
        planned_runnable(
          runnable_key: record_approval_key,
          idempotency_key: record_approval_key,
          step: "record_approval"
        )
      ]),
      runnable_applied(
        runnable_key: approval_gate_key,
        result: %{approval: %{}},
        occurred_at: @completed_at
      ),
      runnable_applied(
        runnable_key: record_approval_key,
        result: %{approval: %{status: "approved", actor: "ops_123"}},
        occurred_at: approval_recorded_at
      ),
      run_terminal(:completed, occurred_at: approval_recorded_at)
    ])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: approval_recorded_at)

    assert snapshot.context.approval == %{status: "approved", actor: "ops_123"}
  end

  test "shows manual pause state without suggesting dispatch recovery" do
    append_run_entries([run_started(), runnables_planned(), manual_step_paused()])
    append_dispatch_entries([attempt_scheduled()])

    assert {:ok, %Snapshot{} = snapshot} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @visible_at)

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
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @expired_at)

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
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: @started_at)

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
               Inspection.snapshot(@storage, @run_id, queue: @queue, now: @expired_at)

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
             Inspection.snapshot(@storage, "missing_run",
               queue: @queue,
               now: @visible_at
             )
  end

  test "returns invalid option errors for invalid option values" do
    assert {:error, {:invalid_option, {:now, :invalid}}} =
             Inspection.snapshot(@storage, @run_id, queue: @queue, now: :soon)

    assert {:error, {:invalid_option, {:queue, :invalid}}} =
             Inspection.snapshot(@storage, @run_id,
               queue: %{name: @queue},
               now: @visible_at
             )
  end

  test "returns invalid option errors for malformed or unsupported options" do
    assert {:error, {:invalid_option, {:opts, :invalid}}} =
             Inspection.snapshot(@storage, @run_id, %{queue: @queue})

    assert {:error, {:invalid_option, {:opts, :invalid}}} =
             Inspection.snapshot(@storage, @run_id, [:bad])

    assert {:error, {:invalid_option, {:option, :unknown}}} =
             Inspection.snapshot(@storage, @run_id, unknown: true)
  end

  test "redacts malformed option values from public errors" do
    assert {:error, reason} =
             Inspection.snapshot(@storage, @run_id, %{claim_token: "super-secret-token"})

    assert reason == {:invalid_option, {:opts, :invalid}}
    refute inspect(reason) =~ "super-secret-token"

    assert {:error, reason} =
             Inspection.snapshot(@storage, @run_id, [
               {:claim_token, "super-secret-token"},
               :bad
             ])

    assert reason == {:invalid_option, {:opts, :invalid}}
    refute inspect(reason) =~ "super-secret-token"

    assert {:error, reason} =
             Inspection.snapshot(@storage, @run_id,
               queue: %{claim_token: "super-secret-token"},
               now: @visible_at
             )

    assert reason == {:invalid_option, {:queue, :invalid}}
    refute inspect(reason) =~ "super-secret-token"

    assert {:error, reason} =
             Inspection.snapshot(@storage, @run_id,
               queue: @queue,
               now: %{claim_token: "super-secret-token"}
             )

    assert reason == {:invalid_option, {:now, :invalid}}
    refute inspect(reason) =~ "super-secret-token"
  end

  defp append_run_entries(entries) do
    assert {:ok, _thread} = Journal.append_entries(@storage, entries)
  end

  defp append_dispatch_entries(entries) do
    assert {:ok, _thread} = Journal.append_entries(@storage, entries)
  end

  defp run_started(context \\ %{}) do
    entry!(:run_started, %{
      run_id: @run_id,
      workflow: @workflow,
      context: context,
      occurred_at: @started_at
    })
  end

  defp child_run_started(metadata \\ %{subscription_id: "sub_123"}) do
    entry!(:child_run_started, %{
      run_id: @run_id,
      child_run_id: "child_run_123",
      child_workflow: @workflow,
      child_trigger: "manual",
      child_key: "digest_subscription_1",
      origin: %{
        runnable_key: @runnable_key,
        step: "charge_card",
        attempt: 1
      },
      metadata: metadata,
      occurred_at: @visible_at
    })
  end

  defp runnables_planned(runnables \\ [planned_runnable()]) do
    entry!(:runnables_planned, %{
      run_id: @run_id,
      runnables: runnables,
      occurred_at: @visible_at
    })
  end

  defp runnable_applied(overrides \\ []) do
    entry!(:runnable_applied, %{
      run_id: @run_id,
      runnable_key: Keyword.get(overrides, :runnable_key, @runnable_key),
      result: Keyword.get(overrides, :result, %{"status" => "captured"}),
      occurred_at: Keyword.get(overrides, :occurred_at, @completed_at)
    })
  end

  defp run_terminal(status, overrides \\ []) do
    entry!(:run_terminal, %{
      run_id: @run_id,
      status: status,
      occurred_at: Keyword.get(overrides, :occurred_at, @completed_at)
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

  defp table_name(:checkpoints), do: :squid_mesh_read_model_inspection_test_checkpoints
  defp table_name(:threads), do: :squid_mesh_read_model_inspection_test_threads
  defp table_name(:thread_meta), do: :squid_mesh_read_model_inspection_test_thread_meta

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
