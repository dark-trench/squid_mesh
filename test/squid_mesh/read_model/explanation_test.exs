defmodule SquidMesh.ReadModel.ExplanationTest do
  use ExUnit.Case, async: false

  alias SquidMesh.ReadModel.Explanation
  alias SquidMesh.ReadModel.Explanation.Diagnostic
  alias SquidMesh.ReadModel.Inspection.Snapshot
  alias SquidMesh.Runtime.DispatchProtocol
  alias SquidMesh.Runtime.Journal

  @storage {Jido.Storage.ETS, table: :squid_mesh_read_model_explanation_test}
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

  test "explains planned runnables that are missing dispatch entries" do
    append_run_entries([run_started(), runnables_planned()])

    assert {:ok, %Diagnostic{} = explanation} =
             Explanation.explain(@storage, @run_id, queue: @queue, now: @visible_at)

    assert explanation.run_id == @run_id
    assert explanation.workflow == @workflow
    assert explanation.queue == @queue
    assert explanation.status == :running
    assert explanation.reason == :planned_dispatch_pending_schedule
    assert explanation.step == "charge_card"
    assert explanation.next_actions == [:schedule_pending_dispatch]

    assert explanation.details == %{
             pending_dispatch_count: 1,
             runnable_keys: [@runnable_key]
           }

    assert explanation.evidence.thread_revisions == %{run: 2, dispatch: 0}
    assert explanation.evidence.snapshot_reason == :planned_dispatch_pending_schedule
  end

  test "explains attempts scheduled for future visibility" do
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

    assert {:ok, %Diagnostic{} = explanation} =
             Explanation.explain(@storage, @run_id, queue: @queue, now: @started_at)

    assert explanation.reason == :attempt_scheduled_for_later
    assert explanation.step == "charge_card"
    assert explanation.next_actions == [:wait_until_attempt_visible]

    assert explanation.details == %{
             scheduled_attempt_count: 2,
             runnable_keys: [@runnable_key, @second_runnable_key],
             next_visible_at: @visible_at
           }

    assert explanation.evidence.next_visible_at == @visible_at
    assert explanation.evidence.attempt_counts.available == 2
  end

  test "explains completed dispatch results that are not applied to the run thread" do
    append_run_entries([run_started(), runnables_planned()])
    append_dispatch_entries([attempt_scheduled(), attempt_claimed(), attempt_completed()])

    assert {:ok, %Diagnostic{} = explanation} =
             Explanation.explain(@storage, @run_id, queue: @queue, now: @completed_at)

    assert explanation.reason == :completed_result_pending_apply
    assert explanation.step == "charge_card"
    assert explanation.next_actions == [:apply_pending_result]

    assert explanation.details == %{
             pending_result_count: 1,
             runnable_keys: [@runnable_key]
           }

    assert explanation.evidence.attempt_counts.completed == 1
  end

  test "explains expired claims as recoverable dispatch work" do
    append_run_entries([run_started(), runnables_planned()])
    append_dispatch_entries([attempt_scheduled(), attempt_claimed()])

    assert {:ok, %Diagnostic{} = explanation} =
             Explanation.explain(@storage, @run_id, queue: @queue, now: @expired_at)

    assert explanation.reason == :expired_claim
    assert explanation.step == "charge_card"
    assert explanation.next_actions == [:recover_expired_claim]

    assert explanation.details == %{
             expired_claim_count: 1,
             runnable_keys: [@runnable_key],
             oldest_lease_until: @lease_until
           }
  end

  test "explains manual pause state as operator intervention" do
    append_run_entries([run_started(), runnables_planned(), manual_step_paused()])
    append_dispatch_entries([attempt_scheduled()])

    assert {:ok, %Diagnostic{} = explanation} =
             Explanation.explain(@storage, @run_id, queue: @queue, now: @visible_at)

    assert explanation.status == :paused
    assert explanation.reason == :manual_intervention_required
    assert explanation.step == "wait_for_review"
    assert explanation.next_actions == [:resolve_manual_step]

    assert explanation.details == %{
             step: "wait_for_review",
             kind: "approval",
             paused_at: @completed_at,
             metadata: %{output_key: "approval"}
           }

    assert explanation.evidence.manual_state == explanation.details
  end

  test "explains terminal runs without suggesting dispatch recovery" do
    append_run_entries([run_started(), runnables_planned(), run_terminal(:completed)])
    append_dispatch_entries([attempt_scheduled(), attempt_claimed()])

    assert {:ok, %Diagnostic{} = explanation} =
             Explanation.explain(@storage, @run_id, queue: @queue, now: @expired_at)

    assert explanation.status == :completed
    assert explanation.reason == :terminal
    assert explanation.step == nil
    assert explanation.next_actions == [:inspect_terminal_run]
    assert explanation.details == %{terminal?: true, terminal_status: :completed}
    assert explanation.evidence.terminal_status == :completed
  end

  test "explains failed and cancelled terminal runs with their terminal status" do
    for status <- [:failed, :cancelled] do
      cleanup_storage()
      append_run_entries([run_started(), runnables_planned(), run_terminal(status)])
      append_dispatch_entries([attempt_scheduled(), attempt_claimed()])

      assert {:ok, %Diagnostic{} = explanation} =
               Explanation.explain(@storage, @run_id, queue: @queue, now: @expired_at)

      assert explanation.status == status
      assert explanation.reason == :terminal
      assert explanation.step == nil
      assert explanation.next_actions == [:inspect_terminal_run]
      assert explanation.details == %{terminal?: true, terminal_status: status}
      assert explanation.evidence.terminal_status == status
    end
  end

  test "derives an explanation from an existing snapshot without rereading storage" do
    snapshot = %Snapshot{
      run_id: @run_id,
      workflow: @workflow,
      queue: @queue,
      status: :running,
      reason: :attempt_visible,
      terminal?: false,
      terminal_status: nil,
      thread_revisions: %{run: 2, dispatch: 1},
      planned_runnable_keys: [@runnable_key],
      visible_attempts: [
        %{
          "runnable_key" => @runnable_key,
          "step" => "charge_card",
          "status" => :available,
          "visible_at" => @visible_at
        }
      ],
      attempts: [
        %{
          "runnable_key" => @runnable_key,
          "step" => "charge_card",
          "status" => :available,
          "visible_at" => @visible_at
        }
      ]
    }

    explanation = Explanation.from_snapshot(snapshot)

    assert explanation.reason == :attempt_visible
    assert explanation.step == "charge_card"
    assert explanation.next_actions == [:wait_for_worker_claim]
    assert explanation.details.runnable_keys == [@runnable_key]
    assert explanation.evidence.attempt_counts.available == 1
  end

  test "returns projected inspection errors unchanged" do
    assert {:error, :not_found} =
             Explanation.explain(@storage, "missing_run",
               queue: @queue,
               now: @visible_at
             )

    assert {:error, {:invalid_option, {:option, :unknown}}} =
             Explanation.explain(@storage, @run_id, unknown: true)
  end

  test "returns a structured error for invalid run identifiers" do
    assert {:error, {:invalid_option, {:run_id, 123}}} =
             Explanation.explain(@storage, 123, queue: @queue, now: @visible_at)
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

  defp table_name(:checkpoints), do: :squid_mesh_read_model_explanation_test_checkpoints
  defp table_name(:threads), do: :squid_mesh_read_model_explanation_test_threads
  defp table_name(:thread_meta), do: :squid_mesh_read_model_explanation_test_thread_meta

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
