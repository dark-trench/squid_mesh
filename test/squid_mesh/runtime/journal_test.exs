defmodule SquidMesh.Runtime.JournalTest do
  use ExUnit.Case, async: false

  alias SquidMesh.Runtime.DispatchProtocol
  alias SquidMesh.Runtime.DispatchProtocol.Projection
  alias SquidMesh.Runtime.Journal
  alias SquidMesh.Runtime.Journal.Checkpoint

  @storage {Jido.Storage.ETS, table: :squid_mesh_journal_test}
  @run_id "run_123"
  @runnable_key "run_123:charge_card:1"
  @idempotency_key "run_123:charge_card:payment_456"
  @claim_id "claim_1"
  @claim_token_hash "token_hash_1"
  @owner_id "worker_1"
  @started_at ~U[2026-05-14 00:00:00Z]
  @visible_at ~U[2026-05-14 00:00:10Z]
  @claimed_at ~U[2026-05-14 00:00:20Z]
  @lease_until ~U[2026-05-14 00:01:00Z]
  @completed_at ~U[2026-05-14 00:00:30Z]

  setup do
    cleanup_storage()

    on_exit(fn ->
      cleanup_storage()
    end)
  end

  test "appends runtime entries to Jido storage and rebuilds dispatch projections" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, thread} = Journal.append_entries(@storage, [scheduled_entry])
    assert thread.id == "squid_mesh:dispatch:default"
    assert thread.rev == 1

    assert {:ok, restored_entries} = Journal.load_entries(@storage, {:dispatch, "default"})
    assert restored_entries == [scheduled_entry]
    assert {:ok, projection} = Journal.rebuild_dispatch_projection(@storage, "default")

    assert [%{runnable_key: @runnable_key, status: :available}] =
             Projection.visible_attempts(projection, @visible_at)
  end

  test "replays multiple dispatch entries in order and rebuilds final projection state" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, claimed_entry} =
             DispatchProtocol.new_entry(:attempt_claimed, claimed_attrs())

    assert {:ok, completed_entry} =
             DispatchProtocol.new_entry(:attempt_completed, completed_attrs())

    entries = [scheduled_entry, claimed_entry, completed_entry]

    assert {:ok, %{rev: 3}} = Journal.append_entries(@storage, entries)
    assert {:ok, ^entries} = Journal.load_entries(@storage, {:dispatch, "default"})

    assert {:ok, projection} = Journal.rebuild_dispatch_projection(@storage, "default")

    assert Projection.visible_attempts(projection, @visible_at) == []

    assert [
             %{
               runnable_key: @runnable_key,
               status: :completed,
               result: %{"status" => "captured"}
             }
           ] = Projection.completed_results(projection)
  end

  test "loads thread metadata with decoded entries" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, claimed_entry} =
             DispatchProtocol.new_entry(:attempt_claimed, claimed_attrs())

    entries = [scheduled_entry, claimed_entry]

    assert {:ok, %{rev: 2}} = Journal.append_entries(@storage, entries)

    assert {:ok,
            %{
              thread: {:dispatch, "default"},
              thread_id: "squid_mesh:dispatch:default",
              rev: 2,
              entries: ^entries
            }} = Journal.load_thread(@storage, {:dispatch, "default"})
  end

  @tag :tmp_dir
  test "restores entries through file-backed Jido storage", %{tmp_dir: tmp_dir} do
    storage = {Jido.Storage.File, path: tmp_dir}

    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, %{rev: 1}} = Journal.append_entries(storage, [scheduled_entry])
    assert {:ok, [^scheduled_entry]} = Journal.load_entries(storage, {:dispatch, "default"})

    restored_storage = {Jido.Storage.File, path: tmp_dir}
    assert {:ok, projection} = Journal.rebuild_dispatch_projection(restored_storage, "default")

    assert [%{runnable_key: @runnable_key, status: :available}] =
             Projection.visible_attempts(projection, @visible_at)
  end

  test "rejects stale optimistic appends with the current Jido thread revision" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, thread} =
             Journal.append_entries(@storage, [scheduled_entry], expected_rev: 0)

    assert thread.rev == 1

    assert {:error, :conflict} =
             Journal.append_entries(@storage, [scheduled_entry], expected_rev: 0)

    assert {:ok, thread} =
             Journal.append_entries(@storage, [scheduled_entry], expected_rev: 1)

    assert thread.rev == 2
  end

  test "stores projection checkpoints with explicit applied thread revisions" do
    assert {:ok, scheduled_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:ok, thread} = Journal.append_entries(@storage, [scheduled_entry])
    assert {:ok, entries} = Journal.load_entries(@storage, {:dispatch, "default"})

    projection = Projection.rebuild(entries)

    assert :ok =
             Journal.put_checkpoint(@storage, {:dispatch, "default"}, projection, thread.rev,
               updated_at: @visible_at
             )

    assert {:ok,
            %Checkpoint{
              thread: {:dispatch, "default"},
              thread_id: "squid_mesh:dispatch:default",
              thread_rev: 1,
              projection: ^projection,
              updated_at: @visible_at
            }} = Journal.fetch_checkpoint(@storage, {:dispatch, "default"})
  end

  test "returns structured not found errors for absent threads and checkpoints" do
    assert {:error, :not_found} = Journal.load_entries(@storage, {:dispatch, "missing"})
    assert {:error, :not_found} = Journal.load_thread(@storage, {:dispatch, "missing"})
    assert {:error, :not_found} = Journal.fetch_checkpoint(@storage, {:dispatch, "missing"})
  end

  test "returns structured errors for incompatible persisted thread entries" do
    assert {:ok, _thread} =
             Jido.Storage.ETS.append_thread(
               Journal.thread_id({:dispatch, "default"}),
               [%{kind: :note, payload: %{}}],
               table: :squid_mesh_journal_test
             )

    assert {:error, {:invalid_journal_entry, 0, :missing_data}} =
             Journal.load_thread(@storage, {:dispatch, "default"})
  end

  test "returns structured errors for invalid persisted timestamps" do
    assert {:ok, _thread} =
             Jido.Storage.ETS.append_thread(
               Journal.thread_id({:dispatch, "default"}),
               [
                 %{
                   kind: :attempt_scheduled,
                   at: "not-a-unix-millisecond",
                   payload: %{data: scheduled_attrs()}
                 }
               ],
               table: :squid_mesh_journal_test
             )

    assert {:error, {:invalid_journal_entry, 0, :invalid_timestamp}} =
             Journal.load_entries(@storage, {:dispatch, "default"})
  end

  test "rejects appending entries that belong to different durable threads" do
    assert {:ok, run_entry} =
             DispatchProtocol.new_entry(:run_started, %{
               run_id: @run_id,
               workflow: "BillingWorkflow",
               occurred_at: @started_at
             })

    assert {:ok, dispatch_entry} =
             DispatchProtocol.new_entry(:attempt_scheduled, scheduled_attrs())

    assert {:error, {:mixed_threads, [{:run, @run_id}, {:dispatch, "default"}]}} =
             Journal.append_entries(@storage, [run_entry, dispatch_entry])
  end

  defp scheduled_attrs(attrs \\ %{}) do
    Map.merge(
      %{
        run_id: @run_id,
        runnable_key: @runnable_key,
        idempotency_key: @idempotency_key,
        attempt_number: 1,
        queue: "default",
        step: "charge_card",
        input: %{"payment_id" => "pay_123"},
        visible_at: @visible_at,
        occurred_at: @started_at
      },
      Map.new(attrs)
    )
  end

  defp claimed_attrs(attrs \\ %{}) do
    Map.merge(
      %{
        run_id: @run_id,
        runnable_key: @runnable_key,
        claim_id: @claim_id,
        claim_token_hash: @claim_token_hash,
        owner_id: @owner_id,
        queue: "default",
        lease_until: @lease_until,
        occurred_at: @claimed_at
      },
      Map.new(attrs)
    )
  end

  defp completed_attrs(attrs \\ %{}) do
    Map.merge(
      %{
        run_id: @run_id,
        runnable_key: @runnable_key,
        claim_id: @claim_id,
        claim_token_hash: @claim_token_hash,
        queue: "default",
        result: %{"status" => "captured"},
        occurred_at: @completed_at
      },
      Map.new(attrs)
    )
  end

  defp cleanup_storage do
    for suffix <- [:checkpoints, :threads, :thread_meta] do
      table = :"squid_mesh_journal_test_#{suffix}"
      delete_table_if_present(table)
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
