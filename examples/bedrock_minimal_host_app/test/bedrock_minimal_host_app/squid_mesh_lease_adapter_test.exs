defmodule BedrockMinimalHostApp.SquidMeshLeaseAdapterTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL.Sandbox
  alias BedrockMinimalHostApp.Jobs.SquidMeshPayload
  alias BedrockMinimalHostApp.JobQueue
  alias BedrockMinimalHostApp.Repo
  alias BedrockMinimalHostApp.SquidMeshLeaseAdapter
  alias BedrockMinimalHostApp.WorkflowRuns
  alias BedrockMinimalHostApp.Workflows.DailyDigest
  alias SquidMesh.Executor.Payload

  setup do
    :ok = Sandbox.checkout(Repo)
    cleanup_runtime_state()
    original_payload_config = Application.get_env(:bedrock_minimal_host_app, SquidMeshPayload, [])

    on_exit(fn ->
      Application.put_env(:bedrock_minimal_host_app, SquidMeshPayload, original_payload_config)
    end)

    queue = "lease_tenant_#{System.unique_integer([:positive])}"
    {:ok, queue: queue}
  end

  test "claims, heartbeats, and completes leased Squid Mesh payloads", %{queue: queue} do
    now = System.system_time(:millisecond)

    assert {:ok, _item_id} =
             JobQueue.enqueue(
               queue,
               "squid_mesh:payload",
               %{"kind" => "step", "run_id" => "run_123", "step" => "charge_card"},
               now: now
             )

    assert {:ok, [claim]} =
             SquidMeshLeaseAdapter.claim(%{}, queue, "worker_a",
               lease_duration_ms: 1_000,
               now: now
             )

    assert claim.queue == queue
    assert claim.owner == "worker_a"
    assert claim.lease_until == now + 1_000
    assert claim.payload == %{"kind" => "step", "run_id" => "run_123", "step" => "charge_card"}

    assert {:ok, []} =
             SquidMeshLeaseAdapter.claim(%{}, queue, "worker_b",
               lease_duration_ms: 1_000,
               now: now + 500
             )

    assert {:ok, heartbeated_claim} =
             SquidMeshLeaseAdapter.heartbeat(%{}, claim,
               lease_duration_ms: 5_000,
               now: now + 500
             )

    assert heartbeated_claim.lease_until == now + 5_500
    assert heartbeated_claim.payload == claim.payload

    assert :ok = SquidMeshLeaseAdapter.complete(%{}, heartbeated_claim, [])
    assert {:ok, []} = SquidMeshLeaseAdapter.claim(%{}, queue, "worker_c", now: now + 5_500)
    assert %{pending_count: 0, processing_count: 0} = JobQueue.stats(queue)
  end

  test "fails claims through backend retry and dead-letter policy", %{queue: queue} do
    now = System.system_time(:millisecond)

    assert {:ok, _item_id} =
             JobQueue.enqueue(
               queue,
               "squid_mesh:payload",
               %{"kind" => "step", "run_id" => "run_456", "step" => "capture_payment"},
               max_retries: 2,
               now: now
             )

    assert {:ok, [claim]} = SquidMeshLeaseAdapter.claim(%{}, queue, "worker_a", now: now)

    assert {:ok, :requeued} =
             SquidMeshLeaseAdapter.fail(%{}, claim, %{message: "gateway timeout"},
               base_delay: 1_000,
               now: now
             )

    assert {:ok, []} = SquidMeshLeaseAdapter.claim(%{}, queue, "worker_b", now: now + 999)

    assert {:ok, [retry_claim]} =
             SquidMeshLeaseAdapter.claim(%{}, queue, "worker_b", now: now + 1_000)

    assert retry_claim.payload == %{
             "kind" => "step",
             "run_id" => "run_456",
             "step" => "capture_payment"
           }

    assert {:ok, :dead_lettered} =
             SquidMeshLeaseAdapter.fail(%{}, retry_claim, %{message: "still failing"},
               base_delay: 1_000,
               now: now + 1_000
             )

    assert %{pending_count: 0, processing_count: 0} = JobQueue.stats(queue)
  end

  test "executes a journal workflow while holding a Bedrock lease", %{queue: queue} do
    payload =
      Payload.cron(
        DailyDigest,
        :daily_digest,
        signal_id: "bedrock-lease-daily-digest-#{System.unique_integer([:positive])}"
      )

    assert {:ok, _item_id} = JobQueue.enqueue(queue, "squid_mesh:payload", payload)
    assert {:ok, [claim]} = SquidMeshLeaseAdapter.claim(%{}, queue, "worker_a", [])

    assert :ok = SquidMeshPayload.perform(claim.payload, %{})
    assert :ok = SquidMeshLeaseAdapter.complete(%{}, claim, [])

    assert {:ok, [run]} = WorkflowRuns.list_daily_digest_runs()
    assert run.status == :completed

    assert {:ok, inspected_run} = WorkflowRuns.inspect_run(run.run_id)
    assert inspected_run.status == :completed
    assert inspected_run.context.digest_delivery.channel == "ops"
  end

  test "requeues a leased journal payload after a bounded drain failure", %{queue: queue} do
    now = System.system_time(:millisecond)

    payload =
      Payload.cron(
        DailyDigest,
        :daily_digest,
        signal_id: "bedrock-lease-retry-daily-digest-#{System.unique_integer([:positive])}"
      )

    Application.put_env(:bedrock_minimal_host_app, SquidMeshPayload, max_journal_attempts: 1)

    assert {:ok, _item_id} = JobQueue.enqueue(queue, "squid_mesh:payload", payload, now: now)
    assert {:ok, [claim]} = SquidMeshLeaseAdapter.claim(%{}, queue, "worker_a", now: now)

    assert {:error, :journal_drain_limit_exceeded} = SquidMeshPayload.perform(claim.payload, %{})

    assert {:ok, :requeued} =
             SquidMeshLeaseAdapter.fail(%{}, claim, %{message: "drain limit"},
               base_delay: 1,
               now: now
             )

    assert {:ok, [retry_claim]} =
             SquidMeshLeaseAdapter.claim(%{}, queue, "worker_b", now: now + 1)

    Application.put_env(:bedrock_minimal_host_app, SquidMeshPayload, max_journal_attempts: 50)

    assert :ok = SquidMeshPayload.perform(retry_claim.payload, %{})
    assert :ok = SquidMeshLeaseAdapter.complete(%{}, retry_claim, [])

    assert {:ok, [run]} = WorkflowRuns.list_daily_digest_runs()
    assert run.status == :completed
  end

  defp cleanup_runtime_state do
    Repo.delete_all("squid_mesh_journal_entries")
    Repo.delete_all("squid_mesh_journal_checkpoints")
    Repo.delete_all("squid_mesh_journal_threads")
  end
end
