defmodule BedrockMinimalHostApp.SquidMeshLeaseExecutorTest do
  use ExUnit.Case, async: false

  alias BedrockMinimalHostApp.JobQueue
  alias BedrockMinimalHostApp.SquidMeshLeaseExecutor

  setup do
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
             SquidMeshLeaseExecutor.claim(%{}, queue, "worker_a",
               lease_duration_ms: 1_000,
               now: now
             )

    assert claim.queue == queue
    assert claim.owner == "worker_a"
    assert claim.lease_until == now + 1_000
    assert claim.payload == %{"kind" => "step", "run_id" => "run_123", "step" => "charge_card"}

    assert {:ok, []} =
             SquidMeshLeaseExecutor.claim(%{}, queue, "worker_b",
               lease_duration_ms: 1_000,
               now: now + 500
             )

    assert {:ok, heartbeated_claim} =
             SquidMeshLeaseExecutor.heartbeat(%{}, claim,
               lease_duration_ms: 5_000,
               now: now + 500
             )

    assert heartbeated_claim.lease_until == now + 5_500
    assert heartbeated_claim.payload == claim.payload

    assert :ok = SquidMeshLeaseExecutor.complete(%{}, heartbeated_claim, [])
    assert {:ok, []} = SquidMeshLeaseExecutor.claim(%{}, queue, "worker_c", now: now + 5_500)
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

    assert {:ok, [claim]} = SquidMeshLeaseExecutor.claim(%{}, queue, "worker_a", now: now)

    assert {:ok, :requeued} =
             SquidMeshLeaseExecutor.fail(%{}, claim, %{message: "gateway timeout"},
               base_delay: 1_000,
               now: now
             )

    assert {:ok, []} = SquidMeshLeaseExecutor.claim(%{}, queue, "worker_b", now: now + 999)

    assert {:ok, [retry_claim]} =
             SquidMeshLeaseExecutor.claim(%{}, queue, "worker_b", now: now + 1_000)

    assert retry_claim.payload == %{
             "kind" => "step",
             "run_id" => "run_456",
             "step" => "capture_payment"
           }

    assert {:ok, :dead_lettered} =
             SquidMeshLeaseExecutor.fail(%{}, retry_claim, %{message: "still failing"},
               base_delay: 1_000,
               now: now + 1_000
             )

    assert %{pending_count: 0, processing_count: 0} = JobQueue.stats(queue)
  end
end
