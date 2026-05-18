defmodule SquidMesh.Runtime.RunnerTest do
  use SquidMesh.DataCase, async: false

  alias SquidMesh.Executor.Payload
  alias SquidMesh.Runtime.Runner
  alias SquidMesh.Test.Executor

  defmodule IdempotentCronWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :scheduled_digest do
        cron "0 9 * * *", timezone: "UTC", idempotency: :return_existing_run
      end

      step :deliver_digest, IdempotentCronWorkflow.DeliverDigest
    end
  end

  defmodule IdempotentCronWorkflow.DeliverDigest do
    use Jido.Action,
      name: "deliver_digest",
      description: "Delivers a scheduled digest",
      schema: []

    @impl Jido.Action
    def run(_params, _context), do: {:ok, %{}}
  end

  defmodule NonIdempotentCronWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :scheduled_digest do
        cron "0 9 * * *", timezone: "UTC"
      end

      step :deliver_digest, NonIdempotentCronWorkflow.DeliverDigest
    end
  end

  defmodule NonIdempotentCronWorkflow.DeliverDigest do
    use Jido.Action,
      name: "deliver_digest",
      description: "Delivers a scheduled digest",
      schema: []

    @impl Jido.Action
    def run(_params, _context), do: {:ok, %{}}
  end

  defmodule SkipIdempotentCronWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :scheduled_digest do
        cron "0 9 * * *", timezone: "UTC", idempotency: :skip_duplicate
      end

      step :deliver_digest, SkipIdempotentCronWorkflow.DeliverDigest
    end
  end

  defmodule SkipIdempotentCronWorkflow.DeliverDigest do
    use Jido.Action,
      name: "deliver_digest",
      description: "Delivers a scheduled digest",
      schema: []

    @impl Jido.Action
    def run(_params, _context), do: {:ok, %{}}
  end

  test "reuses an existing run for duplicate idempotent cron deliveries" do
    payload = cron_payload(IdempotentCronWorkflow)

    assert :ok = Runner.perform(payload)

    assert {:ok, {:duplicate_schedule_start, duplicate_run_id}} =
             Runner.start_cron_trigger(
               payload["workflow"],
               payload["trigger"],
               payload,
               []
             )

    assert [%SquidMesh.Persistence.Run{id: run_id, context: context}] =
             Repo.all(SquidMesh.Persistence.Run)

    assert duplicate_run_id == run_id
    assert get_in(context, ["schedule", "idempotency"]) == "return_existing_run"
    assert get_in(context, ["schedule", "idempotency_key"]) == "digest-2026-05-16T09"
  end

  test "surfaces duplicate cron delivery as skipped when configured" do
    payload = cron_payload(SkipIdempotentCronWorkflow)

    assert :ok = Runner.perform(payload)

    assert {:ok, {:skipped_schedule_start, skipped_run_id}} =
             Runner.start_cron_trigger(
               payload["workflow"],
               payload["trigger"],
               payload,
               []
             )

    assert [%SquidMesh.Persistence.Run{id: run_id, context: context}] =
             Repo.all(SquidMesh.Persistence.Run)

    assert skipped_run_id == run_id
    assert get_in(context, ["schedule", "idempotency"]) == "skip_duplicate"
  end

  test "preserves reserved schedule metadata when a step returns schedule output" do
    payload = cron_payload(__MODULE__.ScheduleClobberWorkflow)

    assert :ok = Runner.perform(payload)
    assert %{success: 1, failure: 0} = Executor.drain()

    assert [%SquidMesh.Persistence.Run{context: context}] = Repo.all(SquidMesh.Persistence.Run)
    assert get_in(context, ["schedule", "idempotency"]) == "return_existing_run"
    assert get_in(context, ["schedule", "idempotency_key"]) == "digest-2026-05-16T09"
    assert get_in(context, ["digest_delivery", "ok"]) == true
  end

  test "allows duplicate cron deliveries when idempotency is not enabled" do
    payload = cron_payload(NonIdempotentCronWorkflow)

    assert :ok = Runner.perform(payload)
    assert :ok = Runner.perform(payload)

    assert Repo.aggregate(SquidMesh.Persistence.Run, :count) == 2
  end

  defp cron_payload(workflow) do
    Payload.cron(workflow, :scheduled_digest,
      signal_id: "digest-2026-05-16T09",
      intended_window: %{
        start_at: "2026-05-16T09:00:00Z",
        end_at: "2026-05-16T10:00:00Z"
      }
    )
  end

  defmodule ScheduleClobberWorkflow do
    use SquidMesh.Workflow

    workflow do
      trigger :scheduled_digest do
        cron "0 9 * * *", timezone: "UTC", idempotency: :return_existing_run
      end

      step :deliver_digest, SquidMesh.Runtime.RunnerTest.ScheduleClobberWorkflow.DeliverDigest
    end
  end

  defmodule ScheduleClobberWorkflow.DeliverDigest do
    use Jido.Action,
      name: "deliver_digest",
      description: "Delivers a scheduled digest with an accidental reserved key",
      schema: []

    @impl Jido.Action
    def run(_params, _context) do
      {:ok,
       %{
         "schedule" => %{"idempotency_key" => "string-tampered"},
         schedule: %{idempotency_key: "tampered"},
         digest_delivery: %{ok: true}
       }}
    end
  end
end
