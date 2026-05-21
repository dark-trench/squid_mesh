defmodule SquidMesh.Steps.StoreTest do
  use SquidMesh.DataCase, async: false

  alias SquidMesh.AttemptStore
  alias SquidMesh.Steps.Store

  test "claims a step once and skips concurrent duplicate claims" do
    {:ok, run} =
      %SquidMesh.Persistence.Run{}
      |> SquidMesh.Persistence.Run.changeset(%{
        workflow: "Elixir.SquidMesh.Steps.StoreTest.Workflow",
        trigger: "manual",
        status: "running",
        input: %{}
      })
      |> Repo.insert()

    results =
      1..4
      |> Task.async_stream(
        fn _ignored_index ->
          Store.begin_step(Repo, run.id, :load_invoice, %{account_id: "acct_123"})
        end,
        max_concurrency: 4,
        ordered: false,
        timeout: 5_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(results, fn
             {:ok, _step_run, :execute} -> true
             _other -> false
           end) == 1

    assert Enum.count(results, fn
             {:ok, _step_run, :skip} -> true
             _other -> false
           end) == 3
  end

  test "claims a scheduled step and skips duplicate schedules" do
    {:ok, run} =
      %SquidMesh.Persistence.Run{}
      |> SquidMesh.Persistence.Run.changeset(%{
        workflow: "Elixir.SquidMesh.Steps.StoreTest.Workflow",
        trigger: "manual",
        status: "running",
        input: %{}
      })
      |> Repo.insert()

    assert {:ok, scheduled_step_run, :schedule} =
             Store.schedule_step(Repo, run.id, :load_invoice, %{account_id: "acct_123"})

    assert scheduled_step_run.status == "pending"

    Process.sleep(1)

    assert {:ok, duplicate_schedule, :skip} =
             Store.schedule_step(Repo, run.id, :load_invoice, %{account_id: "acct_123"})

    assert duplicate_schedule.id == scheduled_step_run.id
    assert duplicate_schedule.status == "pending"

    assert {:ok, claimed_step_run, :execute} =
             Store.begin_step(Repo, run.id, :load_invoice, %{account_id: "acct_123"})

    assert claimed_step_run.id == scheduled_step_run.id
    assert claimed_step_run.status == "running"
    assert DateTime.compare(claimed_step_run.updated_at, scheduled_step_run.updated_at) == :gt

    assert {:ok, duplicate_claim, :skip} =
             Store.begin_step(Repo, run.id, :load_invoice, %{account_id: "acct_123"})

    assert duplicate_claim.id == scheduled_step_run.id
    assert duplicate_claim.status == "running"
  end

  test "rejects stale terminal updates after a step is finalized" do
    {:ok, run} =
      %SquidMesh.Persistence.Run{}
      |> SquidMesh.Persistence.Run.changeset(%{
        workflow: "Elixir.SquidMesh.Steps.StoreTest.Workflow",
        trigger: "manual",
        status: "running",
        input: %{}
      })
      |> Repo.insert()

    assert {:ok, step_run, :execute} =
             Store.begin_step(Repo, run.id, :load_invoice, %{account_id: "acct_123"})

    assert {:ok, completed_step} =
             Store.complete_step(Repo, step_run.id, %{invoice: %{id: "inv_456"}})

    assert completed_step.status == "completed"

    assert {:error, {:stale_step_run, "completed"}} =
             Store.fail_step(Repo, step_run.id, %{message: "late failure"})

    assert {:error, {:stale_step_run, "completed"}} =
             Store.complete_step(Repo, step_run.id, %{invoice: %{id: "inv_999"}})

    assert Repo.get!(SquidMesh.Persistence.StepRun, step_run.id).output == %{
             "invoice" => %{"id" => "inv_456"}
           }
  end

  test "orders compensation by forward completion time after recovery metadata updates" do
    {:ok, run} =
      %SquidMesh.Persistence.Run{}
      |> SquidMesh.Persistence.Run.changeset(%{
        workflow: "Elixir.SquidMesh.Steps.StoreTest.Workflow",
        trigger: "manual",
        status: "failed",
        input: %{}
      })
      |> Repo.insert()

    {:ok, first_step, :execute} =
      Store.begin_step(Repo, run.id, :reserve_inventory, %{order_id: "ord_123"})

    {:ok, first_attempt} = AttemptStore.begin_attempt(Repo, first_step.id)
    {:ok, _first_attempt} = AttemptStore.complete_attempt(Repo, first_attempt.id)

    {:ok, completed_first_step} =
      Store.complete_step(Repo, first_step.id, %{reserved: true})

    Process.sleep(1)

    {:ok, second_step, :execute} =
      Store.begin_step(Repo, run.id, :authorize_payment, %{order_id: "ord_123"})

    {:ok, second_attempt} = AttemptStore.begin_attempt(Repo, second_step.id)
    {:ok, _second_attempt} = AttemptStore.complete_attempt(Repo, second_attempt.id)

    {:ok, completed_second_step} =
      Store.complete_step(Repo, second_step.id, %{authorized: true})

    Process.sleep(1)

    assert {:ok, _updated_first_step} =
             Store.update_recovery(Repo, completed_first_step.id, %{
               compensation: %{status: :completed}
             })

    assert Enum.map(
             Store.completed_step_runs_for_compensation(Repo, run.id),
             & &1.id
           ) == [completed_second_step.id, completed_first_step.id]
  end
end
