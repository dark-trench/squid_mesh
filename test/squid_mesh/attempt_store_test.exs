defmodule SquidMesh.AttemptStoreTest do
  use SquidMesh.DataCase, async: false

  alias SquidMesh.AttemptStore
  alias SquidMesh.Persistence.StepRun

  test "records attempt history for a step run" do
    {:ok, run} =
      %SquidMesh.Persistence.Run{}
      |> SquidMesh.Persistence.Run.changeset(%{
        workflow: "Elixir.SquidMesh.AttemptStoreTest.Workflow",
        trigger: "manual",
        status: "pending",
        input: %{}
      })
      |> Repo.insert()

    {:ok, step_run} =
      %StepRun{}
      |> StepRun.changeset(%{
        run_id: run.id,
        step: "load_invoice",
        status: "running"
      })
      |> Repo.insert()

    assert {:ok, attempt_one} = AttemptStore.begin_attempt(Repo, step_run.id)

    assert {:ok, attempt_one} =
             AttemptStore.fail_attempt(Repo, attempt_one.id, %{message: "timeout"})

    assert {:ok, attempt_two} = AttemptStore.begin_attempt(Repo, step_run.id)

    assert {:ok, attempt_two} =
             AttemptStore.fail_attempt(Repo, attempt_two.id, %{message: "still failing"})

    assert attempt_one.attempt_number == 1
    assert attempt_two.attempt_number == 2
    assert AttemptStore.attempt_count(Repo, step_run.id) == 2

    assert AttemptStore.latest_attempt(Repo, step_run.id).attempt_number == 2
  end

  test "allocates unique attempt numbers under concurrent writers" do
    {:ok, run} =
      %SquidMesh.Persistence.Run{}
      |> SquidMesh.Persistence.Run.changeset(%{
        workflow: "Elixir.SquidMesh.AttemptStoreTest.Workflow",
        trigger: "manual",
        status: "pending",
        input: %{}
      })
      |> Repo.insert()

    {:ok, step_run} =
      %StepRun{}
      |> StepRun.changeset(%{
        run_id: run.id,
        step: "load_invoice",
        status: "running"
      })
      |> Repo.insert()

    attempts =
      1..4
      |> Task.async_stream(
        fn _ignored_index ->
          {:ok, attempt} = AttemptStore.begin_attempt(Repo, step_run.id)
          attempt
        end,
        max_concurrency: 4,
        ordered: false,
        timeout: 5_000
      )
      |> Enum.map(fn {:ok, attempt} -> attempt end)

    assert Enum.sort(Enum.map(attempts, & &1.attempt_number)) == [1, 2, 3, 4]
    assert AttemptStore.attempt_count(Repo, step_run.id) == 4
  end

  test "rejects stale terminal updates after an attempt is finalized" do
    {:ok, run} =
      %SquidMesh.Persistence.Run{}
      |> SquidMesh.Persistence.Run.changeset(%{
        workflow: "Elixir.SquidMesh.AttemptStoreTest.Workflow",
        trigger: "manual",
        status: "pending",
        input: %{}
      })
      |> Repo.insert()

    {:ok, step_run} =
      %StepRun{}
      |> StepRun.changeset(%{
        run_id: run.id,
        step: "load_invoice",
        status: "running"
      })
      |> Repo.insert()

    assert {:ok, attempt} = AttemptStore.begin_attempt(Repo, step_run.id)
    assert {:ok, completed_attempt} = AttemptStore.complete_attempt(Repo, attempt.id)
    assert completed_attempt.status == "completed"

    assert {:error, {:stale_attempt, "completed"}} =
             AttemptStore.fail_attempt(Repo, attempt.id, %{message: "late failure"})

    assert Repo.get!(SquidMesh.Persistence.StepAttempt, attempt.id).status == "completed"
  end

  test "requires the attempt to be the latest running attempt" do
    {:ok, run} =
      %SquidMesh.Persistence.Run{}
      |> SquidMesh.Persistence.Run.changeset(%{
        workflow: "Elixir.SquidMesh.AttemptStoreTest.Workflow",
        trigger: "manual",
        status: "pending",
        input: %{}
      })
      |> Repo.insert()

    {:ok, step_run} =
      %StepRun{}
      |> StepRun.changeset(%{
        run_id: run.id,
        step: "wait_for_approval",
        status: "running"
      })
      |> Repo.insert()

    assert {:ok, first_attempt} = AttemptStore.begin_attempt(Repo, step_run.id)
    assert :ok = AttemptStore.ensure_latest_running_attempt(Repo, step_run.id, first_attempt.id)

    assert {:ok, _second_attempt} = AttemptStore.begin_attempt(Repo, step_run.id)

    assert {:error, {:stale_attempt, "running"}} =
             AttemptStore.ensure_latest_running_attempt(Repo, step_run.id, first_attempt.id)
  end
end
