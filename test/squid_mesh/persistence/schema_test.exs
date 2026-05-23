defmodule SquidMesh.Persistence.SchemaTest do
  use ExUnit.Case, async: true

  alias SquidMesh.Persistence.Run
  alias SquidMesh.Persistence.StepAttempt
  alias SquidMesh.Persistence.StepRun

  describe "Run" do
    test "defines persisted run fields and associations" do
      assert Run.__schema__(:fields) == [
               :id,
               :workflow,
               :trigger,
               :status,
               :input,
               :context,
               :current_step,
               :last_error,
               :replayed_from_run_id,
               :inserted_at,
               :updated_at
             ]

      assert Run.__schema__(:associations) == [:replayed_from_run, :step_runs]
    end

    test "requires workflow, trigger, status, and input in the changeset" do
      changeset = Run.changeset(%Run{}, %{})

      refute changeset.valid?

      assert errors_on(changeset) == %{
               input: ["can't be blank"],
               status: ["can't be blank"],
               trigger: ["can't be blank"],
               workflow: ["can't be blank"]
             }
    end
  end

  describe "StepRun" do
    test "defines persisted step fields and associations" do
      assert StepRun.__schema__(:fields) == [
               :id,
               :step,
               :status,
               :input,
               :output,
               :last_error,
               :recovery,
               :resume,
               :manual,
               :transition,
               :run_id,
               :inserted_at,
               :updated_at
             ]

      assert Enum.sort(StepRun.__schema__(:associations)) == [:attempts, :run]
    end

    test "requires run_id, step, and status in the changeset" do
      changeset = StepRun.changeset(%StepRun{}, %{})

      refute changeset.valid?

      assert errors_on(changeset) == %{
               run_id: ["can't be blank"],
               status: ["can't be blank"],
               step: ["can't be blank"]
             }
    end
  end

  describe "StepAttempt" do
    test "defines persisted attempt fields and associations" do
      assert StepAttempt.__schema__(:fields) == [
               :id,
               :attempt_number,
               :status,
               :error,
               :step_run_id,
               :inserted_at,
               :updated_at
             ]

      assert StepAttempt.__schema__(:associations) == [:step_run]
    end

    test "requires step_run_id, attempt_number, and status in the changeset" do
      changeset = StepAttempt.changeset(%StepAttempt{}, %{})

      refute changeset.valid?

      assert errors_on(changeset) == %{
               attempt_number: ["can't be blank"],
               status: ["can't be blank"],
               step_run_id: ["can't be blank"]
             }
    end

    test "requires a positive attempt number" do
      attrs = %{step_run_id: Ecto.UUID.generate(), attempt_number: 0, status: "failed"}
      changeset = StepAttempt.changeset(%StepAttempt{}, attrs)

      refute changeset.valid?
      assert errors_on(changeset) == %{attempt_number: ["must be greater than 0"]}
    end
  end

  defp errors_on(changeset) do
    Ecto.Changeset.traverse_errors(changeset, fn {message, opts} ->
      Enum.reduce(opts, message, fn {key, value}, acc ->
        String.replace(acc, "%{#{key}}", to_string(value))
      end)
    end)
  end
end
