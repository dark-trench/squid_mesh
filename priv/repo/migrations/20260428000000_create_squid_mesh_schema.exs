defmodule SquidMesh.Repo.Migrations.CreateSquidMeshSchema do
  use Ecto.Migration

  def change do
    create table(:squid_mesh_runs, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :workflow, :string, null: false
      add :trigger, :string, null: false
      add :status, :string, null: false
      add :input, :map, null: false
      add :context, :map, null: false, default: %{}
      add :current_step, :string
      add :last_error, :map
      add :replayed_from_run_id, references(:squid_mesh_runs, type: :binary_id, on_delete: :nilify_all)

      timestamps(type: :utc_datetime_usec)
    end

    create index(:squid_mesh_runs, [:workflow])
    create index(:squid_mesh_runs, [:status])
    create index(:squid_mesh_runs, [:inserted_at])
    create index(:squid_mesh_runs, [:replayed_from_run_id])

    execute(
      """
      CREATE UNIQUE INDEX squid_mesh_runs_schedule_idempotency_index
      ON squid_mesh_runs (workflow, trigger, ((context->'schedule'->>'idempotency_key')))
      WHERE context->'schedule'->>'idempotency_key' IS NOT NULL
      """,
      "DROP INDEX squid_mesh_runs_schedule_idempotency_index"
    )

    create table(:squid_mesh_step_runs, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :run_id, references(:squid_mesh_runs, type: :binary_id, on_delete: :delete_all),
        null: false

      add :step, :string, null: false
      add :status, :string, null: false
      add :input, :map, null: false, default: %{}
      add :output, :map
      add :last_error, :map
      add :recovery, :map
      add :resume, :map
      add :manual, :map

      timestamps(type: :utc_datetime_usec)
    end

    create index(:squid_mesh_step_runs, [:run_id])
    create index(:squid_mesh_step_runs, [:status])
    create unique_index(:squid_mesh_step_runs, [:run_id, :step])

    create table(:squid_mesh_step_attempts, primary_key: false) do
      add :id, :binary_id, primary_key: true
      add :step_run_id, references(:squid_mesh_step_runs, type: :binary_id, on_delete: :delete_all),
        null: false

      add :attempt_number, :integer, null: false
      add :status, :string, null: false
      add :error, :map

      timestamps(type: :utc_datetime_usec)
    end

    create index(:squid_mesh_step_attempts, [:step_run_id])
    create unique_index(:squid_mesh_step_attempts, [:step_run_id, :attempt_number])
  end
end
