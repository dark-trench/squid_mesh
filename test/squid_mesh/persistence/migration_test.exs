defmodule SquidMesh.Persistence.MigrationTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.Postgres
  alias Ecto.Adapters.SQL

  defmodule MigrationRepo do
    alias Ecto.Adapters.Postgres

    use Ecto.Repo,
      otp_app: :squid_mesh,
      adapter: Postgres
  end

  test "the schema migration rolls up and down" do
    repo_config = repo_config()

    assert :ok = Postgres.storage_up(repo_config)

    {:ok, repo_pid} = MigrationRepo.start_link(repo_config)
    Process.unlink(repo_pid)

    on_exit(fn ->
      if Process.alive?(repo_pid), do: GenServer.stop(repo_pid, :normal, 5_000)
      Postgres.storage_down(repo_config)
    end)

    migrations_path = Application.app_dir(:squid_mesh, "priv/repo/migrations")
    unload_migration_module()

    assert [_version] = run_migrations_without_module_conflict_warning(migrations_path, :up)

    assert table_exists?("squid_mesh_runs")
    assert table_exists?("squid_mesh_step_runs")
    assert table_exists?("squid_mesh_step_attempts")

    assert [_version] = run_migrations_without_module_conflict_warning(migrations_path, :down)

    refute table_exists?("squid_mesh_runs")
    refute table_exists?("squid_mesh_step_runs")
    refute table_exists?("squid_mesh_step_attempts")
  end

  defp repo_config do
    SquidMesh.Test.Repo.config()
    |> Keyword.put(:database, "squid_mesh_migration_test_#{System.unique_integer([:positive])}")
    |> Keyword.delete(:pool)
  end

  defp unload_migration_module do
    module = SquidMesh.Repo.Migrations.CreateSquidMeshSchema
    :code.purge(module)
    :code.delete(module)
  end

  defp run_migrations_without_module_conflict_warning(migrations_path, direction) do
    compiler_options = Code.compiler_options()
    Code.compiler_options(ignore_module_conflict: true)

    try do
      Ecto.Migrator.run(MigrationRepo, migrations_path, direction, all: true)
    after
      Code.compiler_options(compiler_options)
    end
  end

  defp table_exists?(table_name) do
    query = "select to_regclass($1)::text"

    %{rows: [[result]]} = SQL.query!(MigrationRepo, query, ["public.#{table_name}"])

    result == table_name
  end
end
