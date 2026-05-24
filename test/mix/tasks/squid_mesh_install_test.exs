defmodule Mix.Tasks.SquidMesh.InstallTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

  alias Mix.Tasks.SquidMesh.Install

  @task "squid_mesh.install"

  setup do
    tmp_dir =
      Path.join(System.tmp_dir!(), "squid_mesh-install-#{System.unique_integer([:positive])}")

    File.mkdir_p!(Path.join(tmp_dir, "priv/repo/migrations"))

    on_exit(fn ->
      File.rm_rf!(tmp_dir)
      Mix.Task.reenable(@task)
    end)

    {:ok, tmp_dir: tmp_dir}
  end

  test "creates one current-schema migration", %{tmp_dir: tmp_dir} do
    output =
      File.cd!(tmp_dir, fn ->
        capture_io(fn ->
          Install.run([])
        end)
      end)

    installed_migrations = File.ls!(Path.join(tmp_dir, "priv/repo/migrations"))

    assert [migration] = installed_migrations
    assert String.ends_with?(migration, "create_squid_mesh_schema.exs")
    assert output =~ "creating"

    migration_body = File.read!(Path.join([tmp_dir, "priv/repo/migrations", migration]))

    refute migration_body =~ "create table(:squid_mesh_runs"
    refute migration_body =~ "squid_mesh_runs_schedule_idempotency_index"
    refute migration_body =~ "create table(:squid_mesh_step_runs"
    refute migration_body =~ "create table(:squid_mesh_step_attempts"
    assert migration_body =~ "create table(:squid_mesh_journal_threads"
    assert migration_body =~ "create table(:squid_mesh_journal_entries"
    assert migration_body =~ "create table(:squid_mesh_journal_checkpoints"

    assert output =~ "runtime: :journal"
    assert output =~ "read_model: :read_model"
    assert output =~ "SquidMesh.execute_next"
    refute output =~ "SquidMesh.Runtime.Runner.perform(payload)"
  end

  test "skips the current-schema migration when it already exists", %{tmp_dir: tmp_dir} do
    File.write!(
      Path.join(tmp_dir, "priv/repo/migrations/20260101000000_create_squid_mesh_schema.exs"),
      "# existing migration\n"
    )

    output =
      File.cd!(tmp_dir, fn ->
        capture_io(fn ->
          Install.run([])
        end)
      end)

    assert File.ls!(Path.join(tmp_dir, "priv/repo/migrations")) == [
             "20260101000000_create_squid_mesh_schema.exs"
           ]

    assert output =~ "skipping create_squid_mesh_schema.exs"
  end
end
