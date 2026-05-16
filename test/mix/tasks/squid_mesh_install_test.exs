defmodule Mix.Tasks.SquidMesh.InstallTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureIO

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
          Mix.Tasks.SquidMesh.Install.run([])
        end)
      end)

    installed_migrations = File.ls!(Path.join(tmp_dir, "priv/repo/migrations"))

    assert [migration] = installed_migrations
    assert String.ends_with?(migration, "create_squid_mesh_schema.exs")
    assert output =~ "creating"

    migration_body = File.read!(Path.join([tmp_dir, "priv/repo/migrations", migration]))

    assert migration_body =~ "create table(:squid_mesh_runs"
    assert migration_body =~ "add :trigger, :string, null: false"
    assert migration_body =~ "squid_mesh_runs_schedule_idempotency_index"
    assert migration_body =~ "create table(:squid_mesh_step_runs"
    assert migration_body =~ "add :recovery, :map"
    assert migration_body =~ "add :resume, :map"
    assert migration_body =~ "add :manual, :map"
    assert migration_body =~ "create table(:squid_mesh_step_attempts"
  end

  test "skips the current-schema migration when it already exists", %{tmp_dir: tmp_dir} do
    File.write!(
      Path.join(tmp_dir, "priv/repo/migrations/20260101000000_create_squid_mesh_schema.exs"),
      "# existing migration\n"
    )

    output =
      File.cd!(tmp_dir, fn ->
        capture_io(fn ->
          Mix.Tasks.SquidMesh.Install.run([])
        end)
      end)

    assert File.ls!(Path.join(tmp_dir, "priv/repo/migrations")) == [
             "20260101000000_create_squid_mesh_schema.exs"
           ]

    assert output =~ "skipping create_squid_mesh_schema.exs"
  end
end
