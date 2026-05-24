defmodule Mix.Tasks.SquidMesh.Install do
  @moduledoc """
  Installs Squid Mesh by creating its migration in the host application.

  ## Usage

      $ mix squid_mesh.install

  This task creates one current-schema Squid Mesh migration in
  `priv/repo/migrations` so the host application can run it through its normal
  Ecto migration flow.

  Executor-specific migrations are intentionally not copied. Squid Mesh assumes
  the host application owns the queueing backend used by its executor.
  """

  @shortdoc "Installs Squid Mesh migrations into the host application"
  @schema_migration_name "create_squid_mesh_schema.exs"

  use Mix.Task

  @impl Mix.Task
  def run(_args) do
    source_file =
      Application.app_dir(:squid_mesh, ["priv", "repo", "migrations", source_filename()])

    dest_dir = Path.join(["priv", "repo", "migrations"])

    unless File.regular?(source_file) do
      Mix.raise("Could not find Squid Mesh migration at #{source_file}")
    end

    unless File.dir?(dest_dir) do
      Mix.raise("""
      Could not find migrations directory at #{dest_dir}.
      Please ensure your application has an Ecto repository set up.
      """)
    end

    base_timestamp = timestamp()

    if migration_installed?(dest_dir) do
      Mix.shell().info("* skipping #{@schema_migration_name} (already installed)")
    else
      new_filename = "#{base_timestamp}_#{@schema_migration_name}"
      File.cp!(source_file, Path.join(dest_dir, new_filename))
      Mix.shell().info("* creating #{new_filename}")
    end

    Mix.shell().info("""

    Squid Mesh migrations have been installed!

    Next steps:
      1. Run `mix ecto.migrate` to apply the migrations
      2. Configure Squid Mesh in your config:

          config :squid_mesh,
            repo: YourApp.Repo,
            runtime: :journal,
            read_model: :read_model

      3. Start your chosen executor and have workers call
         `SquidMesh.execute_next(owner_id: "your-worker-id")` when capacity is
         available. Bedrock is the recommended executor for distributed hosts.

    See docs/host_app_integration.md for a copy-paste host setup.
    """)
  end

  defp migration_installed?(dest_dir) do
    dest_dir
    |> File.ls!()
    |> Enum.any?(&String.ends_with?(&1, @schema_migration_name))
  end

  defp source_filename do
    "20260428000000_#{@schema_migration_name}"
  end

  defp timestamp do
    {{year, month, day}, {hour, minute, second}} = :calendar.universal_time()
    "#{year}#{pad(month)}#{pad(day)}#{pad(hour)}#{pad(minute)}#{pad(second)}"
  end

  defp pad(value) when value < 10, do: "0#{value}"
  defp pad(value), do: Integer.to_string(value)
end
