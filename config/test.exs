import Config

config :squid_mesh,
  ecto_repos: [SquidMesh.Test.Repo],
  repo: SquidMesh.Test.Repo

config :squid_mesh, SquidMesh.Test.Repo,
  pool: Ecto.Adapters.SQL.Sandbox,
  priv: "priv/repo",
  show_sensitive_data_on_connection_error: true,
  stacktrace: true,
  url:
    System.get_env("DATABASE_URL") ||
      "postgres://postgres:postgres@localhost:5432/squid_mesh_test"

config :logger, level: :warning
