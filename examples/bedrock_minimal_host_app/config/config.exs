import Config

config :bedrock_minimal_host_app,
  ecto_repos: [BedrockMinimalHostApp.Repo]

config :bedrock_minimal_host_app, BedrockMinimalHostApp.Repo,
  url:
    System.get_env(
      "DATABASE_URL",
      "ecto://postgres:postgres@localhost/bedrock_minimal_host_app_dev"
    ),
  pool_size: String.to_integer(System.get_env("POOL_SIZE", "10")),
  show_sensitive_data_on_connection_error: true,
  stacktrace: true

config :bedrock_minimal_host_app, BedrockMinimalHostApp.SquidMeshExecutor,
  queue_id: "tenant_a",
  topic: "squid_mesh:payload"

config :squid_mesh,
  repo: BedrockMinimalHostApp.Repo,
  executor: BedrockMinimalHostApp.SquidMeshExecutor

import_config "#{config_env()}.exs"
