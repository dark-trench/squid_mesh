defmodule BedrockMinimalHostApp.Repo do
  @moduledoc """
  Repo used by the example host application.
  """

  use Ecto.Repo,
    otp_app: :bedrock_minimal_host_app,
    adapter: Ecto.Adapters.Postgres
end
