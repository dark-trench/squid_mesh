defmodule BedrockMinimalHostApp.JobQueue do
  @moduledoc """
  Bedrock-backed job queue for Squid Mesh delivery payloads and stress probes.
  """

  use Bedrock.JobQueue,
    otp_app: :bedrock_minimal_host_app,
    repo: BedrockMinimalHostApp.BedrockRepo,
    workers: %{
      "squid_mesh:payload" => BedrockMinimalHostApp.Jobs.SquidMeshPayload,
      "stress:probe" => BedrockMinimalHostApp.Jobs.StressProbe
    }
end
