defmodule BedrockMinimalHostApp.Jobs.SquidMeshPayload do
  @moduledoc """
  Bedrock job that delivers Squid Mesh executor payloads back to the runtime.
  """

  use Bedrock.JobQueue.Job,
    topic: "squid_mesh:payload",
    max_retries: 3,
    priority: 100

  @impl true
  def perform(args, _meta) when is_map(args) do
    SquidMesh.Runtime.Runner.perform(args)
  end
end
