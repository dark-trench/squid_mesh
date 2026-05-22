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
    # Delivery is Bedrock-backed here, but execution intentionally re-enters the
    # normal Squid Mesh runtime so this stays an executor spike, not a runtime fork.
    SquidMesh.Runtime.Runner.perform(args, executor: BedrockMinimalHostApp.SquidMeshExecutor)
  end
end
