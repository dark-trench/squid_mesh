defmodule BedrockMinimalHostApp.Jobs.SquidMeshPayload do
  @moduledoc """
  Bedrock job that delivers Squid Mesh executor payloads back to the runtime.
  """

  use Bedrock.JobQueue.Job,
    topic: "squid_mesh:payload",
    max_retries: 3,
    priority: 100

  alias SquidMesh.ReadModel.Inspection.Snapshot
  alias SquidMesh.Runtime.Runner

  @default_max_journal_attempts 50

  @impl true
  def perform(args, _meta) when is_map(args) do
    case Runner.perform(args) do
      :ok -> drain_journal_attempts(0)
      {:ok, %Snapshot{}} -> drain_journal_attempts(0)
      {:error, _reason} = error -> error
    end
  end

  defp drain_journal_attempts(count) do
    if count >= max_journal_attempts() do
      {:error, :journal_drain_limit_exceeded}
    else
      case SquidMesh.execute_next(owner_id: "bedrock-minimal-host-app") do
        {:ok, :none} -> :ok
        {:ok, %Snapshot{}} -> drain_journal_attempts(count + 1)
        {:error, _reason} = error -> error
      end
    end
  end

  defp max_journal_attempts do
    :bedrock_minimal_host_app
    |> Application.get_env(__MODULE__, [])
    |> Keyword.get(:max_journal_attempts, @default_max_journal_attempts)
  end
end
