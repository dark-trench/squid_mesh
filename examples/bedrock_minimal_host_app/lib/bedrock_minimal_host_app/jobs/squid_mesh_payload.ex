defmodule BedrockMinimalHostApp.Jobs.SquidMeshPayload do
  @moduledoc """
  Bedrock job that delivers Squid Mesh delivery payloads back to the runtime.

  A `%{"kind" => "drain", "queue" => queue}` payload is the example app's
  explicit leased command for draining a journal queue that was not activated by
  the original delivery payload, such as a child workflow queue.
  """

  use Bedrock.JobQueue.Job,
    topic: "squid_mesh:payload",
    max_retries: 3,
    priority: 100

  alias SquidMesh.ReadModel.Inspection.Snapshot
  alias SquidMesh.Runtime.Runner

  @default_max_journal_attempts 50

  @impl true
  def perform(%{"kind" => "drain", "queue" => queue}, _meta) when is_binary(queue) do
    drain_journal_attempts(0, queue)
  end

  def perform(%{"kind" => "drain"} = args, _meta) do
    {:error, {:invalid_runtime_payload, args}}
  end

  def perform(args, _meta) when is_map(args) do
    case Runner.perform(args) do
      :ok -> drain_journal_attempts(0, journal_queue(args))
      {:ok, %Snapshot{}} -> drain_journal_attempts(0, journal_queue(args))
      {:error, _reason} = error -> error
    end
  end

  defp drain_journal_attempts(count, queue) do
    if count >= max_journal_attempts() do
      {:error, :journal_drain_limit_exceeded}
    else
      case SquidMesh.execute_next(owner_id: "bedrock-minimal-host-app", queue: queue) do
        {:ok, :none} -> :ok
        {:ok, %Snapshot{}} -> drain_journal_attempts(count + 1, queue)
        {:error, _reason} = error -> error
      end
    end
  end

  defp journal_queue(%{"queue" => queue}) when is_binary(queue), do: queue
  defp journal_queue(_args), do: "default"

  defp max_journal_attempts do
    :bedrock_minimal_host_app
    |> Application.get_env(__MODULE__, [])
    |> Keyword.get(:max_journal_attempts, @default_max_journal_attempts)
  end
end
