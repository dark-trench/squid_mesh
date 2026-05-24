defmodule MinimalHostApp.Workers.SquidMeshWorker do
  @moduledoc """
  Generic Oban delivery adapter for Squid Mesh cron payloads.
  """

  use Oban.Worker, queue: :squid_mesh, max_attempts: 1

  alias SquidMesh.Runtime.Runner

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"kind" => "cron"} = args}) do
    Runner.perform(args)
  end

  def perform(%Oban.Job{args: args}) do
    {:error, {:invalid_squid_mesh_payload, args}}
  end
end
