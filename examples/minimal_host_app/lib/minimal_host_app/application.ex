defmodule MinimalHostApp.Application do
  @moduledoc """
  Supervision tree for the example host application.

  The example starts a repo and Oban in development-like environments so Squid
  Mesh can run with the same kind of application wiring expected in production
  host apps.
  """

  use Application

  @impl true
  def start(_type, _args) do
    children = Application.get_env(:minimal_host_app, :runtime_children, default_children())

    opts = [strategy: :one_for_one, name: MinimalHostApp.Supervisor]
    Supervisor.start_link(children, opts)
  end

  @spec default_children() :: [Supervisor.child_spec()]
  defp default_children do
    [
      MinimalHostApp.Repo,
      {Oban, Application.fetch_env!(:minimal_host_app, Oban)},
      MinimalHostApp.JournalExecutor
    ]
  end
end
