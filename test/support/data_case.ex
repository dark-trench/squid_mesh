defmodule SquidMesh.DataCase do
  @moduledoc false

  use ExUnit.CaseTemplate

  import ExUnit.Assertions

  alias Ecto.Adapters.SQL.Sandbox
  alias SquidMesh.Test.Executor

  using do
    quote do
      alias SquidMesh.Test.Repo

      import ExUnit.Assertions
      import Ecto
      import Ecto.Changeset
      import Ecto.Query
      import SquidMesh.DataCase
    end
  end

  def all_enqueued(_opts \\ []) do
    Executor.jobs()
  end

  def assert_enqueued(opts) do
    if Enum.any?(all_enqueued(), &job_matches?(&1, opts)) do
      :ok
    else
      flunk("expected queued job matching #{inspect(opts)}")
    end
  end

  setup tags do
    Executor.reset!()

    pid = Sandbox.start_owner!(SquidMesh.Test.Repo, shared: not tags[:async])
    on_exit(fn -> Sandbox.stop_owner(pid) end)
    :ok
  end

  @spec errors_on(Ecto.Changeset.t()) :: map()
  def errors_on(changeset) do
    Ecto.Changeset.traverse_errors(changeset, fn {message, opts} ->
      Regex.replace(~r"%{(\w+)}", message, fn _, key ->
        opts |> Keyword.get(String.to_existing_atom(key), key) |> to_string()
      end)
    end)
  end

  defp job_matches?(job, opts) do
    args_match?(job, Keyword.get(opts, :args, %{})) and
      worker_match?(job, Keyword.get(opts, :worker)) and
      queue_match?(job, Keyword.get(opts, :queue))
  end

  defp args_match?(%{args: args}, expected_args) do
    Enum.all?(expected_args, fn {key, value} -> Map.get(args, to_string(key)) == value end)
  end

  defp worker_match?(_job, nil), do: true

  defp worker_match?(%{worker: worker}, expected_worker) when is_atom(expected_worker) do
    worker == module_worker_name(expected_worker)
  end

  defp worker_match?(%{worker: worker}, expected_worker), do: worker == expected_worker

  defp queue_match?(_job, nil), do: true
  defp queue_match?(%{queue: queue}, expected_queue), do: queue == to_string(expected_queue)

  defp module_worker_name(module) do
    module
    |> Atom.to_string()
    |> String.trim_leading("Elixir.")
  end
end
