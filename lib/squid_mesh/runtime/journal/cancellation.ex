defmodule SquidMesh.Runtime.Journal.Cancellation do
  @moduledoc """
  Journal-backed workflow cancellation.

  Cancellation appends a terminal run fact to the run thread. Dispatch
  projections already overlay terminal run facts from run threads, so stale
  claims and later completions are fenced by the same durable source of truth.
  The configured queue selects the returned dispatch projection; cancellation
  itself is scoped by the globally unique run id.
  """

  alias Jido.Agent
  alias SquidMesh.ReadModel.Inspection
  alias SquidMesh.Runtime.DispatchProtocol
  alias SquidMesh.Runtime.DispatchProtocol.Entry
  alias SquidMesh.Runtime.Journal
  alias SquidMesh.Runtime.Journal.Options
  alias SquidMesh.Runtime.WorkflowAgent
  alias SquidMesh.Runtime.WorkflowAgent.Projection

  @run_append_retries 25
  @terminal_statuses [:completed, :failed, :cancelled]

  @type cancel_error ::
          :not_found
          | :invalid_run_id
          | {:invalid_option, term()}
          | {:invalid_transition, atom(), :cancelling}
          | term()

  @doc """
  Cancels a journal-backed workflow run.
  """
  @spec cancel(String.t(), keyword()) :: {:ok, Inspection.Snapshot.t()} | {:error, cancel_error()}
  def cancel(run_id, opts \\ [])

  def cancel(run_id, opts) when is_binary(run_id) and is_list(opts) do
    with {:ok, run_id} <- run_id(run_id),
         {:ok, storage} <- journal_storage(opts),
         {:ok, queue} <- queue(opts),
         {:ok, now} <- now(opts),
         {:ok, _workflow_agent} <- cancel_or_repair(storage, run_id, now, @run_append_retries) do
      Inspection.snapshot(storage, run_id, queue: queue, now: now)
    end
  end

  def cancel(_run_id, _opts), do: {:error, :invalid_run_id}

  defp cancel_or_repair(_storage, _run_id, _now, 0), do: {:error, :conflict}

  defp cancel_or_repair(storage, run_id, %DateTime{} = now, retries_left) do
    with {:ok, workflow_agent} <- rebuild_workflow_agent(storage, run_id),
         :ok <- cancellable?(storage, workflow_agent),
         {:ok, terminal_entry} <- run_terminal_entry(run_id, :cancelled, now) do
      append_cancellation(storage, workflow_agent, terminal_entry, now, retries_left)
    end
  end

  defp append_cancellation(storage, workflow_agent, %Entry{} = terminal_entry, now, retries_left) do
    case Journal.append_entries(storage, [terminal_entry],
           expected_rev: workflow_agent.state.thread_rev
         ) do
      {:ok, _thread} ->
        with {:ok, workflow_agent} <- WorkflowAgent.rebuild(storage, workflow_agent.state.run_id) do
          _checkpoint_result =
            WorkflowAgent.put_checkpoint(storage, workflow_agent, updated_at: now)

          {:ok, workflow_agent}
        end

      {:error, :conflict} ->
        cancel_or_repair(storage, workflow_agent.state.run_id, now, retries_left - 1)

      {:error, _reason} = error ->
        error
    end
  end

  defp cancellable?(storage, %Agent{
         agent_module: WorkflowAgent,
         state: %{projection: %Projection{} = projection}
       }) do
    status = Projection.status(projection)

    if status in @terminal_statuses do
      {:error, {:invalid_transition, status, :cancelling}}
    else
      linked_children_started?(storage, projection)
    end
  end

  defp linked_children_started?(storage, %Projection{} = projection) do
    projection
    |> Projection.child_runs()
    |> Enum.reduce_while(:ok, fn child_run, :ok ->
      case Journal.load_thread(storage, {:run, Map.fetch!(child_run, :child_run_id)}) do
        {:ok, _thread} ->
          {:cont, :ok}

        {:error, :not_found} ->
          {:halt, {:error, {:invalid_transition, :child_starting, :cancelling}}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  defp rebuild_workflow_agent(storage, run_id) do
    case WorkflowAgent.rebuild(storage, run_id) do
      {:ok, _workflow_agent} = ok -> ok
      {:error, :not_found} -> {:error, :not_found}
      {:error, _reason} = error -> error
    end
  end

  defp run_terminal_entry(run_id, status, %DateTime{} = now) do
    DispatchProtocol.new_entry(:run_terminal, %{
      run_id: run_id,
      status: status,
      occurred_at: now
    })
  end

  defp run_id(run_id) do
    case Ecto.UUID.cast(run_id) do
      {:ok, uuid} -> {:ok, uuid}
      :error -> {:error, :invalid_run_id}
    end
  end

  defp journal_storage(opts) do
    opts
    |> Keyword.get(:journal_storage)
    |> Options.storage()
  end

  defp queue(opts) do
    opts
    |> Keyword.get(:queue, "default")
    |> Options.queue()
  end

  defp now(opts) do
    case Keyword.get(opts, :now, DateTime.utc_now()) do
      %DateTime{} = now -> {:ok, now}
      _invalid -> {:error, {:invalid_option, {:now, :invalid}}}
    end
  end
end
