defmodule SquidMesh.ReadModel.Listing do
  @moduledoc """
  Projection-backed run listing for the journal-backed runtime.

  The journal catalog is a global lookup projection, so this module can list all
  known journal-backed runs without adapter-specific storage scans.
  """

  alias Jido.Agent
  alias SquidMesh.ReadModel.Listing.Summary
  alias SquidMesh.Runtime.Journal
  alias SquidMesh.Runtime.Journal.Options
  alias SquidMesh.Runtime.RunCatalogProjection
  alias SquidMesh.Runtime.WorkflowAgent
  alias SquidMesh.Runtime.WorkflowAgent.Projection
  alias SquidMesh.Workflow.Definition

  @supported_filters [:workflow, :status, :limit]
  @supported_options [:queue, :now]

  @type list_filter ::
          {:workflow, module() | String.t()} | {:status, atom()} | {:limit, pos_integer()}
  @type list_option :: {:queue, atom() | String.t()} | {:now, DateTime.t()}
  @type list_error ::
          {:invalid_option,
           {:filters, :invalid}
           | {:filter, atom()}
           | {:workflow, :invalid | :required}
           | {:status, :invalid}
           | {:limit, :invalid}
           | {:opts, :invalid}
           | {:option, atom()}
           | {:queue, :invalid}
           | {:now, :invalid}}
          | {:run_catalog_anomalies, [RunCatalogProjection.anomaly()]}
          | {:run_catalog_summary_failed, String.t(), term()}
          | term()

  @doc """
  Lists redacted summaries from the global journal run catalog.

  Results are ordered newest first by the durable catalog timestamp. Optional
  `:workflow` and `:status` filters are applied without scanning journal storage
  tables. The `:status` filter is applied after rebuilding each run-thread
  projection so it reflects current journal state instead of stale catalog
  metadata. Use `SquidMesh.inspect_run/2` when callers need detailed attempts,
  inputs, results, or claim metadata for one run.
  """
  @spec list(Journal.storage_config(), [list_filter()], [list_option()]) ::
          {:ok, [Summary.t()]} | {:error, list_error()}
  def list(storage, filters, opts \\ [])

  def list(storage, filters, opts) when is_list(filters) and is_list(opts) do
    with {:ok, filters} <- validate_filters(filters),
         {:ok, opts} <- validate_options(opts),
         {:ok, workflow} <- workflow_filter(filters),
         :ok <- validate_queue_option(opts),
         {:ok, _now} <- now_option(opts),
         {:ok, projection} <- Journal.rebuild_run_catalog_projection(storage),
         :ok <- reject_catalog_anomalies(projection) do
      summaries(
        storage,
        projection,
        workflow,
        Keyword.get(filters, :status),
        Keyword.get(filters, :limit)
      )
    end
  end

  def list(_storage, _filters, opts) when is_list(opts) do
    {:error, {:invalid_option, {:filters, :invalid}}}
  end

  def list(_storage, _filters, _opts) do
    {:error, {:invalid_option, {:opts, :invalid}}}
  end

  defp validate_filters(filters) do
    cond do
      not Keyword.keyword?(filters) ->
        {:error, {:invalid_option, {:filters, :invalid}}}

      unsupported = Enum.find(Keyword.keys(filters), &(&1 not in @supported_filters)) ->
        {:error, {:invalid_option, {:filter, unsupported}}}

      not valid_status?(Keyword.get(filters, :status)) ->
        {:error, {:invalid_option, {:status, :invalid}}}

      not valid_limit?(Keyword.get(filters, :limit)) ->
        {:error, {:invalid_option, {:limit, :invalid}}}

      true ->
        {:ok, filters}
    end
  end

  defp validate_options(opts) do
    cond do
      not Keyword.keyword?(opts) ->
        {:error, {:invalid_option, {:opts, :invalid}}}

      unsupported = Enum.find(Keyword.keys(opts), &(&1 not in @supported_options)) ->
        {:error, {:invalid_option, {:option, unsupported}}}

      true ->
        {:ok, opts}
    end
  end

  defp workflow_filter(filters) do
    case Keyword.fetch(filters, :workflow) do
      {:ok, workflow} -> normalize_workflow(workflow)
      :error -> {:ok, nil}
    end
  end

  defp normalize_workflow(workflow) when is_atom(workflow) and not is_nil(workflow) do
    workflow
    |> Definition.serialize_workflow()
    |> Options.thread_part(:workflow)
  end

  defp normalize_workflow(workflow) when is_binary(workflow) do
    Options.thread_part(workflow, :workflow)
  end

  defp normalize_workflow(_workflow), do: {:error, {:invalid_option, {:workflow, :invalid}}}

  defp validate_queue_option(opts) do
    opts
    |> Keyword.get(:queue, "default")
    |> Options.queue()
    |> case do
      {:ok, _queue} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp now_option(opts) do
    case Keyword.get(opts, :now, DateTime.utc_now()) do
      %DateTime{} = now -> {:ok, now}
      _invalid -> {:error, {:invalid_option, {:now, :invalid}}}
    end
  end

  defp reject_catalog_anomalies(%RunCatalogProjection{} = projection) do
    case RunCatalogProjection.anomalies(projection) do
      [] ->
        :ok

      anomalies ->
        {:error, {:run_catalog_anomalies, anomalies}}
    end
  end

  defp summaries(
         storage,
         %RunCatalogProjection{} = projection,
         workflow_filter,
         status_filter,
         limit
       ) do
    projection
    |> RunCatalogProjection.runs()
    |> Enum.reverse()
    |> Enum.reduce_while({:ok, []}, fn run_index_summary, {:ok, summaries} ->
      case summary(storage, run_index_summary) do
        {:ok, %Summary{} = summary} ->
          maybe_collect_summary(summary, summaries, workflow_filter, status_filter, limit)

        {:error, reason} ->
          {:halt, {:error, {:run_catalog_summary_failed, run_index_summary.run_id, reason}}}
      end
    end)
    |> case do
      {:ok, summaries} -> {:ok, Enum.reverse(summaries)}
      {:error, _reason} = error -> error
    end
  end

  defp summary(storage, %{
         run_id: run_id,
         workflow: workflow,
         queue: queue,
         indexed_at: indexed_at
       }) do
    with {:ok, %Agent{state: %{projection: %Projection{} = projection, thread_rev: thread_rev}}} <-
           WorkflowAgent.rebuild(storage, run_id),
         :ok <- validate_catalog_summary(projection, run_id, workflow, queue) do
      {:ok,
       %Summary{
         run_id: run_id,
         workflow: workflow,
         queue: queue,
         status: Projection.status(projection),
         terminal?: Projection.terminal?(projection),
         terminal_status: Projection.terminal_status(projection),
         indexed_at: indexed_at,
         thread_revision: thread_rev,
         anomalies: Projection.anomalies(projection)
       }}
    end
  end

  defp validate_catalog_summary(%Projection{} = projection, run_id, workflow, queue) do
    cond do
      projection.run_id != run_id ->
        {:error,
         {:catalog_run_mismatch, %{expected: run_id, actual: projection.run_id, run_id: run_id}}}

      projection.workflow != workflow ->
        {:error,
         {:catalog_workflow_mismatch,
          %{expected: workflow, actual: projection.workflow, run_id: run_id}}}

      not catalog_queue_matches?(projection, queue) ->
        {:error,
         {:catalog_queue_mismatch,
          %{expected: queue, actual: projection_queues(projection), run_id: run_id}}}

      true ->
        :ok
    end
  end

  defp catalog_queue_matches?(%Projection{} = projection, queue) do
    case projection_queues(projection) do
      [] -> true
      queues -> queues == [queue]
    end
  end

  defp projection_queues(%Projection{} = projection) do
    projection
    |> Projection.planned_runnables()
    |> Enum.map(&Map.get(&1, :queue))
    |> Enum.filter(&is_binary/1)
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp maybe_collect_summary(
         %Summary{} = summary,
         summaries,
         workflow_filter,
         status_filter,
         limit
       ) do
    if workflow_match?(summary, workflow_filter) and status_match?(summary, status_filter) do
      summaries = [summary | summaries]

      if limit_reached?(summaries, limit) do
        {:halt, {:ok, summaries}}
      else
        {:cont, {:ok, summaries}}
      end
    else
      {:cont, {:ok, summaries}}
    end
  end

  defp workflow_match?(%Summary{}, nil), do: true
  defp workflow_match?(%Summary{workflow: current}, expected), do: current == expected

  defp status_match?(%Summary{}, nil), do: true
  defp status_match?(%Summary{status: current}, expected), do: current == expected

  defp limit_reached?(_summaries, nil), do: false
  defp limit_reached?(summaries, limit), do: length(summaries) >= limit

  defp valid_status?(nil), do: true
  defp valid_status?(status), do: is_atom(status)

  defp valid_limit?(nil), do: true
  defp valid_limit?(limit), do: is_integer(limit) and limit > 0
end
