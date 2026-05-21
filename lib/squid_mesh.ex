defmodule SquidMesh do
  @moduledoc """
  Public entrypoint for the Squid Mesh runtime.

  The API exposed here stays focused on declarative workflow operations. Host
  applications start, inspect, and later control runs through this surface
  without needing to work directly with persistence internals.
  """

  alias SquidMesh.Config
  alias SquidMesh.ReadModel.Inspection
  alias SquidMesh.Run
  alias SquidMesh.Runs
  alias SquidMesh.Runs.Explanation
  alias SquidMesh.Runtime.Dispatcher
  alias SquidMesh.Runtime.Reviewer
  alias SquidMesh.Runtime.Unblocker

  @read_models [:runtime_tables, :journal_projection]
  @projection_read_options [:journal_storage, :queue, :now]

  @typedoc """
  Structured validation errors returned by the public read-model APIs.
  """
  @type read_option_error ::
          {:invalid_option,
           {:opts, term()}
           | {:read_model, term()}
           | {:journal_storage, nil}
           | {:run_id, term()}}

  @doc """
  Loads Squid Mesh configuration from the application environment with optional
  runtime overrides.
  """
  @spec config(keyword()) :: {:ok, Config.t()} | {:error, {:missing_config, [atom()]}}
  defdelegate config(overrides \\ []), to: Config, as: :load

  @doc """
  Loads Squid Mesh configuration and raises if required keys are missing.
  """
  @spec config!(keyword()) :: Config.t()
  defdelegate config!(overrides \\ []), to: Config, as: :load!

  @doc """
  Starts a new workflow run with the given payload through the workflow's
  default trigger.
  """
  @spec start_run(module(), map()) ::
          {:ok, Run.t()}
          | {:error, {:missing_config, [atom()]}}
          | {:error, Runs.Store.create_error()}
          | {:error, {:dispatch_failed, term()}}
  def start_run(workflow, payload) when is_map(payload) do
    start_run(workflow, payload, [])
  end

  @spec start_run(module(), map(), keyword()) ::
          {:ok, Run.t()}
          | {:error, {:missing_config, [atom()]}}
          | {:error, {:invalid_option, atom()}}
          | {:error, Runs.Store.create_error()}
          | {:error, {:dispatch_failed, term()}}
  def start_run(workflow, payload, overrides) when is_map(payload) and is_list(overrides) do
    with :ok <- reject_public_start_options(overrides),
         {:ok, config} <- Config.load(overrides) do
      start_default_run(config, workflow, payload)
    else
      {:error, reason} -> {:error, reason}
    end
  end

  def start_run(_workflow, _payload, overrides) when is_list(overrides) do
    {:error, {:invalid_payload, :expected_map}}
  end

  @doc """
  Starts a new workflow run through a named trigger with the given payload.
  """
  @spec start_run(module(), atom(), map()) ::
          {:ok, Run.t()}
          | {:error, {:missing_config, [atom()]}}
          | {:error, Runs.Store.create_error()}
          | {:error, {:dispatch_failed, term()}}
  def start_run(workflow, trigger_name, payload)
      when is_atom(trigger_name) and is_map(payload) do
    start_run(workflow, trigger_name, payload, [])
  end

  @doc """
  Starts a named trigger while applying runtime configuration overrides.

  Overrides are intended for host-app test and integration boundaries. Runtime
  context injection is kept out of this public API so scheduled starts and other
  internal callers can keep their idempotency metadata isolated.
  """
  @spec start_run(module(), atom(), map(), keyword()) ::
          {:ok, Run.t()}
          | {:error, {:missing_config, [atom()]}}
          | {:error, {:invalid_option, atom()}}
          | {:error, Runs.Store.create_error()}
          | {:error, {:dispatch_failed, term()}}
  def start_run(workflow, trigger_name, payload, overrides)
      when is_atom(trigger_name) and is_map(payload) and is_list(overrides) do
    with :ok <- reject_public_start_options(overrides),
         {:ok, config} <- Config.load(overrides) do
      start_triggered_run(config, workflow, trigger_name, payload)
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc false
  @spec start_run_with_initial_context(module(), atom(), map(), map(), keyword()) ::
          {:ok, Run.t()}
          | {:ok, {:duplicate_schedule_start, Run.t()}}
          | {:error, {:missing_config, [atom()]}}
          | {:error, {:invalid_option, atom()}}
          | {:error, Runs.Store.create_error()}
          | {:error, {:dispatch_failed, term()}}
  def start_run_with_initial_context(workflow, trigger_name, payload, initial_context, overrides)
      when is_atom(trigger_name) and is_map(payload) and is_map(initial_context) and
             is_list(overrides) do
    with :ok <- reject_public_start_options(overrides),
         {:ok, config} <- Config.load(overrides) do
      start_initial_context_run(config, workflow, trigger_name, payload, initial_context)
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Fetches one workflow run by id.

  By default this reads the stable runtime tables and returns `SquidMesh.Run`.
  Pass `read_model: :journal_projection` with `journal_storage:` to rebuild the
  Jido-native projection-backed snapshot from durable journal entries instead.
  """
  @spec inspect_run(String.t(), keyword()) ::
          {:ok, Run.t() | SquidMesh.ReadModel.Inspection.Snapshot.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | read_option_error()
             | Config.config_error()
             | Inspection.snapshot_error()}
  def inspect_run(run_id, overrides \\ []) do
    with {:ok, read_model} <- read_model(overrides) do
      case read_model do
        :runtime_tables -> inspect_runtime_table_run(run_id, overrides)
        :journal_projection -> inspect_projected_run(run_id, overrides)
      end
    end
  end

  @doc """
  Explains the current runtime state of one workflow run.

  The result is structured diagnostic data for host apps, CLIs, and dashboards.
  Use `inspect_run/2` for the factual run snapshot and `explain_run/2` when an
  operator-facing surface needs the reason, evidence, and valid next actions for
  the run's current state.

  By default this reads the stable runtime tables and returns
  `SquidMesh.Runs.Explanation`. Pass `read_model: :journal_projection` with
  `journal_storage:` to derive the explanation from durable Jido journal
  projections instead.
  """
  @spec explain_run(String.t(), keyword()) ::
          {:ok, Explanation.t() | SquidMesh.ReadModel.Explanation.Diagnostic.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | read_option_error()
             | Config.config_error()
             | SquidMesh.ReadModel.Explanation.explanation_error()}
  def explain_run(run_id, overrides \\ []) do
    with {:ok, read_model} <- read_model(overrides) do
      case read_model do
        :runtime_tables -> explain_runtime_table_run(run_id, overrides)
        :journal_projection -> explain_projected_run(run_id, overrides)
      end
    end
  end

  @doc """
  Lists workflow runs with optional filters.
  """
  @spec list_runs(Runs.Store.list_filters(), keyword()) ::
          {:ok, [Run.t()]} | {:error, {:missing_config, [atom()]}}
  def list_runs(filters \\ [], overrides \\ []) do
    with {:ok, config} <- Config.load(overrides) do
      Runs.Store.list_runs(config.repo, filters)
    end
  end

  @doc """
  Requests cancellation for an eligible workflow run.
  """
  @spec cancel_run(Ecto.UUID.t(), keyword()) ::
          {:ok, Run.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | {:missing_config, [atom()]}
             | Runs.Store.transition_error()}
  def cancel_run(run_id, overrides \\ []) do
    with {:ok, config} <- Config.load(overrides) do
      Runs.Store.cancel_run(config.repo, run_id)
    end
  end

  @doc """
  Resumes a run that is intentionally paused for manual intervention.
  """
  @spec unblock_run(Ecto.UUID.t()) ::
          {:ok, Run.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | {:missing_config, [atom()]}
             | Runs.Store.transition_error()
             | term()}
  def unblock_run(run_id), do: unblock_run(run_id, %{}, [])

  @doc """
  Resumes a paused run with either configuration overrides or manual action
  attributes.

  Pass a keyword list to override runtime configuration, or a map to provide
  manual action attributes. Attribute maps are validated against the paused
  step's manual action contract before the runtime appends resume events or
  dispatches successor work.
  """
  @spec unblock_run(Ecto.UUID.t(), keyword()) ::
          {:ok, Run.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | {:missing_config, [atom()]}
             | Runs.Store.transition_error()
             | term()}
  def unblock_run(run_id, overrides) when is_list(overrides) do
    unblock_run(run_id, %{}, overrides)
  end

  @spec unblock_run(Ecto.UUID.t(), map()) ::
          {:ok, Run.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | {:missing_config, [atom()]}
             | Runs.Store.transition_error()
             | term()}
  def unblock_run(run_id, attrs) when is_map(attrs) do
    unblock_run(run_id, attrs, [])
  end

  @doc """
  Resumes a paused run with manual action attributes and configuration overrides.
  """
  @spec unblock_run(Ecto.UUID.t(), map(), keyword()) ::
          {:ok, Run.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | {:missing_config, [atom()]}
             | Runs.Store.transition_error()
             | term()}
  def unblock_run(run_id, attrs, overrides) when is_map(attrs) and is_list(overrides) do
    with {:ok, config} <- Config.load(overrides),
         {:ok, run} <- Runs.Store.get_run(config.repo, run_id),
         :ok <- Unblocker.unblock(config, run, attrs) do
      Runs.Store.get_run(config.repo, run_id)
    end
  end

  @doc """
  Approves a paused approval step and resumes the run through its success path.
  """
  @spec approve_run(Ecto.UUID.t(), map(), keyword()) ::
          {:ok, Run.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | {:missing_config, [atom()]}
             | Runs.Store.transition_error()
             | term()}
  def approve_run(run_id, attrs, overrides \\ []) when is_map(attrs) and is_list(overrides) do
    with {:ok, config} <- Config.load(overrides),
         {:ok, run} <- Runs.Store.get_run(config.repo, run_id),
         :ok <- Reviewer.review(config, run, :approved, attrs) do
      Runs.Store.get_run(config.repo, run_id)
    end
  end

  @doc """
  Rejects a paused approval step and resumes the run through its rejection path.
  """
  @spec reject_run(Ecto.UUID.t(), map(), keyword()) ::
          {:ok, Run.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | {:missing_config, [atom()]}
             | Runs.Store.transition_error()
             | term()}
  def reject_run(run_id, attrs, overrides \\ []) when is_map(attrs) and is_list(overrides) do
    with {:ok, config} <- Config.load(overrides),
         {:ok, run} <- Runs.Store.get_run(config.repo, run_id),
         :ok <- Reviewer.review(config, run, :rejected, attrs) do
      Runs.Store.get_run(config.repo, run_id)
    end
  end

  @doc """
  Creates a new run from a prior run and links it to the original run.

  Replays are blocked by default once the source run completed an irreversible
  or non-compensatable step. Pass `allow_irreversible: true` only after an
  operator has reviewed the side effect and accepted re-execution.
  """
  @spec replay_run(Ecto.UUID.t(), keyword()) ::
          {:ok, Run.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | {:missing_config, [atom()]}
             | Runs.Store.replay_error()}
          | {:error, {:dispatch_failed, term()}}
  def replay_run(run_id, overrides \\ []) do
    {replay_opts, config_overrides} = Keyword.split(overrides, [:allow_irreversible])

    with {:ok, config} <- Config.load(config_overrides),
         {:ok, run} <-
           Runs.Store.replay_and_dispatch_run(
             config.repo,
             run_id,
             fn run ->
               Dispatcher.dispatch_run(config, run)
             end,
             replay_opts
           ) do
      SquidMesh.Observability.emit_run_replayed(run)
      {:ok, run}
    else
      {:error, reason} = error when reason in [:not_found, :invalid_run_id] ->
        error

      {:error, {:unsafe_replay, _details} = reason} ->
        {:error, reason}

      {:error, {:invalid_run, _changeset} = reason} ->
        {:error, reason}

      {:error, %_struct{} = reason} ->
        {:error, {:dispatch_failed, reason}}

      {:error, reason} ->
        {:error, {:dispatch_failed, reason}}
    end
  end

  defp normalize_start_error(reason) when is_tuple(reason) and elem(reason, 0) == :invalid_run,
    do: {:error, reason}

  defp normalize_start_error(reason) when reason in [:not_found], do: {:error, reason}
  defp normalize_start_error(%_struct{} = reason), do: {:error, {:dispatch_failed, reason}}
  defp normalize_start_error(reason) when is_tuple(reason), do: {:error, reason}
  defp normalize_start_error(reason), do: {:error, {:dispatch_failed, reason}}

  defp start_default_run(config, workflow, payload) do
    config.repo
    |> Runs.Store.create_and_dispatch_run(workflow, payload, fn run ->
      Dispatcher.dispatch_run(config, run)
    end)
    |> normalize_created_run()
  end

  defp start_triggered_run(config, workflow, trigger_name, payload) do
    config.repo
    |> Runs.Store.create_and_dispatch_run(workflow, trigger_name, payload, fn run ->
      Dispatcher.dispatch_run(config, run)
    end)
    |> normalize_created_run()
  end

  defp normalize_created_run({:ok, %Run{} = run}) do
    SquidMesh.Observability.emit_run_created(run)
    {:ok, run}
  end

  defp normalize_created_run({:error, reason}), do: normalize_start_error(reason)

  defp start_initial_context_run(config, workflow, trigger_name, payload, initial_context) do
    config.repo
    |> Runs.Store.create_and_dispatch_run(
      workflow,
      trigger_name,
      payload,
      fn run -> Dispatcher.dispatch_run(config, run) end,
      initial_context: initial_context
    )
    |> normalize_initial_context_run(config)
  end

  defp normalize_initial_context_run({:ok, %Run{} = run}, _config) do
    SquidMesh.Observability.emit_run_created(run)
    {:ok, run}
  end

  defp normalize_initial_context_run({:error, {:duplicate_schedule_start, identity}}, config) do
    case Runs.Store.get_run_by_schedule_idempotency(config.repo, identity) do
      {:ok, run} -> {:ok, {:duplicate_schedule_start, run}}
      {:error, reason} -> normalize_start_error(reason)
    end
  end

  defp normalize_initial_context_run({:error, reason}, _config), do: normalize_start_error(reason)

  defp reject_public_start_options(overrides) do
    cond do
      Keyword.has_key?(overrides, :context) ->
        {:error, {:invalid_option, :context}}

      Keyword.has_key?(overrides, :initial_context) ->
        {:error, {:invalid_option, :initial_context}}

      true ->
        :ok
    end
  end

  defp inspect_runtime_table_run(run_id, overrides) do
    overrides = runtime_table_read_options(overrides)
    {inspect_opts, config_overrides} = Keyword.split(overrides, [:include_history])

    with {:ok, config} <- Config.load(config_overrides) do
      Runs.Store.get_run(config.repo, run_id, inspect_opts)
    end
  end

  defp explain_runtime_table_run(run_id, overrides) do
    overrides = runtime_table_read_options(overrides)

    with {:ok, config} <- Config.load(overrides) do
      Explanation.explain(config, run_id)
    end
  end

  defp inspect_projected_run(run_id, overrides) when is_binary(run_id) do
    with {:ok, storage} <- journal_storage(overrides) do
      Inspection.snapshot(storage, run_id, projected_snapshot_options(overrides))
    end
  end

  defp inspect_projected_run(run_id, _overrides) do
    {:error, {:invalid_option, {:run_id, run_id}}}
  end

  defp explain_projected_run(run_id, overrides) do
    with {:ok, storage} <- journal_storage(overrides) do
      SquidMesh.ReadModel.Explanation.explain(
        storage,
        run_id,
        projected_snapshot_options(overrides)
      )
    end
  end

  defp read_model(overrides) when is_list(overrides) do
    if Keyword.keyword?(overrides) do
      case Keyword.get(overrides, :read_model, :runtime_tables) do
        read_model when read_model in @read_models -> {:ok, read_model}
        read_model -> {:error, {:invalid_option, {:read_model, read_model}}}
      end
    else
      {:error, {:invalid_option, {:opts, overrides}}}
    end
  end

  defp read_model(overrides), do: {:error, {:invalid_option, {:opts, overrides}}}

  defp journal_storage(overrides) do
    case Keyword.get(overrides, :journal_storage) do
      nil -> {:error, {:invalid_option, {:journal_storage, nil}}}
      storage -> {:ok, storage}
    end
  end

  defp projected_snapshot_options(overrides) do
    Keyword.take(overrides, [:queue, :now])
  end

  defp runtime_table_read_options(overrides) do
    Keyword.drop(overrides, [:read_model | @projection_read_options])
  end
end
