defmodule SquidMesh do
  @moduledoc """
  Public entrypoint for the Squid Mesh runtime.

  The API exposed here stays focused on declarative workflow operations. Host
  applications start, inspect, and later control runs through this surface
  without needing to work directly with persistence internals.
  """

  alias SquidMesh.Config
  alias SquidMesh.ReadModel.Inspection
  alias SquidMesh.ReadModel.Listing
  alias SquidMesh.Runs.GraphInspection
  alias SquidMesh.Runtime.Journal.Cancellation
  alias SquidMesh.Runtime.Journal.Executor
  alias SquidMesh.Runtime.Journal.ManualControl
  alias SquidMesh.Runtime.Journal.Options
  alias SquidMesh.Runtime.Journal.Replay
  alias SquidMesh.Runtime.Journal.Starter
  alias SquidMesh.Runtime.ScheduleIdentity

  @read_models [:read_model]
  @runtimes [:journal]
  @projection_snapshot_options [:queue, :now]
  @projection_list_options [:queue, :now]
  @journal_start_options [:runtime, :journal_storage, :queue, :now, :run_id]
  @journal_control_options [:runtime, :journal_storage, :queue, :now]
  @journal_execute_options [:runtime, :journal_storage, :queue, :owner_id, :lease_for, :now]

  @typedoc """
  Structured validation errors returned by the public read-model APIs.
  """
  @type read_option_error ::
          {:invalid_option,
           {:opts, term()}
           | {:read_model, term()}
           | {:journal_storage, nil}
           | {:run_id, term()}}

  @typedoc """
  Structured validation errors returned by the public start APIs.
  """
  @type start_option_error ::
          {:invalid_option,
           {:opts, term()}
           | {:runtime, term()}
           | {:journal_storage, nil}
           | {:queue, term()}
           | {:now, term()}
           | {:run_id, term()}
           | {:schedule_idempotency_key, term()}}

  @doc """
  Loads Squid Mesh configuration from the application environment with optional
  runtime overrides.
  """
  @spec config(keyword()) :: {:ok, Config.t()} | {:error, Config.config_error()}
  defdelegate config(overrides \\ []), to: Config, as: :load

  @doc """
  Loads Squid Mesh configuration and raises if required keys are missing.
  """
  @spec config!(keyword()) :: Config.t()
  defdelegate config!(overrides \\ []), to: Config, as: :load!

  @doc """
  Starts a new workflow run with the given payload through the workflow's
  default trigger.

  The Jido journal runtime returns a projection-backed
  `SquidMesh.ReadModel.Inspection.Snapshot`.
  """
  @spec start_run(module(), map()) ::
          {:ok, SquidMesh.ReadModel.Inspection.Snapshot.t()}
          | {:error, Config.config_error()}
          | {:error, start_option_error()}
          | {:error, Starter.start_error()}
          | {:error, {:dispatch_failed, term()}}
  def start_run(workflow, payload) when is_map(payload) do
    start_run(workflow, payload, [])
  end

  @spec start_run(module(), map(), keyword()) ::
          {:ok, SquidMesh.ReadModel.Inspection.Snapshot.t()}
          | {:error, Config.config_error()}
          | {:error, {:invalid_option, atom()}}
          | {:error, start_option_error()}
          | {:error, Starter.start_error()}
          | {:error, {:dispatch_failed, term()}}
  def start_run(workflow, payload, overrides) when is_map(payload) and is_list(overrides) do
    with :ok <- reject_public_start_options(overrides),
         {:ok, :journal} <- runtime(overrides) do
      start_default_run_with_runtime(:journal, workflow, payload, overrides)
    end
  end

  def start_run(_workflow, _payload, overrides) when is_list(overrides) do
    {:error, {:invalid_payload, :expected_map}}
  end

  @doc """
  Starts a new workflow run through a named trigger with the given payload.
  """
  @spec start_run(module(), atom(), map()) ::
          {:ok, SquidMesh.ReadModel.Inspection.Snapshot.t()}
          | {:error, Config.config_error()}
          | {:error, start_option_error()}
          | {:error, Starter.start_error()}
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

  The Jido journal runtime is the default and infers Ecto-backed journal storage
  from the configured repo. Pass `journal_storage:` only when a host, test, or
  integration boundary needs a non-default storage adapter. Journal execution
  supports normal action steps, immediate built-in `:log` steps, built-in
  `:wait` steps in transition and dependency workflows, and manual `:pause` or
  `:approval` boundaries.
  """
  @spec start_run(module(), atom(), map(), keyword()) ::
          {:ok, SquidMesh.ReadModel.Inspection.Snapshot.t()}
          | {:error, Config.config_error()}
          | {:error, {:invalid_option, atom()}}
          | {:error, start_option_error()}
          | {:error, Starter.start_error()}
          | {:error, {:dispatch_failed, term()}}
  def start_run(workflow, trigger_name, payload, overrides)
      when is_atom(trigger_name) and is_map(payload) and is_list(overrides) do
    with :ok <- reject_public_start_options(overrides),
         {:ok, :journal} <- runtime(overrides) do
      start_triggered_run_with_runtime(:journal, workflow, trigger_name, payload, overrides)
    end
  end

  @doc false
  @spec start_run_with_initial_context(module(), atom(), map(), map(), keyword()) ::
          {:ok, SquidMesh.ReadModel.Inspection.Snapshot.t()}
          | {:ok, {:duplicate_schedule_start, SquidMesh.ReadModel.Inspection.Snapshot.t()}}
          | {:error, Config.config_error()}
          | {:error, start_option_error()}
          | {:error, Starter.start_error()}
          | {:error, {:dispatch_failed, term()}}
  def start_run_with_initial_context(workflow, trigger_name, payload, initial_context, overrides)
      when is_atom(trigger_name) and is_map(payload) and is_map(initial_context) and
             is_list(overrides) do
    with :ok <- reject_public_start_options(overrides),
         {:ok, :journal} <- runtime(overrides) do
      start_initial_context_run_with_runtime(
        :journal,
        workflow,
        trigger_name,
        payload,
        initial_context,
        overrides
      )
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Fetches one workflow run by id.

  The selected read model comes from host configuration unless overridden. The
  read model rebuilds a projection-backed snapshot from durable journal entries.
  """
  @spec inspect_run(String.t(), keyword()) ::
          {:ok, SquidMesh.ReadModel.Inspection.Snapshot.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | read_option_error()
             | Config.config_error()
             | Inspection.snapshot_error()}
  def inspect_run(run_id, overrides \\ []) do
    with {:ok, :read_model} <- read_model(overrides) do
      inspect_projected_run(run_id, overrides)
    end
  end

  @doc """
  Fetches one workflow run as graph-oriented inspection data.

  The graph projection preserves `inspect_run/2` as the factual run snapshot and
  derives nodes and edges from that same durable state. The selected read model
  comes from host configuration unless overridden.
  """
  @spec inspect_run_graph(String.t(), keyword()) ::
          {:ok, GraphInspection.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | read_option_error()
             | Config.config_error()
             | Inspection.snapshot_error()}
  def inspect_run_graph(run_id, overrides \\ []) do
    with {:ok, :read_model} <- read_model(overrides),
         {:ok, inspection} <- inspect_graph_source(run_id, :read_model, overrides) do
      {:ok, graph_inspection(inspection, :read_model, overrides)}
    end
  end

  @doc """
  Explains the current runtime state of one workflow run.

  The result is structured diagnostic data for host apps, CLIs, and dashboards.
  Use `inspect_run/2` for the factual run snapshot and `explain_run/2` when an
  operator-facing surface needs the reason, evidence, and valid next actions for
  the run's current state.

  The selected read model comes from host configuration unless overridden. The
  read model derives diagnostics from durable journal projections.
  """
  @spec explain_run(String.t(), keyword()) ::
          {:ok, SquidMesh.ReadModel.Explanation.Diagnostic.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | read_option_error()
             | Config.config_error()
             | SquidMesh.ReadModel.Explanation.explanation_error()}
  def explain_run(run_id, overrides \\ []) do
    with {:ok, :read_model} <- read_model(overrides) do
      explain_projected_run(run_id, overrides)
    end
  end

  @doc """
  Executes the next visible workflow attempt through the selected runtime.

  The default journal runtime claims one visible Jido journal-backed attempt,
  runs its declared step, and appends durable attempt completion or failure
  facts. Pass `journal_storage:` only when overriding the inferred Ecto storage
  boundary.
  """
  @spec execute_next(keyword()) :: Executor.execute_result()
  def execute_next(overrides \\ [])

  def execute_next(overrides) when is_list(overrides) do
    with :ok <- public_execute_options(overrides),
         {:ok, :journal} <- runtime(overrides) do
      Executor.execute_next(journal_execute_options(overrides))
    end
  end

  def execute_next(overrides), do: Executor.execute_next(overrides)

  defp public_execute_options(opts) do
    cond do
      not Keyword.keyword?(opts) ->
        :ok

      unsupported = Enum.find(Keyword.keys(opts), &(&1 not in public_execute_option_keys())) ->
        {:error, {:invalid_option, {:option, unsupported}}}

      true ->
        :ok
    end
  end

  defp public_execute_option_keys do
    [:runtime, :journal_storage, :queue, :owner_id, :lease_for, :now]
  end

  @doc """
  Lists workflow runs with optional filters.

  Journal-backed runtime calls return redacted listing summaries. Use
  `inspect_run/2` or `inspect_run_graph/2` with a run id when callers need
  detailed runtime state for one run.
  """
  @spec list_runs([Listing.list_filter()], keyword()) ::
          {:ok, [Listing.Summary.t()]}
          | {:error, Config.config_error() | Listing.list_error()}
  def list_runs(filters \\ [], overrides \\ []) do
    with {:ok, :journal} <- runtime(overrides) do
      list_runs_with_runtime(:journal, filters, overrides)
    end
  end

  @doc """
  Requests cancellation for an eligible workflow run.
  """
  @spec cancel_run(Ecto.UUID.t(), keyword()) ::
          {:ok, SquidMesh.ReadModel.Inspection.Snapshot.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | Config.config_error()
             | Cancellation.cancel_error()}
  def cancel_run(run_id, overrides \\ []) do
    with {:ok, :journal} <- runtime(overrides) do
      cancel_run_with_runtime(:journal, run_id, overrides)
    end
  end

  @doc """
  Resumes a run that is intentionally paused for manual intervention.

  This arity uses the configured runtime. By default, it resolves an inspectable
  journal pause boundary using the inferred Ecto journal storage.
  """
  @spec unblock_run(Ecto.UUID.t()) ::
          {:ok, SquidMesh.ReadModel.Inspection.Snapshot.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | Config.config_error()
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
          {:ok, SquidMesh.ReadModel.Inspection.Snapshot.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | Config.config_error()
             | term()}
  def unblock_run(run_id, overrides) when is_list(overrides) do
    unblock_run(run_id, %{}, overrides)
  end

  @spec unblock_run(Ecto.UUID.t(), map()) ::
          {:ok, SquidMesh.ReadModel.Inspection.Snapshot.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | Config.config_error()
             | term()}
  def unblock_run(run_id, attrs) when is_map(attrs) do
    unblock_run(run_id, attrs, [])
  end

  @doc """
  Resumes a paused run with manual action attributes and configuration overrides.

  By default, this resolves an inspectable journal pause boundary and persists
  the manual action attributes in journal resolution metadata. Pass
  `journal_storage:` only when overriding the inferred Ecto storage boundary.
  """
  @spec unblock_run(Ecto.UUID.t(), map(), keyword()) ::
          {:ok, SquidMesh.ReadModel.Inspection.Snapshot.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | Config.config_error()
             | term()}
  def unblock_run(run_id, attrs, overrides) when is_map(attrs) and is_list(overrides) do
    with {:ok, :journal} <- runtime(overrides) do
      ManualControl.resume(run_id, attrs, journal_control_options(overrides))
    end
  end

  @doc """
  Approves a paused approval step and resumes the run through its success path.

  By default, this resolves an inspectable journal approval boundary and
  persists the decision as journal facts. Pass `journal_storage:` only when
  overriding the inferred Ecto storage boundary.
  """
  @spec approve_run(Ecto.UUID.t(), map(), keyword()) ::
          {:ok, SquidMesh.ReadModel.Inspection.Snapshot.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | Config.config_error()
             | term()}
  def approve_run(run_id, attrs, overrides \\ []) when is_map(attrs) and is_list(overrides) do
    with {:ok, :journal} <- runtime(overrides) do
      ManualControl.approve(run_id, attrs, journal_control_options(overrides))
    end
  end

  @doc """
  Rejects a paused approval step and resumes the run through its rejection path.

  By default, this resolves an inspectable journal approval boundary and
  persists the decision as journal facts. Pass `journal_storage:` only when
  overriding the inferred Ecto storage boundary.
  """
  @spec reject_run(Ecto.UUID.t(), map(), keyword()) ::
          {:ok, SquidMesh.ReadModel.Inspection.Snapshot.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | Config.config_error()
             | term()}
  def reject_run(run_id, attrs, overrides \\ []) when is_map(attrs) and is_list(overrides) do
    with {:ok, :journal} <- runtime(overrides) do
      ManualControl.reject(run_id, attrs, journal_control_options(overrides))
    end
  end

  @doc """
  Creates a new run from a prior run and links it to the original run.

  Replays are blocked by default once the source run completed an irreversible
  or non-compensatable step. Pass `allow_irreversible: true` only after an
  operator has reviewed the side effect and accepted re-execution.
  """
  @spec replay_run(Ecto.UUID.t(), keyword()) ::
          {:ok, SquidMesh.ReadModel.Inspection.Snapshot.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | Config.config_error()
             | Replay.replay_error()}
          | {:error, {:dispatch_failed, term()}}
  def replay_run(run_id, overrides \\ []) do
    {replay_opts, config_overrides} = Keyword.split(overrides, [:allow_irreversible])

    with {:ok, :journal} <- runtime(config_overrides),
         {:ok, run} <- replay_run_with_runtime(:journal, run_id, replay_opts, config_overrides) do
      {:ok, run}
    else
      {:error, reason} = error when reason in [:not_found, :invalid_run_id] ->
        error

      {:error, {:unsafe_replay, _details} = reason} ->
        {:error, reason}

      {:error, {:invalid_run, _changeset} = reason} ->
        {:error, reason}

      {:error, {:invalid_option, _details} = reason} ->
        {:error, reason}

      {:error, {:incompatible_workflow_definition, _operation} = reason} ->
        {:error, reason}

      {:error, {:invalid_replay_source, _details} = reason} ->
        {:error, reason}

      {:error, %_struct{} = reason} ->
        {:error, {:dispatch_failed, reason}}

      {:error, reason} ->
        {:error, {:dispatch_failed, reason}}
    end
  end

  defp replay_run_with_runtime(:journal, run_id, replay_opts, config_overrides) do
    Replay.replay(run_id, replay_opts, journal_control_options(config_overrides))
  end

  defp start_default_run_with_runtime(:journal, workflow, payload, overrides) do
    Starter.start_run(workflow, nil, payload, journal_start_options(overrides))
  end

  defp start_triggered_run_with_runtime(:journal, workflow, trigger_name, payload, overrides) do
    Starter.start_run(workflow, trigger_name, payload, journal_start_options(overrides))
  end

  defp start_initial_context_run_with_runtime(
         :journal,
         workflow,
         trigger_name,
         payload,
         initial_context,
         overrides
       ) do
    with {:ok, opts} <-
           journal_initial_context_start_options(
             workflow,
             trigger_name,
             initial_context,
             overrides
           ) do
      Starter.start_run(workflow, trigger_name, payload, opts)
    end
  end

  defp list_runs_with_runtime(:journal, filters, overrides) do
    with {:ok, storage} <- journal_storage(overrides) do
      Listing.list(storage, filters, journal_list_options(overrides))
    end
  end

  defp cancel_run_with_runtime(:journal, run_id, overrides) do
    Cancellation.cancel(run_id, journal_control_options(overrides))
  end

  defp journal_initial_context_start_options(workflow, trigger_name, initial_context, overrides) do
    opts =
      overrides
      |> journal_start_options()
      |> Keyword.put(:initial_context, initial_context)

    with {:ok, idempotency_key} <- schedule_idempotency_key(initial_context) do
      case idempotency_key do
        nil ->
          {:ok, opts}

        idempotency_key ->
          opts =
            opts
            |> Keyword.put(:run_id, schedule_run_id(workflow, trigger_name, idempotency_key))
            |> Keyword.put(:duplicate_schedule_start, true)

          {:ok, opts}
      end
    end
  end

  defp schedule_idempotency_key(context) when is_map(context) do
    context
    |> schedule_context()
    |> schedule_value(:idempotency_key)
    |> validate_schedule_idempotency_key()
  end

  defp validate_schedule_idempotency_key(nil), do: {:ok, nil}

  defp validate_schedule_idempotency_key(key) when is_binary(key), do: {:ok, key}

  defp validate_schedule_idempotency_key(_key) do
    {:error, {:invalid_option, {:schedule_idempotency_key, :invalid}}}
  end

  defp schedule_context(context) do
    case Map.fetch(context, :schedule) do
      {:ok, schedule} -> schedule
      :error -> Map.get(context, "schedule", %{})
    end
  end

  defp schedule_value(schedule, key) when is_map(schedule) do
    case Map.fetch(schedule, key) do
      {:ok, value} -> value
      :error -> Map.get(schedule, Atom.to_string(key))
    end
  end

  defp schedule_value(_schedule, _key), do: nil

  defp schedule_run_id(workflow, trigger_name, idempotency_key) do
    workflow_name = SquidMesh.Workflow.Definition.serialize_workflow(workflow)
    trigger = SquidMesh.Workflow.Definition.serialize_trigger(trigger_name)

    {:ok, run_id} = ScheduleIdentity.run_id(workflow_name, trigger, idempotency_key)
    run_id
  end

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

  defp inspect_projected_run(run_id, overrides) when is_binary(run_id) do
    with {:ok, storage} <- journal_storage(overrides) do
      Inspection.snapshot(storage, run_id, projected_snapshot_options(overrides))
    end
  end

  defp inspect_projected_run(_run_id, _overrides) do
    {:error, {:invalid_option, {:run_id, :invalid}}}
  end

  defp inspect_graph_source(run_id, :read_model, overrides) do
    inspect_projected_run(run_id, overrides)
  end

  defp graph_inspection(%Inspection.Snapshot{} = snapshot, read_model, overrides) do
    GraphInspection.from_snapshot(snapshot, graph_inspection_options(read_model, overrides))
  end

  defp graph_inspection_options(read_model, overrides) do
    [
      source: read_model,
      include_details: Keyword.get(overrides, :include_history, false)
    ]
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
    with :ok <- validate_keyword_options(overrides) do
      configured_read_model(overrides)
    end
  end

  defp read_model(_overrides), do: {:error, {:invalid_option, {:opts, :invalid}}}

  defp runtime(overrides) when is_list(overrides) do
    with :ok <- validate_keyword_options(overrides) do
      configured_runtime(overrides)
    end
  end

  defp runtime(_overrides), do: {:error, {:invalid_option, {:opts, :invalid}}}

  defp validate_keyword_options(overrides) do
    cond do
      not Keyword.keyword?(overrides) ->
        {:error, {:invalid_option, {:opts, :invalid}}}

      Keyword.has_key?(overrides, :executor) ->
        {:error, {:invalid_option, {:executor, :unsupported}}}

      Keyword.has_key?(overrides, :stale_step_timeout) ->
        {:error, {:invalid_option, {:stale_step_timeout, :unsupported}}}

      true ->
        :ok
    end
  end

  defp configured_read_model(overrides) do
    case Keyword.fetch(overrides, :read_model) do
      {:ok, read_model} when read_model in @read_models ->
        {:ok, read_model}

      {:ok, _read_model} ->
        {:error, {:invalid_option, {:read_model, :invalid}}}

      :error ->
        load_configured_read_model(overrides)
    end
  end

  defp load_configured_read_model(overrides) do
    case Config.load(config_routing_overrides(overrides)) do
      {:ok, %Config{read_model: read_model}} -> {:ok, read_model}
      {:error, _reason} = error -> error
    end
  end

  defp configured_runtime(overrides) do
    case Keyword.fetch(overrides, :runtime) do
      {:ok, runtime} when runtime in @runtimes ->
        {:ok, runtime}

      {:ok, _runtime} ->
        {:error, {:invalid_option, {:runtime, :invalid}}}

      :error ->
        load_configured_runtime(overrides)
    end
  end

  defp load_configured_runtime(overrides) do
    case Config.load(config_routing_overrides(overrides)) do
      {:ok, %Config{runtime: runtime}} -> {:ok, runtime}
      {:error, _reason} = error -> error
    end
  end

  defp journal_storage(overrides) do
    case Keyword.fetch(overrides, :journal_storage) do
      {:ok, storage} ->
        Options.storage(storage)

      :error ->
        case Config.load(config_routing_overrides(overrides)) do
          {:ok, %Config{} = config} -> Options.storage(config.journal_storage)
          {:error, {:missing_config, [:journal_storage]}} -> Options.storage(nil)
          {:error, _reason} = error -> error
        end
    end
  end

  defp projected_snapshot_options(overrides) do
    configured_journal_options(overrides, @projection_snapshot_options)
  end

  defp journal_start_options(overrides) do
    configured_journal_options(overrides, @journal_start_options)
  end

  defp journal_control_options(overrides) do
    configured_journal_options(overrides, @journal_control_options)
  end

  defp journal_execute_options(overrides) do
    configured_journal_options(overrides, @journal_execute_options)
  end

  defp journal_list_options(overrides) do
    configured_journal_options(overrides, @projection_list_options)
  end

  defp configured_journal_options(overrides, keys) do
    case load_config_for_journal_options(overrides) do
      {:ok, %Config{} = config} ->
        [
          runtime: :journal,
          journal_storage: config.journal_storage,
          queue: config.queue
        ]
        |> Keyword.merge(Keyword.take(overrides, keys))
        |> Keyword.take(keys)

      {:error, _reason} ->
        Keyword.take(overrides, keys)
    end
  end

  defp load_config_for_journal_options(overrides) do
    case Config.load(config_routing_overrides(overrides)) do
      {:ok, %Config{} = config} ->
        {:ok, config}

      {:error, _reason} ->
        overrides
        |> Keyword.delete(:journal_storage)
        |> config_routing_overrides()
        |> Config.load()
    end
  end

  defp config_routing_overrides(overrides) do
    Keyword.take(overrides, [
      :repo,
      :runtime,
      :read_model,
      :journal_storage
    ])
  end
end
