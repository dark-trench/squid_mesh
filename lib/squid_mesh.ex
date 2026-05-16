defmodule SquidMesh do
  @moduledoc """
  Public entrypoint for the Squid Mesh runtime.

  The API exposed here stays focused on declarative workflow operations. Host
  applications start, inspect, and later control runs through this surface
  without needing to work directly with persistence internals.
  """

  alias SquidMesh.Config
  alias SquidMesh.Run
  alias SquidMesh.RunExplanation
  alias SquidMesh.RunStore
  alias SquidMesh.Runtime.Dispatcher
  alias SquidMesh.Runtime.Reviewer
  alias SquidMesh.Runtime.Unblocker

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
          | {:error, RunStore.create_error()}
          | {:error, {:dispatch_failed, term()}}
  def start_run(workflow, payload) when is_map(payload) do
    start_run(workflow, payload, [])
  end

  @spec start_run(module(), map(), keyword()) ::
          {:ok, Run.t()}
          | {:error, {:missing_config, [atom()]}}
          | {:error, {:invalid_option, atom()}}
          | {:error, RunStore.create_error()}
          | {:error, {:dispatch_failed, term()}}
  def start_run(workflow, payload, overrides) when is_map(payload) and is_list(overrides) do
    with :ok <- reject_public_start_options(overrides),
         {:ok, config} <- Config.load(overrides),
         {:ok, run} <-
           RunStore.create_and_dispatch_run(
             config.repo,
             workflow,
             payload,
             fn run ->
               Dispatcher.dispatch_run(config, run)
             end
           ) do
      SquidMesh.Observability.emit_run_created(run)
      {:ok, run}
    else
      {:error, reason} when is_tuple(reason) and elem(reason, 0) == :invalid_run ->
        {:error, reason}

      {:error, reason} = error when reason in [:not_found] ->
        error

      {:error, %_{} = reason} ->
        {:error, {:dispatch_failed, reason}}

      {:error, reason} when is_tuple(reason) ->
        {:error, reason}

      {:error, reason} ->
        {:error, {:dispatch_failed, reason}}
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
          | {:error, RunStore.create_error()}
          | {:error, {:dispatch_failed, term()}}
  def start_run(workflow, trigger_name, payload)
      when is_atom(trigger_name) and is_map(payload) do
    start_run(workflow, trigger_name, payload, [])
  end

  @spec start_run(module(), atom(), map(), keyword()) ::
          {:ok, Run.t()}
          | {:error, {:missing_config, [atom()]}}
          | {:error, {:invalid_option, atom()}}
          | {:error, RunStore.create_error()}
          | {:error, {:dispatch_failed, term()}}
  def start_run(workflow, trigger_name, payload, overrides)
      when is_atom(trigger_name) and is_map(payload) and is_list(overrides) do
    with :ok <- reject_public_start_options(overrides),
         {:ok, config} <- Config.load(overrides),
         {:ok, run} <-
           RunStore.create_and_dispatch_run(
             config.repo,
             workflow,
             trigger_name,
             payload,
             fn run -> Dispatcher.dispatch_run(config, run) end
           ) do
      SquidMesh.Observability.emit_run_created(run)
      {:ok, run}
    else
      {:error, reason} when is_tuple(reason) and elem(reason, 0) == :invalid_run ->
        {:error, reason}

      {:error, reason} = error when reason in [:not_found] ->
        error

      {:error, %_{} = reason} ->
        {:error, {:dispatch_failed, reason}}

      {:error, reason} when is_tuple(reason) ->
        {:error, reason}

      {:error, reason} ->
        {:error, {:dispatch_failed, reason}}
    end
  end

  @doc false
  @spec start_run_with_initial_context(module(), atom(), map(), map(), keyword()) ::
          {:ok, Run.t()}
          | {:ok, {:duplicate_schedule_start, Run.t()}}
          | {:error, {:missing_config, [atom()]}}
          | {:error, {:invalid_option, atom()}}
          | {:error, RunStore.create_error()}
          | {:error, {:dispatch_failed, term()}}
  def start_run_with_initial_context(workflow, trigger_name, payload, initial_context, overrides)
      when is_atom(trigger_name) and is_map(payload) and is_map(initial_context) and
             is_list(overrides) do
    with :ok <- reject_public_start_options(overrides),
         {:ok, config} <- Config.load(overrides) do
      case RunStore.create_and_dispatch_run(
             config.repo,
             workflow,
             trigger_name,
             payload,
             fn run -> Dispatcher.dispatch_run(config, run) end,
             initial_context: initial_context
           ) do
        {:ok, run} ->
          SquidMesh.Observability.emit_run_created(run)
          {:ok, run}

        {:error, {:duplicate_schedule_start, identity}} ->
          with {:ok, run} <- RunStore.get_run_by_schedule_idempotency(config.repo, identity) do
            {:ok, {:duplicate_schedule_start, run}}
          end

        {:error, reason} ->
          normalize_start_error(reason)
      end
    else
      {:error, reason} when is_tuple(reason) and elem(reason, 0) == :invalid_run ->
        {:error, reason}

      {:error, reason} = error when reason in [:not_found] ->
        error

      {:error, %_{} = reason} ->
        {:error, {:dispatch_failed, reason}}

      {:error, reason} when is_tuple(reason) ->
        {:error, reason}

      {:error, reason} ->
        {:error, {:dispatch_failed, reason}}
    end
  end

  defp normalize_start_error(reason) when is_tuple(reason) and elem(reason, 0) == :invalid_run,
    do: {:error, reason}

  defp normalize_start_error(reason) when reason in [:not_found], do: {:error, reason}
  defp normalize_start_error(%_{} = reason), do: {:error, {:dispatch_failed, reason}}
  defp normalize_start_error(reason) when is_tuple(reason), do: {:error, reason}
  defp normalize_start_error(reason), do: {:error, {:dispatch_failed, reason}}

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

  @doc """
  Fetches one workflow run by id.
  """
  @spec inspect_run(Ecto.UUID.t(), keyword()) ::
          {:ok, Run.t()} | {:error, :not_found | :invalid_run_id | {:missing_config, [atom()]}}
  def inspect_run(run_id, overrides \\ []) do
    {inspect_opts, config_overrides} = Keyword.split(overrides, [:include_history])

    with {:ok, config} <- Config.load(config_overrides) do
      RunStore.get_run(config.repo, run_id, inspect_opts)
    end
  end

  @doc """
  Explains the current runtime state of one workflow run.

  The result is structured diagnostic data for host apps, CLIs, and dashboards.
  Use `inspect_run/2` for the factual run snapshot and `explain_run/2` when an
  operator-facing surface needs the reason, evidence, and valid next actions for
  the run's current state.
  """
  @spec explain_run(Ecto.UUID.t(), keyword()) ::
          {:ok, RunExplanation.t()}
          | {:error, :not_found | :invalid_run_id | Config.config_error()}
  def explain_run(run_id, overrides \\ []) do
    with {:ok, config} <- Config.load(overrides) do
      RunExplanation.explain(config, run_id)
    end
  end

  @doc """
  Lists workflow runs with optional filters.
  """
  @spec list_runs(RunStore.list_filters(), keyword()) ::
          {:ok, [Run.t()]} | {:error, {:missing_config, [atom()]}}
  def list_runs(filters \\ [], overrides \\ []) do
    with {:ok, config} <- Config.load(overrides) do
      RunStore.list_runs(config.repo, filters)
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
             | RunStore.transition_error()}
  def cancel_run(run_id, overrides \\ []) do
    with {:ok, config} <- Config.load(overrides) do
      RunStore.cancel_run(config.repo, run_id)
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
             | RunStore.transition_error()
             | term()}
  def unblock_run(run_id), do: unblock_run(run_id, %{}, [])

  @spec unblock_run(Ecto.UUID.t(), keyword()) ::
          {:ok, Run.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | {:missing_config, [atom()]}
             | RunStore.transition_error()
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
             | RunStore.transition_error()
             | term()}
  def unblock_run(run_id, attrs) when is_map(attrs) do
    unblock_run(run_id, attrs, [])
  end

  @spec unblock_run(Ecto.UUID.t(), map(), keyword()) ::
          {:ok, Run.t()}
          | {:error,
             :not_found
             | :invalid_run_id
             | {:missing_config, [atom()]}
             | RunStore.transition_error()
             | term()}
  def unblock_run(run_id, attrs, overrides) when is_map(attrs) and is_list(overrides) do
    with {:ok, config} <- Config.load(overrides),
         {:ok, run} <- RunStore.get_run(config.repo, run_id),
         :ok <- Unblocker.unblock(config, run, attrs) do
      RunStore.get_run(config.repo, run_id)
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
             | RunStore.transition_error()
             | term()}
  def approve_run(run_id, attrs, overrides \\ []) when is_map(attrs) and is_list(overrides) do
    with {:ok, config} <- Config.load(overrides),
         {:ok, run} <- RunStore.get_run(config.repo, run_id),
         :ok <- Reviewer.review(config, run, :approved, attrs) do
      RunStore.get_run(config.repo, run_id)
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
             | RunStore.transition_error()
             | term()}
  def reject_run(run_id, attrs, overrides \\ []) when is_map(attrs) and is_list(overrides) do
    with {:ok, config} <- Config.load(overrides),
         {:ok, run} <- RunStore.get_run(config.repo, run_id),
         :ok <- Reviewer.review(config, run, :rejected, attrs) do
      RunStore.get_run(config.repo, run_id)
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
             :not_found | :invalid_run_id | {:missing_config, [atom()]} | RunStore.replay_error()}
          | {:error, {:dispatch_failed, term()}}
  def replay_run(run_id, overrides \\ []) do
    {replay_opts, config_overrides} = Keyword.split(overrides, [:allow_irreversible])

    with {:ok, config} <- Config.load(config_overrides),
         {:ok, run} <-
           RunStore.replay_and_dispatch_run(
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

      {:error, %_{} = reason} ->
        {:error, {:dispatch_failed, reason}}

      {:error, reason} ->
        {:error, {:dispatch_failed, reason}}
    end
  end
end
