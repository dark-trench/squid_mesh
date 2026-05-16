defmodule SquidMesh.RunStore.Persistence do
  @moduledoc """
  Write-side persistence helpers for workflow runs.

  These helpers keep record construction and serialization close to the
  database-facing code while `SquidMesh.RunStore` continues to expose the
  public lifecycle API.
  """

  alias SquidMesh.Persistence.Run, as: RunRecord
  alias SquidMesh.Run
  alias SquidMesh.RunStore.Serialization
  alias SquidMesh.Workflow.Definition, as: WorkflowDefinition

  # Replays intentionally drop step-derived context. Only reserved run-level
  # facts that describe how the run was started are copied into the new run.
  @reserved_run_context_keys [:schedule]
  @schedule_idempotency_index "squid_mesh_runs_schedule_idempotency_index"

  @type transition_attrs :: %{
          optional(:context) => map(),
          optional(:current_step) => String.t() | atom() | nil,
          optional(:last_error) => map() | nil
        }
  @type schedule_start_identity :: %{
          workflow: String.t(),
          trigger: String.t(),
          idempotency_key: String.t()
        }

  @spec build_run_attrs(module(), atom(), WorkflowDefinition.t(), map(), keyword()) :: map()
  def build_run_attrs(workflow, trigger, definition, resolved_payload, opts \\ []) do
    %{
      workflow: WorkflowDefinition.serialize_workflow(workflow),
      trigger: WorkflowDefinition.serialize_trigger(trigger),
      status: "pending",
      input: resolved_payload,
      context: initial_context(opts),
      current_step: initial_current_step(definition)
    }
  end

  @spec replay_run_attrs(RunRecord.t(), WorkflowDefinition.t()) :: map()
  def replay_run_attrs(source_run, definition) do
    %{
      workflow: source_run.workflow,
      trigger: source_run.trigger,
      status: "pending",
      input: source_run.input || %{},
      context: replay_context(source_run.context || %{}),
      current_step: initial_current_step(definition),
      replayed_from_run_id: source_run.id
    }
  end

  @spec transition_changeset_attrs(Run.status(), transition_attrs()) :: map()
  def transition_changeset_attrs(to_status, attrs) do
    attrs
    |> Map.take([:context, :current_step, :last_error])
    |> serialize_transition_attrs()
    |> Map.put(:status, Serialization.serialize_status(to_status))
  end

  @spec serialize_transition_attrs(map()) :: map()
  def serialize_transition_attrs(attrs) do
    Map.update(attrs, :current_step, nil, fn
      nil -> nil
      current_step when is_atom(current_step) -> Atom.to_string(current_step)
      current_step -> current_step
    end)
  end

  @spec insert_run_record(module(), map()) ::
          {:ok, Run.t()}
          | {:error, {:invalid_run, Ecto.Changeset.t()}}
          | {:error, {:duplicate_schedule_start, schedule_start_identity()}}
  def insert_run_record(repo, attrs) do
    %RunRecord{}
    |> RunRecord.changeset(attrs)
    |> repo.insert()
    |> case do
      {:ok, run} ->
        {:ok, Serialization.to_public_run(run)}

      {:error, changeset} ->
        if schedule_start_duplicate?(changeset) do
          {:error, {:duplicate_schedule_start, schedule_start_identity(attrs)}}
        else
          {:error, {:invalid_run, changeset}}
        end
    end
  end

  @spec update_run_record(module(), RunRecord.t(), map()) ::
          {:ok, Run.t()} | {:error, {:invalid_run, Ecto.Changeset.t()}}
  def update_run_record(repo, run, attrs) do
    run
    |> RunRecord.changeset(attrs)
    |> repo.update()
    |> case do
      {:ok, updated_run} -> {:ok, Serialization.to_public_run(updated_run)}
      {:error, changeset} -> {:error, {:invalid_run, changeset}}
    end
  end

  @spec insert_run_with_dispatch(module(), map(), (Run.t() -> {:ok, term()} | {:error, term()})) ::
          {:ok, Run.t()}
          | {:error,
             {:invalid_run, Ecto.Changeset.t()}
             | {:duplicate_schedule_start, schedule_start_identity()}
             | term()}
  def insert_run_with_dispatch(repo, attrs, dispatch_fun) do
    case repo.transaction(fn ->
           with {:ok, run} <- insert_run_record(repo, attrs),
                {:ok, _result} <- dispatch_fun.(run) do
             run
           else
             {:error, reason} -> repo.rollback(reason)
           end
         end) do
      {:ok, run} -> {:ok, run}
      {:error, _reason} = error -> error
    end
  end

  @spec noop_dispatch(Run.t()) :: {:ok, :noop}
  def noop_dispatch(_run), do: {:ok, :noop}

  @spec cancellation_target_status(Run.status()) ::
          {:ok, Run.status()} | {:error, {:invalid_transition, Run.status(), Run.status()}}
  def cancellation_target_status(:pending), do: {:ok, :cancelled}
  def cancellation_target_status(:running), do: {:ok, :cancelling}
  def cancellation_target_status(:retrying), do: {:ok, :cancelling}
  def cancellation_target_status(:paused), do: {:ok, :cancelled}
  def cancellation_target_status(state), do: {:error, {:invalid_transition, state, :cancelling}}

  defp initial_current_step(definition) do
    if WorkflowDefinition.dependency_mode?(definition) do
      nil
    else
      WorkflowDefinition.serialize_step(WorkflowDefinition.initial_step(definition))
    end
  end

  defp replay_context(context) do
    context
    |> pick_reserved_context()
    |> Map.update(:schedule, nil, &replay_schedule_context/1)
    |> Map.reject(fn {_key, value} -> is_nil(value) end)
  end

  defp replay_schedule_context(schedule) when is_map(schedule) do
    Map.drop(schedule, [:idempotency, "idempotency", :idempotency_key, "idempotency_key"])
  end

  defp replay_schedule_context(_schedule), do: nil

  defp replay_context_value(context, key) do
    case Map.fetch(context, Atom.to_string(key)) do
      {:ok, value} -> value
      :error -> Map.get(context, key)
    end
  end

  defp initial_context(opts) do
    opts
    |> Keyword.get(:initial_context, %{})
    |> pick_reserved_context()
  end

  defp pick_reserved_context(context) do
    Map.new(@reserved_run_context_keys, fn key ->
      {key, replay_context_value(context, key)}
    end)
    |> Map.reject(fn {_key, value} -> is_nil(value) end)
  end

  defp schedule_start_duplicate?(changeset) do
    Enum.any?(changeset.errors, fn
      {_field, {_message, meta}} ->
        meta[:constraint] == :unique and
          to_string(meta[:constraint_name]) == @schedule_idempotency_index

      _other ->
        false
    end)
  end

  defp schedule_start_identity(attrs) do
    schedule = Map.get(attrs, :context, %{}) |> replay_context_value(:schedule)

    %{
      workflow: Map.fetch!(attrs, :workflow),
      trigger: Map.fetch!(attrs, :trigger),
      idempotency_key: replay_context_value(schedule, :idempotency_key)
    }
  end
end
