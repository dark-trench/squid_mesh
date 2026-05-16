defmodule SquidMesh.RunStore do
  @moduledoc """
  Durable run persistence and lifecycle operations.

  This module translates between the public `SquidMesh.Run` struct and the
  underlying persistence schema while applying workflow-level rules such as
  payload validation, trigger resolution, replay lineage, and legal run-state
  transitions.
  """

  import Ecto.Query

  alias SquidMesh.AttemptStore
  alias SquidMesh.Persistence.Run, as: RunRecord
  alias SquidMesh.Persistence.StepAttempt
  alias SquidMesh.Persistence.StepRun
  alias SquidMesh.Observability
  alias SquidMesh.Run
  alias SquidMesh.RunStore.Persistence
  alias SquidMesh.RunStore.Serialization
  alias SquidMesh.Runtime.StateMachine
  alias SquidMesh.StepRunStore
  alias SquidMesh.Workflow.Definition, as: WorkflowDefinition

  @type list_filter :: {:workflow, module()} | {:status, Run.status()} | {:limit, pos_integer()}
  @type list_filters :: [list_filter()]

  @type create_error ::
          {:invalid_payload, :expected_map}
          | {:invalid_payload, WorkflowDefinition.payload_error_details()}
          | {:invalid_trigger, atom() | String.t()}
          | {:invalid_workflow, module() | String.t()}
          | {:invalid_run, Ecto.Changeset.t()}
          | {:duplicate_schedule_start, Persistence.schedule_start_identity()}

  @type get_error :: :not_found | :invalid_run_id
  @type transition_attrs :: %{
          optional(:context) => map(),
          optional(:current_step) => String.t() | atom() | nil,
          optional(:last_error) => map() | nil
        }
  @type transition_error ::
          get_error() | StateMachine.transition_error() | {:invalid_run, Ecto.Changeset.t()}
  @type replay_error :: get_error() | create_error() | {:unsafe_replay, map()}
  @type create_option :: {:initial_context, map()}
  @type replay_option :: {:allow_irreversible, boolean()}
  @type update_error :: get_error() | {:invalid_run, Ecto.Changeset.t()}
  @type get_option :: {:include_history, boolean()}
  @type dispatch_fun :: (Run.t() -> {:ok, term()} | {:error, term()})
  @type attrs_fun :: (Run.t() -> transition_attrs())
  @type failure_attrs_fun :: (Run.t(), term() -> transition_attrs())
  @type run_transition_event :: {:run_transition, Run.t(), Run.status(), Run.status()}
  @type progress_event :: run_transition_event() | term()
  @type progress_operation ::
          :update
          | {:transition, Run.status()}
          | {:dispatch, dispatch_fun()}
          | {:dispatch_or_fail, dispatch_fun(), failure_attrs_fun()}
          | {:transition_or_dispatch, Run.status(), dispatch_fun()}
          | {:transition_or_dispatch_or_fail, Run.status(), dispatch_fun(), failure_attrs_fun()}
  @type progress_result :: Run.t() | :noop

  @doc """
  Creates a new run for a workflow using the workflow's default trigger.
  """
  @spec create_run(module(), module(), map()) :: {:ok, Run.t()} | {:error, create_error()}
  def create_run(repo, workflow, payload) when is_map(payload) do
    case create_and_dispatch_run(repo, workflow, payload, &Persistence.noop_dispatch/1) do
      {:ok, run} ->
        Observability.emit_run_created(run)
        {:ok, run}

      {:error, _reason} = error ->
        error
    end
  end

  def create_run(_repo, _workflow, _payload), do: {:error, {:invalid_payload, :expected_map}}

  @doc """
  Creates a new run for a workflow through an explicit trigger.
  """
  @spec create_run(module(), module(), atom(), map()) :: {:ok, Run.t()} | {:error, create_error()}
  def create_run(repo, workflow, trigger_name, payload)
      when is_atom(trigger_name) and is_map(payload) do
    case create_and_dispatch_run(
           repo,
           workflow,
           trigger_name,
           payload,
           &Persistence.noop_dispatch/1
         ) do
      {:ok, run} ->
        Observability.emit_run_created(run)
        {:ok, run}

      {:error, _reason} = error ->
        error
    end
  end

  def create_run(_repo, _workflow, _trigger_name, _payload),
    do: {:error, {:invalid_payload, :expected_map}}

  @doc """
  Creates a new pending run from a prior run while preserving replay lineage.
  """
  @spec replay_run(module(), Ecto.UUID.t(), [replay_option()]) ::
          {:ok, Run.t()} | {:error, replay_error()}
  def replay_run(repo, run_id, opts \\ []) do
    case replay_and_dispatch_run(repo, run_id, &Persistence.noop_dispatch/1, opts) do
      {:ok, replay_run} ->
        Observability.emit_run_replayed(replay_run)
        {:ok, replay_run}

      {:error, _reason} = error ->
        error
    end
  end

  @doc false
  @spec create_and_dispatch_run(module(), module(), map(), dispatch_fun()) ::
          {:ok, Run.t()} | {:error, create_error() | term()}
  @spec create_and_dispatch_run(module(), module(), map(), dispatch_fun(), [create_option()]) ::
          {:ok, Run.t()} | {:error, create_error() | term()}
  @spec create_and_dispatch_run(module(), module(), atom(), map(), dispatch_fun()) ::
          {:ok, Run.t()} | {:error, create_error() | term()}
  @spec create_and_dispatch_run(
          module(),
          module(),
          atom(),
          map(),
          dispatch_fun(),
          [create_option()]
        ) ::
          {:ok, Run.t()} | {:error, create_error() | term()}
  def create_and_dispatch_run(repo, workflow, payload, dispatch_fun)
      when is_map(payload) and is_function(dispatch_fun, 1) do
    create_and_dispatch_run(repo, workflow, payload, dispatch_fun, [])
  end

  def create_and_dispatch_run(repo, workflow, trigger_name, payload, dispatch_fun)
      when is_atom(trigger_name) and is_map(payload) and is_function(dispatch_fun, 1) do
    create_and_dispatch_run(repo, workflow, trigger_name, payload, dispatch_fun, [])
  end

  def create_and_dispatch_run(repo, workflow, payload, dispatch_fun, opts)
      when is_map(payload) and is_function(dispatch_fun, 1) and is_list(opts) do
    with {:ok, definition} <- WorkflowDefinition.load(workflow),
         {:ok, trigger_definition} <-
           WorkflowDefinition.trigger(
             definition,
             WorkflowDefinition.default_trigger(definition)
           ),
         {:ok, resolved_payload} <-
           WorkflowDefinition.resolve_payload(trigger_definition, payload) do
      trigger = Map.fetch!(trigger_definition, :name)
      attrs = Persistence.build_run_attrs(workflow, trigger, definition, resolved_payload, opts)
      Persistence.insert_run_with_dispatch(repo, attrs, dispatch_fun)
    end
  end

  def create_and_dispatch_run(repo, workflow, trigger_name, payload, dispatch_fun, opts)
      when is_atom(trigger_name) and is_map(payload) and is_function(dispatch_fun, 1) and
             is_list(opts) do
    with {:ok, definition} <- WorkflowDefinition.load(workflow),
         {:ok, trigger_definition} <- WorkflowDefinition.trigger(definition, trigger_name),
         {:ok, resolved_payload} <-
           WorkflowDefinition.resolve_payload(trigger_definition, payload) do
      trigger = Map.fetch!(trigger_definition, :name)
      attrs = Persistence.build_run_attrs(workflow, trigger, definition, resolved_payload, opts)
      Persistence.insert_run_with_dispatch(repo, attrs, dispatch_fun)
    end
  end

  @doc false
  @spec replay_and_dispatch_run(module(), Ecto.UUID.t(), dispatch_fun(), [replay_option()]) ::
          {:ok, Run.t()} | {:error, replay_error() | term()}
  def replay_and_dispatch_run(repo, run_id, dispatch_fun, opts \\ [])
      when is_function(dispatch_fun, 1) and is_list(opts) do
    with {:ok, valid_run_id} <- cast_run_id(run_id) do
      replay_valid_run(repo, valid_run_id, dispatch_fun, opts)
    end
  end

  @doc """
  Fetches one persisted run and returns the public run representation.
  """
  @spec get_run(module(), Ecto.UUID.t(), [get_option()]) :: {:ok, Run.t()} | {:error, get_error()}
  def get_run(repo, run_id, opts \\ []) do
    with {:ok, valid_run_id} <- cast_run_id(run_id) do
      get_valid_run(repo, valid_run_id, opts)
    end
  end

  @doc false
  @spec get_run_for_update(module(), Ecto.UUID.t()) :: {:ok, Run.t()} | {:error, get_error()}
  def get_run_for_update(repo, run_id) do
    with {:ok, valid_run_id} <- cast_run_id(run_id) do
      case get_run_record_for_update(repo, valid_run_id) do
        %RunRecord{} = run -> {:ok, Serialization.to_public_run(run)}
        nil -> {:error, :not_found}
      end
    end
  end

  @doc false
  @spec get_run_by_schedule_idempotency(module(), Persistence.schedule_start_identity()) ::
          {:ok, Run.t()} | {:error, :not_found}
  def get_run_by_schedule_idempotency(
        repo,
        %{workflow: workflow, trigger: trigger, idempotency_key: idempotency_key}
      )
      when is_binary(workflow) and is_binary(trigger) and is_binary(idempotency_key) do
    query =
      RunRecord
      |> where([run], run.workflow == ^workflow)
      |> where([run], run.trigger == ^trigger)
      |> where(
        [run],
        fragment("?->'schedule'->>'idempotency_key' = ?", run.context, ^idempotency_key)
      )
      |> order_by([run], desc: run.inserted_at, desc: run.id)
      |> limit(1)

    case repo.one(query) do
      %RunRecord{} = run -> {:ok, Serialization.to_public_run(run)}
      nil -> {:error, :not_found}
    end
  end

  @doc """
  Lists runs using the supported filter set.
  """
  @spec list_runs(module(), list_filters()) :: {:ok, [Run.t()]}
  def list_runs(repo, filters \\ []) do
    runs =
      repo
      |> query_runs(filters)
      |> Enum.map(&Serialization.to_public_run/1)

    {:ok, runs}
  end

  @doc """
  Applies a validated run-state transition and persists the updated run.
  """
  @spec transition_run(module(), Ecto.UUID.t(), Run.status(), transition_attrs()) ::
          {:ok, Run.t()} | {:error, transition_error()}
  def transition_run(repo, run_id, to_status, attrs \\ %{}) when is_map(attrs) do
    case transition_run_silent(repo, run_id, to_status, attrs) do
      {:ok, {run, from_status, to_status}} ->
        Observability.emit_run_transition(run, from_status, to_status)
        {:ok, run}

      {:error, _reason} = error ->
        error
    end
  end

  @doc false
  @spec transition_run_silent(module(), Ecto.UUID.t(), Run.status(), transition_attrs()) ::
          {:ok, {Run.t(), Run.status(), Run.status()}} | {:error, transition_error()}
  def transition_run_silent(repo, run_id, to_status, attrs \\ %{}) when is_map(attrs) do
    case repo.transaction(fn ->
           case get_run_record_for_update(repo, run_id) do
             %RunRecord{} = run ->
               transition_locked_run(repo, run, to_status, attrs)

             nil ->
               repo.rollback(:not_found)
           end
         end) do
      {:ok, {_run, _from_status, _to_status}} = ok -> ok
      {:error, _reason} = error -> error
    end
  end

  defp transition_locked_run(repo, run, to_status, attrs) do
    from_status = Serialization.deserialize_status(run.status)

    with {:ok, _next_status} <- StateMachine.transition(from_status, to_status) do
      run
      |> RunRecord.changeset(Persistence.transition_changeset_attrs(to_status, attrs))
      |> repo.update()
      |> case do
        {:ok, updated_run} ->
          {Serialization.to_public_run(updated_run), from_status, to_status}

        {:error, changeset} ->
          repo.rollback({:invalid_run, changeset})
      end
    else
      {:error, reason} -> repo.rollback(reason)
    end
  end

  @doc false
  @spec transition_and_dispatch_run(
          module(),
          Ecto.UUID.t(),
          Run.status(),
          transition_attrs(),
          dispatch_fun()
        ) :: {:ok, Run.t()} | {:error, transition_error() | term()}
  def transition_and_dispatch_run(repo, run_id, to_status, attrs, dispatch_fun)
      when is_map(attrs) and is_function(dispatch_fun, 1) do
    case repo.transaction(fn ->
           case get_run_record_for_update(repo, run_id) do
             %RunRecord{} = run ->
               from_status = Serialization.deserialize_status(run.status)

               with {:ok, _next_status} <- StateMachine.transition(from_status, to_status),
                    {:ok, updated_run} <-
                      Persistence.update_run_record(
                        repo,
                        run,
                        Persistence.transition_changeset_attrs(to_status, attrs)
                      ),
                    {:ok, _result} <- dispatch_fun.(updated_run) do
                 {updated_run, from_status}
               else
                 {:error, reason} -> repo.rollback(reason)
               end

             nil ->
               repo.rollback(:not_found)
           end
         end) do
      {:ok, {run, from_status}} ->
        Observability.emit_run_transition(run, from_status, to_status)
        {:ok, run}

      {:error, _reason} = error ->
        error
    end
  end

  @doc """
  Requests cancellation for a run if its current status allows it.
  """
  @spec cancel_run(module(), Ecto.UUID.t()) :: {:ok, Run.t()} | {:error, transition_error()}
  def cancel_run(repo, run_id) do
    cancellation_error = pause_cancellation_error()

    with {:ok, valid_run_id} <- cast_run_id(run_id) do
      cancel_valid_run(repo, valid_run_id, cancellation_error)
    end
  end

  defp cancel_valid_run(repo, run_id, cancellation_error) do
    case repo.transaction(fn -> do_cancel_run(repo, run_id, cancellation_error) end) do
      {:ok, {:transition, updated_run, from_status, to_status}} ->
        Observability.emit_run_transition(updated_run, from_status, to_status)
        {:ok, updated_run}

      {:ok, {:paused_cancelled, updated_run, paused_run, failure_event}} ->
        emit_paused_cancellation_failure(paused_run, failure_event, cancellation_error)
        Observability.emit_run_transition(updated_run, :paused, :cancelled)
        {:ok, updated_run}

      {:error, _reason} = error ->
        error
    end
  end

  defp do_cancel_run(repo, run_id, cancellation_error) do
    case get_run_record_for_update(repo, run_id) do
      %RunRecord{} = run ->
        run
        |> Serialization.to_public_run()
        |> cancel_locked_run(repo, run, cancellation_error)

      nil ->
        repo.rollback(:not_found)
    end
  end

  defp cancel_locked_run(%Run{status: :paused} = current_run, repo, run, cancellation_error) do
    cancel_locked_paused_run(repo, run, current_run, cancellation_error)
  end

  defp cancel_locked_run(%Run{status: status}, repo, run, _cancellation_error) do
    with {:ok, target_status} <- Persistence.cancellation_target_status(status),
         {:ok, _next_status} <- StateMachine.transition(status, target_status),
         {:ok, updated_run} <-
           Persistence.update_run_record(
             repo,
             run,
             Persistence.transition_changeset_attrs(target_status, %{})
           ) do
      {:transition, updated_run, status, target_status}
    else
      {:error, reason} -> repo.rollback(reason)
    end
  end

  @doc """
  Updates durable run fields without changing the run state machine directly.
  """
  @spec update_run(module(), Ecto.UUID.t(), transition_attrs()) ::
          {:ok, Run.t()} | {:error, update_error()}
  def update_run(repo, run_id, attrs) when is_map(attrs) do
    repo.transaction(fn ->
      case get_run_record_for_update(repo, run_id) do
        %RunRecord{} = run ->
          run
          |> RunRecord.changeset(
            Persistence.serialize_transition_attrs(
              Map.take(attrs, [:context, :current_step, :last_error])
            )
          )
          |> repo.update()
          |> case do
            {:ok, updated_run} -> Serialization.to_public_run(updated_run)
            {:error, changeset} -> repo.rollback({:invalid_run, changeset})
          end

        nil ->
          repo.rollback(:not_found)
      end
    end)
  end

  @doc false
  @spec update_run_with(module(), Ecto.UUID.t(), attrs_fun()) ::
          {:ok, Run.t()} | {:error, update_error()}
  def update_run_with(repo, run_id, attrs_fun) when is_function(attrs_fun, 1) do
    repo.transaction(fn ->
      case get_run_record_for_update(repo, run_id) do
        %RunRecord{} = run ->
          current_run = Serialization.to_public_run(run)
          attrs = attrs_fun.(current_run)

          run
          |> RunRecord.changeset(
            Persistence.serialize_transition_attrs(
              Map.take(attrs, [:context, :current_step, :last_error])
            )
          )
          |> repo.update()
          |> case do
            {:ok, updated_run} -> Serialization.to_public_run(updated_run)
            {:error, changeset} -> repo.rollback({:invalid_run, changeset})
          end

        nil ->
          repo.rollback(:not_found)
      end
    end)
  end

  @doc false
  @spec progress_run_with(module(), Ecto.UUID.t(), attrs_fun(), progress_operation()) ::
          {:ok, progress_result()} | {:error, update_error() | transition_error() | term()}
  def progress_run_with(repo, run_id, attrs_fun, operation)
      when is_function(attrs_fun, 1) do
    case progress_run_with_events(repo, run_id, attrs_fun, operation) do
      {:ok, result, events} ->
        emit_run_transition_events(events)
        {:ok, result}

      {:error, _reason} = error ->
        error
    end
  end

  @doc false
  @spec progress_run_with_events(module(), Ecto.UUID.t(), attrs_fun(), progress_operation()) ::
          {:ok, progress_result(), [progress_event()]}
          | {:error, update_error() | transition_error() | term()}
  def progress_run_with_events(repo, run_id, attrs_fun, operation)
      when is_function(attrs_fun, 1) do
    case repo.transaction(fn -> do_progress_run(repo, run_id, attrs_fun, operation) end) do
      {:ok, result} ->
        {run, events} = normalize_progress_events(result)
        {:ok, run, events}

      {:error, _reason} = error ->
        error
    end
  end

  defp normalize_progress_events(:noop), do: {:noop, []}

  defp normalize_progress_events({run, from_status, to_status}) do
    {run, [{:run_transition, run, from_status, to_status}]}
  end

  defp normalize_progress_events(%Run{} = run), do: {run, []}

  defp normalize_progress_events({%Run{} = run, events}) when is_list(events), do: {run, events}

  defp normalize_progress_events({%Run{} = run, from_status, to_status, events})
       when is_list(events) do
    {run, [{:run_transition, run, from_status, to_status} | events]}
  end

  defp emit_run_transition_events(events) do
    Enum.each(events, fn
      {:run_transition, run, from_status, to_status} ->
        Observability.emit_run_transition(run, from_status, to_status)

      _other ->
        :ok
    end)
  end

  defp do_progress_run(repo, run_id, attrs_fun, operation) do
    case get_run_record_for_update(repo, run_id) do
      %RunRecord{} = run ->
        run
        |> Serialization.to_public_run()
        |> progress_locked_run(repo, run, attrs_fun, operation)

      nil ->
        repo.rollback(:not_found)
    end
  end

  defp progress_locked_run(%Run{status: status}, _repo, _run, _attrs_fun, _operation)
       when status in [:failed, :completed, :cancelled] do
    :noop
  end

  defp progress_locked_run(
         %Run{status: :cancelling} = current_run,
         repo,
         run,
         attrs_fun,
         _operation
       ) do
    attrs =
      current_run
      |> attrs_fun.()
      |> cancellation_progress_attrs()

    with {:ok, _next_status} <- StateMachine.transition(:cancelling, :cancelled),
         {:ok, updated_run} <-
           Persistence.update_run_record(
             repo,
             run,
             Persistence.transition_changeset_attrs(:cancelled, attrs)
           ) do
      {updated_run, :cancelling, :cancelled}
    else
      {:error, reason} -> repo.rollback(reason)
    end
  end

  defp progress_locked_run(
         %Run{status: from_status} = current_run,
         repo,
         run,
         attrs_fun,
         operation
       ) do
    attrs = attrs_fun.(current_run)
    execute_progress_operation(repo, run, from_status, attrs, operation)
  end

  @doc false
  @spec update_and_dispatch_run(module(), Ecto.UUID.t(), transition_attrs(), dispatch_fun()) ::
          {:ok, Run.t()} | {:error, update_error() | term()}
  def update_and_dispatch_run(repo, run_id, attrs, dispatch_fun)
      when is_map(attrs) and is_function(dispatch_fun, 1) do
    case repo.transaction(fn ->
           case get_run_record_for_update(repo, run_id) do
             %RunRecord{} = run ->
               with {:ok, updated_run} <-
                      Persistence.update_run_record(
                        repo,
                        run,
                        Persistence.serialize_transition_attrs(
                          Map.take(attrs, [:context, :current_step, :last_error])
                        )
                      ),
                    {:ok, _result} <- dispatch_fun.(updated_run) do
                 updated_run
               else
                 {:error, reason} -> repo.rollback(reason)
               end

             nil ->
               repo.rollback(:not_found)
           end
         end) do
      {:ok, run} -> {:ok, run}
      {:error, _reason} = error -> error
    end
  end

  @doc false
  @spec update_and_dispatch_run_with(module(), Ecto.UUID.t(), attrs_fun(), dispatch_fun()) ::
          {:ok, Run.t()} | {:error, update_error() | term()}
  def update_and_dispatch_run_with(repo, run_id, attrs_fun, dispatch_fun)
      when is_function(attrs_fun, 1) and is_function(dispatch_fun, 1) do
    case repo.transaction(fn ->
           case get_run_record_for_update(repo, run_id) do
             %RunRecord{} = run ->
               current_run = Serialization.to_public_run(run)
               attrs = attrs_fun.(current_run)

               with {:ok, updated_run} <-
                      Persistence.update_run_record(
                        repo,
                        run,
                        Persistence.serialize_transition_attrs(
                          Map.take(attrs, [:context, :current_step, :last_error])
                        )
                      ),
                    {:ok, _result} <- dispatch_fun.(updated_run) do
                 updated_run
               else
                 {:error, reason} -> repo.rollback(reason)
               end

             nil ->
               repo.rollback(:not_found)
           end
         end) do
      {:ok, run} -> {:ok, run}
      {:error, _reason} = error -> error
    end
  end

  @type pause_result ::
          %{
            run: Run.t(),
            from_status: Run.status(),
            to_status: Run.status(),
            terminal_noop?: true,
            finalized_step?: boolean(),
            error: map()
          }
          | %{
              run: Run.t(),
              from_status: Run.status(),
              to_status: Run.status()
            }

  @doc false
  @spec pause_run(module(), Ecto.UUID.t(), Ecto.UUID.t(), Ecto.UUID.t(), transition_attrs()) ::
          {:ok, pause_result()} | {:error, update_error() | transition_error() | term()}
  def pause_run(repo, run_id, step_run_id, attempt_id, attrs) when is_map(attrs) do
    cancellation_error = pause_cancellation_error()

    case repo.transaction(fn ->
           case get_run_record_for_update(repo, run_id) do
             %RunRecord{} = run ->
               current_run = Serialization.to_public_run(run)

               case current_run.status do
                 status when status in [:failed, :completed, :cancelled] ->
                   terminal_error = terminal_pause_error(status)

                   with {:ok, finalized_step?} <-
                          finalize_terminal_pause_history(
                            repo,
                            step_run_id,
                            attempt_id,
                            terminal_error
                          ) do
                     %{
                       run: current_run,
                       from_status: status,
                       to_status: status,
                       terminal_noop?: true,
                       finalized_step?: finalized_step?,
                       error: terminal_error
                     }
                   else
                     {:error, reason} -> repo.rollback(reason)
                   end

                 :cancelling ->
                   with {:ok, _attempt} <-
                          AttemptStore.fail_attempt(repo, attempt_id, cancellation_error),
                        {:ok, _step_run} <-
                          StepRunStore.fail_step(repo, step_run_id, cancellation_error),
                        {:ok, _next_status} <- StateMachine.transition(:cancelling, :cancelled),
                        {:ok, updated_run} <-
                          Persistence.update_run_record(
                            repo,
                            run,
                            Persistence.transition_changeset_attrs(
                              :cancelled,
                              cancellation_progress_attrs(attrs)
                            )
                          ) do
                     %{run: updated_run, from_status: :cancelling, to_status: :cancelled}
                   else
                     {:error, reason} -> repo.rollback(reason)
                   end

                 from_status ->
                   with {:ok, _next_status} <- StateMachine.transition(from_status, :paused),
                        {:ok, updated_run} <-
                          Persistence.update_run_record(
                            repo,
                            run,
                            Persistence.transition_changeset_attrs(:paused, attrs)
                          ) do
                     %{run: updated_run, from_status: from_status, to_status: :paused}
                   else
                     {:error, reason} -> repo.rollback(reason)
                   end
               end

             nil ->
               repo.rollback(:not_found)
           end
         end) do
      {:ok, %{} = result} ->
        {:ok, result}

      {:error, _reason} = error ->
        error
    end
  end

  @doc false
  @spec transition_run_with(module(), Ecto.UUID.t(), Run.status(), attrs_fun()) ::
          {:ok, Run.t()} | {:error, transition_error()}
  def transition_run_with(repo, run_id, to_status, attrs_fun)
      when is_function(attrs_fun, 1) do
    repo.transaction(fn ->
      case get_run_record_for_update(repo, run_id) do
        %RunRecord{} = run ->
          from_status = Serialization.deserialize_status(run.status)
          current_run = Serialization.to_public_run(run)

          with {:ok, _next_status} <- StateMachine.transition(from_status, to_status) do
            attrs = attrs_fun.(current_run)

            run
            |> RunRecord.changeset(Persistence.transition_changeset_attrs(to_status, attrs))
            |> repo.update()
            |> case do
              {:ok, updated_run} ->
                public_run = Serialization.to_public_run(updated_run)
                Observability.emit_run_transition(public_run, from_status, to_status)
                public_run

              {:error, changeset} ->
                repo.rollback({:invalid_run, changeset})
            end
          else
            {:error, reason} -> repo.rollback(reason)
          end

        nil ->
          repo.rollback(:not_found)
      end
    end)
  end

  @doc """
  Returns whether a run in the given state should schedule additional step work.
  """
  @spec schedule_next_step?(Run.t() | Run.status()) :: boolean()
  def schedule_next_step?(%Run{status: status}), do: StateMachine.schedule_next_step?(status)

  def schedule_next_step?(status) when is_atom(status),
    do: StateMachine.schedule_next_step?(status)

  @spec query_runs(module(), list_filters()) :: [RunRecord.t()]
  defp query_runs(repo, filters) do
    if function_exported?(repo, :list_runs, 1) do
      repo.list_runs(Serialization.serialize_filters(filters))
    else
      RunRecord
      |> maybe_filter_workflow(filters)
      |> maybe_filter_status(filters)
      |> order_by([run], desc: run.inserted_at, desc: run.id)
      |> maybe_limit(filters)
      |> repo.all()
    end
  end

  @spec maybe_filter_workflow(Ecto.Queryable.t(), list_filters()) :: Ecto.Query.t()
  defp maybe_filter_workflow(query, filters) do
    case Keyword.get(filters, :workflow) do
      nil ->
        query

      workflow ->
        where(query, [run], run.workflow == ^WorkflowDefinition.serialize_workflow(workflow))
    end
  end

  @spec maybe_filter_status(Ecto.Queryable.t(), list_filters()) :: Ecto.Query.t()
  defp maybe_filter_status(query, filters) do
    case Keyword.get(filters, :status) do
      nil ->
        query

      status ->
        where(query, [run], run.status == ^Serialization.serialize_status(status))
    end
  end

  @spec maybe_limit(Ecto.Queryable.t(), list_filters()) :: Ecto.Query.t()
  defp maybe_limit(query, filters) do
    case Keyword.get(filters, :limit) do
      limit when is_integer(limit) and limit > 0 ->
        limit(query, ^limit)

      _ ->
        query
    end
  end

  @spec execute_progress_operation(
          module(),
          RunRecord.t(),
          Run.status(),
          transition_attrs(),
          progress_operation()
        ) ::
          Run.t()
          | {Run.t(), [progress_event()]}
          | {Run.t(), Run.status(), Run.status()}
          | {Run.t(), Run.status(), Run.status(), [progress_event()]}
          | no_return()
  defp execute_progress_operation(repo, run, _from_status, attrs, :update) do
    case Persistence.update_run_record(
           repo,
           run,
           Persistence.serialize_transition_attrs(
             Map.take(attrs, [:context, :current_step, :last_error])
           )
         ) do
      {:ok, updated_run} ->
        updated_run

      {:error, reason} ->
        repo.rollback(reason)
    end
  end

  defp execute_progress_operation(repo, run, from_status, attrs, {:transition, to_status}) do
    with {:ok, _next_status} <- StateMachine.transition(from_status, to_status),
         {:ok, updated_run} <-
           Persistence.update_run_record(
             repo,
             run,
             Persistence.transition_changeset_attrs(to_status, attrs)
           ) do
      {updated_run, from_status, to_status}
    else
      {:error, reason} -> repo.rollback(reason)
    end
  end

  defp execute_progress_operation(repo, run, _from_status, attrs, {:dispatch, dispatch_fun}) do
    with {:ok, updated_run} <-
           Persistence.update_run_record(
             repo,
             run,
             Persistence.serialize_transition_attrs(
               Map.take(attrs, [:context, :current_step, :last_error])
             )
           ),
         {:ok, dispatch_result} <- dispatch_fun.(updated_run) do
      append_progress_events(updated_run, dispatch_result)
    else
      {:error, reason} -> repo.rollback(reason)
    end
  end

  defp execute_progress_operation(
         repo,
         run,
         from_status,
         attrs,
         {:dispatch_or_fail, dispatch_fun, failure_attrs_fun}
       ) do
    with {:ok, updated_run} <- update_run_progress(repo, run, attrs) do
      dispatch_or_fail(repo, updated_run, from_status, dispatch_fun, failure_attrs_fun)
    else
      {:error, reason} -> repo.rollback(reason)
    end
  end

  defp execute_progress_operation(
         repo,
         run,
         from_status,
         attrs,
         {:transition_or_dispatch, to_status, dispatch_fun}
       ) do
    if from_status == to_status do
      with {:ok, updated_run} <-
             Persistence.update_run_record(
               repo,
               run,
               Persistence.serialize_transition_attrs(
                 Map.take(attrs, [:context, :current_step, :last_error])
               )
             ),
           {:ok, dispatch_result} <- dispatch_fun.(updated_run) do
        append_progress_events(updated_run, dispatch_result)
      else
        {:error, reason} -> repo.rollback(reason)
      end
    else
      with {:ok, _next_status} <- StateMachine.transition(from_status, to_status),
           {:ok, updated_run} <-
             Persistence.update_run_record(
               repo,
               run,
               Persistence.transition_changeset_attrs(to_status, attrs)
             ),
           {:ok, dispatch_result} <- dispatch_fun.(updated_run) do
        append_transition_events(updated_run, from_status, to_status, dispatch_result)
      else
        {:error, reason} -> repo.rollback(reason)
      end
    end
  end

  defp execute_progress_operation(
         repo,
         run,
         from_status,
         attrs,
         {:transition_or_dispatch_or_fail, to_status, dispatch_fun, failure_attrs_fun}
       ) do
    if from_status == to_status do
      dispatch_existing_status_or_fail(
        repo,
        run,
        from_status,
        attrs,
        dispatch_fun,
        failure_attrs_fun
      )
    else
      transition_and_dispatch_or_fail(
        repo,
        run,
        from_status,
        to_status,
        attrs,
        dispatch_fun,
        failure_attrs_fun
      )
    end
  end

  defp dispatch_existing_status_or_fail(
         repo,
         run,
         from_status,
         attrs,
         dispatch_fun,
         failure_attrs_fun
       ) do
    execute_progress_operation(
      repo,
      run,
      from_status,
      attrs,
      {:dispatch_or_fail, dispatch_fun, failure_attrs_fun}
    )
  end

  defp transition_and_dispatch_or_fail(
         repo,
         run,
         from_status,
         to_status,
         attrs,
         dispatch_fun,
         failure_attrs_fun
       ) do
    with {:ok, _next_status} <- StateMachine.transition(from_status, to_status),
         {:ok, updated_run} <-
           Persistence.update_run_record(
             repo,
             run,
             Persistence.transition_changeset_attrs(to_status, attrs)
           ) do
      dispatch_transitioned_run(
        repo,
        updated_run,
        from_status,
        to_status,
        dispatch_fun,
        failure_attrs_fun
      )
    else
      {:error, reason} -> repo.rollback(reason)
    end
  end

  defp dispatch_transitioned_run(
         repo,
         updated_run,
         from_status,
         to_status,
         dispatch_fun,
         failure_attrs_fun
       ) do
    case dispatch_fun.(updated_run) do
      {:ok, dispatch_result} ->
        append_transition_events(updated_run, from_status, to_status, dispatch_result)

      {:error, reason} ->
        fail_updated_run(repo, updated_run, from_status, failure_attrs_fun, reason)
    end
  end

  defp update_run_progress(repo, run, attrs) do
    Persistence.update_run_record(
      repo,
      run,
      Persistence.serialize_transition_attrs(
        Map.take(attrs, [:context, :current_step, :last_error])
      )
    )
  end

  defp dispatch_or_fail(repo, updated_run, from_status, dispatch_fun, failure_attrs_fun) do
    case dispatch_fun.(updated_run) do
      {:ok, dispatch_result} ->
        append_progress_events(updated_run, dispatch_result)

      {:error, reason} ->
        # Dispatch failure is part of progression: mark the run failed in this
        # transaction instead of rolling back and attempting a second update.
        fail_updated_run(repo, updated_run, from_status, failure_attrs_fun, reason)
    end
  end

  defp append_progress_events(run, dispatch_result) do
    case dispatch_result_events(dispatch_result) do
      [] -> run
      events -> {run, events}
    end
  end

  defp append_transition_events(run, from_status, to_status, dispatch_result) do
    case dispatch_result_events(dispatch_result) do
      [] -> {run, from_status, to_status}
      events -> {run, from_status, to_status, events}
    end
  end

  defp dispatch_result_events({:dispatch_events, events}) when is_list(events), do: events
  defp dispatch_result_events(_dispatch_result), do: []

  defp fail_updated_run(repo, updated_run, from_status, failure_attrs_fun, reason) do
    failure_attrs = failure_attrs_fun.(updated_run, reason)

    # `Persistence.update_run_record/3` returns the public run; refetch the
    # locked row so the failure transition stays in the same transaction.
    with %RunRecord{} = updated_record <- get_run_record_for_update(repo, updated_run.id),
         {:ok, _next_status} <- StateMachine.transition(updated_run.status, :failed),
         {:ok, failed_run} <-
           Persistence.update_run_record(
             repo,
             updated_record,
             Persistence.transition_changeset_attrs(:failed, failure_attrs)
           ) do
      {failed_run, from_status, :failed}
    else
      nil -> repo.rollback(:not_found)
      {:error, reason} -> repo.rollback(reason)
    end
  end

  @spec cancellation_progress_attrs(transition_attrs()) :: transition_attrs()
  defp cancellation_progress_attrs(attrs) do
    attrs
    |> Map.take([:context])
    |> Map.put(:current_step, nil)
    |> Map.put(:last_error, nil)
  end

  @spec cast_run_id(term()) :: {:ok, Ecto.UUID.t()} | {:error, :invalid_run_id}
  defp cast_run_id(run_id) do
    case Ecto.UUID.cast(run_id) do
      {:ok, valid_run_id} -> {:ok, valid_run_id}
      :error -> {:error, :invalid_run_id}
    end
  end

  @spec replay_valid_run(module(), Ecto.UUID.t(), dispatch_fun(), [replay_option()]) ::
          {:ok, Run.t()} | {:error, replay_error() | term()}
  defp replay_valid_run(repo, run_id, dispatch_fun, opts) do
    case repo.get(RunRecord, run_id) do
      %RunRecord{} = source_run ->
        with {:ok, _workflow, definition} <-
               WorkflowDefinition.load_serialized(source_run.workflow),
             :ok <- ensure_replay_allowed(repo, source_run, definition, opts) do
          attrs = Persistence.replay_run_attrs(source_run, definition)
          Persistence.insert_run_with_dispatch(repo, attrs, dispatch_fun)
        end

      nil ->
        {:error, :not_found}
    end
  end

  @spec ensure_replay_allowed(module(), RunRecord.t(), WorkflowDefinition.t(), [replay_option()]) ::
          :ok | {:error, {:unsafe_replay, map()}}
  defp ensure_replay_allowed(repo, source_run, definition, opts) do
    if Keyword.get(opts, :allow_irreversible) == true do
      :ok
    else
      validate_safe_replay(repo, source_run, definition)
    end
  end

  defp validate_safe_replay(repo, source_run, definition) do
    unsafe_steps =
      source_run
      |> completed_step_recovery_policies(repo)
      |> then(&WorkflowDefinition.unsafe_replay_steps(definition, &1))

    case unsafe_steps do
      [] ->
        :ok

      steps ->
        {:error,
         {:unsafe_replay,
          %{
            message:
              "replay requires explicit approval after irreversible or non-compensatable steps",
            steps: steps
          }}}
    end
  end

  defp completed_step_recovery_policies(%RunRecord{id: run_id}, repo) do
    StepRun
    |> where([step_run], step_run.run_id == ^run_id and step_run.status == "completed")
    |> order_by([step_run], asc: step_run.inserted_at, asc: step_run.id)
    |> select([step_run], {step_run.step, step_run.recovery})
    |> repo.all()
  end

  @spec get_valid_run(module(), Ecto.UUID.t(), [get_option()]) ::
          {:ok, Run.t()} | {:error, :not_found}
  defp get_valid_run(repo, run_id, opts) do
    include_history? = Keyword.get(opts, :include_history, false)

    query =
      RunRecord
      |> where([run], run.id == ^run_id)
      |> Serialization.maybe_preload_history(include_history?)

    case repo.one(query) do
      %RunRecord{} = run -> {:ok, Serialization.to_public_run(run)}
      nil -> {:error, :not_found}
    end
  end

  @spec get_run_record_for_update(module(), Ecto.UUID.t()) :: RunRecord.t() | nil
  defp get_run_record_for_update(repo, run_id) do
    RunRecord
    |> where([run], run.id == ^run_id)
    |> lock("FOR UPDATE")
    |> repo.one()
  end

  @spec cancel_locked_paused_run(module(), RunRecord.t(), Run.t(), map()) ::
          {:paused_cancelled, Run.t(), Run.t(), map() | nil}
          | no_return()
  defp cancel_locked_paused_run(repo, run, current_run, cancellation_error) do
    with {:ok, _next_status} <- StateMachine.transition(:paused, :cancelled),
         {:ok, failure_event} <-
           finalize_paused_step_history(
             repo,
             run.id,
             run.current_step,
             cancellation_error
           ),
         {:ok, updated_run} <-
           Persistence.update_run_record(
             repo,
             run,
             Persistence.transition_changeset_attrs(:cancelled, %{current_step: nil})
           ) do
      {:paused_cancelled, updated_run, current_run, failure_event}
    else
      {:error, reason} -> repo.rollback(reason)
    end
  end

  @spec finalize_paused_step_history(module(), Ecto.UUID.t(), String.t() | nil, map()) ::
          {:ok, %{attempt_number: pos_integer(), duration_native: non_neg_integer()} | nil}
          | {:error, Ecto.Changeset.t() | :not_found}
  defp finalize_paused_step_history(_repo, _run_id, nil, _error), do: {:ok, nil}

  defp finalize_paused_step_history(repo, run_id, current_step, error) do
    case locked_step_run(repo, run_id, current_step) do
      %StepRun{id: step_run_id, status: "running"} ->
        with {:ok, failure_event} <- fail_running_attempt(repo, step_run_id, error),
             {:ok, _step_run} <- StepRunStore.fail_step(repo, step_run_id, error) do
          {:ok, failure_event}
        end

      %StepRun{} ->
        {:ok, nil}

      nil ->
        {:ok, nil}
    end
  end

  @spec fail_running_attempt(module(), Ecto.UUID.t(), map()) ::
          {:ok, %{attempt_number: pos_integer(), duration_native: non_neg_integer()} | nil}
          | {:error, Ecto.Changeset.t() | :not_found}
  defp fail_running_attempt(repo, step_run_id, error) do
    case locked_latest_attempt(repo, step_run_id) do
      %StepAttempt{id: attempt_id, status: "running"} ->
        case AttemptStore.fail_attempt(repo, attempt_id, error) do
          {:ok, attempt} ->
            {:ok,
             %{
               attempt_number: attempt.attempt_number,
               duration_native: Observability.duration_since(attempt.inserted_at)
             }}

          {:error, reason} ->
            {:error, reason}
        end

      %StepAttempt{} ->
        {:ok, nil}

      nil ->
        {:ok, nil}
    end
  end

  @spec finalize_terminal_pause_history(module(), Ecto.UUID.t(), Ecto.UUID.t(), map()) ::
          {:ok, boolean()} | {:error, Ecto.Changeset.t() | :not_found}
  defp finalize_terminal_pause_history(repo, step_run_id, attempt_id, error) do
    with {:ok, attempt_finalized?} <- fail_attempt_if_running(repo, attempt_id, error),
         {:ok, step_finalized?} <- fail_step_if_running(repo, step_run_id, error) do
      {:ok, attempt_finalized? or step_finalized?}
    end
  end

  @spec fail_attempt_if_running(module(), Ecto.UUID.t(), map()) ::
          {:ok, boolean()} | {:error, Ecto.Changeset.t() | :not_found}
  defp fail_attempt_if_running(repo, attempt_id, error) do
    case locked_attempt(repo, attempt_id) do
      %StepAttempt{status: "running"} ->
        case AttemptStore.fail_attempt(repo, attempt_id, error) do
          {:ok, _attempt} -> {:ok, true}
          {:error, reason} -> {:error, reason}
        end

      %StepAttempt{} ->
        {:ok, false}

      nil ->
        {:error, :not_found}
    end
  end

  @spec fail_step_if_running(module(), Ecto.UUID.t(), map()) ::
          {:ok, boolean()} | {:error, Ecto.Changeset.t() | :not_found}
  defp fail_step_if_running(repo, step_run_id, error) do
    case locked_step_run_by_id(repo, step_run_id) do
      %StepRun{status: "running"} ->
        case StepRunStore.fail_step(repo, step_run_id, error) do
          {:ok, _step_run} -> {:ok, true}
          {:error, reason} -> {:error, reason}
        end

      %StepRun{} ->
        {:ok, false}

      nil ->
        {:error, :not_found}
    end
  end

  @spec emit_paused_cancellation_failure(Run.t(), map() | nil, map()) :: :ok
  defp emit_paused_cancellation_failure(_paused_run, nil, _error), do: :ok

  defp emit_paused_cancellation_failure(paused_run, failure_event, error) do
    Observability.emit_step_failed(
      paused_run,
      paused_run.current_step,
      failure_event.attempt_number,
      failure_event.duration_native,
      error
    )
  end

  @doc false
  @spec pause_cancellation_error() :: map()
  def pause_cancellation_error do
    %{message: "run cancelled while paused", reason: "cancelled"}
  end

  @spec terminal_pause_error(Run.status()) :: map()
  defp terminal_pause_error(:cancelled), do: pause_cancellation_error()

  defp terminal_pause_error(status) do
    %{
      message: "run already finalized before pause progression",
      status: status
    }
  end

  @spec locked_attempt(module(), Ecto.UUID.t()) :: StepAttempt.t() | nil
  defp locked_attempt(repo, attempt_id) do
    StepAttempt
    |> where([attempt], attempt.id == ^attempt_id)
    |> lock("FOR UPDATE")
    |> repo.one()
  end

  @spec locked_step_run(module(), Ecto.UUID.t(), String.t()) :: StepRun.t() | nil
  defp locked_step_run(repo, run_id, step_name) do
    StepRun
    |> where([step_run], step_run.run_id == ^run_id and step_run.step == ^step_name)
    |> lock("FOR UPDATE")
    |> repo.one()
  end

  @spec locked_step_run_by_id(module(), Ecto.UUID.t()) :: StepRun.t() | nil
  defp locked_step_run_by_id(repo, step_run_id) do
    StepRun
    |> where([step_run], step_run.id == ^step_run_id)
    |> lock("FOR UPDATE")
    |> repo.one()
  end

  @spec locked_latest_attempt(module(), Ecto.UUID.t()) :: StepAttempt.t() | nil
  defp locked_latest_attempt(repo, step_run_id) do
    StepAttempt
    |> where([attempt], attempt.step_run_id == ^step_run_id)
    |> order_by([attempt],
      desc: attempt.attempt_number,
      desc: attempt.inserted_at,
      desc: attempt.id
    )
    |> limit(1)
    |> lock("FOR UPDATE")
    |> repo.one()
  end
end
