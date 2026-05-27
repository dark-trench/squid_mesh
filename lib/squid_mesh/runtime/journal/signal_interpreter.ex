defmodule SquidMesh.Runtime.Journal.SignalInterpreter do
  @moduledoc false

  alias SquidMesh.Runtime.Journal.Cancellation
  alias SquidMesh.Runtime.Journal.ManualControl
  alias SquidMesh.Runtime.Journal.Replay
  alias SquidMesh.Runtime.Journal.Starter
  alias SquidMesh.Runtime.ScheduleIdentity
  alias SquidMesh.Runtime.Signal
  alias SquidMesh.Workflow.Definition

  @manual_signal_types [:approve_run, :reject_run, :resume_run]
  @start_signal_types [:start_run, :start_cron]

  @doc false
  @spec apply(Signal.t(), keyword()) :: {:ok, term()} | {:error, term()}
  def apply(%Signal{type: type} = signal, opts)
      when type in @start_signal_types and is_list(opts) do
    start_from_signal(signal, opts)
  end

  def apply(%Signal{type: :replay_run} = signal, opts) when is_list(opts) do
    replay_from_signal(signal, opts)
  end

  def apply(%Signal{type: :cancel_run} = signal, opts) when is_list(opts) do
    Cancellation.apply_signal(signal, opts)
  end

  def apply(%Signal{type: type} = signal, opts)
      when type in @manual_signal_types and is_list(opts) do
    ManualControl.apply_signal(signal, opts)
  end

  def apply(%Signal{type: type}, opts) when is_list(opts),
    do: {:error, {:unsupported_signal, type}}

  def apply(%Signal{}, _opts), do: {:error, {:invalid_option, {:opts, :invalid}}}
  def apply(_signal, _opts), do: {:error, :invalid_signal}

  defp start_from_signal(
         %Signal{
           type: type,
           payload: %{workflow: workflow_name, trigger: trigger_name, input: input}
         } = signal,
         opts
       )
       when is_binary(workflow_name) and is_map(input) do
    with {:ok, workflow, definition} <- Definition.load_serialized(workflow_name),
         {:ok, trigger} <- signal_trigger(definition, trigger_name, type),
         {:ok, start_opts} <- start_options(signal, workflow, trigger, opts) do
      Starter.start_run(workflow, trigger, input, start_opts)
    end
  end

  defp start_from_signal(%Signal{type: type}, _opts), do: {:error, {:invalid_signal, type}}

  defp replay_from_signal(
         %Signal{
           type: :replay_run,
           payload: %{run_id: run_id, allow_irreversible: allow_irreversible}
         } = signal,
         opts
       )
       when is_binary(run_id) and is_boolean(allow_irreversible) do
    with {:ok, signal_opts} <-
           command_idempotency_options(
             signal,
             "SquidMesh.Runtime.Signal",
             "replay_run:#{run_id}",
             opts
           ) do
      signal_opts = signal_options(signal, signal_opts)

      Replay.replay(run_id, [allow_irreversible: allow_irreversible], signal_opts)
    end
  end

  defp replay_from_signal(%Signal{type: type}, _opts), do: {:error, {:invalid_signal, type}}

  defp signal_trigger(_definition, nil, :start_run), do: {:ok, nil}

  defp signal_trigger(definition, trigger_name, type) when is_binary(trigger_name) do
    case Definition.deserialize_trigger(definition, trigger_name) do
      trigger when is_atom(trigger) -> {:ok, trigger}
      _invalid -> {:error, {:invalid_signal, type}}
    end
  end

  defp signal_trigger(_definition, _trigger_name, type), do: {:error, {:invalid_signal, type}}

  defp start_options(%Signal{type: :start_cron} = signal, workflow, trigger, opts) do
    with {:ok, opts} <- cron_idempotency_options(workflow, trigger, opts) do
      {:ok, signal_options(signal, opts)}
    end
  end

  defp start_options(%Signal{type: :start_run} = signal, workflow, trigger, opts) do
    workflow_name = Definition.serialize_workflow(workflow)
    trigger_name = signal_trigger_name(trigger)

    with {:ok, opts} <- command_idempotency_options(signal, workflow_name, trigger_name, opts) do
      {:ok, signal_options(signal, opts)}
    end
  end

  defp signal_options(%Signal{} = signal, opts) do
    opts
    |> Keyword.put(:now, signal.occurred_at)
    |> Keyword.put(:command_signal, signal)
  end

  defp cron_idempotency_options(workflow, trigger, opts) do
    case schedule_idempotency_key(Keyword.get(opts, :initial_context, %{})) do
      {:ok, nil} ->
        {:ok, opts}

      {:ok, idempotency_key} ->
        workflow_name = Definition.serialize_workflow(workflow)
        trigger_name = Definition.serialize_trigger(trigger)

        with {:ok, run_id} <-
               ScheduleIdentity.run_id(workflow_name, trigger_name, idempotency_key) do
          opts =
            opts
            |> Keyword.put(:run_id, run_id)
            |> Keyword.put(:duplicate_schedule_start, true)

          {:ok, opts}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp command_idempotency_options(%Signal{idempotency_key: nil}, _workflow, _trigger, opts) do
    {:ok, opts}
  end

  defp command_idempotency_options(
         %Signal{idempotency_key: idempotency_key},
         workflow,
         trigger,
         opts
       )
       when is_binary(idempotency_key) and idempotency_key != "" do
    with {:ok, run_id} <- ScheduleIdentity.run_id(workflow, trigger, idempotency_key) do
      {:ok, Keyword.put(opts, :run_id, run_id)}
    end
  end

  defp command_idempotency_options(%Signal{}, _workflow, _trigger, _opts) do
    {:error, {:invalid_signal, {:idempotency_key, :expected_non_empty_string}}}
  end

  defp signal_trigger_name(nil), do: "__default__"

  defp signal_trigger_name(trigger) when is_atom(trigger),
    do: Definition.serialize_trigger(trigger)

  defp schedule_idempotency_key(context) when is_map(context) do
    context
    |> schedule_context()
    |> schedule_value(:idempotency_key)
    |> validate_schedule_idempotency_key()
  end

  defp schedule_idempotency_key(_context), do: {:ok, nil}

  defp validate_schedule_idempotency_key(nil), do: {:ok, nil}

  defp validate_schedule_idempotency_key(key) when is_binary(key) and key != "", do: {:ok, key}

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
end
