defmodule SquidMesh.Runtime.Journal.SignalInterpreter do
  @moduledoc false

  alias SquidMesh.Runtime.Journal.Cancellation
  alias SquidMesh.Runtime.Journal.ManualControl
  alias SquidMesh.Runtime.Journal.Replay
  alias SquidMesh.Runtime.Journal.Starter
  alias SquidMesh.Runtime.ScheduleIdentity
  alias SquidMesh.Runtime.ScheduleMetadata
  alias SquidMesh.Runtime.Signal
  alias SquidMesh.Workflow.Definition

  @manual_signal_types [:approve_run, :reject_run, :resume_run]

  @doc false
  @spec apply(Signal.t(), keyword()) :: {:ok, term()} | {:error, term()}
  def apply(%Signal{type: :start_run} = signal, opts) when is_list(opts) do
    apply_start_run(signal, opts)
  end

  def apply(%Signal{type: :start_cron} = signal, opts) when is_list(opts) do
    apply_start_cron(signal, opts)
  end

  def apply(%Signal{type: :replay_run} = signal, opts) when is_list(opts) do
    apply_replay_run(signal, opts)
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

  defp apply_start_run(%Signal{payload: payload} = signal, opts) do
    with {:ok, workflow_name} <- payload_string(payload, :workflow, :start_run),
         {:ok, trigger_name} <- payload_optional_string(payload, :trigger, :start_run),
         {:ok, input} <- payload_map(payload, :input, :start_run),
         {:ok, workflow, definition} <- Definition.load_serialized(workflow_name),
         {:ok, trigger} <- deserialize_trigger(definition, trigger_name),
         {:ok, idempotency_trigger_name} <- idempotency_trigger_name(definition, trigger),
         start_opts <- start_options(opts, signal, workflow_name, idempotency_trigger_name) do
      workflow
      |> Starter.start_run(trigger, input, start_opts)
      |> normalize_start_result()
    end
  end

  defp apply_start_cron(%Signal{payload: payload} = signal, opts) do
    with {:ok, workflow_name} <- payload_string(payload, :workflow, :start_cron),
         {:ok, trigger_name} <- payload_string(payload, :trigger, :start_cron),
         {:ok, input} <- payload_map(payload, :input, :start_cron),
         {:ok, workflow, definition} <- Definition.load_serialized(workflow_name),
         {:ok, trigger} <- deserialize_trigger(definition, trigger_name),
         {:ok, trigger_definition} <- Definition.trigger(definition, trigger),
         {:ok, schedule_context} <-
           ScheduleMetadata.cron_context(workflow, trigger_definition, input),
         start_opts <-
           opts
           |> Keyword.put(:initial_context, schedule_context)
           |> start_options(signal, workflow_name, trigger_name) do
      workflow
      |> Starter.start_run(trigger, %{}, start_opts)
      |> normalize_start_result()
    end
  end

  defp apply_replay_run(%Signal{payload: payload} = signal, opts) do
    with {:ok, run_id} <- payload_string(payload, :run_id, :replay_run),
         {:ok, allow_irreversible} <- payload_boolean(payload, :allow_irreversible, :replay_run) do
      run_id
      |> Replay.replay(
        [allow_irreversible: allow_irreversible],
        replay_options(opts, signal, run_id)
      )
      |> normalize_start_result()
    end
  end

  defp payload_string(payload, key, signal_type) when is_map(payload) and is_atom(key) do
    case payload_value(payload, key) do
      value when is_binary(value) and value != "" -> {:ok, value}
      _invalid -> {:error, {:invalid_signal, signal_type}}
    end
  end

  defp payload_string(_payload, _key, signal_type), do: {:error, {:invalid_signal, signal_type}}

  defp payload_optional_string(payload, key, signal_type) when is_map(payload) and is_atom(key) do
    case payload_value(payload, key) do
      nil -> {:ok, nil}
      value when is_binary(value) and value != "" -> {:ok, value}
      _invalid -> {:error, {:invalid_signal, signal_type}}
    end
  end

  defp payload_optional_string(_payload, _key, signal_type),
    do: {:error, {:invalid_signal, signal_type}}

  defp payload_map(payload, key, signal_type) when is_map(payload) and is_atom(key) do
    case payload_value(payload, key) do
      value when is_map(value) -> {:ok, value}
      _invalid -> {:error, {:invalid_signal, signal_type}}
    end
  end

  defp payload_map(_payload, _key, signal_type), do: {:error, {:invalid_signal, signal_type}}

  defp payload_boolean(payload, key, signal_type) when is_map(payload) and is_atom(key) do
    case payload_value(payload, key) do
      value when is_boolean(value) -> {:ok, value}
      _invalid -> {:error, {:invalid_signal, signal_type}}
    end
  end

  defp payload_boolean(_payload, _key, signal_type),
    do: {:error, {:invalid_signal, signal_type}}

  defp payload_value(payload, key) do
    string_key = Atom.to_string(key)

    cond do
      Map.has_key?(payload, key) -> Map.fetch!(payload, key)
      Map.has_key?(payload, string_key) -> Map.fetch!(payload, string_key)
      true -> nil
    end
  end

  defp deserialize_trigger(_definition, nil), do: {:ok, nil}

  defp deserialize_trigger(definition, trigger_name) when is_binary(trigger_name) do
    case Definition.deserialize_trigger(definition, trigger_name) do
      trigger when is_atom(trigger) -> {:ok, trigger}
      invalid -> {:error, {:invalid_trigger, invalid}}
    end
  end

  defp idempotency_trigger_name(definition, nil) do
    idempotency_trigger_name(definition, Definition.default_trigger(definition))
  end

  defp idempotency_trigger_name(_definition, trigger) when is_atom(trigger),
    do: {:ok, Atom.to_string(trigger)}

  defp start_options(opts, %Signal{} = signal, workflow_name, trigger_name) do
    opts
    |> Keyword.put(:now, signal.occurred_at)
    |> Keyword.put(:start_signal, signal)
    |> put_idempotent_run_id(workflow_name, trigger_name, signal.idempotency_key)
  end

  defp replay_options(opts, %Signal{} = signal, source_run_id) do
    opts
    |> Keyword.put(:now, signal.occurred_at)
    |> Keyword.put(:start_signal, signal)
    |> put_idempotent_run_id("replay", source_run_id, signal.idempotency_key)
  end

  defp put_idempotent_run_id(opts, workflow_name, trigger_name, idempotency_key)
       when is_binary(workflow_name) and is_binary(trigger_name) and is_binary(idempotency_key) do
    case ScheduleIdentity.run_id(workflow_name, trigger_name, idempotency_key) do
      {:ok, run_id} ->
        opts
        |> Keyword.put(:run_id, run_id)
        |> Keyword.put(:duplicate_schedule_start, true)

      {:error, _reason} ->
        opts
    end
  end

  defp put_idempotent_run_id(opts, _workflow_name, _trigger_name, _idempotency_key), do: opts

  defp normalize_start_result({:ok, {:duplicate_schedule_start, snapshot}}), do: {:ok, snapshot}
  defp normalize_start_result(result), do: result
end
