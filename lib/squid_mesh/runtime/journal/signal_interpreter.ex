defmodule SquidMesh.Runtime.Journal.SignalInterpreter do
  @moduledoc false

  alias SquidMesh.Runtime.Journal.Cancellation
  alias SquidMesh.Runtime.Journal.ManualControl
  alias SquidMesh.Runtime.Signal

  @manual_signal_types [:approve_run, :reject_run, :resume_run]

  @doc false
  @spec apply(Signal.t(), keyword()) :: {:ok, term()} | {:error, term()}
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
end
