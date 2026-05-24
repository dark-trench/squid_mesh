defmodule SquidMesh.Step.Context do
  @moduledoc """
  Durable runtime context passed to native Squid Mesh steps.

  The context intentionally exposes Squid Mesh concepts only. It gives steps the
  current run identity, workflow module, step name, attempt number, and the
  durable run state available before the current attempt started.
  """

  @enforce_keys [:run_id, :workflow, :step, :attempt, :state]
  defstruct [:run_id, :workflow, :step, :attempt, state: %{}]

  @type t :: %__MODULE__{
          run_id: Ecto.UUID.t(),
          workflow: module(),
          step: atom(),
          attempt: pos_integer() | nil,
          state: map()
        }

  @doc false
  @spec from_map(map()) :: t()
  def from_map(context) when is_map(context) do
    %__MODULE__{
      run_id: Map.fetch!(context, :run_id),
      workflow: Map.fetch!(context, :workflow),
      step: Map.fetch!(context, :step),
      attempt: Map.get(context, :attempt),
      state: Map.get(context, :state, %{})
    }
  end
end
