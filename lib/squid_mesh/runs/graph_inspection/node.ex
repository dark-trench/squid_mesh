defmodule SquidMesh.Runs.GraphInspection.Node do
  @moduledoc """
  Public graph node for workflow run inspection.

  Nodes represent declared workflow steps. The graph projection keeps node
  identifiers as strings so host UIs can use the same shape across persisted
  inspection snapshots.
  """

  @type t :: %__MODULE__{
          id: String.t(),
          status: atom(),
          current?: boolean(),
          input: map() | nil,
          output: map() | nil,
          error: map() | nil,
          recovery: map() | nil,
          transition: map() | nil,
          manual_state: map() | nil,
          attempts: [map()]
        }

  @enforce_keys [:id, :status, :current?]

  defstruct [
    :id,
    :status,
    :current?,
    :input,
    :output,
    :error,
    :recovery,
    :transition,
    :manual_state,
    attempts: []
  ]
end
