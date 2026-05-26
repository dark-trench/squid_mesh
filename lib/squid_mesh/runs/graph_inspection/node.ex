defmodule SquidMesh.Runs.GraphInspection.Node do
  @moduledoc """
  Public graph node for workflow run inspection.

  Nodes represent declared workflow steps. The graph projection keeps node
  identifiers as strings so host UIs can use the same shape across persisted
  inspection snapshots.
  """

  @type t :: %__MODULE__{
          id: String.t(),
          action: atom() | String.t() | nil,
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
    :action,
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

  @doc """
  Converts a graph node into the stable host UI map shape.
  """
  @spec to_map(t()) :: map()
  def to_map(%__MODULE__{} = node) do
    %{
      id: node.id,
      action: node.action,
      status: node.status,
      current?: node.current?,
      input: node.input,
      output: node.output,
      error: node.error,
      recovery: node.recovery,
      transition: node.transition,
      manual_state: node.manual_state,
      attempts: node.attempts
    }
  end
end
