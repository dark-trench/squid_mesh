defmodule SquidMesh.Runs.GraphInspection.Edge do
  @moduledoc """
  Public graph edge for workflow run inspection.

  Edge statuses are derived from durable step and attempt state only. Conditional
  route selection can add richer skipped-edge evidence later without changing
  the node shape.
  """

  @type edge_type :: :transition | :dependency
  @type edge_status :: :selected | :skipped | :pending | :blocked

  @type t :: %__MODULE__{
          id: String.t(),
          from: String.t(),
          to: String.t(),
          type: edge_type(),
          status: edge_status(),
          outcome: atom() | nil,
          condition: map() | nil,
          recovery: atom() | nil
        }

  @enforce_keys [:id, :from, :to, :type, :status]

  defstruct [
    :id,
    :from,
    :to,
    :type,
    :status,
    :outcome,
    :condition,
    :recovery
  ]
end
