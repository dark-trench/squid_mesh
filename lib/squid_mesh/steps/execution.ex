defmodule SquidMesh.Steps.Execution do
  @moduledoc """
  Public representation of one workflow step execution.

  Step runs provide the run timeline needed for inspection, debugging, and
  replay decisions without exposing internal persistence records directly.
  """

  @type status :: :pending | :running | :completed | :failed

  @type t :: %__MODULE__{}

  defstruct [
    :id,
    :step,
    :status,
    :input,
    :output,
    :last_error,
    :recovery,
    :attempts,
    :inserted_at,
    :updated_at
  ]
end
