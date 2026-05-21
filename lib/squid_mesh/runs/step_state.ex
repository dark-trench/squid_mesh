defmodule SquidMesh.Runs.StepState do
  @moduledoc """
  Public representation of one logical workflow step within a run.

  This read model complements `SquidMesh.Steps.Execution` by showing the declared step
  graph together with the latest known execution state for each step.
  """

  @type status :: :pending | :running | :completed | :failed | :waiting

  @type t :: %__MODULE__{}

  defstruct [
    :step,
    :status,
    :depends_on,
    :input,
    :output,
    :last_error,
    :recovery,
    :attempts,
    :inserted_at,
    :updated_at
  ]
end
