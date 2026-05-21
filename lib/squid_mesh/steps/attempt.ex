defmodule SquidMesh.Steps.Attempt do
  @moduledoc """
  Public representation of one workflow step attempt.

  Attempt history is exposed separately from the persistence schema so host
  applications can inspect retry history without depending on Ecto structs.
  """

  @type status :: :running | :completed | :failed

  @type t :: %__MODULE__{}

  defstruct [
    :id,
    :attempt_number,
    :status,
    :error,
    :inserted_at,
    :updated_at
  ]
end
