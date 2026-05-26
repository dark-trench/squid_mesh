defmodule SquidMesh.Workflow.TriggerSpec do
  @moduledoc """
  Spark entity for one Squid Mesh workflow trigger declaration.
  """

  defstruct [
    :name,
    :__identifier__,
    __spark_metadata__: nil,
    definitions: [],
    invalid_fields: [],
    payload: []
  ]

  @type t :: %__MODULE__{
          name: atom(),
          definitions: [SquidMesh.Workflow.TriggerDefinitionSpec.t()],
          invalid_fields: [SquidMesh.Workflow.PayloadFieldSpec.t()],
          payload: [SquidMesh.Workflow.PayloadSpec.t()],
          __identifier__: term(),
          __spark_metadata__: term()
        }
end
