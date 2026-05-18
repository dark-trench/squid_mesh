defmodule SquidMesh.Runtime.StepExecutor.PreparedStep do
  @moduledoc """
  Immutable handoff from preparation into execution and apply.

  The struct keeps the claimed step-run record, resolved workflow step
  definition, normalized input snapshot, and current run context together so
  later phases do not need to repeat preparation work.
  """

  alias SquidMesh.Config
  alias SquidMesh.Run

  @enforce_keys [:config, :definition, :run, :step_name, :step, :step_run, :input]
  defstruct [:config, :definition, :run, :step_name, :step, :step_run, :input]

  @type t :: %__MODULE__{
          config: Config.t(),
          definition: SquidMesh.Workflow.Definition.t(),
          run: Run.t(),
          step_name: atom(),
          step: SquidMesh.Workflow.Definition.step(),
          step_run: SquidMesh.Persistence.StepRun.t(),
          input: map()
        }
end
