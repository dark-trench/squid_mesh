defmodule SquidMesh.Runtime.BuiltInStep do
  @moduledoc """
  Executes declarative built-in workflow steps.

  Built-in steps let workflows express simple runtime primitives without
  requiring host applications to define dedicated Jido actions for them.
  """

  require Logger

  alias SquidMesh.Run
  alias SquidMesh.Workflow.Definition, as: WorkflowDefinition

  @type built_in_step_error :: {:unknown_built_in_step, WorkflowDefinition.built_in_step_kind()}
  @type execution_result :: {:ok, map(), keyword()} | {:error, built_in_step_error()}

  @doc false
  @spec execute(WorkflowDefinition.built_in_step_kind(), keyword(), map(), Run.t()) ::
          execution_result()
  def execute(:wait, opts, _input, _run) do
    duration = Keyword.fetch!(opts, :duration)
    {:ok, %{}, [schedule_in: ceil(duration / 1_000)]}
  end

  def execute(:log, opts, _input, _run) do
    level = Keyword.get(opts, :level, :info)
    message = Keyword.fetch!(opts, :message)

    Logger.log(level, message)

    {:ok, %{}, []}
  end

  def execute(:pause, _opts, _input, _run) do
    {:ok, %{}, [pause: true]}
  end

  def execute(:approval, _opts, _input, _run) do
    {:ok, %{}, [pause: true]}
  end

  def execute(kind, _opts, _input, _run), do: {:error, {:unknown_built_in_step, kind}}
end
