defmodule SquidMesh.Workflow.SparkExtension do
  @moduledoc """
  Spark extension that defines the Squid Mesh workflow step section.

  The extension owns `step`, `approval_step`, and `manual_review_step`
  declarations. It stores native `SquidMesh.Step` metadata when available and
  marks built-in or raw Jido actions as explicit interop contracts so the
  compiled spec remains inspectable without changing the runtime execution
  model.
  """

  @step_schema [
    name: [
      type: :atom,
      required: true,
      doc: "The workflow step name."
    ],
    module: [
      type: :atom,
      required: true,
      doc: "The native Squid Mesh step, raw Jido action, or built-in step kind."
    ],
    opts: [
      type: :keyword_list,
      default: [],
      doc: "Step runtime options such as input, output, retry, recovery, and dependencies."
    ]
  ]

  @step %Spark.Dsl.Entity{
    name: :step,
    target: SquidMesh.Workflow.StepSpec,
    args: [:name, :module, {:optional, :opts, []}],
    schema: @step_schema,
    identifier: :name,
    transform: {__MODULE__, :put_step_metadata, []}
  }

  @approval_step %Spark.Dsl.Entity{
    name: :approval_step,
    target: SquidMesh.Workflow.StepSpec,
    args: [:name, {:optional, :opts, []}],
    schema: Keyword.delete(@step_schema, :module),
    auto_set_fields: [module: :approval],
    identifier: :name,
    transform: {__MODULE__, :put_step_metadata, []}
  }

  @manual_review_step %Spark.Dsl.Entity{
    name: :manual_review_step,
    target: SquidMesh.Workflow.StepSpec,
    args: [:name, {:optional, :opts, []}],
    schema: Keyword.delete(@step_schema, :module),
    auto_set_fields: [module: :approval],
    identifier: :name,
    transform: {__MODULE__, :put_step_metadata, []}
  }

  @workflow_schema [
    version: [
      type: :string,
      doc: "Human-readable workflow definition version for operator diagnostics."
    ]
  ]

  @workflow %Spark.Dsl.Section{
    name: :workflow,
    schema: @workflow_schema,
    entities: [@step, @approval_step, @manual_review_step],
    describe: "Declares Squid Mesh workflow steps."
  }

  use Spark.Dsl.Extension, sections: [@workflow]

  @doc false
  @spec put_step_metadata(SquidMesh.Workflow.StepSpec.t()) ::
          {:ok, SquidMesh.Workflow.StepSpec.t()}
  def put_step_metadata(%SquidMesh.Workflow.StepSpec{} = step) do
    metadata =
      case SquidMesh.Step.metadata(step.module) do
        %{} = native_metadata -> native_metadata
        nil -> interop_metadata(step.module)
      end

    {:ok, %{step | metadata: metadata}}
  end

  defp interop_metadata(module) when module in [:wait, :log, :pause, :approval] do
    %{contract: :built_in, kind: module}
  end

  defp interop_metadata(module) when is_atom(module) do
    %{contract: :jido_action, module: module}
  end
end
