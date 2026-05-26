defmodule SquidMesh.Workflow.Dsl do
  @moduledoc """
  Spark DSL wrapper for Squid Mesh workflow declarations.

  This module installs the Squid Mesh Spark extension used by `use
  SquidMesh.Workflow`. Keeping the wrapper small lets the public workflow module
  focus on compiling validated definitions while Spark owns the declaration
  metadata.
  """

  use Spark.Dsl,
    default_extensions: [
      extensions: [SquidMesh.Workflow.SparkExtension]
    ]
end
