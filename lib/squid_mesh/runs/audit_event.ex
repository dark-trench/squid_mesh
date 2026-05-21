defmodule SquidMesh.Runs.AuditEvent do
  @moduledoc """
  Public representation of one durable workflow audit event.

  Audit events summarize when a run paused for manual intervention and when an
  operator later resumed, approved, or rejected it. They also surface explicit
  failure recovery routes such as compensation and undo paths.
  """

  @type type :: :paused | :resumed | :approved | :rejected | :compensation_routed | :undo_routed

  @type t :: %__MODULE__{}

  defstruct [
    :type,
    :step,
    :actor,
    :comment,
    :metadata,
    :at
  ]
end
