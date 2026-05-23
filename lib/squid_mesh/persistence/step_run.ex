defmodule SquidMesh.Persistence.StepRun do
  @moduledoc """
  Persisted state for one workflow step execution.

  A step run belongs to a workflow run and tracks the step input, output, and
  latest error payload for that step within the durable execution history.
  """

  use Ecto.Schema

  import Ecto.Changeset

  alias SquidMesh.Persistence.Run
  alias SquidMesh.Persistence.StepAttempt

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  @timestamps_opts [type: :utc_datetime_usec]

  @type status :: String.t()

  @type t :: %__MODULE__{}

  @required_fields ~w(run_id step status)a
  @optional_fields ~w(input output last_error recovery resume manual transition)a

  schema "squid_mesh_step_runs" do
    field(:step, :string)
    field(:status, :string)
    field(:input, :map, default: %{})
    field(:output, :map)
    field(:last_error, :map)
    field(:recovery, :map)
    field(:resume, :map)
    field(:manual, :map)
    field(:transition, :map)

    belongs_to(:run, Run)
    has_many(:attempts, StepAttempt)

    timestamps()
  end

  @doc """
  Builds a changeset for persisted step run state.
  """
  @spec changeset(t(), map()) :: Ecto.Changeset.t()
  def changeset(step_run, attrs) do
    step_run
    |> cast(attrs, @required_fields ++ @optional_fields)
    |> validate_required(@required_fields)
    |> foreign_key_constraint(:run_id)
    |> unique_constraint([:run_id, :step])
  end
end
