defmodule SquidMesh.Persistence.Run do
  @moduledoc """
  Persisted workflow run state.

  A run is the top-level durable record for one workflow execution. It stores
  the workflow identity, the initial workflow payload, the current step
  pointer, and any replay lineage needed for later inspection or recovery.
  """

  use Ecto.Schema

  import Ecto.Changeset

  alias SquidMesh.Persistence.StepRun

  @primary_key {:id, :binary_id, autogenerate: true}
  @foreign_key_type :binary_id
  @timestamps_opts [type: :utc_datetime_usec]

  @type status :: String.t()

  @type t :: %__MODULE__{}

  @required_fields ~w(workflow trigger status input)a
  @optional_fields ~w(context current_step last_error replayed_from_run_id)a
  @schedule_idempotency_index :squid_mesh_runs_schedule_idempotency_index

  schema "squid_mesh_runs" do
    field(:workflow, :string)
    field(:trigger, :string)
    field(:status, :string)
    field(:input, :map)
    field(:context, :map, default: %{})
    field(:current_step, :string)
    field(:last_error, :map)

    belongs_to(:replayed_from_run, __MODULE__)
    has_many(:step_runs, StepRun)

    timestamps()
  end

  @doc """
  Builds a changeset for persisted workflow run state.
  """
  @spec changeset(t(), map()) :: Ecto.Changeset.t()
  def changeset(run, attrs) do
    run
    |> cast(attrs, @required_fields ++ @optional_fields)
    |> validate_required(@required_fields)
    |> unique_constraint(:context,
      name: @schedule_idempotency_index,
      message: "has already been used for this scheduled start"
    )
    |> foreign_key_constraint(:replayed_from_run_id)
  end
end
